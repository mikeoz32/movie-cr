require "./settings"
require "./protocol"
require "./daemon"
require "./events"
require "./event_bus"
require "./reachability"
require "./telemetry"
require "./transport"
require "./membership"

module Movie::Cluster
  class ClusterExtension < Movie::Extension
    Log = ::Log.for(self)

    DAEMON_NAME  = CLUSTER_DAEMON_NAME
    PROTOCOL_TAG = "movie.cluster.protocol.v1"

    getter settings : ClusterSettings
    getter self_unique_address : UniqueAddress

    @membership : MembershipCoordinator
    @stopped = Atomic(Bool).new(false)
    @daemon : Movie::ActorRef(ProtocolMessage)?
    @seed_nodes : Array(Movie::Address)
    @seed_mutex = Mutex.new
    @remote : Movie::Remote::RemoteExtension
    @seen_digests = {} of UniqueAddress => String
    @seen_mutex = Mutex.new
    @pending_terminal_deliveries = Set(UniqueAddress).new
    @terminal_mutex = Mutex.new
    @departure_guards = {} of String => Proc(UniqueAddress, Bool)
    @departure_guards_mutex = Mutex.new
    @leave_requested = Atomic(Bool).new(false)
    @heartbeat_sequence = Atomic(Int64).new(0_i64)
    @telemetry = ClusterTelemetry.new
    @transport : ClusterTransport

    def initialize(
      @system : Movie::AbstractActorSystem,
      @settings : ClusterSettings = ClusterSettings.new,
    )
      remote = @system.remote || raise ClusterConfigurationError.new("cluster membership requires remoting")
      @remote = remote
      @self_unique_address = UniqueAddress.new(@system.address, remote.node_uid)
      @seed_nodes = @settings.seed_nodes
      @membership = MembershipCoordinator.new(
        @self_unique_address,
        @settings.roles,
        @settings.max_members,
        @telemetry
      )
      @transport = ClusterTransport.new(remote)
    end

    def start : Bool
      Movie::Remote::MessageRegistry.register(ProtocolMessage, PROTOCOL_TAG)
      initial_status = seed_node? ? MemberStatus::Up : MemberStatus::Joining
      @membership.initialize_self(initial_status)
      @daemon = @system.spawn_system_actor(
        ClusterDaemon.new(self),
        DAEMON_NAME,
        restart_strategy: Movie::RestartStrategy::STOP
      )
      @transport.daemon = @daemon.not_nil!
      start_join_loop unless @seed_nodes.empty?
      start_gossip_loop
      start_heartbeat_loop
      true
    end

    def stop : Nil
      return if @stopped.swap(true)
      @daemon.try &.send_system(Movie::STOP)
      @transport.close
      @terminal_mutex.synchronize { @pending_terminal_deliveries.clear }
    end

    def snapshot : ClusterSnapshot
      @membership.snapshot
    end

    def self_member : Member
      @membership.self_member
    end

    def up? : Bool
      @membership.up?
    end

    def stats : ClusterStats
      @telemetry.snapshot(@membership.subscriber_count)
    end

    def converged? : Bool
      seen = @seen_mutex.synchronize { @seen_digests.dup }
      @membership.converged?(seen)
    end

    def await_up(timeout_span : Time::Span = 10.seconds) : Nil
      deadline = Time.instant + timeout_span
      until up?
        raise ClusterConfigurationError.new("cluster node did not join within #{timeout_span}") if Time.instant >= deadline
        sleep 5.milliseconds
      end
    end

    def await_removed(timeout_span : Time::Span = 10.seconds) : Nil
      deadline = Time.instant + timeout_span
      until self_member.status.removed?
        raise ClusterConfigurationError.new("cluster node did not leave within #{timeout_span}") if Time.instant >= deadline
        sleep 5.milliseconds
      end
    end

    # Adds a seed dynamically and sends an idempotent join immediately.
    def join(seed : Movie::Address) : Nil
      raise ArgumentError.new("cluster seed node must use a remote address") unless seed.remote?
      @seed_mutex.synchronize { @seed_nodes << seed unless @seed_nodes.includes?(seed) }
      spawn { send_join(seed) }
    end

    # Graceful leave is coordinated by the current reachable leader and is
    # terminal for this process incarnation.
    def leave : Bool
      accepted = request_departure(ProtocolMessage::Kind::LeaveRequest, @self_unique_address)
      if accepted && self_member.status.up?
        @leave_requested.set(true)
      end
      accepted
    end

    # Manual downing never runs from reachability alone. The operator chooses
    # the exact process incarnation after resolving any partition ambiguity.
    def down(target : UniqueAddress) : Bool
      return false if target == @self_unique_address
      request_departure(ProtocolMessage::Kind::DownRequest, target)
    end

    def subscribe(subscriber : Movie::ActorRef(ClusterEvent), replay_state : Bool = true) : Nil
      @membership.subscribe(subscriber, replay_state)
    end

    # Registers an advanced extension-level barrier for graceful departure.
    # The callback must be non-blocking and return true only when `member` may
    # advance from Leaving to Exiting. Exceptions fail closed for that round.
    def register_departure_guard(name : String, &guard : UniqueAddress -> Bool) : Nil
      raise ArgumentError.new("cluster departure guard name must not be empty") if name.empty?
      @departure_guards_mutex.synchronize { @departure_guards[name] = guard }
    end

    # Removes a previously registered graceful-departure barrier.
    def unregister_departure_guard(name : String) : Nil
      @departure_guards_mutex.synchronize { @departure_guards.delete(name) }
    end

    def unsubscribe(subscriber : Movie::ActorRef(ClusterEvent)) : Nil
      @membership.unsubscribe(subscriber)
    end

    # Internal actor entrypoint. Remote protocol traffic must carry both the
    # actor address and process incarnation authenticated by remoting.
    def handle_protocol(
      message : ProtocolMessage,
      sender_path : Movie::ActorPath?,
      remote_address : Movie::Address?,
      remote_node_uid : String?,
    ) : Nil
      return if @stopped.get
      unless message.cluster_name == @settings.cluster_name
        @telemetry.protocol_rejected
        return
      end

      if message.kind.join_tick?
        attempt_join
        return
      elsif message.kind.gossip_tick?
        gossip_round
        return
      elsif message.kind.heartbeat_tick?
        heartbeat_round
        return
      end

      unless sender_path &&
             sender_path.address == message.sender.address &&
             sender_path.elements == ["system", DAEMON_NAME] &&
             remote_address == message.sender.address &&
             remote_node_uid == message.sender.node_uid
        @telemetry.protocol_rejected
        Log.warn { "Rejected cluster #{message.kind} with mismatched sender identity" }
        return
      end

      case message.kind
      when .join?
        handle_join(message)
      when .welcome?
        handle_welcome(message)
      when .gossip?
        handle_gossip(message)
      when .gossip_ack?
        handle_gossip_ack(message)
      when .heartbeat?
        handle_heartbeat(message)
      when .heartbeat_ack?
        handle_heartbeat_ack(message)
      when .leave_request?
        handle_leave_request(message)
      when .down_request?
        handle_down_request(message)
      when .join_tick?, .gossip_tick?, .heartbeat_tick?
        # handled before remote identity validation
      end
    rescue ex : MembershipCapacityError
      @telemetry.membership_capacity_rejected
      Log.warn { "Rejected cluster #{message.kind} at membership capacity: #{ex.message}" }
    end

    private def handle_join(message : ProtocolMessage) : Nil
      candidate = message.member
      return unless candidate &&
                    candidate.unique_address == message.sender &&
                    (candidate.status.joining? ||
                    (candidate.status.up? && seed_address?(candidate.unique_address.address))) &&
                    candidate.changed_by == message.sender.node_uid

      if @membership.local_leader?
        @membership.merge([candidate])
        @membership.promote_joining_if_leader
      end
      @transport.send(candidate.unique_address.address, ProtocolMessage.welcome(
        @settings.cluster_name,
        @self_unique_address,
        @membership.all_members
      ))
    end

    private def handle_welcome(message : ProtocolMessage) : Nil
      return unless seed_address?(message.sender.address)
      @membership.merge(message.members)
      @membership.promote_joining_if_leader
      if self_member.status.joining?
        if leader = snapshot.leader
          join(leader.address) unless leader == @self_unique_address
        end
      end
    end

    private def handle_gossip(message : ProtocolMessage) : Nil
      return unless @membership.known_active?(message.sender)
      @telemetry.gossip_received
      records = message.members
      previously_exiting = records.compact_map do |candidate|
        current = @membership.member(candidate.unique_address)
        candidate.unique_address if current.try(&.status.exiting?) && candidate.status.removed?
      end
      @membership.merge_gossip(records, message.sender)
      register_terminal_deliveries(previously_exiting)
      @membership.promote_joining_if_leader
      round = message.gossip_round || return
      @transport.send_async(message.sender.address, ProtocolMessage.gossip_ack(
        @settings.cluster_name,
        @self_unique_address,
        round,
        @membership.digest
      ))
    end

    private def handle_gossip_ack(message : ProtocolMessage) : Nil
      digest = message.digest || return
      if terminal_delivery_pending?(message.sender)
        @telemetry.gossip_ack
        clear_terminal_delivery(message.sender) if digest == @membership.digest
        return
      end
      return unless @membership.known_active?(message.sender)
      @telemetry.gossip_ack
      @seen_mutex.synchronize { @seen_digests[message.sender] = digest }
    end

    private def handle_heartbeat(message : ProtocolMessage) : Nil
      return unless @membership.known_active?(message.sender)
      sequence = message.heartbeat_sequence || return
      @telemetry.heartbeat_received
      @transport.send_async(message.sender.address, ProtocolMessage.heartbeat_ack(
        @settings.cluster_name,
        @self_unique_address,
        sequence
      ))
    end

    private def handle_heartbeat_ack(message : ProtocolMessage) : Nil
      return unless @membership.known_active?(message.sender)
      @membership.mark_reachable(message.sender)
    end

    private def handle_leave_request(message : ProtocolMessage) : Nil
      target = message.target || return
      return unless target == message.sender
      @membership.apply_departure(target, MemberStatus::Leaving) if @membership.local_leader?
    end

    private def handle_down_request(message : ProtocolMessage) : Nil
      unless @remote.settings.shared_secret
        Log.warn { "Rejected unauthenticated remote cluster down request from #{message.sender}" }
        return
      end
      target = message.target || return
      @membership.apply_departure(target, MemberStatus::Down) if @membership.local_leader?
    end

    private def start_join_loop : Nil
      spawn do
        until @stopped.get
          attempt_join
          sleep @settings.join_retry_interval
        end
      end
    end

    private def start_gossip_loop : Nil
      spawn do
        until @stopped.get
          sleep @settings.gossip_interval
          break if @stopped.get
          @daemon.try { |daemon| daemon << ProtocolMessage.gossip_tick(@settings.cluster_name, @self_unique_address) }
        end
      rescue ex : Exception
        Log.debug { "Cluster gossip loop stopped: #{ex.message}" }
      end
    end

    private def start_heartbeat_loop : Nil
      spawn do
        until @stopped.get
          sleep @settings.heartbeat_interval
          break if @stopped.get
          @daemon.try { |daemon| daemon << ProtocolMessage.heartbeat_tick(@settings.cluster_name, @self_unique_address) }
        end
      rescue ex : Exception
        Log.debug { "Cluster heartbeat loop stopped: #{ex.message}" }
      end
    end

    private def attempt_join : Nil
      status = self_member.status
      return unless status.joining? || status.up?
      seeds = @seed_mutex.synchronize { @seed_nodes.dup }
      known_addresses = if status.up?
                          @membership.active_members.map(&.unique_address.address).to_set
                        else
                          Set(Movie::Address).new
                        end
      seeds
        .reject { |seed| seed == @self_unique_address.address || known_addresses.includes?(seed) }
        .each { |seed| send_join(seed) }
    end

    private def send_join(seed : Movie::Address) : Nil
      @telemetry.join_attempt
      current = self_member
      candidate = Member.new(
        @self_unique_address,
        current.status,
        current.roles,
        current.revision,
        @self_unique_address.node_uid
      )
      @transport.send(seed, ProtocolMessage.join(
        @settings.cluster_name,
        @self_unique_address,
        candidate
      ))
    rescue ex : Exception
      Log.debug { "Cluster join attempt to #{seed} failed: #{ex.message}" }
    end

    private def gossip_round : Nil
      return unless @membership.participating?
      retry_leave_request
      round = @telemetry.next_gossip_round
      members = @membership.all_members
      digest = @membership.digest
      peers = @membership.active_members.select do |member|
        member.unique_address != @self_unique_address &&
          (member.status.up? || member.status.joining? || member.status.leaving?)
      end
      unless peers.empty?
        start = ((round - 1) % peers.size).to_i
        {peers.size, @settings.gossip_fanout}.min.times do |offset|
          peer = peers[(start + offset) % peers.size]
          @telemetry.gossip_sent
          @transport.send_async(peer.unique_address.address, ProtocolMessage.gossip(
            @settings.cluster_name,
            @self_unique_address,
            members,
            round,
            digest
          ))
        end
      end
      retry_terminal_deliveries(round, members, digest)
      advance_departures(round) if @membership.departure_coordinator?
    end

    private def heartbeat_round : Nil
      return unless @membership.participating?
      peers = @membership.active_members.select do |member|
        member.unique_address != @self_unique_address &&
          (member.status.up? || member.status.leaving? || member.status.exiting?)
      end
      @membership.check_reachability(peers, @settings.heartbeat_timeout)

      peers.each do |peer|
        sequence = @heartbeat_sequence.add(1) + 1
        @telemetry.heartbeat_sent
        @transport.send_async(peer.unique_address.address, ProtocolMessage.heartbeat(
          @settings.cluster_name,
          @self_unique_address,
          sequence
        ))
      end
    end

    private def seed_node? : Bool
      seeds = @seed_mutex.synchronize { @seed_nodes.dup }
      seeds.empty? || seeds.includes?(@self_unique_address.address)
    end

    private def seed_address?(address : Movie::Address) : Bool
      @seed_mutex.synchronize { @seed_nodes.includes?(address) }
    end

    private def request_departure(kind : ProtocolMessage::Kind, target : UniqueAddress) : Bool
      current = @membership.member(target)
      return false unless current && !current.status.removed?
      leader = snapshot.leader
      return false unless leader

      if leader == @self_unique_address
        status = kind.leave_request? ? MemberStatus::Leaving : MemberStatus::Down
        @membership.apply_departure(target, status)
      else
        if kind.down_request? && @remote.settings.shared_secret.nil?
          raise ClusterConfigurationError.new(
            "remote manual downing requires a remoting shared secret; invoke down on the leader otherwise"
          )
        end
        message = if kind.leave_request?
                    ProtocolMessage.leave_request(@settings.cluster_name, @self_unique_address)
                  else
                    ProtocolMessage.down_request(@settings.cluster_name, @self_unique_address, target)
                  end
        @transport.send_async(leader.address, message)
        true
      end
    end

    private def advance_departures(round : Int64) : Nil
      graceful = @membership.active_members
        .select(&.status.exiting?)
        .map(&.unique_address)
        .to_set
      removed = @membership.advance_departures do |member|
        departure_ready?(member.unique_address)
      end
      return if removed.empty?

      final_members = @membership.all_members
      final_digest = @membership.digest
      removed.each do |member|
        register_terminal_delivery(member) if graceful.includes?(member)
        send_terminal_delivery(member, round, final_members, final_digest)
      end
    end

    private def departure_ready?(member : UniqueAddress) : Bool
      guards = @departure_guards_mutex.synchronize { @departure_guards.values }
      guards.all? { |guard| guard.call(member) }
    rescue error
      Log.warn(exception: error) { "Cluster departure guard failed for #{member}" }
      false
    end

    private def retry_leave_request : Nil
      return unless @leave_requested.get
      unless self_member.status.up?
        @leave_requested.set(false)
        return
      end
      request_departure(ProtocolMessage::Kind::LeaveRequest, @self_unique_address)
    end

    private def register_terminal_deliveries(addresses : Enumerable(UniqueAddress)) : Nil
      addresses.each do |address|
        register_terminal_delivery(address) if @membership.member(address).try(&.status.removed?)
      end
    end

    private def register_terminal_delivery(address : UniqueAddress) : Nil
      return if address == @self_unique_address
      @terminal_mutex.synchronize { @pending_terminal_deliveries << address }
    end

    private def terminal_delivery_pending?(address : UniqueAddress) : Bool
      @terminal_mutex.synchronize { @pending_terminal_deliveries.includes?(address) }
    end

    private def clear_terminal_delivery(address : UniqueAddress) : Nil
      @terminal_mutex.synchronize { @pending_terminal_deliveries.delete(address) }
    end

    private def retry_terminal_deliveries(
      round : Int64,
      members : Array(Member),
      digest : String,
    ) : Nil
      pending = @terminal_mutex.synchronize { @pending_terminal_deliveries.to_a }
      pending.each { |address| send_terminal_delivery(address, round, members, digest) }
    end

    private def send_terminal_delivery(
      address : UniqueAddress,
      round : Int64,
      members : Array(Member),
      digest : String,
    ) : Nil
      @telemetry.gossip_sent
      @transport.send_async(address.address, ProtocolMessage.gossip(
        @settings.cluster_name,
        @self_unique_address,
        members,
        round,
        digest
      ))
    end
  end
end
