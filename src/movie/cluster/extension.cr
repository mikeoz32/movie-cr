require "./settings"
require "./protocol"
require "./daemon"

module Movie::Cluster
  private module ClusterClock
    extend self

    EPOCH = Time.instant

    def now_nanoseconds : Int64
      (Time.instant - EPOCH).total_nanoseconds.to_i64
    end
  end

  record ClusterStats,
    join_attempts : Int64,
    gossip_rounds : Int64,
    gossip_sent : Int64,
    gossip_received : Int64,
    gossip_acks : Int64,
    membership_merges : Int64,
    heartbeats_sent : Int64,
    heartbeats_received : Int64,
    heartbeat_timeouts : Int64,
    reachability_restorations : Int64

  class ClusterExtension < Movie::Extension
    Log = ::Log.for(self)

    DAEMON_NAME  = "cluster"
    PROTOCOL_TAG = "movie.cluster.protocol.v1"

    getter settings : ClusterSettings
    getter self_unique_address : UniqueAddress

    @state : MembershipState
    @revision = Atomic(Int64).new(0_i64)
    @stopped = Atomic(Bool).new(false)
    @daemon : Movie::ActorRef(ProtocolMessage)?
    @seed_nodes : Array(Movie::Address)
    @seed_mutex = Mutex.new
    @remote_refs = {} of String => Movie::Remote::RemoteActorRef(ProtocolMessage)
    @remote_refs_mutex = Mutex.new
    @remote : Movie::Remote::RemoteExtension
    @join_attempts = Atomic(Int64).new(0_i64)
    @gossip_rounds = Atomic(Int64).new(0_i64)
    @gossip_sent = Atomic(Int64).new(0_i64)
    @gossip_received = Atomic(Int64).new(0_i64)
    @gossip_acks = Atomic(Int64).new(0_i64)
    @membership_merges = Atomic(Int64).new(0_i64)
    @seen_digests = {} of String => String
    @seen_mutex = Mutex.new
    @last_heartbeat_ns = {} of String => Int64
    @unreachable = Set(String).new
    @reachability_mutex = Mutex.new
    @heartbeat_sequence = Atomic(Int64).new(0_i64)
    @heartbeats_sent = Atomic(Int64).new(0_i64)
    @heartbeats_received = Atomic(Int64).new(0_i64)
    @heartbeat_timeouts = Atomic(Int64).new(0_i64)
    @reachability_restorations = Atomic(Int64).new(0_i64)

    def initialize(
      @system : Movie::AbstractActorSystem,
      @settings : ClusterSettings = ClusterSettings.new,
    )
      remote = @system.remote || raise ClusterConfigurationError.new("cluster membership requires remoting")
      @remote = remote
      @self_unique_address = UniqueAddress.new(@system.address, remote.node_uid)
      @seed_nodes = @settings.seed_nodes
      @state = MembershipState.new(@settings.max_members)
    end

    def start : Bool
      Movie::Remote::MessageRegistry.register(ProtocolMessage, PROTOCOL_TAG)
      initial_status = seed_node? ? MemberStatus::Up : MemberStatus::Joining
      record_merge(@state.merge([new_self_member(initial_status)]))
      @daemon = @system.spawn_system_actor(
        ClusterDaemon.new(self),
        DAEMON_NAME,
        restart_strategy: Movie::RestartStrategy::STOP
      )
      start_join_loop unless @seed_nodes.empty?
      start_gossip_loop
      start_heartbeat_loop
      true
    end

    def stop : Nil
      return if @stopped.swap(true)
      @daemon.try &.send_system(Movie::STOP)
      @remote_refs_mutex.synchronize { @remote_refs.clear }
    end

    def snapshot : ClusterSnapshot
      unreachable = @reachability_mutex.synchronize { @unreachable.dup }
      @state.snapshot(@self_unique_address, unreachable)
    end

    def self_member : Member
      @state.member(@self_unique_address) || raise "cluster self member is missing"
    end

    def up? : Bool
      self_member.status.up?
    end

    def stats : ClusterStats
      ClusterStats.new(
        join_attempts: @join_attempts.get,
        gossip_rounds: @gossip_rounds.get,
        gossip_sent: @gossip_sent.get,
        gossip_received: @gossip_received.get,
        gossip_acks: @gossip_acks.get,
        membership_merges: @membership_merges.get,
        heartbeats_sent: @heartbeats_sent.get,
        heartbeats_received: @heartbeats_received.get,
        heartbeat_timeouts: @heartbeat_timeouts.get,
        reachability_restorations: @reachability_restorations.get
      )
    end

    def converged? : Bool
      digest = @state.digest
      active = @state.active_members.select(&.status.up?)
      unreachable = @reachability_mutex.synchronize { @unreachable.dup }
      @seen_mutex.synchronize do
        active.all? do |member|
          member.unique_address == @self_unique_address ||
            unreachable.includes?(member.unique_address.key) ||
            @seen_digests[member.unique_address.key]? == digest
        end
      end
    end

    def await_up(timeout_span : Time::Span = 10.seconds) : Nil
      deadline = Time.instant + timeout_span
      until up?
        raise ClusterConfigurationError.new("cluster node did not join within #{timeout_span}") if Time.instant >= deadline
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
      request_departure(ProtocolMessage::Kind::LeaveRequest, @self_unique_address)
    end

    # Manual downing never runs from reachability alone. The operator chooses
    # the exact process incarnation after resolving any partition ambiguity.
    def down(target : UniqueAddress) : Bool
      request_departure(ProtocolMessage::Kind::DownRequest, target)
    end

    # Internal actor entrypoint. Remote protocol traffic must carry a sender
    # actor path whose address matches the claimed process address.
    def handle_protocol(message : ProtocolMessage, sender_path : Movie::ActorPath?) : Nil
      return if @stopped.get
      return unless message.cluster_name == @settings.cluster_name

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

      unless sender_path && sender_path.address == message.sender.address
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
    end

    private def handle_join(message : ProtocolMessage) : Nil
      candidate = message.member
      return unless candidate && candidate.unique_address == message.sender

      record_merge(@state.merge([candidate]))
      current = @state.member(candidate.unique_address).not_nil!
      if current.status.joining? && local_leader?
        promoted = Member.new(
          current.unique_address,
          MemberStatus::Up,
          current.roles,
          next_revision,
          @self_unique_address.node_uid
        )
        record_merge(@state.merge([promoted]))
      end
      send_to(candidate.unique_address.address, ProtocolMessage.welcome(
        @settings.cluster_name,
        @self_unique_address,
        @state.all_members
      ))
    end

    private def handle_welcome(message : ProtocolMessage) : Nil
      return unless seed_address?(message.sender.address)
      record_merge(@state.merge(message.members))
      promote_joining_if_leader
    end

    private def handle_gossip(message : ProtocolMessage) : Nil
      return unless known_active_sender?(message.sender)
      @gossip_received.add(1)
      record_merge(@state.merge(message.members))
      promote_joining_if_leader
      round = message.round || return
      send_async(message.sender.address, ProtocolMessage.gossip_ack(
        @settings.cluster_name,
        @self_unique_address,
        round,
        @state.digest
      ))
    rescue ex : MembershipCapacityError
      Log.warn { "Rejected cluster gossip from #{message.sender}: #{ex.message}" }
    end

    private def handle_gossip_ack(message : ProtocolMessage) : Nil
      return unless known_active_sender?(message.sender)
      digest = message.digest || return
      @gossip_acks.add(1)
      @seen_mutex.synchronize { @seen_digests[message.sender.key] = digest }
    end

    private def handle_heartbeat(message : ProtocolMessage) : Nil
      return unless known_active_sender?(message.sender)
      sequence = message.round || return
      @heartbeats_received.add(1)
      send_async(message.sender.address, ProtocolMessage.heartbeat_ack(
        @settings.cluster_name,
        @self_unique_address,
        sequence
      ))
    end

    private def handle_heartbeat_ack(message : ProtocolMessage) : Nil
      return unless known_active_sender?(message.sender)
      mark_reachable(message.sender)
    end

    private def handle_leave_request(message : ProtocolMessage) : Nil
      target = message.target || return
      return unless target == message.sender
      apply_departure(target, MemberStatus::Leaving) if local_leader?
    end

    private def handle_down_request(message : ProtocolMessage) : Nil
      unless @remote.settings.shared_secret
        Log.warn { "Rejected unauthenticated remote cluster down request from #{message.sender}" }
        return
      end
      target = message.target || return
      apply_departure(target, MemberStatus::Down) if local_leader?
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
      return if self_member.status.removed? || self_member.status.down?
      seeds = @seed_mutex.synchronize { @seed_nodes.dup }
      seeds.reject { |seed| seed == @self_unique_address.address }.each { |seed| send_join(seed) }
    end

    private def send_join(seed : Movie::Address) : Nil
      @join_attempts.add(1)
      send_to(seed, ProtocolMessage.join(
        @settings.cluster_name,
        @self_unique_address,
        self_member
      ))
    rescue ex : Exception
      Log.debug { "Cluster join attempt to #{seed} failed: #{ex.message}" }
    end

    private def gossip_round : Nil
      return unless participating?
      round = @gossip_rounds.add(1) + 1
      members = @state.all_members
      digest = @state.digest
      peers = @state.active_members.select do |member|
        member.unique_address != @self_unique_address &&
          (member.status.up? || member.status.joining? || member.status.leaving?)
      end
      unless peers.empty?
        start = ((round - 1) % peers.size).to_i
        {peers.size, @settings.gossip_fanout}.min.times do |offset|
          peer = peers[(start + offset) % peers.size]
          @gossip_sent.add(1)
          send_async(peer.unique_address.address, ProtocolMessage.gossip(
            @settings.cluster_name,
            @self_unique_address,
            members,
            round,
            digest
          ))
        end
      end
      advance_departures(round) if departure_coordinator?
    end

    private def send_async(address : Movie::Address, message : ProtocolMessage) : Nil
      spawn { send_to(address, message) }
    end

    private def heartbeat_round : Nil
      return unless participating?
      now = ClusterClock.now_nanoseconds
      peers = @state.active_members.select do |member|
        member.unique_address != @self_unique_address &&
          (member.status.up? || member.status.leaving? || member.status.exiting?)
      end
      active_keys = peers.map(&.unique_address.key).to_set
      @reachability_mutex.synchronize do
        @last_heartbeat_ns.reject! { |key, _| !active_keys.includes?(key) }
        @unreachable.reject! { |key| !active_keys.includes?(key) }
        peers.each do |peer|
          key = peer.unique_address.key
          last_seen = @last_heartbeat_ns[key]? || begin
            @last_heartbeat_ns[key] = now
            now
          end
          if now - last_seen > @settings.heartbeat_timeout.total_nanoseconds && @unreachable.add?(key)
            @heartbeat_timeouts.add(1)
          end
        end
      end

      peers.each do |peer|
        sequence = @heartbeat_sequence.add(1) + 1
        @heartbeats_sent.add(1)
        send_async(peer.unique_address.address, ProtocolMessage.heartbeat(
          @settings.cluster_name,
          @self_unique_address,
          sequence
        ))
      end
    end

    private def send_to(address : Movie::Address, message : ProtocolMessage) : Nil
      daemon = @daemon || return
      ref = remote_ref(address)
      ref.tell_from(daemon, message)
    end

    private def remote_ref(address : Movie::Address) : Movie::Remote::RemoteActorRef(ProtocolMessage)
      key = address.to_s
      @remote_refs_mutex.synchronize do
        @remote_refs[key] ||= @remote.actor_ref(
          Movie::ActorPath.new(address, ["system", DAEMON_NAME]),
          ProtocolMessage
        )
      end
    end

    private def new_self_member(status : MemberStatus) : Member
      Member.new(
        @self_unique_address,
        status,
        @settings.roles,
        next_revision,
        @self_unique_address.node_uid
      )
    end

    private def next_revision : Int64
      @revision.add(1) + 1
    end

    private def seed_node? : Bool
      seeds = @seed_mutex.synchronize { @seed_nodes.dup }
      seeds.empty? || seeds.includes?(@self_unique_address.address)
    end

    private def seed_address?(address : Movie::Address) : Bool
      @seed_mutex.synchronize { @seed_nodes.includes?(address) }
    end

    private def local_leader? : Bool
      snapshot.leader == @self_unique_address
    end

    private def departure_coordinator? : Bool
      leader = snapshot.leader
      leader.nil? || leader == @self_unique_address
    end

    private def known_active_sender?(sender : UniqueAddress) : Bool
      member = @state.member(sender)
      !member.nil? && !member.status.removed? && !member.status.down?
    end

    private def promote_joining_if_leader : Nil
      return unless local_leader?
      @state.active_members.select(&.status.joining?).each do |joining|
        promoted = Member.new(
          joining.unique_address,
          MemberStatus::Up,
          joining.roles,
          next_revision,
          @self_unique_address.node_uid
        )
        record_merge(@state.merge([promoted]))
      end
    end

    private def record_merge(changes : Int32) : Nil
      @membership_merges.add(changes.to_i64) if changes > 0
    end

    private def participating? : Bool
      status = self_member.status
      status.up? || status.leaving? || status.exiting?
    end

    private def request_departure(kind : ProtocolMessage::Kind, target : UniqueAddress) : Bool
      current = @state.member(target)
      return false unless current && !current.status.removed?
      leader = snapshot.leader
      return false unless leader

      if leader == @self_unique_address
        status = kind.leave_request? ? MemberStatus::Leaving : MemberStatus::Down
        apply_departure(target, status)
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
        send_async(leader.address, message)
        true
      end
    end

    private def apply_departure(target : UniqueAddress, status : MemberStatus) : Bool
      current = @state.member(target)
      return false unless current
      return false if current.status.precedence >= status.precedence

      changed = Member.new(
        current.unique_address,
        status,
        current.roles,
        next_revision,
        @self_unique_address.node_uid
      )
      changes = @state.merge([changed])
      record_merge(changes)
      changes > 0
    end

    private def advance_departures(round : Int64) : Nil
      removed = [] of UniqueAddress
      @state.active_members.each do |member|
        next_status = case member.status
                      when .leaving?         then MemberStatus::Exiting
                      when .exiting?, .down? then MemberStatus::Removed
                      else                        next
                      end
        if apply_departure(member.unique_address, next_status) && next_status.removed?
          removed << member.unique_address
        end
      end
      return if removed.empty?

      final_members = @state.all_members
      final_digest = @state.digest
      removed.each do |member|
        @gossip_sent.add(1)
        send_async(member.address, ProtocolMessage.gossip(
          @settings.cluster_name,
          @self_unique_address,
          final_members,
          round,
          final_digest
        ))
      end
    end

    private def mark_reachable(sender : UniqueAddress) : Nil
      restored = @reachability_mutex.synchronize do
        @last_heartbeat_ns[sender.key] = ClusterClock.now_nanoseconds
        @unreachable.delete(sender.key)
      end
      @reachability_restorations.add(1) if restored
    end
  end
end
