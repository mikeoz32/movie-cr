require "./settings"
require "./protocol"
require "./daemon"

module Movie::Cluster
  record ClusterStats,
    join_attempts : Int64,
    gossip_rounds : Int64,
    gossip_sent : Int64,
    gossip_received : Int64,
    gossip_acks : Int64,
    membership_merges : Int64

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
      true
    end

    def stop : Nil
      return if @stopped.swap(true)
      @daemon.try &.send_system(Movie::STOP)
      @remote_refs_mutex.synchronize { @remote_refs.clear }
    end

    def snapshot : ClusterSnapshot
      @state.snapshot(@self_unique_address, Set(String).new)
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
        membership_merges: @membership_merges.get
      )
    end

    def converged? : Bool
      digest = @state.digest
      active = @state.active_members.select(&.status.up?)
      @seen_mutex.synchronize do
        active.all? do |member|
          member.unique_address == @self_unique_address ||
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
      when .join_tick?, .gossip_tick?
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

    private def attempt_join : Nil
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
      return unless up?
      round = @gossip_rounds.add(1) + 1
      members = @state.all_members
      digest = @state.digest
      peers = @state.active_members.select do |member|
        member.unique_address != @self_unique_address &&
          (member.status.up? || member.status.joining? || member.status.leaving?)
      end
      return if peers.empty?

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

    private def send_async(address : Movie::Address, message : ProtocolMessage) : Nil
      spawn { send_to(address, message) }
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
  end
end
