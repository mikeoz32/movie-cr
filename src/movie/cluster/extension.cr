require "./settings"
require "./protocol"
require "./daemon"

module Movie::Cluster
  class ClusterExtension < Movie::Extension
    Log = ::Log.for(self)

    DAEMON_NAME  = "cluster"
    PROTOCOL_TAG = "movie.cluster.protocol.v1"

    getter settings : ClusterSettings
    getter self_unique_address : UniqueAddress

    @state = MembershipState.new
    @revision = Atomic(Int64).new(0_i64)
    @stopped = Atomic(Bool).new(false)
    @daemon : Movie::ActorRef(ProtocolMessage)?
    @seed_nodes : Array(Movie::Address)
    @seed_mutex = Mutex.new
    @remote_refs = {} of String => Movie::Remote::RemoteActorRef(ProtocolMessage)
    @remote_refs_mutex = Mutex.new
    @remote : Movie::Remote::RemoteExtension

    def initialize(
      @system : Movie::AbstractActorSystem,
      @settings : ClusterSettings = ClusterSettings.new,
    )
      remote = @system.remote || raise ClusterConfigurationError.new("cluster membership requires remoting")
      @remote = remote
      @self_unique_address = UniqueAddress.new(@system.address, remote.node_uid)
      @seed_nodes = @settings.seed_nodes
    end

    def start : Bool
      Movie::Remote::MessageRegistry.register(ProtocolMessage, PROTOCOL_TAG)
      initial_status = seed_node? ? MemberStatus::Up : MemberStatus::Joining
      @state.merge([new_self_member(initial_status)])
      @daemon = @system.spawn_system_actor(
        ClusterDaemon.new(self),
        DAEMON_NAME,
        restart_strategy: Movie::RestartStrategy::STOP
      )
      start_join_loop unless initial_status.up?
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
        attempt_join unless up?
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
      when .join_tick?
        # handled before remote identity validation
      end
    end

    private def handle_join(message : ProtocolMessage) : Nil
      candidate = message.member
      return unless candidate && candidate.unique_address == message.sender

      @state.merge([candidate])
      current = @state.member(candidate.unique_address).not_nil!
      if current.status.joining? && local_leader?
        promoted = Member.new(
          current.unique_address,
          MemberStatus::Up,
          current.roles,
          next_revision,
          @self_unique_address.node_uid
        )
        @state.merge([promoted])
      end
      send_to(candidate.unique_address.address, ProtocolMessage.welcome(
        @settings.cluster_name,
        @self_unique_address,
        @state.all_members
      ))
    end

    private def handle_welcome(message : ProtocolMessage) : Nil
      return unless seed_address?(message.sender.address)
      @state.merge(message.members)
    end

    private def start_join_loop : Nil
      spawn do
        until @stopped.get || up?
          attempt_join
          sleep @settings.join_retry_interval
        end
      end
    end

    private def attempt_join : Nil
      seeds = @seed_mutex.synchronize { @seed_nodes.dup }
      seeds.each { |seed| send_join(seed) }
    end

    private def send_join(seed : Movie::Address) : Nil
      send_to(seed, ProtocolMessage.join(
        @settings.cluster_name,
        @self_unique_address,
        self_member
      ))
    rescue ex : Exception
      Log.debug { "Cluster join attempt to #{seed} failed: #{ex.message}" }
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
  end
end
