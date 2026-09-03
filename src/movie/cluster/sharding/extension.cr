require "set"
require "log"
require "../extension"
require "./model"
require "./provider"
require "./daemon"
require "./entity_ref"
require "./telemetry"

module Movie::Cluster
  record ShardKey, entity_type : String, shard_id : Int32
  record PendingShardingDelivery,
    envelope : ShardingEnvelope,
    sender : Movie::ActorRefBase?
  record PlanBootstrapCandidate,
    allocations : ShardAllocations,
    generation : Int64
  record ShardingPlanState,
    allocations : ShardAllocations,
    coordinator : UniqueAddress,
    generation : Int64
end

module Movie
  class ClusterShardingExtension < Extension
    Log = ::Log.for(self)

    DAEMON_NAME     = "sharding"
    PROTOCOL_TAG    = "movie.cluster.sharding.envelope.v1"
    CONTROL_ACK_TAG = "movie.cluster.sharding.control-ack.v1"

    LEASE_RETRY_INTERVAL       = 25.milliseconds
    RECONCILE_INTERVAL         = 100.milliseconds
    PLAN_SYNC_INTERVAL         = 250.milliseconds
    HANDOFF_TIMEOUT            = 2.seconds
    HANDOFF_BUFFER_CAPACITY    = 1_024
    DELIVERY_RETRY_DRAIN_BATCH =    64

    @providers = {} of String => Cluster::ShardedEntityProvider
    @providers_mutex = Mutex.new
    @remote_refs = {} of String => Remote::RemoteActorRef(Cluster::ShardingEnvelope)
    @remote_refs_mutex = Mutex.new
    @plans = {} of String => Cluster::ShardAllocations
    @plan_coordinators = {} of String => Cluster::UniqueAddress
    @plan_generations = {} of String => Int64
    @plans_mutex = Mutex.new
    @handoffs = Set(Cluster::ShardKey).new
    @pending_deliveries = {} of Cluster::ShardKey => Array(Cluster::PendingShardingDelivery)
    @handoffs_mutex = Mutex.new
    @delivery_retries = Set(Cluster::ShardKey).new
    @pending_delivery_retries = {} of Cluster::ShardKey => Array(Cluster::PendingShardingDelivery)
    @delivery_retry_deadlines = {} of Cluster::ShardKey => Time::Instant
    @delivery_retries_mutex = Mutex.new
    @routing_mutex = Mutex.new
    @prepared_shards = {} of Cluster::ShardKey => Cluster::UniqueAddress
    @prepared_shards_mutex = Mutex.new
    @daemon : ActorRef(Cluster::ShardingEnvelope)?
    @event_listener : ActorRef(Cluster::ClusterEvent)?
    @rebalance_scheduled = Atomic(Bool).new(false)
    @plan_syncs = Set(String).new
    @plan_syncs_mutex = Mutex.new
    @plan_bootstrap_deadlines = {} of String => Time::Instant
    @plan_bootstrap_candidates = {} of String => Cluster::PlanBootstrapCandidate
    @plan_bootstrap_mutex = Mutex.new
    @stopped = Atomic(Bool).new(false)
    @telemetry = Cluster::ShardingTelemetry.new
    @idle_sweeps = Set(String).new
    @idle_sweeps_mutex = Mutex.new

    def initialize(@system : AbstractActorSystem)
      @cluster = @system.cluster || raise Cluster::ClusterShardingConfigurationError.new(
        "cluster sharding requires cluster membership"
      )
      @remote = @system.remote || raise Cluster::ClusterShardingConfigurationError.new(
        "cluster sharding requires remoting"
      )
    end

    def start : Bool
      Remote::MessageRegistry.register(Cluster::ShardingEnvelope, PROTOCOL_TAG)
      Remote::MessageRegistry.register(Cluster::ShardingControlAck, CONTROL_ACK_TAG)
      @daemon = @system.spawn_system_actor(
        Cluster::ShardingDaemon.new(self),
        DAEMON_NAME,
        restart_strategy: RestartStrategy::STOP
      )
      @event_listener = @system.spawn_system_actor(
        Cluster::ShardingClusterEventListener.new(self),
        "sharding-events",
        restart_strategy: RestartStrategy::STOP
      )
      @cluster.subscribe(@event_listener.not_nil!)
      @cluster.register_departure_guard("cluster-sharding") do |member|
        ready_for_departure?(member)
      end
      true
    end

    def stop : Nil
      return if @stopped.swap(true)
      providers = @providers_mutex.synchronize do
        current = @providers.values
        @providers.clear
        current
      end
      providers.each(&.stop_all)
      if listener = @event_listener
        @cluster.unsubscribe(listener)
        listener.send_system(STOP)
      end
      @daemon.try &.send_system(STOP)
      @cluster.unregister_departure_guard("cluster-sharding")
      @remote_refs_mutex.synchronize { @remote_refs.clear }
      @plans_mutex.synchronize do
        @plans.clear
        @plan_coordinators.clear
        @plan_generations.clear
      end
      @handoffs_mutex.synchronize do
        @handoffs.clear
        @pending_deliveries.clear
      end
      @delivery_retries_mutex.synchronize do
        @delivery_retries.clear
        @pending_delivery_retries.clear
        @delivery_retry_deadlines.clear
      end
      @prepared_shards_mutex.synchronize { @prepared_shards.clear }
      @plan_syncs_mutex.synchronize { @plan_syncs.clear }
      @plan_bootstrap_mutex.synchronize do
        @plan_bootstrap_deadlines.clear
        @plan_bootstrap_candidates.clear
      end
      @idle_sweeps_mutex.synchronize { @idle_sweeps.clear }
    end

    def init(
      name : String,
      message_type : T.class,
      shard_count : Int32 = 256,
      partitioner : Cluster::EntityPartitioner = Cluster::StableHashPartitioner.new,
      allocation : Cluster::ShardAllocationStrategy = Cluster::LeastLoadedAllocation.new,
      rebalance : Cluster::RebalancePolicy = Cluster::RateLimitedRebalance.new,
      idle_timeout : Time::Span? = nil,
      &factory : String -> AbstractBehavior(T)
    ) : Cluster::ShardedEntityType(T) forall T
      raise ArgumentError.new("entity type name must not be empty") if name.empty?
      settings = Cluster::ShardingSettings.new(
        shard_count,
        partitioner,
        allocation,
        rebalance,
        idle_timeout: idle_timeout
      )
      provider = Cluster::BehaviorEntityProvider(T).new(
        name,
        settings,
        @system,
        @telemetry,
        &factory
      )
      selected = register_provider(name, T.name, settings, provider)
      synchronize_provider(selected)
      schedule_idle_sweep(selected)
      Cluster::ShardedEntityType(T).new(name)
    end

    def entity_ref_for(
      entity_type : Cluster::ShardedEntityType(T),
      entity_id : String,
    ) : Cluster::ShardedEntityRef(T) forall T
      provider_for(entity_type.name)
      Cluster::ShardedEntityRef(T).new(self, entity_type, entity_id)
    end

    def allocations(entity_type : Cluster::ShardedEntityType(T)) : Cluster::ShardAllocations forall T
      provider = provider_for(entity_type.name)
      synchronize_provider(provider) unless plan_for(provider.name)
      plan_for(provider.name).try(&.dup) || Cluster::ShardAllocations.new
    end

    def local_entity_count : Int32
      @providers_mutex.synchronize { @providers.values.sum(&.local_entity_count) }
    end

    def handoffs_in_progress : Int32
      @handoffs_mutex.synchronize { @handoffs.size }
    end

    def handoff_in_progress?(
      entity_type : Cluster::ShardedEntityType(T),
      entity_id : String,
    ) : Bool forall T
      provider = provider_for(entity_type.name)
      shard_id = provider.settings.partitioner.shard_for(
        provider.name,
        entity_id,
        provider.settings.shard_count
      )
      key = Cluster::ShardKey.new(provider.name, shard_id)
      @handoffs_mutex.synchronize { @handoffs.includes?(key) }
    end

    def stats : Cluster::ShardingStats
      @telemetry.snapshot
    end

    private def register_provider(
      name : String,
      message_type_name : String,
      settings : Cluster::ShardingSettings,
      provider : Cluster::ShardedEntityProvider,
    ) : Cluster::ShardedEntityProvider
      selected = @providers_mutex.synchronize do
        if existing = @providers[name]?
          unless existing.message_type_name == message_type_name && existing.settings.compatible?(settings)
            raise Cluster::ClusterShardingConfigurationError.new(
              "Entity type #{name} is already registered with incompatible settings"
            )
          end
          existing
        else
          @providers[name] = provider
          provider
        end
      end
      provider.stop_all unless selected.same?(provider)
      selected
    end

    private def provider_for(name : String) : Cluster::ShardedEntityProvider
      @providers_mutex.synchronize do
        @providers[name]? || raise Cluster::ClusterShardingConfigurationError.new(
          "Entity type #{name} is not registered on this node"
        )
      end
    end

    private def validate_settings(
      provider : Cluster::ShardedEntityProvider,
      envelope : Cluster::ShardingEnvelope,
    ) : Nil
      return if envelope.settings_key == provider.settings.configuration_key
      raise Cluster::ClusterShardingConfigurationError.new(
        "Entity type #{provider.name} received incompatible sharding settings"
      )
    end

    private def current_coordinator : Cluster::UniqueAddress?
      snapshot = @cluster.snapshot
      return nil unless snapshot.unreachable.empty? && @cluster.converged?
      snapshot.members.select(&.status.up?).min_by?(&.unique_address).try(&.unique_address)
    end

    private def ready_for_departure?(member : Cluster::UniqueAddress) : Bool
      providers = @providers_mutex.synchronize { @providers.values }
      providers.all? do |provider|
        plan_for(provider.name).try { |plan| !plan.values.includes?(member) } != false
      end
    end

    private def routing_coordinator : Cluster::UniqueAddress
      coordinator = current_coordinator
      unless coordinator && @cluster.up?
        raise Cluster::NoShardOwnerError.new("cluster", -1)
      end
      coordinator
    end

    private def coordinator? : Bool
      current_coordinator == @cluster.self_unique_address
    end

    private def remote_ref(owner : Cluster::UniqueAddress) : Remote::RemoteActorRef(Cluster::ShardingEnvelope)
      key = owner.to_s
      @remote_refs_mutex.synchronize do
        @remote_refs[key] ||= @remote.actor_ref(
          ActorPath.new(owner.address, ["system", DAEMON_NAME]),
          Cluster::ShardingEnvelope
        )
      end
    end
  end

  class ClusterSharding < ExtensionId(ClusterShardingExtension)
    def create(system : AbstractActorSystem) : ClusterShardingExtension
      ClusterShardingExtension.new(system)
    end
  end
end
