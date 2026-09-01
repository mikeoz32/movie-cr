module Movie::Cluster
  # Persistence integration lives outside the core sharding extension so
  # ordinary clustered actors do not acquire a database/link-time dependency.
  class PersistentEntityProvider(T) < ShardedEntityProvider
    Log = ::Log.for(self)

    @entities = {} of String => Movie::ActorRef(T)
    @entity_shards = {} of String => Int32
    @entity_epochs = {} of String => Int64
    @last_activity = {} of String => Time::Instant
    @draining_shards = {} of Int32 => Channel(Nil)
    @entities_mutex = Mutex.new
    @leases = {} of Int32 => Movie::Persistence::ShardLeaseToken
    @owned_shards = Set(Int32).new
    @leases_mutex = Mutex.new
    @stopped = Atomic(Bool).new(false)

    def initialize(
      name : String,
      settings : ShardingSettings,
      @system : Movie::AbstractActorSystem,
      @cluster : Movie::Cluster::ClusterExtension,
      @database : Movie::DatabaseExtension,
      telemetry : ShardingTelemetry,
      &@resolver : String, Movie::Persistence::ShardLeaseToken -> Movie::ActorRef(T)
    )
      super(name, T.name, settings, telemetry)
    end

    def deliver(envelope : ShardingEnvelope, sender : Movie::ActorRefBase?) : Nil
      token = lease_for(envelope.shard_id)
      wrapper = envelope.message.wrapper || Movie::Remote::MessageRegistry.deserialize(
        envelope.message.message_type,
        envelope.message.payload
      )
      entity_ref(envelope.entity_id, envelope.shard_id, token)
        .tell_from(sender, wrapper.unwrap(T))
    end

    def passivate(entity_id : String) : Bool
      ref = @entities_mutex.synchronize do
        @entity_shards.delete(entity_id)
        @entity_epochs.delete(entity_id)
        @last_activity.delete(entity_id)
        @entities.delete(entity_id)
      end
      return false unless ref
      ref.send_system(Movie::STOP)
      true
    end

    def passivate_shard(shard_id : Int32) : Nil
      drain_shard(shard_id)
    end

    def retain_shards(shard_ids : Set(Int32)) : Nil
      releases = @leases_mutex.synchronize do
        @owned_shards = shard_ids.dup
        stale = @leases.select { |shard_id, _token| !shard_ids.includes?(shard_id) }
        stale.each_key { |shard_id| @leases.delete(shard_id) }
        stale.values
      end
      stop_entities { |_entity_id, shard_id| !shard_ids.includes?(shard_id) }
      releases.each { |token| release(token) }
    end

    def authorize_shard(shard_id : Int32) : Nil
      @leases_mutex.synchronize { @owned_shards << shard_id }
    end

    def prepare_shard(shard_id : Int32) : Nil
      authorize_shard(shard_id)
      lease_for(shard_id)
    end

    def suspend_ownership(release : Bool) : Nil
      leases = @leases_mutex.synchronize do
        current = @leases.values
        @leases.clear
        @owned_shards.clear
        current
      end
      stop_entities { |_entity_id, _shard_id| true }
      @telemetry.leases_lost(leases.size) unless release
      leases.each { |token| self.release(token) } if release
    end

    def passivate_idle(now : Time::Instant) : Int32
      timeout = settings.idle_timeout || return 0
      stop_entities do |entity_id, _shard_id|
        last = @last_activity[entity_id]?
        !last.nil? && now - last >= timeout
      end
    end

    def stop_all : Nil
      return if @stopped.swap(true)
      refs = @entities_mutex.synchronize do
        current = @entities.values
        @entities.clear
        @entity_shards.clear
        @entity_epochs.clear
        @last_activity.clear
        @draining_shards.each_value(&.close)
        @draining_shards.clear
        current
      end
      leases = @leases_mutex.synchronize do
        current = @leases.values
        @leases.clear
        @owned_shards.clear
        current
      end
      refs.each(&.send_system(Movie::STOP))
      leases.each { |token| release(token) }
    end

    def local_entity_count : Int32
      @entities_mutex.synchronize do
        @entities.reject! do |entity_id, ref|
          stopped = @system.context(ref.id).nil?
          if stopped
            @entity_shards.delete(entity_id)
            @entity_epochs.delete(entity_id)
            @last_activity.delete(entity_id)
          end
          stopped
        end
        @entities.size
      end
    end

    private def lease_for(shard_id : Int32) : Movie::Persistence::ShardLeaseToken
      raise ShardLeaseUnavailableError.new(name, shard_id) unless safe_to_own?
      if current = @leases_mutex.synchronize { @leases[shard_id]? }
        return current
      end
      key = Movie::Persistence::ShardLeaseKey.new(
        @cluster.settings.cluster_name,
        name,
        shard_id
      )
      token = nil.as(Movie::Persistence::ShardLeaseToken?)
      token = @database.acquire_shard_lease(
        key,
        @cluster.self_unique_address.to_s,
        settings.lease_duration
      ) || raise ShardLeaseUnavailableError.new(name, shard_id)
      selected = @leases_mutex.synchronize do
        unless @owned_shards.includes?(shard_id)
          raise ShardLeaseUnavailableError.new(name, shard_id)
        end
        @leases[shard_id] ||= token
      end
      schedule_renewal(shard_id, selected.epoch)
      @telemetry.lease_acquired
      selected
    rescue error : ShardLeaseUnavailableError
      release(token) if token
      raise error
    end

    private def schedule_renewal(shard_id : Int32, epoch : Int64) : Nil
      @system.scheduler.schedule_once(settings.lease_renew_interval) do
        renew(shard_id, epoch) unless @stopped.get
      end
    end

    private def renew(shard_id : Int32, epoch : Int64) : Nil
      token = @leases_mutex.synchronize { @leases[shard_id]? }
      return unless token && token.epoch == epoch
      unless safe_to_own?
        suspend_ambiguous_ownership
        return
      end
      owned = @leases_mutex.synchronize { @owned_shards.includes?(shard_id) }
      unless owned
        remove_lease(shard_id, token)
        return
      end
      renewed = @database.renew_shard_lease(token, settings.lease_duration)
      unless renewed
        @leases_mutex.synchronize { @leases.delete(shard_id) if @leases[shard_id]? == token }
        @telemetry.leases_lost
        passivate_shard(shard_id)
        return
      end
      @leases_mutex.synchronize { @leases[shard_id] = renewed }
      @telemetry.lease_renewed
      schedule_renewal(shard_id, renewed.epoch)
    rescue error
      @leases_mutex.synchronize { @leases.delete(shard_id) if token && @leases[shard_id]? == token }
      @telemetry.leases_lost
      passivate_shard(shard_id)
      Log.error(exception: error) { "Shard lease renewal failed for #{name}/#{shard_id}" }
    end

    private def safe_to_own? : Bool
      snapshot = @cluster.snapshot
      @cluster.up? && snapshot.unreachable.empty?
    end

    # A partition has no locally knowable winner. Stop entities immediately and
    # let the database lease expire instead of releasing it early to another
    # independently active side. Manual downing can then relocate after expiry.
    private def suspend_ambiguous_ownership : Nil
      lost = @leases_mutex.synchronize do
        count = @leases.size
        @leases.clear
        count
      end
      @telemetry.leases_lost(lost)
      stop_entities { |_entity_id, _shard_id| true }
    end

    private def remove_lease(
      shard_id : Int32,
      token : Movie::Persistence::ShardLeaseToken,
    ) : Nil
      @leases_mutex.synchronize { @leases.delete(shard_id) if @leases[shard_id]? == token }
      release(token)
    end

    private def release(token : Movie::Persistence::ShardLeaseToken) : Nil
      @database.release_shard_lease(token)
    rescue error
      Log.warn(exception: error) do
        "Failed to release shard lease #{token.key.entity_type}/#{token.key.shard_id}"
      end
    end

    private def entity_ref(
      entity_id : String,
      shard_id : Int32,
      token : Movie::Persistence::ShardLeaseToken,
    ) : Movie::ActorRef(T)
      stale = nil.as(Movie::ActorRef(T)?)
      current = @entities_mutex.synchronize do
        if @draining_shards.has_key?(shard_id)
          raise ShardHandoffInProgressError.new(name, shard_id)
        end
        if ref = @entities[entity_id]?
          if @entity_epochs[entity_id]? == token.epoch
            @last_activity[entity_id] = Time.instant
            next ref
          end
          stale = @entities.delete(entity_id)
          @entity_shards.delete(entity_id)
          @entity_epochs.delete(entity_id)
          @last_activity.delete(entity_id)
        end
        nil
      end
      return current if current
      stale.try &.send_system(Movie::STOP)

      resolved = @resolver.call(entity_id, token)
      @telemetry.activated
      @entities_mutex.synchronize do
        @entities[entity_id] = resolved
        @entity_shards[entity_id] = shard_id
        @entity_epochs[entity_id] = token.epoch
        @last_activity[entity_id] = Time.instant
      end
      resolved
    end

    private def remove_entities(&remove : String, Int32 -> Bool) : Array(Movie::ActorRef(T))
      @entities_mutex.synchronize do
        entity_ids = @entity_shards.select { |entity_id, shard_id| remove.call(entity_id, shard_id) }.keys
        selected = [] of Movie::ActorRef(T)
        entity_ids.each do |entity_id|
          if ref = @entities.delete(entity_id)
            selected << ref
          end
          @entity_shards.delete(entity_id)
          @entity_epochs.delete(entity_id)
          @last_activity.delete(entity_id)
        end
        selected
      end
    end

    private def stop_entities(&remove : String, Int32 -> Bool) : Int32
      refs = remove_entities { |entity_id, shard_id| remove.call(entity_id, shard_id) }
      refs.each(&.send_system(Movie::STOP))
      refs.size
    end

    private def drain_shard(shard_id : Int32) : Nil
      channel = nil.as(Channel(Nil)?)
      refs = [] of Movie::ActorRef(T)
      first = false
      @entities_mutex.synchronize do
        if current = @draining_shards[shard_id]?
          channel = current
        else
          channel = Channel(Nil).new
          @draining_shards[shard_id] = channel.not_nil!
          first = true
          entity_ids = @entity_shards.select do |_entity_id, candidate|
            candidate == shard_id
          end.keys
          entity_ids.each do |entity_id|
            refs << @entities.delete(entity_id).not_nil! if @entities.has_key?(entity_id)
            @entity_shards.delete(entity_id)
            @entity_epochs.delete(entity_id)
            @last_activity.delete(entity_id)
          end
        end
      end

      unless first
        channel.not_nil!.receive?
        return
      end

      begin
        refs.each(&.send_system(Movie::DRAIN_AND_STOP))
        refs.each do |ref|
          while @system.context(ref.id)
            sleep 1.millisecond
          end
        end
        token = @leases_mutex.synchronize do
          @owned_shards.delete(shard_id)
          @leases.delete(shard_id)
        end
        release(token) if token
      ensure
        @entities_mutex.synchronize { @draining_shards.delete(shard_id) }
        channel.not_nil!.close
      end
    end
  end
end

module Movie
  class ClusterShardingExtension
    def init_event_sourced(
      entity_type : Persistence::EntityType(T),
      shard_count : Int32 = 256,
      partitioner : Cluster::EntityPartitioner = Cluster::StableHashPartitioner.new,
      allocation : Cluster::ShardAllocationStrategy = Cluster::LeastLoadedAllocation.new,
      rebalance : Cluster::RebalancePolicy = Cluster::RateLimitedRebalance.new,
      lease_duration : Time::Span = 10.seconds,
      lease_renew_interval : Time::Span = 3.seconds,
      idle_timeout : Time::Span? = nil,
    ) : Cluster::ShardedEntityType(T) forall T
      persistence = Movie::EventSourcing.get(@system)
      init_persistent_entity(
        entity_type,
        shard_count,
        partitioner,
        allocation,
        rebalance,
        lease_duration,
        lease_renew_interval,
        idle_timeout
      ) do |entity_id, fence|
        persistence.get_entity_ref(entity_type.id(entity_id), fence)
      end
    end

    def init_durable_state(
      entity_type : Persistence::EntityType(T),
      shard_count : Int32 = 256,
      partitioner : Cluster::EntityPartitioner = Cluster::StableHashPartitioner.new,
      allocation : Cluster::ShardAllocationStrategy = Cluster::LeastLoadedAllocation.new,
      rebalance : Cluster::RebalancePolicy = Cluster::RateLimitedRebalance.new,
      lease_duration : Time::Span = 10.seconds,
      lease_renew_interval : Time::Span = 3.seconds,
      idle_timeout : Time::Span? = nil,
    ) : Cluster::ShardedEntityType(T) forall T
      persistence = Movie::DurableState.get(@system)
      init_persistent_entity(
        entity_type,
        shard_count,
        partitioner,
        allocation,
        rebalance,
        lease_duration,
        lease_renew_interval,
        idle_timeout
      ) do |entity_id, fence|
        persistence.get_entity_ref(entity_type.id(entity_id), fence)
      end
    end

    private def init_persistent_entity(
      entity_type : Persistence::EntityType(T),
      shard_count : Int32,
      partitioner : Cluster::EntityPartitioner,
      allocation : Cluster::ShardAllocationStrategy,
      rebalance : Cluster::RebalancePolicy,
      lease_duration : Time::Span,
      lease_renew_interval : Time::Span,
      idle_timeout : Time::Span?,
      &resolver : String, Persistence::ShardLeaseToken -> ActorRef(T)
    ) : Cluster::ShardedEntityType(T) forall T
      database = require_postgres_persistence!
      settings = persistent_sharding_settings(
        shard_count,
        partitioner,
        allocation,
        rebalance,
        lease_duration,
        lease_renew_interval,
        idle_timeout
      )
      provider = Cluster::PersistentEntityProvider(T).new(
        entity_type.name,
        settings,
        @system,
        @cluster,
        database,
        @telemetry
      ) { |entity_id, fence| resolver.call(entity_id, fence) }
      selected = register_provider(entity_type.name, T.name, settings, provider)
      synchronize_provider(selected)
      schedule_idle_sweep(selected)
      Cluster::ShardedEntityType(T).new(entity_type.name)
    end

    private def require_postgres_persistence! : DatabaseExtension
      database = Movie::Database.get(@system)
      return database if database.backend_name == "postgres"
      raise Cluster::ClusterShardingConfigurationError.new(
        "clustered persistent entities require the PostgreSQL backend"
      )
    end

    private def persistent_sharding_settings(
      shard_count : Int32,
      partitioner : Cluster::EntityPartitioner,
      allocation : Cluster::ShardAllocationStrategy,
      rebalance : Cluster::RebalancePolicy,
      lease_duration : Time::Span,
      lease_renew_interval : Time::Span,
      idle_timeout : Time::Span?,
    ) : Cluster::ShardingSettings
      Cluster::ShardingSettings.new(
        shard_count,
        partitioner,
        allocation,
        rebalance,
        lease_duration,
        lease_renew_interval,
        idle_timeout
      )
    end
  end
end
