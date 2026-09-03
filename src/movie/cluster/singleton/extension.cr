require "set"

module Movie
  class ClusterSingletonExtension < Extension
    Log = ::Log.for(self)

    INTERNAL_PREFIX = "movie.singleton."
    ENTITY_ID       = "singleton"

    @registrations = {} of String => Cluster::SingletonRegistrationBase
    @registrations_mutex = Mutex.new
    @activation_loops = Set(String).new
    @activation_loops_mutex = Mutex.new
    @activation_inflight = Set(String).new
    @activation_inflight_mutex = Mutex.new
    @activated_once = Set(String).new
    @activated_once_mutex = Mutex.new
    @last_owners = {} of String => Cluster::UniqueAddress?
    @last_owners_mutex = Mutex.new
    @last_activation_errors = {} of String => String
    @last_activation_errors_mutex = Mutex.new
    @telemetry = Cluster::SingletonTelemetry.new
    @stopped = Atomic(Bool).new(false)

    def initialize(@system : AbstractActorSystem)
      @cluster = @system.cluster || raise Cluster::ClusterSingletonConfigurationError.new(
        "cluster singleton requires cluster membership"
      )
      @sharding = ClusterSharding.get(@system)
    end

    def start : Bool
      true
    end

    def stop : Nil
      return if @stopped.swap(true)
      @registrations_mutex.synchronize { @registrations.clear }
      @activation_loops_mutex.synchronize { @activation_loops.clear }
      @activation_inflight_mutex.synchronize { @activation_inflight.clear }
      @activated_once_mutex.synchronize { @activated_once.clear }
      @last_owners_mutex.synchronize { @last_owners.clear }
      @last_activation_errors_mutex.synchronize { @last_activation_errors.clear }
    end

    def init(
      name : String,
      message_type : T.class,
      roles : Array(String) = [] of String,
      activation_interval : Time::Span = 250.milliseconds,
      activation_timeout : Time::Span = 2.seconds,
      &factory : -> AbstractBehavior(T)
    ) : Cluster::ClusterSingletonRef(T) forall T
      validate_name(name)
      settings = Cluster::SingletonSettings.new(roles, activation_interval, activation_timeout)
      backing_identity = Cluster::SingletonBackingIdentity.new(
        Cluster::SingletonBackingKind::Behavior,
        nil,
        T.name,
        ENTITY_ID,
        settings.configuration_key
      )
      if existing = existing_registration(name, message_type, settings, backing_identity)
        return existing.ref
      end
      entity_type = @sharding.init(
        internal_name(name),
        message_type,
        shard_count: 1,
        partitioner: singleton_partitioner(backing_identity, ENTITY_ID),
        allocation: singleton_allocation(settings.roles),
        rebalance: Cluster::NoRebalance.new
      ) { |_entity_id| factory.call }
      registration = Cluster::SingletonRegistration(T).new(
        name, settings, backing_identity, @sharding, entity_type, ENTITY_ID, self
      )
      finish_registration(registration)
    end

    def owner(name : String) : Cluster::UniqueAddress?
      registration_for(name).owner
    end

    def locally_owned?(name : String) : Bool
      owner(name) == @cluster.self_unique_address
    end

    def handoff_in_progress?(name : String) : Bool
      registration_for(name).handoff_in_progress?
    end

    def ensure_active(name : String) : Nil
      registration = registration_for(name)
      ensure_activation_loop(registration)
    end

    def stats : Cluster::SingletonStats
      @telemetry.snapshot(@sharding.handoffs_in_progress, @sharding.stats)
    end

    def last_activation_error(name : String) : String?
      @last_activation_errors_mutex.synchronize { @last_activation_errors[name]? }
    end

    # Internal proxy instrumentation entrypoints.
    def record_tell : Nil
      @telemetry.told
    end

    def record_ask : Nil
      @telemetry.asked
    end

    def record_stop : Nil
      @telemetry.stop_requested
    end

    def record_routing_rejection(error : Exception) : Nil
      @telemetry.routing_rejected if routing_rejection?(error)
    end

    private def register(
      registration : Cluster::SingletonRegistrationBase,
    ) : Cluster::SingletonRegistrationBase
      @registrations_mutex.synchronize do
        if existing = @registrations[registration.name]?
          unless existing.message_type_name == registration.message_type_name &&
                 existing.settings.configuration_key == registration.settings.configuration_key &&
                 existing.backing_identity == registration.backing_identity
            raise Cluster::ClusterSingletonConfigurationError.new(
              "Singleton #{registration.name} is already registered with incompatible settings"
            )
          end
          existing
        else
          @registrations[registration.name] = registration
          @telemetry.registered
          registration
        end
      end
    end

    private def existing_registration(
      name : String,
      message_type : T.class,
      settings : Cluster::SingletonSettings,
      backing_identity : Cluster::SingletonBackingIdentity,
    ) : Cluster::SingletonRegistration(T)? forall T
      @registrations_mutex.synchronize do
        existing = @registrations[name]? || next nil
        unless existing.message_type_name == T.name &&
               existing.settings.configuration_key == settings.configuration_key &&
               existing.backing_identity == backing_identity
          raise Cluster::ClusterSingletonConfigurationError.new(
            "Singleton #{name} is already registered with incompatible settings"
          )
        end
        existing.as(Cluster::SingletonRegistration(T))
      end
    end

    private def finish_registration(
      registration : Cluster::SingletonRegistration(T),
    ) : Cluster::ClusterSingletonRef(T) forall T
      selected = register(registration)
      ensure_activation_loop(selected)
      selected.as(Cluster::SingletonRegistration(T)).ref
    end

    private def registration_for(name : String) : Cluster::SingletonRegistrationBase
      @registrations_mutex.synchronize do
        @registrations[name]? || raise Cluster::ClusterSingletonConfigurationError.new(
          "Singleton #{name} is not registered on this node"
        )
      end
    end

    private def ensure_activation_loop(
      registration : Cluster::SingletonRegistrationBase,
    ) : Nil
      started = @activation_loops_mutex.synchronize do
        @activation_loops.add?(registration.name)
      end
      schedule_activation(registration, 1.millisecond) if started
    end

    private def schedule_activation(
      registration : Cluster::SingletonRegistrationBase,
      delay : Time::Span,
    ) : Nil
      @system.scheduler.schedule_once(delay) do
        attempt_activation(registration) unless @stopped.get
      end
    end

    private def attempt_activation(registration : Cluster::SingletonRegistrationBase) : Nil
      return if @stopped.get
      unless activation_coordinator?
        schedule_activation(registration, registration.settings.activation_interval)
        return
      end
      started = @activation_inflight_mutex.synchronize do
        @activation_inflight.add?(registration.name)
      end
      return unless started

      @telemetry.activation_attempted
      future = registration.activate
      future.on_complete do |result|
        @activation_inflight_mutex.synchronize { @activation_inflight.delete(registration.name) }
        unless @stopped.get
          case result.status
          when FutureStatus::Success
            @last_activation_errors_mutex.synchronize do
              @last_activation_errors.delete(registration.name)
            end
            if result.value.as(Cluster::ShardingControlAck).accepted
              first = @activated_once_mutex.synchronize do
                @activated_once.add?(registration.name)
              end
              @telemetry.activated(first)
            end
          when FutureStatus::Failure, FutureStatus::Cancelled
            @telemetry.activation_failed
            unless result.error.nil?
              report_activation_error(registration, result.error.not_nil!)
            else
              Log.debug { "Singleton activation was cancelled for #{registration.name}" }
            end
          when FutureStatus::Pending
          end
          observe_owner(registration)
          schedule_activation(registration, registration.settings.activation_interval)
        end
      end
    rescue error
      @activation_inflight_mutex.synchronize { @activation_inflight.delete(registration.name) }
      @telemetry.activation_failed
      report_activation_error(registration, error)
      schedule_activation(registration, registration.settings.activation_interval) unless @stopped.get
    end

    private def report_activation_error(
      registration : Cluster::SingletonRegistrationBase,
      error : Exception,
    ) : Nil
      remember_activation_error(registration, error)
      if expected_activation_failure?(error)
        Log.debug(exception: error) { "Singleton activation deferred for #{registration.name}" }
      else
        Log.warn(exception: error) { "Singleton activation failed for #{registration.name}" }
      end
    end

    private def expected_activation_failure?(error : Exception) : Bool
      return true if error.is_a?(Cluster::NoShardOwnerError) ||
                     error.is_a?(Cluster::ShardLeaseUnavailableError)
      remote = error.as?(Remote::RemoteAskError)
      return false unless remote
      remote.remote_class == Cluster::NoShardOwnerError.name ||
        remote.remote_class == Cluster::ShardLeaseUnavailableError.name
    end

    private def routing_rejection?(error : Exception) : Bool
      return true if error.is_a?(Cluster::NoShardOwnerError) ||
                     error.is_a?(Cluster::ShardLeaseUnavailableError) ||
                     error.is_a?(Cluster::ShardHandoffInProgressError) ||
                     error.is_a?(Cluster::ClusterShardingConfigurationError) ||
                     error.is_a?(Remote::RemoteDeliveryError)
      remote = error.as?(Remote::RemoteAskError)
      return false unless remote
      remote.remote_class == Cluster::NoShardOwnerError.name ||
        remote.remote_class == Cluster::ShardLeaseUnavailableError.name ||
        remote.remote_class == Cluster::ShardHandoffInProgressError.name ||
        remote.remote_class == Cluster::ClusterShardingConfigurationError.name ||
        remote.remote_class == Remote::RemoteDeliveryError.name
    end

    private def remember_activation_error(
      registration : Cluster::SingletonRegistrationBase,
      error : Exception,
    ) : Nil
      @last_activation_errors_mutex.synchronize do
        @last_activation_errors[registration.name] = "#{error.class.name}: #{error.message}"
      end
    end

    private def observe_owner(registration : Cluster::SingletonRegistrationBase) : Nil
      current = registration.owner
      changed = @last_owners_mutex.synchronize do
        known = @last_owners.has_key?(registration.name)
        previous = @last_owners[registration.name]?
        @last_owners[registration.name] = current
        known && previous != current
      end
      @telemetry.owner_changed if changed
    end

    private def activation_coordinator? : Bool
      snapshot = @cluster.snapshot
      return false unless @cluster.up? && @cluster.converged? && snapshot.unreachable.empty?
      snapshot.members.select(&.status.up?).min_by?(&.unique_address).try(&.unique_address) ==
        @cluster.self_unique_address
    end

    private def singleton_allocation(roles : Array(String)) : Cluster::ShardAllocationStrategy
      base = Cluster::LeastLoadedAllocation.new
      roles.empty? ? base : Cluster::RoleAwareAllocation.new(base, roles)
    end

    private def singleton_partitioner(
      backing_identity : Cluster::SingletonBackingIdentity,
      entity_id : String,
    ) : Cluster::EntityPartitioner
      Cluster::SingletonPartitioner.new(backing_identity, entity_id)
    end

    private def internal_name(name : String) : String
      "#{INTERNAL_PREFIX}#{name}"
    end

    private def validate_name(name : String) : Nil
      raise ArgumentError.new("singleton name must not be empty") if name.empty?
    end
  end

  class ClusterSingleton < ExtensionId(ClusterSingletonExtension)
    def create(system : AbstractActorSystem) : ClusterSingletonExtension
      ClusterSingletonExtension.new(system)
    end
  end
end
