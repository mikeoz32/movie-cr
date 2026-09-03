module Movie
  class ClusterSingletonExtension
    def init_event_sourced(
      name : String,
      entity_type : Persistence::EntityType(T),
      entity_id : String = name,
      roles : Array(String) = [] of String,
      lease_duration : Time::Span = 10.seconds,
      lease_renew_interval : Time::Span = 3.seconds,
      activation_interval : Time::Span = 250.milliseconds,
      activation_timeout : Time::Span = 2.seconds,
    ) : Cluster::ClusterSingletonRef(T) forall T
      init_persistent_singleton(
        name,
        entity_type,
        entity_id,
        roles,
        lease_duration,
        lease_renew_interval,
        activation_interval,
        activation_timeout,
        Cluster::SingletonBackingKind::EventSourced
      )
    end

    def init_durable_state(
      name : String,
      entity_type : Persistence::EntityType(T),
      entity_id : String = name,
      roles : Array(String) = [] of String,
      lease_duration : Time::Span = 10.seconds,
      lease_renew_interval : Time::Span = 3.seconds,
      activation_interval : Time::Span = 250.milliseconds,
      activation_timeout : Time::Span = 2.seconds,
    ) : Cluster::ClusterSingletonRef(T) forall T
      init_persistent_singleton(
        name,
        entity_type,
        entity_id,
        roles,
        lease_duration,
        lease_renew_interval,
        activation_interval,
        activation_timeout,
        Cluster::SingletonBackingKind::DurableState
      )
    end

    private def init_persistent_singleton(
      name : String,
      entity_type : Persistence::EntityType(T),
      entity_id : String,
      roles : Array(String),
      lease_duration : Time::Span,
      lease_renew_interval : Time::Span,
      activation_interval : Time::Span,
      activation_timeout : Time::Span,
      backing_kind : Cluster::SingletonBackingKind,
    ) : Cluster::ClusterSingletonRef(T) forall T
      persistence_settings = Cluster::SingletonSettings.new(
        roles,
        activation_interval,
        activation_timeout
      )
      validate_persistent_singleton(name, entity_id)
      backing_identity = Cluster::SingletonBackingIdentity.new(
        backing_kind,
        entity_type.name,
        T.name,
        entity_id,
        persistence_settings.configuration_key,
        lease_duration,
        lease_renew_interval
      )
      if existing = existing_registration(name, T, persistence_settings, backing_identity)
        return existing.ref
      end
      entity_type_ref = case backing_kind
                        when .event_sourced?
                          @sharding.init_event_sourced(
                            entity_type,
                            shard_count: 1,
                            partitioner: singleton_partitioner(backing_identity, entity_id),
                            allocation: singleton_allocation(persistence_settings.roles),
                            rebalance: Cluster::NoRebalance.new,
                            lease_duration: lease_duration,
                            lease_renew_interval: lease_renew_interval,
                            routing_name: internal_name(name)
                          )
                        when .durable_state?
                          @sharding.init_durable_state(
                            entity_type,
                            shard_count: 1,
                            partitioner: singleton_partitioner(backing_identity, entity_id),
                            allocation: singleton_allocation(persistence_settings.roles),
                            rebalance: Cluster::NoRebalance.new,
                            lease_duration: lease_duration,
                            lease_renew_interval: lease_renew_interval,
                            routing_name: internal_name(name)
                          )
                        else
                          raise Cluster::ClusterSingletonConfigurationError.new(
                            "Persistent singleton requires a persistent backing kind"
                          )
                        end
      finish_registration(Cluster::SingletonRegistration(T).new(
        name,
        persistence_settings,
        backing_identity,
        @sharding,
        entity_type_ref,
        entity_id,
        self
      ))
    end

    private def validate_persistent_singleton(name : String, entity_id : String) : Nil
      validate_name(name)
      raise ArgumentError.new("singleton entity id must not be empty") if entity_id.empty?
    end
  end
end
