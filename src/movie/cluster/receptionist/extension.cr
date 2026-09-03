require "set"

module Movie
  class ClusterReceptionistExtension < Extension
    Log = ::Log.for(self)

    DAEMON_NAME         = "receptionist"
    EVENT_LISTENER_NAME = "receptionist-events"
    WATCHER_NAME        = "receptionist-watcher"
    PROTOCOL_TAG        = "movie.cluster.receptionist.envelope.v1"
    SYNC_INTERVAL       = 250.milliseconds

    @states = {} of Cluster::UniqueAddress => Cluster::ReceptionistNodeState
    @key_types = {} of String => String
    @subscriptions = [] of Cluster::ReceptionistSubscriptionBase
    @mutex = Mutex.new
    @local_revision = 0_i64
    @daemon : ActorRef(Cluster::ReceptionistEnvelope)?
    @event_listener : ActorRef(Cluster::ClusterEvent)?
    @watcher : ActorRef(Nil)?
    @telemetry = Cluster::ReceptionistTelemetry.new
    @stopped = Atomic(Bool).new(false)

    def initialize(@system : AbstractActorSystem)
      @cluster = @system.cluster || raise Cluster::ReceptionistConfigurationError.new(
        "cluster receptionist requires cluster membership"
      )
      remote = @system.remote || raise Cluster::ReceptionistConfigurationError.new(
        "cluster receptionist requires remoting"
      )
      @transport = Cluster::ReceptionistTransport.new(remote)
    end

    def start : Bool
      Remote::MessageRegistry.register(Cluster::ReceptionistEnvelope, PROTOCOL_TAG)
      @daemon = @system.spawn_system_actor(
        Cluster::ReceptionistDaemon.new(self),
        DAEMON_NAME,
        restart_strategy: RestartStrategy::STOP
      )
      @transport.daemon = @daemon.not_nil!
      @event_listener = @system.spawn_system_actor(
        Cluster::ReceptionistClusterEventListener.new(self),
        EVENT_LISTENER_NAME,
        restart_strategy: RestartStrategy::STOP
      )
      @watcher = @system.spawn_system_actor(
        Cluster::ReceptionistWatcher.new(self),
        WATCHER_NAME,
        restart_strategy: RestartStrategy::STOP
      )
      @mutex.synchronize do
        @states[@cluster.self_unique_address] = Cluster::ReceptionistNodeState.new(
          @local_revision,
          [] of Cluster::ServiceRegistration
        )
      end
      @cluster.subscribe(@event_listener.not_nil!)
      schedule_sync(1.millisecond)
      true
    end

    def stop : Nil
      return if @stopped.swap(true)
      if listener = @event_listener
        @cluster.unsubscribe(listener)
        listener.send_system(STOP)
      end
      @watcher.try &.send_system(STOP)
      @daemon.try &.send_system(STOP)
      @transport.close
      @mutex.synchronize do
        @states.clear
        @key_types.clear
        @subscriptions.clear
      end
    end

    # Registers a local actor under a typed cluster-wide service key.
    def register(key : Cluster::ServiceKey(T), actor : ActorRef(T)) : Bool forall T
      ensure_registration_allowed
      path = local_actor_path(actor)
      registration = Cluster::ServiceRegistration.new(
        key.name,
        T.name,
        path,
        @cluster.self_unique_address
      )
      added = @mutex.synchronize do
        bind_key_type_locked(key.name, T.name)
        current = local_state_locked.registrations
        next false if current.includes?(registration)
        if current.size >= Cluster::ReceptionistLimits::REGISTRATIONS_PER_NODE
          raise Cluster::ReceptionistConfigurationError.new(
            "local receptionist registration capacity " \
            "#{Cluster::ReceptionistLimits::REGISTRATIONS_PER_NODE} exceeded"
          )
        end
        current << registration
        store_local_state_locked(current)
        true
      end
      return false unless added

      unless watch(actor)
        local_actor_terminated(actor)
        raise Cluster::ReceptionistConfigurationError.new(
          "cannot register stopped actor #{path}"
        )
      end
      @telemetry.registered
      publish_listings
      broadcast_local_state
      true
    end

    # Removes one local actor registration. Repeated removal is idempotent.
    def deregister(key : Cluster::ServiceKey(T), actor : ActorRef(T)) : Bool forall T
      path = local_actor_path(actor)
      removed = @mutex.synchronize do
        bind_key_type_locked(key.name, T.name)
        current = local_state_locked.registrations
        updated = current.reject do |registration|
          registration.key_name == key.name && registration.actor_path == path
        end
        next false if updated.size == current.size
        store_local_state_locked(updated)
        true
      end
      return false unless removed

      unwatch_if_unused(actor)
      @telemetry.deregistered
      publish_listings
      broadcast_local_state
      true
    end

    # Returns a defensive, deterministically ordered snapshot of reachable
    # services currently visible from this node.
    def find(key : Cluster::ServiceKey(T)) : Cluster::ReceptionistListing(T) forall T
      @mutex.synchronize { bind_key_type_locked(key.name, T.name) }
      listing_for(key, visible_registrations(key.name, T.name))
    end

    # Subscribes a local typed actor and immediately publishes the current
    # listing. Returns false when the exact subscription already exists.
    def subscribe(
      key : Cluster::ServiceKey(T),
      subscriber : ActorRef(Cluster::ReceptionistListing(T)),
    ) : Bool forall T
      @mutex.synchronize { bind_key_type_locked(key.name, T.name) }
      subscription = Cluster::ReceptionistSubscription(T).new(@system, key, subscriber)
      added = @mutex.synchronize do
        exists = @subscriptions.any? do |candidate|
          candidate.key_name == key.name &&
            candidate.message_type_name == T.name &&
            candidate.subscriber_id == subscriber.id
        end
        unless exists
          if @subscriptions.size >= Cluster::ReceptionistLimits::LOCAL_SUBSCRIPTIONS
            raise Cluster::ReceptionistConfigurationError.new(
              "local receptionist subscription capacity " \
              "#{Cluster::ReceptionistLimits::LOCAL_SUBSCRIPTIONS} exceeded"
            )
          end
          @subscriptions << subscription
          true
        else
          false
        end
      end
      return false unless added

      unless watch(subscriber)
        @mutex.synchronize { @subscriptions.delete(subscription) }
        raise Cluster::ReceptionistConfigurationError.new("cannot subscribe stopped actor")
      end
      unless publish_subscription(subscription)
        @mutex.synchronize { @subscriptions.delete(subscription) }
        unwatch_if_unused(subscriber)
        raise Cluster::ReceptionistConfigurationError.new("cannot publish to stopped subscriber")
      end
      true
    end

    # Removes an exact local typed subscription.
    def unsubscribe(
      key : Cluster::ServiceKey(T),
      subscriber : ActorRef(Cluster::ReceptionistListing(T)),
    ) : Bool forall T
      removed = @mutex.synchronize do
        index = @subscriptions.index do |candidate|
          candidate.key_name == key.name &&
            candidate.message_type_name == T.name &&
            candidate.subscriber_id == subscriber.id
        end
        if index
          @subscriptions.delete_at(index)
          true
        else
          false
        end
      end
      unwatch_if_unused(subscriber) if removed
      removed
    end

    def stats : Cluster::ReceptionistStats
      local_count, known_nodes, subscribers = @mutex.synchronize do
        {
          local_state_locked.registrations.size,
          @states.size,
          @subscriptions.size,
        }
      end
      @telemetry.snapshot(
        local_count,
        known_nodes,
        visible_registrations.size,
        subscribers
      )
    end

    # Internal watcher entrypoint.
    def local_actor_terminated(actor : ActorRefBase) : Nil
      path = actor.path
      registrations_removed = 0
      @mutex.synchronize do
        if path
          current = local_state_locked.registrations
          updated = current.reject(&.actor_path.==(path))
          registrations_removed = current.size - updated.size
          store_local_state_locked(updated) if registrations_removed > 0
        end
        @subscriptions.reject!(&.subscriber_id.==(actor.id))
      end
      return if registrations_removed == 0

      @telemetry.deregistered(registrations_removed)
      publish_listings
      broadcast_local_state
    end

    private def ensure_registration_allowed : Nil
      unless @cluster.up? && !@stopped.get
        raise Cluster::ReceptionistConfigurationError.new(
          "cluster receptionist only registers services while this member is Up"
        )
      end
    end

    private def local_actor_path(actor : ActorRefBase) : ActorPath
      path = actor.path || raise Cluster::ReceptionistConfigurationError.new(
        "receptionist registration requires a named actor path"
      )
      unless @system.local_path?(path) && path.address == @cluster.self_unique_address.address
        raise Cluster::ReceptionistConfigurationError.new(
          "receptionist only accepts actors owned by this cluster member"
        )
      end
      if path.to_s.bytesize > Cluster::ReceptionistLimits::ACTOR_PATH_BYTES
        raise Cluster::ReceptionistConfigurationError.new(
          "receptionist actor path must not exceed " \
          "#{Cluster::ReceptionistLimits::ACTOR_PATH_BYTES} bytes"
        )
      end
      path
    end

    private def watch(actor : ActorRefBase) : Bool
      watcher = @watcher || return false
      @system.context(actor.id).try(&.register_watcher(watcher)) || false
    end

    private def unwatch_if_unused(actor : ActorRefBase) : Nil
      path = actor.path
      used = @mutex.synchronize do
        registered = path && local_state_locked.registrations.any?(&.actor_path.==(path))
        subscribed = @subscriptions.any?(&.subscriber_id.==(actor.id))
        registered || subscribed
      end
      actor.send_system(Unwatch.new(@watcher.not_nil!)) unless used
    end

    private def local_state_locked : Cluster::ReceptionistNodeState
      @states[@cluster.self_unique_address] ||= Cluster::ReceptionistNodeState.new(
        @local_revision,
        [] of Cluster::ServiceRegistration
      )
    end

    private def store_local_state_locked(registrations : Array(Cluster::ServiceRegistration)) : Nil
      @local_revision += 1_i64
      registrations.sort_by! { |registration| {registration.key_name, registration.actor_path.to_s} }
      @states[@cluster.self_unique_address] = Cluster::ReceptionistNodeState.new(
        @local_revision,
        registrations
      )
    end

    private def bind_key_type_locked(key_name : String, message_type_name : String) : Nil
      if existing = @key_types[key_name]?
        unless existing == message_type_name
          raise Cluster::ReceptionistConfigurationError.new(
            "Service key #{key_name} is already bound to #{existing}, not #{message_type_name}"
          )
        end
      else
        if @key_types.size >= Cluster::ReceptionistLimits::KNOWN_SERVICE_KEYS
          raise Cluster::ReceptionistConfigurationError.new(
            "receptionist service-key capacity " \
            "#{Cluster::ReceptionistLimits::KNOWN_SERVICE_KEYS} exceeded"
          )
        end
        @key_types[key_name] = message_type_name
      end
    end

    private def visible_registrations(
      key_name : String? = nil,
      message_type_name : String? = nil,
    ) : Array(Cluster::ServiceRegistration)
      snapshot = @cluster.snapshot
      unreachable = snapshot.unreachable.to_set
      eligible = snapshot.members.each_with_object(Set(Cluster::UniqueAddress).new) do |member, owners|
        if member.status.up? && !unreachable.includes?(member.unique_address)
          owners << member.unique_address
        end
      end
      registrations = @mutex.synchronize do
        @states.compact_map do |owner, state|
          state.registrations if eligible.includes?(owner)
        end.flatten
      end
      registrations.select! do |registration|
        (key_name.nil? || registration.key_name == key_name) &&
          (message_type_name.nil? || registration.message_type_name == message_type_name)
      end
      registrations.sort_by! { |registration| registration.actor_path.to_s }
      registrations
    end

    private def listing_for(
      key : Cluster::ServiceKey(T),
      registrations : Array(Cluster::ServiceRegistration),
    ) : Cluster::ReceptionistListing(T) forall T
      Cluster::ReceptionistListingBuilder.build(@system, key, registrations)
    end

    private def publish_listings : Nil
      subscriptions = @mutex.synchronize { @subscriptions.dup }
      failed = subscriptions.reject { |subscription| publish_subscription(subscription) }
      return if failed.empty?
      @mutex.synchronize { failed.each { |subscription| @subscriptions.delete(subscription) } }
    end

    private def publish_subscription(subscription : Cluster::ReceptionistSubscriptionBase) : Bool
      registrations = visible_registrations(
        subscription.key_name,
        subscription.message_type_name
      )
      published = subscription.publish(registrations)
      @telemetry.listing_updated if published
      published
    end
  end

  class ClusterReceptionist < ExtensionId(ClusterReceptionistExtension)
    def create(system : AbstractActorSystem) : ClusterReceptionistExtension
      ClusterReceptionistExtension.new(system)
    end
  end
end
