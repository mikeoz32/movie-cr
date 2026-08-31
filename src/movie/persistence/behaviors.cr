module Movie
  # Database extension that manages connection pool actors.
  class DatabaseExtension < Extension
    getter pool : ActorRef(Persistence::ConnectionMessage)
    getter operation_timeout : Time::Span
    getter backend_name : String

    def initialize(
      @system : AbstractActorSystem,
      backend : Persistence::Backend,
      @pool_size : Int32,
      queue_capacity : Int32,
      @operation_timeout : Time::Span,
    )
      @backend_name = backend.name
      @pool = @system.spawn(Persistence::ConnectionPool.behavior(backend, @pool_size, queue_capacity))
    end

    def stop
      @pool.send_system(Movie::STOP)
    end
  end

  class Database < ExtensionId(DatabaseExtension)
    def create(system : AbstractActorSystem) : DatabaseExtension
      cfg = system.config
      backend_name = cfg.get_string(ActorSystemConfig::PERSISTENCE_BACKEND, "sqlite")
      pool_size = cfg.get_int(ActorSystemConfig::PERSISTENCE_POOL_SIZE, 1)
      queue_capacity = cfg.get_int(ActorSystemConfig::PERSISTENCE_IO_QUEUE_CAPACITY, 256)
      operation_timeout = cfg.get_duration(ActorSystemConfig::PERSISTENCE_OPERATION_TIMEOUT, 5.seconds)
      backend = Persistence::BackendRegistry.build(backend_name, cfg)
      DatabaseExtension.new(system, backend, pool_size, queue_capacity, operation_timeout)
    end
  end

  class EventEffect(E, S)
    getter events : Array(E)
    getter operation_id : Persistence::OperationId?
    getter callbacks : Array(Proc(S, Nil))
    getter? stop

    def initialize(
      @events : Array(E) = [] of E,
      @operation_id : Persistence::OperationId? = nil,
      @stop : Bool = false,
    )
      @callbacks = [] of Proc(S, Nil)
    end

    def then_run(&callback : S ->) : self
      @callbacks << callback
      self
    end
  end

  enum DurableAction
    None
    Persist
    Delete
  end

  class DurableEffect(S)
    getter action : DurableAction
    getter state : S?
    getter operation_id : Persistence::OperationId?
    getter callbacks : Array(Proc(S, Nil))
    getter? stop

    def initialize(
      @action : DurableAction,
      @state : S? = nil,
      @operation_id : Persistence::OperationId? = nil,
      @stop : Bool = false,
    )
      @callbacks = [] of Proc(S, Nil)
    end

    def then_run(&callback : S ->) : self
      @callbacks << callback
      self
    end
  end

  abstract class EventSourcedBehavior(C, E, S) < AbstractBehavior(C)
    @state : S
    @recovered : Bool = false
    @sequence_nr : Int64 = 0_i64

    def initialize(@persistence_id : String, @store : Movie::Persistence::EventStoreClient)
      @state = empty_state
    end

    def receive(message : C, ctx : ActorContext(C))
      recover(ctx) unless @recovered
      command_state = copy_state(@state)
      effect = handle_command(command_state, message, ctx)
      previous_sequence_nr = @sequence_nr
      next_state = command_state
      effect.events.each do |event|
        next_state = apply_event(next_state, event)
      end

      unless effect.events.empty?
        serialized = effect.events.map do |event|
          Movie::Persistence::SerializedEvent.new(event_manifest(event), serialize_event(event))
        end
        begin
          result = @store.append(
            ctx,
            @persistence_id,
            @sequence_nr,
            effect.operation_id.not_nil!,
            serialized
          )
          @sequence_nr = result.revision
          if result.duplicate
            reset_recovery_state
            recover(ctx)
          else
            @state = next_state
            save_snapshot_if_due(ctx, previous_sequence_nr)
          end
        rescue error
          on_persist_failure(error)
          raise error
        end
      end

      effect.callbacks.each &.call(@state)
      effect.stop? ? Behaviors(C).stopped : Behaviors(C).same
    end

    def on_signal(signal : SystemMessage)
      if signal.is_a?(PreRestart)
        reset_recovery_state
      end
      on_persistence_signal(signal)
    end

    protected abstract def empty_state : S
    protected abstract def apply_event(state : S, event : E) : S
    protected abstract def handle_command(state : S, command : C, ctx : ActorContext(C)) : EventEffect(E, S)

    protected def persist(event : E, operation_id : Persistence::OperationId) : EventEffect(E, S)
      EventEffect(E, S).new([event], operation_id)
    end

    protected def persist_all(
      events : Enumerable(E),
      operation_id : Persistence::OperationId,
    ) : EventEffect(E, S)
      EventEffect(E, S).new(events.to_a, operation_id)
    end

    protected def none : EventEffect(E, S)
      EventEffect(E, S).new
    end

    protected def stop : EventEffect(E, S)
      EventEffect(E, S).new(stop: true)
    end

    protected def event_manifest(event : E) : String
      event.class.name
    end

    protected def deserialize_event(manifest : String, payload : String) : E
      E.from_json(payload)
    end

    protected def snapshot_manifest(state : S) : String
      state.class.name
    end

    protected def deserialize_snapshot(manifest : String, payload : String) : S
      S.from_json(payload)
    end

    protected def snapshot_every : Int32?
      nil
    end

    protected def on_recovery_completed(state : S, sequence_nr : Int64)
    end

    protected def on_recovery_failure(error : Exception)
    end

    protected def on_persist_failure(error : Exception)
    end

    protected def on_persistence_signal(signal : SystemMessage)
    end

    private def serialize_event(event : E) : String
      String.build { |json| event.to_json(json) }
    end

    private def copy_state(state : S) : S
      payload = String.build { |json| state.to_json(json) }
      deserialize_snapshot(snapshot_manifest(state), payload)
    end

    private def recover(ctx : ActorContext(C))
      return if @recovered
      state = copy_state(empty_state)
      sequence_nr = 0_i64

      if snapshot = @store.load_snapshot(ctx, @persistence_id)
        state = deserialize_snapshot(snapshot.manifest, snapshot.payload)
        sequence_nr = snapshot.sequence_nr
      end

      @store.read(ctx, @persistence_id, sequence_nr).each do |stored|
        expected = sequence_nr + 1
        unless stored.sequence_nr == expected
          raise "Journal gap for #{@persistence_id}: expected sequence #{expected}, got #{stored.sequence_nr}"
        end
        event = deserialize_event(stored.manifest, stored.payload)
        state = apply_event(state, event)
        sequence_nr = stored.sequence_nr
      end

      @state = state
      @sequence_nr = sequence_nr
      @recovered = true
      on_recovery_completed(@state, @sequence_nr)
    rescue error
      reset_recovery_state
      on_recovery_failure(error)
      raise error
    end

    private def save_snapshot_if_due(ctx : ActorContext(C), previous_sequence_nr : Int64)
      interval = snapshot_every
      return unless interval && interval > 0
      return unless previous_sequence_nr // interval < @sequence_nr // interval

      payload = String.build { |json| @state.to_json(json) }
      snapshot = Movie::Persistence::SnapshotRecord.new(@sequence_nr, snapshot_manifest(@state), payload)
      @store.save_snapshot(ctx, @persistence_id, snapshot)
    end

    private def reset_recovery_state
      @state = empty_state
      @sequence_nr = 0_i64
      @recovered = false
    end
  end

  abstract class DurableStateBehavior(C, S) < AbstractBehavior(C)
    @state : S
    @loaded : Bool = false
    @revision : Int64 = 0_i64

    def initialize(@persistence_id : String, @store : Movie::Persistence::StateStoreClient)
      @state = empty_state
    end

    def receive(message : C, ctx : ActorContext(C))
      recover(ctx) unless @loaded
      effect = handle_command(copy_state(@state), message, ctx)
      begin
        case effect.action
        when DurableAction::Persist
          next_state = effect.state.not_nil!
          payload = String.build { |json| next_state.to_json(json) }
          result = @store.save(
            ctx,
            @persistence_id,
            @revision,
            effect.operation_id.not_nil!,
            state_manifest(next_state),
            payload
          )
          @revision = result.revision
          if result.duplicate
            reset_recovery_state
            recover(ctx)
          else
            @state = next_state
          end
        when DurableAction::Delete
          result = @store.delete(ctx, @persistence_id, @revision, effect.operation_id.not_nil!)
          @revision = result.revision
          if result.duplicate
            reset_recovery_state
            recover(ctx)
          else
            @state = empty_state
          end
        when DurableAction::None
        end
      rescue error
        on_persist_failure(error)
        raise error
      end

      effect.callbacks.each &.call(@state)
      effect.stop? ? Behaviors(C).stopped : Behaviors(C).same
    end

    def on_signal(signal : SystemMessage)
      if signal.is_a?(PreRestart)
        reset_recovery_state
      end
      on_persistence_signal(signal)
    end

    protected abstract def empty_state : S
    protected abstract def handle_command(state : S, command : C, ctx : ActorContext(C)) : DurableEffect(S)

    protected def persist(state : S, operation_id : Persistence::OperationId) : DurableEffect(S)
      DurableEffect(S).new(DurableAction::Persist, state, operation_id)
    end

    protected def delete(operation_id : Persistence::OperationId) : DurableEffect(S)
      DurableEffect(S).new(DurableAction::Delete, operation_id: operation_id)
    end

    protected def none : DurableEffect(S)
      DurableEffect(S).new(DurableAction::None)
    end

    protected def stop : DurableEffect(S)
      DurableEffect(S).new(DurableAction::None, stop: true)
    end

    protected def state_manifest(state : S) : String
      state.class.name
    end

    protected def deserialize_state(manifest : String, payload : String) : S
      S.from_json(payload)
    end

    protected def on_recovery_completed(state : S, revision : Int64)
    end

    protected def on_recovery_failure(error : Exception)
    end

    protected def on_persist_failure(error : Exception)
    end

    protected def on_persistence_signal(signal : SystemMessage)
    end

    private def copy_state(state : S) : S
      payload = String.build { |json| state.to_json(json) }
      deserialize_state(state_manifest(state), payload)
    end

    private def recover(ctx : ActorContext(C))
      return if @loaded
      if stored = @store.load(ctx, @persistence_id)
        @revision = stored.revision
        if stored.deleted
          @state = empty_state
        else
          @state = deserialize_state(stored.manifest, stored.payload.not_nil!)
        end
      else
        @state = empty_state
        @revision = 0_i64
      end
      @loaded = true
      on_recovery_completed(@state, @revision)
    rescue error
      reset_recovery_state
      on_recovery_failure(error)
      raise error
    end

    private def reset_recovery_state
      @state = empty_state
      @revision = 0_i64
      @loaded = false
    end
  end

  abstract class PersistentEntityExtension(SM, S) < Extension
    def initialize(
      @system : AbstractActorSystem,
      @store_ref : Movie::ActorRef(SM),
      @store : S,
      @registry : Movie::ActorRef(Persistence::RegistryMessage),
      @timeout : Time::Span = 5.seconds,
    )
      @entities = Persistence::EntityProvider.new(@system, @registry, @timeout)
    end

    def stop
      @registry.send_system(Movie::STOP)
      @store_ref.send_system(Movie::STOP)
    end

    def register_entity(
      entity_type : E.class,
      message_type : C.class,
      &factory : Persistence::Id, S -> AbstractBehavior(C)
    ) : Persistence::EntityType(C) forall E, C
      key = entity_type.name
      spawn = ->(ctx : Movie::ActorContext(Persistence::RegistryMessage), id : Persistence::Id) do
        behavior = factory.call(id, @store)
        ctx.spawn(behavior).as(Movie::ActorRefBase)
      end
      @entities.register(key, spawn)
      Persistence::EntityType(C).new(key)
    end

    def get_entity_ref(persistence_id : Persistence::EntityId(T)) : ActorRef(T) forall T
      @entities.resolve(persistence_id.value).as(ActorRef(T))
    end
  end

  class EventSourcingExtension < PersistentEntityExtension(
    Persistence::EventStoreMessage,
    Persistence::EventStoreClient,
  )
    # Keep a concrete virtual entrypoint so Extension#stop dispatch compiles for
    # this specialization on every supported Crystal version.
    def stop
      super
    end
  end

  class DurableStateExtension < PersistentEntityExtension(
    Persistence::StateStoreMessage,
    Persistence::StateStoreClient,
  )
    # See EventSourcingExtension#stop.
    def stop
      super
    end
  end

  class EventSourcing < ExtensionId(EventSourcingExtension)
    def create(system : AbstractActorSystem) : EventSourcingExtension
      db_ext = Movie::Database.get(system)
      store_ref = system.spawn(Persistence::EventStoreActor.new(db_ext.pool, db_ext.operation_timeout))
      store = Persistence::EventStoreClient.new(store_ref, db_ext.operation_timeout)
      registry = system.spawn(Persistence::EntityRegistry.new)
      EventSourcingExtension.new(system, store_ref, store, registry)
    end
  end

  class DurableState < ExtensionId(DurableStateExtension)
    def create(system : AbstractActorSystem) : DurableStateExtension
      db_ext = Movie::Database.get(system)
      store_ref = system.spawn(Persistence::StateStoreActor.new(db_ext.pool, db_ext.operation_timeout))
      store = Persistence::StateStoreClient.new(store_ref, db_ext.operation_timeout)
      registry = system.spawn(Persistence::EntityRegistry.new)
      DurableStateExtension.new(system, store_ref, store, registry)
    end
  end
end
