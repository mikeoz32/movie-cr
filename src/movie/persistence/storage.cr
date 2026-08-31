module Movie
  module Persistence
    # Actor that owns one backend connection worker and executes requests
    # sequentially without exposing SQL to persistence behaviors.
    class ConnectionActor < Movie::AbstractBehavior(ConnectionMessage)
      @worker : ConnectionWorker? = nil

      def initialize(@backend : Backend, @queue_capacity : Int32 = 256)
      end

      def initialize(db_uri : String, @queue_capacity : Int32 = 256)
        @backend = SQLiteBackend.new(db_uri)
      end

      def receive(message, context)
        message.dispatch(ensure_worker, context.sender)
        Movie::Behaviors(ConnectionMessage).same
      end

      def on_signal(signal : SystemMessage)
        case signal
        when PreStart
          ensure_worker
        when PreStop
          begin
            @worker.try &.close
          rescue
          end
        end
      end

      private def ensure_worker : ConnectionWorker
        @worker ||= ConnectionWorker.new(@backend, "movie-db-#{object_id}", @queue_capacity)
      end
    end

    # Pool that routes DB messages to connection actors in round-robin order.
    class ConnectionPool < Movie::AbstractBehavior(ConnectionMessage)
      @next_index : Int32 = 0

      def initialize(@connections : Array(Movie::ActorRef(ConnectionMessage)))
      end

      def receive(message, context)
        raise "Connection pool is empty" if @connections.empty?
        connection = next_connection
        connection.tell_from(context.sender, message)
        Movie::Behaviors(ConnectionMessage).same
      end

      def self.behavior(backend : Backend, pool_size : Int32, queue_capacity : Int32 = 256)
        Movie::Behaviors(ConnectionMessage).setup do |ctx|
          size = pool_size < 1 ? 1 : pool_size
          connections = Array(Movie::ActorRef(ConnectionMessage)).new(size) do |i|
            ctx.spawn(ConnectionActor.new(backend, queue_capacity), name: "db-#{i}")
          end
          ConnectionPool.new(connections)
        end
      end

      def self.behavior(db_uri : String, pool_size : Int32, queue_capacity : Int32 = 256)
        behavior(SQLiteBackend.new(db_uri), pool_size, queue_capacity)
      end

      private def next_connection : Movie::ActorRef(ConnectionMessage)
        index = @next_index % @connections.size
        @next_index = (@next_index + 1) % @connections.size
        @connections[index]
      end
    end

    struct GetEntity
      getter persistence_id : Id
      getter spawn : Proc(Movie::ActorContext(GetEntity), Id, Movie::ActorRefBase)

      def initialize(
        @persistence_id : Id,
        @spawn : Proc(Movie::ActorContext(GetEntity), Id, Movie::ActorRefBase),
      )
      end
    end

    alias RegistryMessage = GetEntity

    # Registry actor that owns persistent entities for a specific extension.
    class EntityRegistry < Movie::AbstractBehavior(RegistryMessage)
      def initialize
        @entities = {} of Id => Movie::ActorRefBase
      end

      def receive(message, context)
        case message
        when GetEntity
          if ref = @entities[message.persistence_id]?
            Movie::Ask.reply_if_asked(context.sender, ref)
          else
            ref = message.spawn.call(context, message.persistence_id)
            if ref.path.nil?
              ref.path = context.path.try { |p| p / Persistence.entity_name(message.persistence_id) }
            end
            @entities[message.persistence_id] = ref
            Movie::Ask.reply_if_asked(context.sender, ref)
          end
        end
        Movie::Behaviors(RegistryMessage).same
      end

      def on_signal(signal : SystemMessage)
        if signal.is_a?(Terminated)
          @entities.reject! { |_id, ref| ref == signal.actor }
        end
      end
    end

    # Shared factory and lookup contract used by persistent extensions.
    class EntityProvider
      alias Factory = Proc(Movie::ActorContext(RegistryMessage), Id, Movie::ActorRefBase)

      def initialize(
        @system : Movie::AbstractActorSystem,
        @registry : Movie::ActorRef(RegistryMessage),
        @timeout : Time::Span,
      )
        @factories = {} of String => Factory
        @mutex = Mutex.new
      end

      def register(key : String, factory : Factory)
        @mutex.synchronize do
          raise "Entity factory already registered for #{key}" if @factories.has_key?(key)
          @factories[key] = factory
        end
      end

      def resolve(persistence_id : Id) : Movie::ActorRefBase
        spawn_proc = @mutex.synchronize do
          @factories[persistence_id.entity_type]? ||
            raise "Entity type not registered: #{persistence_id.entity_type}"
        end
        @system.ask(
          @registry,
          GetEntity.new(persistence_id, spawn_proc),
          Movie::ActorRefBase,
          @timeout
        ).await(@timeout)
      end
    end

    alias EventStoreMessage = AppendEvents | LoadEvents | SaveSnapshot | LoadSnapshot | DeleteSnapshot
    alias StateStoreMessage = SaveState | LoadState | DeleteState

    private module StoreForwarder
      def self.forward(
        ctx : Movie::ActorContext(U),
        pool : Movie::ActorRef(ConnectionMessage),
        timeout : Time::Span,
        message : M,
        response_type : T.class,
        &before : -> B
      ) forall U, M, T, B
        sender = ctx.sender
        begin
          before.call
          future = ctx.ask(pool, message, response_type, timeout)
        rescue error
          Movie::Ask.fail_if_asked(sender, error, response_type)
          return
        end

        future.on_complete do |result|
          case result.status
          when Movie::FutureStatus::Success
            Movie::Ask.reply_if_asked(sender, result.value.as(T))
          when Movie::FutureStatus::Failure
            Movie::Ask.fail_if_asked(sender, result.error.not_nil!, response_type)
          when Movie::FutureStatus::Cancelled
            Movie::Ask.fail_if_asked(sender, Movie::FutureCancelled.new, response_type)
          when Movie::FutureStatus::Pending
          end
        end
      end
    end

    # Actor that serializes access to the event journal.
    class EventStoreActor < Movie::AbstractBehavior(EventStoreMessage)
      @schema_ready : Bool = false
      @pool : Movie::ActorRef(ConnectionMessage)
      @timeout : Time::Span

      def initialize(@pool : Movie::ActorRef(ConnectionMessage), @timeout : Time::Span = 5.seconds)
      end

      def receive(message, context)
        case message
        when AppendEvents
          execute(context, message, WriteResult)
        when LoadEvents
          execute(context, message, Array(StoredEvent))
        when SaveSnapshot
          execute(context, message, Bool)
        when LoadSnapshot
          execute(context, message, SnapshotRecord?)
        when DeleteSnapshot
          execute(context, message, Bool)
        end
        Movie::Behaviors(EventStoreMessage).same
      end

      private def execute(ctx : Movie::ActorContext(U), message : M, response_type : T.class) forall U, M, T
        StoreForwarder.forward(ctx, @pool, @timeout, message, response_type) { ensure_schema(ctx) }
      end

      private def ensure_schema(ctx : Movie::ActorContext(U)) forall U
        return if @schema_ready
        ctx.ask(@pool, EnsureEventStore.new, Bool, @timeout).await(@timeout)
        @schema_ready = true
      end
    end

    # Actor that serializes access to durable state storage.
    class StateStoreActor < Movie::AbstractBehavior(StateStoreMessage)
      @schema_ready : Bool = false
      @pool : Movie::ActorRef(ConnectionMessage)
      @timeout : Time::Span

      def initialize(@pool : Movie::ActorRef(ConnectionMessage), @timeout : Time::Span = 5.seconds)
      end

      def receive(message, context)
        case message
        when SaveState
          execute(context, message, WriteResult)
        when LoadState
          execute(context, message, StateRecord?)
        when DeleteState
          execute(context, message, WriteResult)
        end
        Movie::Behaviors(StateStoreMessage).same
      end

      private def execute(ctx : Movie::ActorContext(U), message : M, response_type : T.class) forall U, M, T
        StoreForwarder.forward(ctx, @pool, @timeout, message, response_type) { ensure_schema(ctx) }
      end

      private def ensure_schema(ctx : Movie::ActorContext(U)) forall U
        return if @schema_ready
        ctx.ask(@pool, EnsureStateStore.new, Bool, @timeout).await(@timeout)
        @schema_ready = true
      end
    end

    class EventStoreClient
      def initialize(@ref : Movie::ActorRef(EventStoreMessage), @timeout : Time::Span = 5.seconds)
      end

      def append(
        ctx : Movie::ActorContext(U),
        persistence_id : String,
        expected_revision : Int64,
        operation_id : OperationId,
        events : Array(SerializedEvent),
      ) : WriteResult forall U
        message = AppendEvents.new(persistence_id, expected_revision, operation_id, events)
        ctx.ask(@ref, message, WriteResult, @timeout).await(@timeout)
      end

      def read(ctx : Movie::ActorContext(U), persistence_id : String, after_sequence_nr : Int64 = 0_i64) : Array(StoredEvent) forall U
        ctx.ask(@ref, LoadEvents.new(persistence_id, after_sequence_nr), Array(StoredEvent), @timeout).await(@timeout)
      end

      def save_snapshot(ctx : Movie::ActorContext(U), persistence_id : String, snapshot : SnapshotRecord) : Bool forall U
        ctx.ask(@ref, SaveSnapshot.new(persistence_id, snapshot), Bool, @timeout).await(@timeout)
      end

      def load_snapshot(ctx : Movie::ActorContext(U), persistence_id : String) : SnapshotRecord? forall U
        ctx.ask(@ref, LoadSnapshot.new(persistence_id), SnapshotRecord?, @timeout).await(@timeout)
      end

      def delete_snapshot(ctx : Movie::ActorContext(U), persistence_id : String) : Bool forall U
        ctx.ask(@ref, DeleteSnapshot.new(persistence_id), Bool, @timeout).await(@timeout)
      end
    end

    class StateStoreClient
      def initialize(@ref : Movie::ActorRef(StateStoreMessage), @timeout : Time::Span = 5.seconds)
      end

      def save(
        ctx : Movie::ActorContext(U),
        persistence_id : String,
        expected_revision : Int64,
        operation_id : OperationId,
        manifest : String,
        payload : String,
      ) : WriteResult forall U
        message = SaveState.new(persistence_id, expected_revision, operation_id, manifest, payload)
        ctx.ask(@ref, message, WriteResult, @timeout).await(@timeout)
      end

      def load(ctx : Movie::ActorContext(U), persistence_id : String) : StateRecord? forall U
        ctx.ask(@ref, LoadState.new(persistence_id), StateRecord?, @timeout).await(@timeout)
      end

      def delete(
        ctx : Movie::ActorContext(U),
        persistence_id : String,
        expected_revision : Int64,
        operation_id : OperationId,
      ) : WriteResult forall U
        message = DeleteState.new(persistence_id, expected_revision, operation_id)
        ctx.ask(@ref, message, WriteResult, @timeout).await(@timeout)
      end
    end
  end
end
