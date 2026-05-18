require "json"
require "sqlite3"
require "./extension_id"
require "./system"
require "./behavior"
require "./context"
require "./ask"

module Movie
  module Persistence
    # Persistence id that combines entity type and entity id.
    record Id, entity_type : String, entity_id : String do
      def persistence_id : String
        "#{@entity_type}:#{@entity_id}"
      end
    end

    def self.id(type : T.class, entity_id : String) : Id forall T
      Id.new(type.name, entity_id)
    end

    def self.entity_name(id : Id) : String
      type = id.entity_type.downcase.gsub(/[^a-z0-9]+/, "-")
      ent = id.entity_id.downcase.gsub(/[^a-z0-9]+/, "-")
      "entity-#{type}-#{ent}"
    end

    # Database connection messages
    alias DbArgs = Array(DB::Any)

    struct DbExec
      getter sql : String
      getter args : DbArgs

      def initialize(@sql : String, @args : DbArgs = [] of DB::Any)
      end
    end

    struct DbExecLastId
      getter sql : String
      getter args : DbArgs

      def initialize(@sql : String, @args : DbArgs = [] of DB::Any)
      end
    end

    struct DbQueryString
      getter sql : String
      getter args : DbArgs

      def initialize(@sql : String, @args : DbArgs = [] of DB::Any)
      end
    end

    struct DbQueryStrings
      getter sql : String
      getter args : DbArgs

      def initialize(@sql : String, @args : DbArgs = [] of DB::Any)
      end
    end

    alias ConnectionMessage = DbExec | DbExecLastId | DbQueryString | DbQueryStrings

    # Actor that owns a single DB connection and executes queries sequentially.
    class ConnectionActor < Movie::AbstractBehavior(ConnectionMessage)
      @conn : DB::Connection? = nil

      def initialize(@db_uri : String)
      end

      def receive(message, ctx)
        case message
        when DbExec
          exec(message)
          Movie::Ask.reply_if_asked(ctx.sender, true)
        when DbExecLastId
          id = exec_last_id(message)
          Movie::Ask.reply_if_asked(ctx.sender, id)
        when DbQueryString
          value = query_string(message)
          Movie::Ask.reply_if_asked(ctx.sender, value)
        when DbQueryStrings
          values = query_strings(message)
          Movie::Ask.reply_if_asked(ctx.sender, values)
        end
        Movie::Behaviors(ConnectionMessage).same
      end

      def on_signal(signal : SystemMessage)
        case signal
        when PreStart
          ensure_connection
        when PreStop
          begin
            @conn.try &.close
          rescue
          end
        end
      end

      private def exec(message : DbExec)
        connection.exec(message.sql, args: message.args)
      end

      private def exec_last_id(message : DbExecLastId) : Int64
        result = connection.exec(message.sql, args: message.args)
        result.last_insert_id
      end

      private def query_string(message : DbQueryString) : String?
        connection.query_one?(message.sql, args: message.args, as: String)
      end

      private def query_strings(message : DbQueryStrings) : Array(String)
        values = [] of String
        connection.query_each(message.sql, args: message.args) do |rs|
          values << rs.read(String)
        end
        values
      end

      private def connection : DB::Connection
        ensure_connection
        @conn.not_nil!
      end

      private def ensure_connection
        return if @conn
        @conn = DB.connect(@db_uri)
        if @db_uri.starts_with?("sqlite3:")
          # Reduce transient write-lock failures when multiple actors share SQLite.
          @conn.not_nil!.exec("PRAGMA busy_timeout = 5000")
          @conn.not_nil!.exec("PRAGMA journal_mode = WAL")
        end
      end
    end

    # Pool that routes DB messages to connection actors in round-robin order.
    class ConnectionPool < Movie::AbstractBehavior(ConnectionMessage)
      @next_index : Int32 = 0

      def initialize(@connections : Array(Movie::ActorRef(ConnectionMessage)))
      end

      def receive(message, ctx)
        raise "Connection pool is empty" if @connections.empty?
        connection = next_connection
        connection.tell_from(ctx.sender, message)
        Movie::Behaviors(ConnectionMessage).same
      end

      def self.behavior(db_uri : String, pool_size : Int32)
        Movie::Behaviors(ConnectionMessage).setup do |ctx|
          size = pool_size < 1 ? 1 : pool_size
          connections = Array(Movie::ActorRef(ConnectionMessage)).new(size) do |i|
            ctx.spawn(ConnectionActor.new(db_uri), name: "db-#{i}")
          end
          ConnectionPool.new(connections)
        end
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
        @spawn : Proc(Movie::ActorContext(GetEntity), Id, Movie::ActorRefBase)
      )
      end
    end

    alias RegistryMessage = GetEntity

    # Registry actor that owns persistent entities for a specific extension.
    class EntityRegistry < Movie::AbstractBehavior(RegistryMessage)
      def initialize
        @entities = {} of Id => Movie::ActorRefBase
      end

      def receive(message, ctx)
        case message
        when GetEntity
          if ref = @entities[message.persistence_id]?
            Movie::Ask.reply_if_asked(ctx.sender, ref)
          else
            ref = message.spawn.call(ctx, message.persistence_id)
            if ref.path.nil?
              ref.path = ctx.path.try { |p| p / Persistence.entity_name(message.persistence_id) }
            end
            @entities[message.persistence_id] = ref
            Movie::Ask.reply_if_asked(ctx.sender, ref)
          end
        end
        Movie::Behaviors(RegistryMessage).same
      end
    end

    # Event store messages
    struct AppendEvent
      getter persistence_id : String
      getter payload : String

      def initialize(@persistence_id : String, @payload : String)
      end
    end

    struct LoadEvents
      getter persistence_id : String

      def initialize(@persistence_id : String)
      end
    end

    alias EventStoreMessage = AppendEvent | LoadEvents

    # Durable state store messages
    struct SaveState
      getter persistence_id : String
      getter payload : String

      def initialize(@persistence_id : String, @payload : String)
      end
    end

    struct LoadState
      getter persistence_id : String

      def initialize(@persistence_id : String)
      end
    end

    struct DeleteState
      getter persistence_id : String

      def initialize(@persistence_id : String)
      end
    end

    alias StateStoreMessage = SaveState | LoadState | DeleteState

    # Actor that serializes access to the event journal.
    class EventStoreActor < Movie::AbstractBehavior(EventStoreMessage)
      @schema_ready : Bool = false
      @pool : Movie::ActorRef(ConnectionMessage)
      @timeout : Time::Span

      def initialize(@pool : Movie::ActorRef(ConnectionMessage), @timeout : Time::Span = 5.seconds)
      end

      def receive(message, ctx)
        ensure_schema(ctx)
        case message
        when AppendEvent
          seq = append_event(ctx, message.persistence_id, message.payload)
          Movie::Ask.reply_if_asked(ctx.sender, seq)
        when LoadEvents
          events = load_events(ctx, message.persistence_id)
          Movie::Ask.reply_if_asked(ctx.sender, events)
        end
        Movie::Behaviors(EventStoreMessage).same
      end

      private def append_event(ctx : Movie::ActorContext(U), persistence_id : String, payload : String) : Int64 forall U
        ctx.ask(
          @pool,
          DbExecLastId.new(
            "INSERT INTO event_journal (persistence_id, payload) VALUES (?, ?)",
            [persistence_id, payload] of DB::Any
          ),
          Int64,
          @timeout
        ).await(@timeout)
      end

      private def load_events(ctx : Movie::ActorContext(U), persistence_id : String) : Array(String) forall U
        ctx.ask(
          @pool,
          DbQueryStrings.new(
            "SELECT payload FROM event_journal WHERE persistence_id = ? ORDER BY id ASC",
            [persistence_id] of DB::Any
          ),
          Array(String),
          @timeout
        ).await(@timeout)
      end

      private def ensure_schema(ctx : Movie::ActorContext(U)) forall U
        return if @schema_ready
        ctx.ask(@pool, DbExec.new(<<-SQL), Bool, @timeout).await(@timeout)
          CREATE TABLE IF NOT EXISTS event_journal (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            persistence_id TEXT NOT NULL,
            payload TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
          );
        SQL
        ctx.ask(@pool, DbExec.new("CREATE INDEX IF NOT EXISTS idx_event_journal_pid ON event_journal(persistence_id)"), Bool, @timeout).await(@timeout)
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

      def receive(message, ctx)
        ensure_schema(ctx)
        case message
        when SaveState
          save_state(ctx, message.persistence_id, message.payload)
          Movie::Ask.reply_if_asked(ctx.sender, true)
        when LoadState
          payload = load_state(ctx, message.persistence_id)
          Movie::Ask.reply_if_asked(ctx.sender, payload)
        when DeleteState
          delete_state(ctx, message.persistence_id)
          Movie::Ask.reply_if_asked(ctx.sender, true)
        end
        Movie::Behaviors(StateStoreMessage).same
      end

      private def save_state(ctx : Movie::ActorContext(U), persistence_id : String, payload : String) forall U
        ctx.ask(
          @pool,
          DbExec.new(
            "INSERT INTO durable_state (persistence_id, payload, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP) " +
            "ON CONFLICT(persistence_id) DO UPDATE SET payload = excluded.payload, updated_at = CURRENT_TIMESTAMP",
            [persistence_id, payload] of DB::Any
          ),
          Bool,
          @timeout
        ).await(@timeout)
      end

      private def load_state(ctx : Movie::ActorContext(U), persistence_id : String) : String? forall U
        ctx.ask(
          @pool,
          DbQueryString.new(
            "SELECT payload FROM durable_state WHERE persistence_id = ?",
            [persistence_id] of DB::Any
          ),
          String?,
          @timeout
        ).await(@timeout)
      end

      private def delete_state(ctx : Movie::ActorContext(U), persistence_id : String) forall U
        ctx.ask(
          @pool,
          DbExec.new(
            "DELETE FROM durable_state WHERE persistence_id = ?",
            [persistence_id] of DB::Any
          ),
          Bool,
          @timeout
        ).await(@timeout)
      end

      private def ensure_schema(ctx : Movie::ActorContext(U)) forall U
        return if @schema_ready
        ctx.ask(@pool, DbExec.new(<<-SQL), Bool, @timeout).await(@timeout)
          CREATE TABLE IF NOT EXISTS durable_state (
            persistence_id TEXT PRIMARY KEY,
            payload TEXT NOT NULL,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
          );
        SQL
        @schema_ready = true
      end
    end

    class EventStoreClient
      def initialize(@ref : Movie::ActorRef(EventStoreMessage), @timeout : Time::Span = 5.seconds)
      end

      def append(ctx : Movie::ActorContext(U), persistence_id : String, payload : String) : Int64 forall U
        ctx.ask(@ref, AppendEvent.new(persistence_id, payload), Int64, @timeout).await(@timeout)
      end

      def read(ctx : Movie::ActorContext(U), persistence_id : String) : Array(String) forall U
        ctx.ask(@ref, LoadEvents.new(persistence_id), Array(String), @timeout).await(@timeout)
      end
    end

    class StateStoreClient
      def initialize(@ref : Movie::ActorRef(StateStoreMessage), @timeout : Time::Span = 5.seconds)
      end

      def save(ctx : Movie::ActorContext(U), persistence_id : String, payload : String) : Bool forall U
        ctx.ask(@ref, SaveState.new(persistence_id, payload), Bool, @timeout).await(@timeout)
      end

      def load(ctx : Movie::ActorContext(U), persistence_id : String) : String? forall U
        ctx.ask(@ref, LoadState.new(persistence_id), String?, @timeout).await(@timeout)
      end

      def delete(ctx : Movie::ActorContext(U), persistence_id : String) : Bool forall U
        ctx.ask(@ref, DeleteState.new(persistence_id), Bool, @timeout).await(@timeout)
      end
    end
  end

  # Database extension that manages connection pool actors.
  class DatabaseExtension < Extension
    getter pool : ActorRef(Persistence::ConnectionMessage)

    def initialize(@system : AbstractActorSystem, @db_uri : String, @pool_size : Int32)
      @pool = @system.spawn(Persistence::ConnectionPool.behavior(@db_uri, @pool_size))
    end

    def stop
      @pool.send_system(Movie::STOP)
    end
  end

  class Database < ExtensionId(DatabaseExtension)
    def create(system : AbstractActorSystem) : DatabaseExtension
      cfg = system.config
      path = cfg.get_string("movie.persistence.db_path", "data/movie_persistence.sqlite3")
      pool_size = cfg.get_int("movie.persistence.pool_size", 1)
      DatabaseExtension.new(system, "sqlite3:#{path}", pool_size)
    end
  end

  abstract class EventSourcedBehavior(C, E, S) < AbstractBehavior(C)
    @state : S
    @recovered : Bool = false

    def initialize(@persistence_id : String, @store : Movie::Persistence::EventStoreClient)
      @state = empty_state
    end

    def receive(message : C, ctx : ActorContext(C))
      recover(ctx) unless @recovered
      events = handle_command(@state, message, ctx)
      events.each do |event|
        @store.append(ctx, @persistence_id, event.to_json)
        @state = apply_event(@state, event)
      end
      after_command(@state, message, events, ctx)
      Behaviors(C).same
    end

    protected abstract def empty_state : S
    protected abstract def apply_event(state : S, event : E) : S
    protected abstract def handle_command(state : S, command : C, ctx : ActorContext(C)) : Array(E)

    protected def after_command(state : S, command : C, events : Array(E), ctx : ActorContext(C))
    end

    private def recover(ctx : ActorContext(C))
      return if @recovered
      events = @store.read(ctx, @persistence_id)
      events.each do |payload|
        event = E.from_json(payload)
        @state = apply_event(@state, event)
      end
      @recovered = true
    end
  end

  abstract class DurableStateBehavior(C, S) < AbstractBehavior(C)
    @state : S
    @loaded : Bool = false

    def initialize(@persistence_id : String, @store : Movie::Persistence::StateStoreClient)
      @state = empty_state
    end

    def receive(message : C, ctx : ActorContext(C))
      recover(ctx) unless @loaded
      new_state = handle_command(@state, message, ctx)
      if new_state
        @store.save(ctx, @persistence_id, new_state.to_json)
        @state = new_state
      end
      after_command(@state, message, ctx)
      Behaviors(C).same
    end

    protected abstract def empty_state : S
    protected abstract def handle_command(state : S, command : C, ctx : ActorContext(C)) : S?

    protected def after_command(state : S, command : C, ctx : ActorContext(C))
    end

    private def recover(ctx : ActorContext(C))
      return if @loaded
      if payload = @store.load(ctx, @persistence_id)
        @state = S.from_json(payload)
      else
        @state = empty_state
      end
      @loaded = true
    end
  end

  class EventSourcingExtension < Extension
    alias EventFactory = Proc(
      Movie::ActorContext(Persistence::RegistryMessage),
      Persistence::Id,
      Movie::ActorRefBase
    )

    def initialize(
      @system : AbstractActorSystem,
      @store_ref : Movie::ActorRef(Persistence::EventStoreMessage),
      @store : Persistence::EventStoreClient,
      @registry : Movie::ActorRef(Persistence::RegistryMessage),
      @timeout : Time::Span = 5.seconds
    )
      @factories = {} of String => EventFactory
    end

    def stop
      @registry.send_system(Movie::STOP)
      @store_ref.send_system(Movie::STOP)
    end

    def register_entity(key : String, &factory : Persistence::Id, Persistence::EventStoreClient -> AbstractBehavior(T)) forall T
      raise "Entity factory already registered for #{key}" if @factories.has_key?(key)
      @factories[key] = ->(ctx : Movie::ActorContext(Persistence::RegistryMessage), id : Persistence::Id) do
        behavior = factory.call(id, @store)
        ctx.spawn(behavior).as(Movie::ActorRefBase)
      end
    end

    def register_entity(type : T.class, &factory : Persistence::Id, Persistence::EventStoreClient -> AbstractBehavior(U)) forall T, U
      register_entity(type.name, &factory)
    end

      def get_entity_ref(persistence_id : Persistence::Id) : Movie::ActorRefBase
        spawn_proc = @factories[persistence_id.entity_type]? ||
          raise "Entity type not registered: #{persistence_id.entity_type}"

        @system.ask(
          @registry,
          Persistence::GetEntity.new(persistence_id, spawn_proc),
          Movie::ActorRefBase,
          @timeout
        ).await(@timeout)
      end

      def get_entity_ref_as(type : T.class, persistence_id : Persistence::Id) : ActorRef(T) forall T
        get_entity_ref(persistence_id).as(ActorRef(T))
      end
    end

  class DurableStateExtension < Extension
    alias StateFactory = Proc(
      Movie::ActorContext(Persistence::RegistryMessage),
      Persistence::Id,
      Movie::ActorRefBase
    )

    def initialize(
      @system : AbstractActorSystem,
      @store_ref : Movie::ActorRef(Persistence::StateStoreMessage),
      @store : Persistence::StateStoreClient,
      @registry : Movie::ActorRef(Persistence::RegistryMessage),
      @timeout : Time::Span = 5.seconds
    )
      @factories = {} of String => StateFactory
    end

    def stop
      @registry.send_system(Movie::STOP)
      @store_ref.send_system(Movie::STOP)
    end

    def register_entity(key : String, &factory : Persistence::Id, Persistence::StateStoreClient -> AbstractBehavior(T)) forall T
      raise "Entity factory already registered for #{key}" if @factories.has_key?(key)
      @factories[key] = ->(ctx : Movie::ActorContext(Persistence::RegistryMessage), id : Persistence::Id) do
        behavior = factory.call(id, @store)
        ctx.spawn(behavior).as(Movie::ActorRefBase)
      end
    end

    def register_entity(type : T.class, &factory : Persistence::Id, Persistence::StateStoreClient -> AbstractBehavior(U)) forall T, U
      register_entity(type.name, &factory)
    end

      def get_entity_ref(persistence_id : Persistence::Id) : Movie::ActorRefBase
        spawn_proc = @factories[persistence_id.entity_type]? ||
          raise "Entity type not registered: #{persistence_id.entity_type}"

        @system.ask(
          @registry,
          Persistence::GetEntity.new(persistence_id, spawn_proc),
          Movie::ActorRefBase,
          @timeout
        ).await(@timeout)
      end

      def get_entity_ref_as(type : T.class, persistence_id : Persistence::Id) : ActorRef(T) forall T
        get_entity_ref(persistence_id).as(ActorRef(T))
      end
    end

  class EventSourcing < ExtensionId(EventSourcingExtension)
    def create(system : AbstractActorSystem) : EventSourcingExtension
      db_ext = Movie::Database.get(system)
      store_ref = system.spawn(Persistence::EventStoreActor.new(db_ext.pool))
      store = Persistence::EventStoreClient.new(store_ref)
      registry = system.spawn(Persistence::EntityRegistry.new)
      EventSourcingExtension.new(system, store_ref, store, registry)
    end
  end

  class DurableState < ExtensionId(DurableStateExtension)
    def create(system : AbstractActorSystem) : DurableStateExtension
      db_ext = Movie::Database.get(system)
      store_ref = system.spawn(Persistence::StateStoreActor.new(db_ext.pool))
      store = Persistence::StateStoreClient.new(store_ref)
      registry = system.spawn(Persistence::EntityRegistry.new)
      DurableStateExtension.new(system, store_ref, store, registry)
    end
  end
end
