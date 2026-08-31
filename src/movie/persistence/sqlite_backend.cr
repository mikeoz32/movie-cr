module Movie
  module Persistence
    class SQLiteBackendConnection < SqlBackendConnection
      @@schema_mutex = Mutex.new

      protected def configure_schema(connection : DB::Connection) : Nil
        connection.exec("PRAGMA journal_mode = WAL")
      end

      protected def table_exists?(connection : DB::Connection, name : String) : Bool
        !connection.query_one?(
          "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
          args: [name] of DB::Any,
          as: String
        ).nil?
      end

      protected def table_columns(connection : DB::Connection, name : String) : Array(String)
        columns = [] of String
        connection.query_each("PRAGMA table_info(#{name})") do |row|
          row.read(Int64)
          columns << row.read(String)
        end
        columns
      end

      protected def database_error?(error : Exception) : Bool
        error.is_a?(SQLite3::Exception)
      end

      protected def with_schema_lock(connection : DB::Connection, &operation : -> T) : T forall T
        @@schema_mutex.synchronize { yield }
      end

      protected def create_event_feed_table(connection : DB::Connection) : Nil
        connection.exec(<<-SQL)
          CREATE TABLE IF NOT EXISTS event_feed (
            event_offset INTEGER PRIMARY KEY AUTOINCREMENT,
            persistence_id TEXT NOT NULL,
            sequence_nr BIGINT NOT NULL,
            manifest TEXT NOT NULL,
            payload TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (persistence_id, sequence_nr)
          )
        SQL
      end

      protected def insert_event_feed(
        connection : DB::Connection,
        persistence_id : String,
        sequence_nr : Int64,
        manifest : String,
        payload : String,
      ) : Int64
        connection.exec(
          "INSERT INTO event_feed (persistence_id, sequence_nr, manifest, payload) VALUES (?, ?, ?, ?)",
          persistence_id,
          sequence_nr,
          manifest,
          payload
        ).last_insert_id
      end

      protected def create_outbox_table(connection : DB::Connection) : Nil
        connection.exec(<<-SQL)
          CREATE TABLE IF NOT EXISTS persistence_outbox (
            outbox_offset INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id TEXT NOT NULL UNIQUE,
            persistence_id TEXT NOT NULL,
            operation_id TEXT NOT NULL,
            destination TEXT NOT NULL,
            manifest TEXT NOT NULL,
            payload TEXT NOT NULL,
            delivered BIGINT NOT NULL DEFAULT 0,
            attempts BIGINT NOT NULL DEFAULT 0,
            lease_owner TEXT,
            lease_until_epoch_ms BIGINT NOT NULL DEFAULT 0,
            last_error TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            delivered_at TIMESTAMP
          )
        SQL
      end

      protected def insert_outbox(
        connection : DB::Connection,
        persistence_id : String,
        operation_id : OperationId,
        entry : OutboxEntry,
      ) : Int64
        connection.exec(
          "INSERT INTO persistence_outbox " +
          "(message_id, persistence_id, operation_id, destination, manifest, payload) " +
          "VALUES (?, ?, ?, ?, ?, ?)",
          entry.message_id,
          persistence_id,
          operation_id.value,
          entry.destination,
          entry.manifest,
          entry.payload
        ).last_insert_id
      end

      protected def maintenance_backend_name : String
        "sqlite"
      end

      protected def perform_maintenance(connection : DB::Connection) : Nil
        connection.exec("PRAGMA optimize")
        connection.exec("VACUUM")
      end
    end

    class SQLiteBackend < Backend
      getter uri : String

      def initialize(@uri : String)
      end

      def name : String
        "sqlite"
      end

      def connect : BackendConnection
        connection = DB.connect(@uri)
        connection.exec("PRAGMA busy_timeout = 5000")
        SQLiteBackendConnection.new(connection)
      end
    end

    class UnknownBackendError < Exception
      def initialize(name : String)
        super("Persistence backend is not registered: #{name}")
      end
    end

    class BackendRegistry
      alias Factory = Proc(Movie::Config, Backend)

      @@factories = {} of String => Factory
      @@mutex = Mutex.new

      def self.register(name : String, &factory : Movie::Config -> Backend) : Nil
        key = normalize(name)
        @@mutex.synchronize do
          if @@factories.has_key?(key)
            raise ArgumentError.new("Persistence backend already registered: #{key}")
          end
          @@factories[key] = factory
        end
      end

      def self.build(name : String, config : Movie::Config) : Backend
        key = normalize(name)
        factory = @@mutex.synchronize { @@factories[key]? }
        raise UnknownBackendError.new(key) unless factory
        factory.call(config)
      end

      def self.registered?(name : String) : Bool
        key = normalize(name)
        @@mutex.synchronize { @@factories.has_key?(key) }
      end

      private def self.normalize(name : String) : String
        name.strip.downcase
      end
    end

    BackendRegistry.register("sqlite") do |config|
      path = config.get_string(Movie::ActorSystemConfig::PERSISTENCE_DB_PATH, "data/movie_persistence.sqlite3")
      parent = File.dirname(path)
      Dir.mkdir_p(parent) unless parent == "." || Dir.exists?(parent)
      SQLiteBackend.new("sqlite3:#{path}").as(Backend)
    end
  end
end
