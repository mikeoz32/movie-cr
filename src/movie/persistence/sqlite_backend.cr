module Movie
  module Persistence
    class SQLiteBackendConnection < SqlBackendConnection
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
