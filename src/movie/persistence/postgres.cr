require "../persistence"
require "pg"

module Movie
  module Persistence
    class BackendConfigurationError < Exception
    end

    # Shared PostgreSQL backend. Every Movie node opens its own bounded pool,
    # while database constraints arbitrate revisions and operation ids.
    class PostgresBackend < Backend
      getter uri : String

      def initialize(@uri : String)
        unless @uri.starts_with?("postgres://") || @uri.starts_with?("postgresql://")
          raise BackendConfigurationError.new(
            "persistence.connection-uri must be a postgres:// or postgresql:// URI"
          )
        end
      end

      def name : String
        "postgres"
      end

      def connect : BackendConnection
        PostgresBackendConnection.new(DB.connect(@uri))
      end
    end

    class PostgresBackendConnection < SqlBackendConnection
      SCHEMA_LOCK_ID = 0x4d4f564945505253_i64
      @bound_sql = {} of String => String

      protected def bind_sql(statement : String) : String
        @bound_sql[statement] ||= begin
          index = 0
          String.build(statement.bytesize + 8) do |io|
            statement.each_char do |char|
              if char == '?'
                index += 1
                io << '$' << index
              else
                io << char
              end
            end
          end
        end
      end

      protected def with_schema_lock(connection : DB::Connection, &operation : -> T) : T forall T
        connection.query_each(
          "SELECT pg_advisory_lock(#{SCHEMA_LOCK_ID})"
        ) { |row| row.read }
        yield
      ensure
        unless connection.closed?
          connection.query_one(
            "SELECT pg_advisory_unlock(#{SCHEMA_LOCK_ID})",
            as: Bool
          )
        end
      end

      protected def table_exists?(connection : DB::Connection, name : String) : Bool
        !connection.query_one?(
          bind_sql("SELECT table_name FROM information_schema.tables " +
                   "WHERE table_schema = current_schema() AND table_name = ?"),
          args: [name] of DB::Any,
          as: String
        ).nil?
      end

      protected def table_columns(connection : DB::Connection, name : String) : Array(String)
        connection.query_all(
          bind_sql("SELECT column_name FROM information_schema.columns " +
                   "WHERE table_schema = current_schema() AND table_name = ? ORDER BY ordinal_position"),
          args: [name] of DB::Any,
          as: String
        )
      end

      protected def database_error?(error : Exception) : Bool
        error.is_a?(PQ::PQError) || error.is_a?(PG::Error)
      end
    end

    BackendRegistry.register("postgres") do |config|
      uri = config.get_string(Movie::ActorSystemConfig::PERSISTENCE_CONNECTION_URI, "")
      if uri.empty?
        raise BackendConfigurationError.new(
          "persistence.connection-uri is required when persistence.backend is postgres"
        )
      end
      PostgresBackend.new(uri).as(Backend)
    end
  end
end
