module Movie
  module Persistence
    # Shared SQL connection primitives. Persistence concerns reopen this class
    # in focused files so dialect hooks and connection ownership stay central.
    abstract class SqlBackendConnection < BackendConnection
      @schema_ready = false

      def initialize(@connection : DB::Connection)
      end

      def exec(message : DbExec) : Nil
        with_connection { |connection| connection.exec(message.sql, args: message.args) }
      end

      def exec_last_id(message : DbExecLastId) : Int64
        with_connection do |connection|
          result = connection.exec(message.sql, args: message.args)
          result.last_insert_id
        end
      end

      def query_string(message : DbQueryString) : String?
        with_connection { |connection| connection.query_one?(message.sql, args: message.args, as: String) }
      end

      def query_strings(message : DbQueryStrings) : Array(String)
        with_connection do |connection|
          values = [] of String
          connection.query_each(message.sql, args: message.args) do |rs|
            values << rs.read(String)
          end
          values
        end
      end

      private def with_connection(&operation : DB::Connection -> T) : T forall T
        yield @connection
      end

      protected def configure_schema(connection : DB::Connection) : Nil
      end

      # Dialects whose generated identities are allocation-ordered rather than
      # commit-ordered override this with a transaction-scoped sequencer lock.
      protected def serialize_event_commit(connection : DB::Connection) : Nil
      end

      protected def bind_sql(statement : String) : String
        statement
      end

      protected def with_schema_lock(connection : DB::Connection, &operation : -> T) : T forall T
        yield
      end

      protected abstract def table_exists?(connection : DB::Connection, name : String) : Bool
      protected abstract def table_columns(connection : DB::Connection, name : String) : Array(String)
      protected abstract def database_error?(error : Exception) : Bool
      protected abstract def create_event_feed_table(connection : DB::Connection) : Nil
      protected abstract def insert_event_feed(
        connection : DB::Connection,
        persistence_id : String,
        sequence_nr : Int64,
        manifest : String,
        payload : String,
      ) : Int64
      protected abstract def create_outbox_table(connection : DB::Connection) : Nil
      protected abstract def insert_outbox(
        connection : DB::Connection,
        persistence_id : String,
        operation_id : OperationId,
        entry : OutboxEntry,
      ) : Int64

      protected def maintenance_backend_name : String
        "sql"
      end

      protected def perform_maintenance(connection : DB::Connection) : Nil
      end

      def connection_lost?(error : Exception) : Bool
        error.is_a?(DB::ConnectionLost) || @connection.closed?
      end

      def close : Nil
        @connection.close
      end
    end
  end
end
