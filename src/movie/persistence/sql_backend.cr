module Movie
  module Persistence
    # Shared SQL implementation. Dialects only supply connection setup and
    # schema introspection; all persistence invariants live here.
    abstract class SqlBackendConnection < BackendConnection
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

      def ensure_event_store : Nil
        with_connection do |connection|
          with_schema_lock(connection) { ensure_event_store(connection) }
        end
      end

      private def ensure_event_store(connection : DB::Connection)
        configure_schema(connection)
        if table_exists?(connection, "event_journal") && !table_columns(connection, "event_journal").includes?("sequence_nr")
          migrate_legacy_event_store(connection)
        end
        create_event_store(connection)
        synchronize_event_stream_revisions(connection)
        connection.exec(<<-SQL)
          CREATE TABLE IF NOT EXISTS snapshot_store (
            persistence_id TEXT PRIMARY KEY,
            sequence_nr BIGINT NOT NULL,
            manifest TEXT NOT NULL,
            payload TEXT NOT NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
          )
        SQL
      end

      private def create_event_store(conn : DB::Connection)
        conn.exec(<<-SQL)
          CREATE TABLE IF NOT EXISTS event_journal (
            persistence_id TEXT NOT NULL,
            sequence_nr BIGINT NOT NULL,
            manifest TEXT NOT NULL,
            payload TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (persistence_id, sequence_nr)
          )
        SQL
        conn.exec(<<-SQL)
          CREATE TABLE IF NOT EXISTS event_stream (
            persistence_id TEXT PRIMARY KEY,
            revision BIGINT NOT NULL
          )
        SQL
        conn.exec(<<-SQL)
          CREATE TABLE IF NOT EXISTS journal_operation (
            persistence_id TEXT NOT NULL,
            operation_id TEXT NOT NULL,
            fingerprint TEXT NOT NULL,
            revision BIGINT NOT NULL,
            PRIMARY KEY (persistence_id, operation_id)
          )
        SQL
      end

      private def synchronize_event_stream_revisions(connection : DB::Connection)
        revisions = connection.query_all(
          "SELECT persistence_id, MAX(sequence_nr) FROM event_journal GROUP BY persistence_id",
          as: {String, Int64}
        )
        revisions.each do |persistence_id, revision|
          connection.exec(
            bind_sql("INSERT INTO event_stream (persistence_id, revision) VALUES (?, ?) " +
                     "ON CONFLICT(persistence_id) DO UPDATE SET revision = excluded.revision " +
                     "WHERE event_stream.revision < excluded.revision"),
            args: [persistence_id, revision] of DB::Any
          )
        end
      end

      def ensure_state_store : Nil
        with_connection do |connection|
          with_schema_lock(connection) { ensure_state_store(connection) }
        end
      end

      private def ensure_state_store(connection : DB::Connection)
        configure_schema(connection)
        if table_exists?(connection, "durable_state") && !table_columns(connection, "durable_state").includes?("revision")
          migrate_legacy_state_store(connection)
        end
        create_state_store(connection)
      end

      private def create_state_store(conn : DB::Connection)
        conn.exec(<<-SQL)
          CREATE TABLE IF NOT EXISTS durable_state (
            persistence_id TEXT PRIMARY KEY,
            revision BIGINT NOT NULL,
            manifest TEXT NOT NULL,
            payload TEXT,
            deleted BIGINT NOT NULL DEFAULT 0,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
          )
        SQL
        conn.exec(<<-SQL)
          CREATE TABLE IF NOT EXISTS state_operation (
            persistence_id TEXT NOT NULL,
            operation_id TEXT NOT NULL,
            fingerprint TEXT NOT NULL,
            revision BIGINT NOT NULL,
            PRIMARY KEY (persistence_id, operation_id)
          )
        SQL
      end

      private def migrate_legacy_event_store(connection : DB::Connection)
        connection.transaction do |transaction|
          conn = transaction.connection
          conn.exec("ALTER TABLE event_journal RENAME TO event_journal_legacy")
          create_event_store(conn)
          conn.exec(<<-SQL)
            INSERT INTO event_journal (persistence_id, sequence_nr, manifest, payload, created_at)
            SELECT current.persistence_id,
                   (SELECT COUNT(*) FROM event_journal_legacy previous
                    WHERE previous.persistence_id = current.persistence_id AND previous.id <= current.id),
                   'legacy', current.payload, COALESCE(current.created_at, CURRENT_TIMESTAMP)
            FROM event_journal_legacy current
            ORDER BY current.id
          SQL
          conn.exec("DROP TABLE event_journal_legacy")
        end
      end

      private def migrate_legacy_state_store(connection : DB::Connection)
        connection.transaction do |transaction|
          conn = transaction.connection
          conn.exec("ALTER TABLE durable_state RENAME TO durable_state_legacy")
          create_state_store(conn)
          conn.exec(<<-SQL)
            INSERT INTO durable_state (persistence_id, revision, manifest, payload, deleted, updated_at)
            SELECT persistence_id, 1, 'legacy', payload, 0, COALESCE(updated_at, CURRENT_TIMESTAMP)
            FROM durable_state_legacy
          SQL
          conn.exec("DROP TABLE durable_state_legacy")
        end
      end

      def append_events(message : AppendEvents) : WriteResult
        with_connection { |connection| append_events(connection, message) }
      end

      private def append_events(connection : DB::Connection, message : AppendEvents) : WriteResult
        fingerprint = event_fingerprint(message.events)
        result = connection.transaction do |transaction|
          conn = transaction.connection
          operation = conn.exec(
            bind_sql("INSERT INTO journal_operation (persistence_id, operation_id, fingerprint, revision) " +
                     "VALUES (?, ?, ?, ?) ON CONFLICT(persistence_id, operation_id) DO NOTHING"),
            args: [
              message.persistence_id,
              message.operation_id.value,
              fingerprint,
              message.expected_revision + message.events.size,
            ] of DB::Any
          )
          unless operation.rows_affected == 1
            stored = conn.query_one(
              bind_sql("SELECT fingerprint, revision FROM journal_operation WHERE persistence_id = ? AND operation_id = ?"),
              args: [message.persistence_id, message.operation_id.value] of DB::Any,
              as: {String, Int64}
            )
            unless stored[0] == fingerprint
              raise OperationConflictError.new(message.persistence_id, message.operation_id)
            end
            next WriteResult.new(stored[1], true)
          end

          conn.exec(
            bind_sql("INSERT INTO event_stream (persistence_id, revision) VALUES (?, 0) ON CONFLICT(persistence_id) DO NOTHING"),
            args: [message.persistence_id] of DB::Any
          )

          if message.events.empty?
            actual = event_revision(conn, message.persistence_id)
            unless actual == message.expected_revision
              raise ConcurrentWriteError.new(message.persistence_id, message.expected_revision, actual)
            end
            next WriteResult.new(actual, false)
          end

          next_revision = message.expected_revision + message.events.size
          updated = conn.exec(
            bind_sql("UPDATE event_stream SET revision = ? WHERE persistence_id = ? AND revision = ?"),
            args: [next_revision, message.persistence_id, message.expected_revision] of DB::Any
          )
          unless updated.rows_affected == 1
            actual = event_revision(conn, message.persistence_id)
            raise ConcurrentWriteError.new(message.persistence_id, message.expected_revision, actual)
          end

          sequence_nr = message.expected_revision
          message.events.each do |event|
            sequence_nr += 1
            conn.exec(
              bind_sql("INSERT INTO event_journal (persistence_id, sequence_nr, manifest, payload) VALUES (?, ?, ?, ?)"),
              args: [message.persistence_id, sequence_nr, event.manifest, event.payload] of DB::Any
            )
          end
          WriteResult.new(sequence_nr, false)
        end
        result.not_nil!
      rescue error
        raise error unless database_error?(error)
        actual = event_revision(connection, message.persistence_id)
        if actual != message.expected_revision
          raise ConcurrentWriteError.new(message.persistence_id, message.expected_revision, actual)
        end
        raise error
      end

      private def event_fingerprint(events : Array(SerializedEvent)) : String
        Digest::SHA256.hexdigest do |digest|
          events.each do |event|
            digest << event.manifest.bytesize.to_s << ":" << event.manifest
            digest << event.payload.bytesize.to_s << ":" << event.payload
          end
        end
      end

      private def event_revision(conn : DB::Connection, persistence_id : String) : Int64
        conn.query_one?(
          bind_sql("SELECT revision FROM event_stream WHERE persistence_id = ?"),
          args: [persistence_id] of DB::Any,
          as: Int64
        ) || 0_i64
      end

      def load_events(message : LoadEvents) : Array(StoredEvent)
        with_connection do |connection|
          rows = connection.query_all(
            bind_sql("SELECT sequence_nr, manifest, payload FROM event_journal " +
                     "WHERE persistence_id = ? AND sequence_nr > ? ORDER BY sequence_nr ASC"),
            args: [message.persistence_id, message.after_sequence_nr] of DB::Any,
            as: {Int64, String, String}
          )
          rows.map { |row| StoredEvent.new(row[0], row[1], row[2]) }
        end
      end

      def save_snapshot(message : SaveSnapshot) : Nil
        with_connection do |connection|
          snapshot = message.snapshot
          connection.exec(
            bind_sql("INSERT INTO snapshot_store (persistence_id, sequence_nr, manifest, payload, updated_at) " +
                     "VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP) " +
                     "ON CONFLICT(persistence_id) DO UPDATE SET sequence_nr = excluded.sequence_nr, " +
                     "manifest = excluded.manifest, payload = excluded.payload, updated_at = CURRENT_TIMESTAMP " +
                     "WHERE excluded.sequence_nr >= snapshot_store.sequence_nr"),
            args: [message.persistence_id, snapshot.sequence_nr, snapshot.manifest, snapshot.payload] of DB::Any
          )
        end
      end

      def load_snapshot(message : LoadSnapshot) : SnapshotRecord?
        with_connection do |connection|
          row = connection.query_one?(
            bind_sql("SELECT sequence_nr, manifest, payload FROM snapshot_store WHERE persistence_id = ?"),
            args: [message.persistence_id] of DB::Any,
            as: {Int64, String, String}
          )
          row.try { |value| SnapshotRecord.new(value[0], value[1], value[2]) }
        end
      end

      def delete_snapshot(message : DeleteSnapshot) : Nil
        with_connection do |connection|
          connection.exec(
            bind_sql("DELETE FROM snapshot_store WHERE persistence_id = ?"),
            args: [message.persistence_id] of DB::Any
          )
        end
      end

      def save_state(message : SaveState) : WriteResult
        write_state(
          message.persistence_id,
          message.expected_revision,
          message.operation_id,
          message.manifest,
          message.payload,
          false
        )
      end

      def delete_state(message : DeleteState) : WriteResult
        write_state(message.persistence_id, message.expected_revision, message.operation_id, "deleted", nil, true)
      end

      private def write_state(
        persistence_id : String,
        expected_revision : Int64,
        operation_id : OperationId,
        manifest : String,
        payload : String?,
        deleted : Bool,
      ) : WriteResult
        with_connection do |connection|
          write_state(connection, persistence_id, expected_revision, operation_id, manifest, payload, deleted)
        end
      end

      private def write_state(
        connection : DB::Connection,
        persistence_id : String,
        expected_revision : Int64,
        operation_id : OperationId,
        manifest : String,
        payload : String?,
        deleted : Bool,
      ) : WriteResult
        revision = expected_revision + 1
        fingerprint = state_fingerprint(manifest, payload, deleted)
        result = connection.transaction do |transaction|
          conn = transaction.connection
          operation = conn.exec(
            bind_sql("INSERT INTO state_operation (persistence_id, operation_id, fingerprint, revision) " +
                     "VALUES (?, ?, ?, ?) ON CONFLICT(persistence_id, operation_id) DO NOTHING"),
            args: [persistence_id, operation_id.value, fingerprint, revision] of DB::Any
          )
          unless operation.rows_affected == 1
            stored = conn.query_one(
              bind_sql("SELECT fingerprint, revision FROM state_operation WHERE persistence_id = ? AND operation_id = ?"),
              args: [persistence_id, operation_id.value] of DB::Any,
              as: {String, Int64}
            )
            unless stored[0] == fingerprint
              raise OperationConflictError.new(persistence_id, operation_id)
            end
            next WriteResult.new(stored[1], true)
          end

          write = conn.exec(
            bind_sql("INSERT INTO durable_state (persistence_id, revision, manifest, payload, deleted, updated_at) " +
                     "SELECT ?, ?, ?, ?, ?, CURRENT_TIMESTAMP " +
                     "WHERE ? = 0 OR EXISTS (SELECT 1 FROM durable_state WHERE persistence_id = ?) " +
                     "ON CONFLICT(persistence_id) DO UPDATE SET revision = excluded.revision, " +
                     "manifest = excluded.manifest, payload = excluded.payload, deleted = excluded.deleted, " +
                     "updated_at = CURRENT_TIMESTAMP WHERE durable_state.revision = ?"),
            args: [
              persistence_id,
              revision,
              manifest,
              payload,
              deleted ? 1_i64 : 0_i64,
              expected_revision,
              persistence_id,
              expected_revision,
            ] of DB::Any
          )
          unless write.rows_affected == 1
            actual = state_revision(conn, persistence_id)
            raise ConcurrentWriteError.new(persistence_id, expected_revision, actual)
          end
          WriteResult.new(revision, false)
        end
        result.not_nil!
      rescue error
        raise error unless database_error?(error)
        actual = state_revision(connection, persistence_id)
        if actual != expected_revision
          raise ConcurrentWriteError.new(persistence_id, expected_revision, actual)
        end
        raise error
      end

      private def state_fingerprint(manifest : String, payload : String?, deleted : Bool) : String
        Digest::SHA256.hexdigest do |digest|
          digest << manifest.bytesize.to_s << ":" << manifest
          if value = payload
            digest << value.bytesize.to_s << ":" << value
          else
            digest << "nil"
          end
          digest << (deleted ? "1" : "0")
        end
      end

      private def state_revision(conn : DB::Connection, persistence_id : String) : Int64
        conn.query_one?(
          bind_sql("SELECT revision FROM durable_state WHERE persistence_id = ?"),
          args: [persistence_id] of DB::Any,
          as: Int64
        ) || 0_i64
      end

      def load_state(message : LoadState) : StateRecord?
        with_connection do |connection|
          row = connection.query_one?(
            bind_sql("SELECT revision, manifest, payload, CAST(deleted AS BIGINT) FROM durable_state WHERE persistence_id = ?"),
            args: [message.persistence_id] of DB::Any,
            as: {Int64, String, String?, Int64}
          )
          row.try { |value| StateRecord.new(value[0], value[1], value[2], value[3] != 0) }
        end
      end

      private def with_connection(&operation : DB::Connection -> T) : T forall T
        yield @connection
      end

      protected def configure_schema(connection : DB::Connection) : Nil
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

      def connection_lost?(error : Exception) : Bool
        error.is_a?(DB::ConnectionLost) || @connection.closed?
      end

      def close : Nil
        @connection.close
      end
    end
  end
end
