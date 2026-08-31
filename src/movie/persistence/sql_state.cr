module Movie
  module Persistence
    abstract class SqlBackendConnection
      def save_state(message : SaveState) : WriteResult
        write_state(
          message.persistence_id,
          message.expected_revision,
          message.operation_id,
          message.manifest,
          message.payload,
          false,
          message.outbox
        )
      end

      def delete_state(message : DeleteState) : WriteResult
        write_state(
          message.persistence_id,
          message.expected_revision,
          message.operation_id,
          "deleted",
          nil,
          true,
          message.outbox
        )
      end

      private def write_state(
        persistence_id : String,
        expected_revision : Int64,
        operation_id : OperationId,
        manifest : String,
        payload : String?,
        deleted : Bool,
        outbox : Array(OutboxEntry),
      ) : WriteResult
        with_connection do |connection|
          write_state(connection, persistence_id, expected_revision, operation_id, manifest, payload, deleted, outbox)
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
        outbox : Array(OutboxEntry),
      ) : WriteResult
        revision = expected_revision + 1
        fingerprint = state_fingerprint(manifest, payload, deleted, outbox)
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
          write_outbox_entries(conn, persistence_id, operation_id, outbox)
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

      private def state_fingerprint(
        manifest : String,
        payload : String?,
        deleted : Bool,
        outbox : Array(OutboxEntry),
      ) : String
        Digest::SHA256.hexdigest do |digest|
          digest << "movie:state-operation:v2;" unless outbox.empty?
          digest << manifest.bytesize.to_s << ":" << manifest
          if value = payload
            digest << value.bytesize.to_s << ":" << value
          else
            digest << "nil"
          end
          digest << (deleted ? "1" : "0")
          append_outbox_fingerprint(digest, outbox)
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
    end
  end
end
