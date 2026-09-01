module Movie
  module Persistence
    abstract class SqlBackendConnection
      def append_events(message : AppendEvents) : WriteResult
        with_connection { |connection| append_events(connection, message) }
      end

      private def append_events(connection : DB::Connection, message : AppendEvents) : WriteResult
        fingerprint = event_fingerprint(message.events, message.outbox)
        result = connection.transaction do |transaction|
          conn = transaction.connection
          validate_fence(conn, message.fence)
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
            write_outbox_entries(conn, message.persistence_id, message.operation_id, message.outbox)
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
          write_outbox_entries(conn, message.persistence_id, message.operation_id, message.outbox)

          # Keep the global sequencer at the transaction tail: stream and
          # outbox validation can run concurrently, while offset allocation and
          # commit remain ordered without holding the lock across all writes.
          serialize_event_commit(conn)
          sequence_nr = message.expected_revision
          message.events.each do |event|
            sequence_nr += 1
            insert_event_feed(conn, message.persistence_id, sequence_nr, event.manifest, event.payload)
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

      private def event_fingerprint(events : Array(SerializedEvent), outbox : Array(OutboxEntry)) : String
        Digest::SHA256.hexdigest do |digest|
          unless outbox.empty?
            digest << "movie:event-operation:v2;events:" << events.size.to_s << ";"
          end
          events.each do |event|
            digest << event.manifest.bytesize.to_s << ":" << event.manifest
            digest << event.payload.bytesize.to_s << ":" << event.payload
          end
          append_outbox_fingerprint(digest, outbox)
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
          connection.transaction do |transaction|
            conn = transaction.connection
            validate_fence(conn, message.fence)
            conn.exec(
              bind_sql("INSERT INTO snapshot_store (persistence_id, sequence_nr, manifest, payload, updated_at) " +
                       "VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP) " +
                       "ON CONFLICT(persistence_id) DO UPDATE SET sequence_nr = excluded.sequence_nr, " +
                       "manifest = excluded.manifest, payload = excluded.payload, updated_at = CURRENT_TIMESTAMP " +
                       "WHERE excluded.sequence_nr >= snapshot_store.sequence_nr"),
              args: [message.persistence_id, snapshot.sequence_nr, snapshot.manifest, snapshot.payload] of DB::Any
            )
          end
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
          connection.transaction do |transaction|
            conn = transaction.connection
            validate_fence(conn, message.fence)
            conn.exec(
              bind_sql("DELETE FROM snapshot_store WHERE persistence_id = ?"),
              args: [message.persistence_id] of DB::Any
            )
          end
        end
      end
    end
  end
end
