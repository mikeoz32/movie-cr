module Movie
  module Persistence
    abstract class SqlBackendConnection
      def claim_outbox(message : ClaimOutbox) : Array(StoredOutboxEntry)
        ensure_schema
        with_connection do |connection|
          result = connection.transaction do |transaction|
            conn = transaction.connection
            rows = conn.query_all(
              bind_sql("SELECT outbox_offset, message_id, persistence_id, operation_id, destination, " +
                       "manifest, payload, attempts, last_error FROM persistence_outbox " +
                       "WHERE delivered = 0 AND (lease_owner IS NULL OR lease_until_epoch_ms <= ? OR lease_owner = ?) " +
                       "ORDER BY outbox_offset LIMIT ?"),
              args: [message.now_epoch_ms, message.owner, message.limit.to_i64] of DB::Any,
              as: {Int64, String, String, String, String, String, String, Int64, String?}
            )
            claimed = [] of StoredOutboxEntry
            rows.each do |row|
              updated = conn.exec(
                bind_sql("UPDATE persistence_outbox SET lease_owner = ?, lease_until_epoch_ms = ? " +
                         "WHERE message_id = ? AND delivered = 0 " +
                         "AND (lease_owner IS NULL OR lease_until_epoch_ms <= ? OR lease_owner = ?)"),
                args: [
                  message.owner,
                  message.lease_until_epoch_ms,
                  row[1],
                  message.now_epoch_ms,
                  message.owner,
                ] of DB::Any
              )
              next unless updated.rows_affected == 1_i64
              claimed << StoredOutboxEntry.new(
                row[0], row[1], row[2], OperationId.new(row[3]), row[4], row[5], row[6], row[7], row[8]
              )
            end
            claimed
          end
          result.not_nil!
        end
      end

      def acknowledge_outbox(message : AcknowledgeOutbox) : Int64
        return 0_i64 if message.message_ids.empty?
        ensure_schema
        with_connection do |connection|
          connection.transaction do |transaction|
            conn = transaction.connection
            message.message_ids.sum(0_i64) do |message_id|
              conn.exec(
                bind_sql("UPDATE persistence_outbox SET delivered = 1, lease_owner = NULL, " +
                         "lease_until_epoch_ms = 0, delivered_at = CURRENT_TIMESTAMP " +
                         "WHERE message_id = ? AND lease_owner = ? AND delivered = 0"),
                args: [message_id, message.owner] of DB::Any
              ).rows_affected
            end
          end.not_nil!
        end
      end

      def release_outbox(message : ReleaseOutbox) : Bool
        ensure_schema
        with_connection do |connection|
          connection.exec(
            bind_sql("UPDATE persistence_outbox SET attempts = attempts + 1, last_error = ?, " +
                     "lease_owner = NULL, lease_until_epoch_ms = 0 " +
                     "WHERE message_id = ? AND lease_owner = ? AND delivered = 0"),
            args: [message.error, message.message_id, message.owner] of DB::Any
          ).rows_affected == 1_i64
        end
      end

      private def append_outbox_fingerprint(digest : Digest::SHA256, outbox : Array(OutboxEntry))
        return if outbox.empty?
        digest << "outbox:" << outbox.size.to_s << ";"
        outbox.each do |entry|
          digest << entry.message_id.bytesize.to_s << ":" << entry.message_id
          digest << entry.destination.bytesize.to_s << ":" << entry.destination
          digest << entry.manifest.bytesize.to_s << ":" << entry.manifest
          digest << entry.payload.bytesize.to_s << ":" << entry.payload
        end
      end

      private def write_outbox_entries(
        connection : DB::Connection,
        persistence_id : String,
        operation_id : OperationId,
        outbox : Array(OutboxEntry),
      )
        outbox.each do |entry|
          insert_outbox(connection, persistence_id, operation_id, entry)
        end
      end
    end
  end
end
