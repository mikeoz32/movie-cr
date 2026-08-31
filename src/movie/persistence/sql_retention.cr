module Movie
  module Persistence
    abstract class SqlBackendConnection
      def delete_events_to(message : DeleteEventsTo) : RetentionResult
        ensure_schema
        with_connection do |connection|
          result = connection.transaction do |transaction|
            conn = transaction.connection
            snapshot_sequence_nr = conn.query_one?(
              bind_sql("SELECT sequence_nr FROM snapshot_store WHERE persistence_id = ?"),
              args: [message.persistence_id] of DB::Any,
              as: Int64
            )
            unless snapshot_sequence_nr && message.sequence_nr <= snapshot_sequence_nr
              raise UnsafeRetentionError.new(
                message.persistence_id,
                message.sequence_nr,
                snapshot_sequence_nr
              )
            end
            deleted_query_offset = conn.query_one(
              bind_sql("SELECT MAX(event_offset) FROM event_feed WHERE persistence_id = ? AND sequence_nr <= ?"),
              args: [message.persistence_id, message.sequence_nr] of DB::Any,
              as: Int64?
            )
            if required_offset = deleted_query_offset
              projection_offset = conn.query_one(
                "SELECT MIN(event_offset) FROM projection_checkpoint",
                as: Int64?
              )
              if projection_offset && projection_offset < required_offset
                raise ProjectionBehindRetentionError.new(projection_offset, required_offset)
              end
            end
            conn.exec(
              bind_sql("DELETE FROM event_feed WHERE persistence_id = ? AND sequence_nr <= ?"),
              args: [message.persistence_id, message.sequence_nr] of DB::Any
            )
            deleted = conn.exec(
              bind_sql("DELETE FROM event_journal WHERE persistence_id = ? AND sequence_nr <= ?"),
              args: [message.persistence_id, message.sequence_nr] of DB::Any
            ).rows_affected
            RetentionResult.new(deleted, message.sequence_nr)
          end
          result.not_nil!
        end
      end

      def run_maintenance(message : RunMaintenance) : MaintenanceResult
        ensure_schema
        with_connection do |connection|
          perform_maintenance(connection)
          MaintenanceResult.new(maintenance_backend_name, true)
        end
      end
    end
  end
end
