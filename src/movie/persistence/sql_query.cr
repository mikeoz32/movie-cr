module Movie
  module Persistence
    abstract class SqlBackendConnection
      def query_events(message : QueryEvents) : EventPage
        ensure_schema
        with_connection do |connection|
          args = [message.after_offset] of DB::Any
          sql = "SELECT event_offset, persistence_id, sequence_nr, manifest, payload " +
                "FROM event_feed WHERE event_offset > ?"
          if persistence_id = message.persistence_id
            sql += " AND persistence_id = ?"
            args << persistence_id
          end
          sql += " ORDER BY event_offset LIMIT ?"
          args << (message.limit + 1).to_i64
          rows = connection.query_all(
            bind_sql(sql),
            args: args,
            as: {Int64, String, Int64, String, String}
          )
          has_more = rows.size > message.limit
          selected = has_more ? rows.first(message.limit) : rows
          events = selected.map do |row|
            EventEnvelope.new(row[0], row[1], row[2], row[3], row[4])
          end
          next_offset = events.last?.try(&.offset) || message.after_offset
          EventPage.new(events, next_offset, has_more)
        end
      end

      def load_projection_offset(message : LoadProjectionOffset) : Int64
        ensure_schema
        with_connection do |connection|
          connection.query_one?(
            bind_sql("SELECT event_offset FROM projection_checkpoint WHERE projection_name = ?"),
            args: [message.projection] of DB::Any,
            as: Int64
          ) || 0_i64
        end
      end

      def save_projection_offset(message : SaveProjectionOffset) : Int64
        ensure_schema
        with_connection do |connection|
          result = connection.exec(
            bind_sql("INSERT INTO projection_checkpoint (projection_name, event_offset) VALUES (?, ?) " +
                     "ON CONFLICT(projection_name) DO UPDATE SET event_offset = excluded.event_offset, " +
                     "updated_at = CURRENT_TIMESTAMP WHERE projection_checkpoint.event_offset <= excluded.event_offset"),
            args: [message.projection, message.offset] of DB::Any
          )
          if result.rows_affected == 0_i64
            stored = connection.query_one(
              bind_sql("SELECT event_offset FROM projection_checkpoint WHERE projection_name = ?"),
              args: [message.projection] of DB::Any,
              as: Int64
            )
            raise ProjectionOffsetRegressionError.new(message.projection, message.offset, stored)
          end
          message.offset
        end
      end

      def delete_projection_offset(message : DeleteProjectionOffset) : Bool
        ensure_schema
        with_connection do |connection|
          connection.exec(
            bind_sql("DELETE FROM projection_checkpoint WHERE projection_name = ?"),
            args: [message.projection] of DB::Any
          ).rows_affected == 1_i64
        end
      end
    end
  end
end
