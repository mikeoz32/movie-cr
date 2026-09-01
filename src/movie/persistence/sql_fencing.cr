module Movie
  module Persistence
    abstract class SqlBackendConnection
      def acquire_shard_lease(message : AcquireShardLease) : ShardLeaseToken?
        ensure_schema
        with_connection do |connection|
          result = connection.transaction do |transaction|
            conn = transaction.connection
            key = message.key
            inserted = conn.exec(
              bind_sql(
                "INSERT INTO shard_lease " +
                "(cluster_name, entity_type, shard_id, owner, epoch, lease_until_epoch_ms) " +
                "VALUES (?, ?, ?, ?, 1, #{current_epoch_ms_sql} + ?) " +
                "ON CONFLICT(cluster_name, entity_type, shard_id) DO NOTHING"
              ),
              args: [
                key.cluster_name,
                key.entity_type,
                key.shard_id.to_i64,
                message.owner,
                message.lease_ms,
              ] of DB::Any
            )
            if inserted.rows_affected == 1
              next load_shard_lease(conn, key)
            end

            renewed = conn.exec(
              bind_sql(
                "UPDATE shard_lease SET lease_until_epoch_ms = #{current_epoch_ms_sql} + ?, " +
                "updated_at = CURRENT_TIMESTAMP " +
                "WHERE cluster_name = ? AND entity_type = ? AND shard_id = ? AND owner = ? " +
                "AND lease_until_epoch_ms > #{current_epoch_ms_sql}"
              ),
              args: [
                message.lease_ms,
                key.cluster_name,
                key.entity_type,
                key.shard_id.to_i64,
                message.owner,
              ] of DB::Any
            )
            if renewed.rows_affected == 1
              next load_shard_lease(conn, key)
            end

            transferred = conn.exec(
              bind_sql(
                "UPDATE shard_lease SET owner = ?, epoch = epoch + 1, " +
                "lease_until_epoch_ms = #{current_epoch_ms_sql} + ?, updated_at = CURRENT_TIMESTAMP " +
                "WHERE cluster_name = ? AND entity_type = ? AND shard_id = ? " +
                "AND lease_until_epoch_ms <= #{current_epoch_ms_sql}"
              ),
              args: [
                message.owner,
                message.lease_ms,
                key.cluster_name,
                key.entity_type,
                key.shard_id.to_i64,
              ] of DB::Any
            )
            transferred.rows_affected == 1 ? load_shard_lease(conn, key) : nil
          end
          result
        end
      end

      def renew_shard_lease(message : RenewShardLease) : ShardLeaseToken?
        ensure_schema
        with_connection do |connection|
          token = message.token
          key = token.key
          renewed = connection.exec(
            bind_sql(
              "UPDATE shard_lease SET lease_until_epoch_ms = #{current_epoch_ms_sql} + ?, " +
              "updated_at = CURRENT_TIMESTAMP " +
              "WHERE cluster_name = ? AND entity_type = ? AND shard_id = ? " +
              "AND owner = ? AND epoch = ? AND lease_until_epoch_ms > #{current_epoch_ms_sql}"
            ),
            args: [
              message.lease_ms,
              key.cluster_name,
              key.entity_type,
              key.shard_id.to_i64,
              token.owner,
              token.epoch,
            ] of DB::Any
          )
          renewed.rows_affected == 1 ? load_shard_lease(connection, key) : nil
        end
      end

      def release_shard_lease(message : ReleaseShardLease) : Bool
        ensure_schema
        with_connection do |connection|
          token = message.token
          key = token.key
          connection.exec(
            bind_sql(
              "UPDATE shard_lease SET lease_until_epoch_ms = 0, updated_at = CURRENT_TIMESTAMP " +
              "WHERE cluster_name = ? AND entity_type = ? AND shard_id = ? AND owner = ? AND epoch = ?"
            ),
            args: [
              key.cluster_name,
              key.entity_type,
              key.shard_id.to_i64,
              token.owner,
              token.epoch,
            ] of DB::Any
          ).rows_affected == 1
        end
      end

      private def validate_fence(connection : DB::Connection, token : ShardLeaseToken?) : Nil
        return unless token
        key = token.key
        valid = connection.query_one?(
          bind_sql(
            "SELECT epoch FROM shard_lease WHERE cluster_name = ? AND entity_type = ? " +
            "AND shard_id = ? AND owner = ? AND epoch = ? " +
            "AND lease_until_epoch_ms > #{current_epoch_ms_sql}#{fence_validation_lock_sql}"
          ),
          args: [
            key.cluster_name,
            key.entity_type,
            key.shard_id.to_i64,
            token.owner,
            token.epoch,
          ] of DB::Any,
          as: Int64
        ) == token.epoch
        raise StaleShardOwnerError.new(token) unless valid
      end

      private def load_shard_lease(
        connection : DB::Connection,
        key : ShardLeaseKey,
      ) : ShardLeaseToken
        owner, epoch, lease_until = connection.query_one(
          bind_sql(
            "SELECT owner, epoch, lease_until_epoch_ms FROM shard_lease " +
            "WHERE cluster_name = ? AND entity_type = ? AND shard_id = ?"
          ),
          args: [key.cluster_name, key.entity_type, key.shard_id.to_i64] of DB::Any,
          as: {String, Int64, Int64}
        )
        ShardLeaseToken.new(key, owner, epoch, lease_until)
      end
    end
  end
end
