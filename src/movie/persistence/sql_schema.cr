module Movie
  module Persistence
    abstract class SqlBackendConnection
      def ensure_event_store : Nil
        ensure_schema
      end

      def ensure_state_store : Nil
        ensure_schema
      end

      def ensure_schema : Nil
        return if @schema_ready
        with_connection do |connection|
          configure_schema(connection)
          with_schema_lock(connection) do
            create_migration_history(connection)
            applied = validate_migration_history(connection)
            SCHEMA_MIGRATIONS.each do |migration|
              next if applied.includes?(migration.version)
              connection.transaction do |transaction|
                conn = transaction.connection
                apply_migration(conn, migration)
                conn.exec(
                  bind_sql("INSERT INTO movie_schema_migration (version, name, checksum) VALUES (?, ?, ?)"),
                  args: [migration.version, migration.name, migration.checksum] of DB::Any
                )
              end
            end
          end
        end
        @schema_ready = true
      end

      def schema_version : Int64
        ensure_schema
        with_connection do |connection|
          connection.query_one("SELECT COALESCE(MAX(version), 0) FROM movie_schema_migration", as: Int64)
        end
      end

      private def create_migration_history(connection : DB::Connection)
        connection.exec(<<-SQL)
          CREATE TABLE IF NOT EXISTS movie_schema_migration (
            version BIGINT PRIMARY KEY,
            name TEXT NOT NULL,
            checksum TEXT NOT NULL,
            applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
          )
        SQL
      end

      private def validate_migration_history(connection : DB::Connection) : Array(Int64)
        history = connection.query_all(
          "SELECT version, name, checksum FROM movie_schema_migration ORDER BY version",
          as: {Int64, String, String}
        )
        history.each_with_index do |(version, _name, checksum), index|
          expected_version = index.to_i64 + 1_i64
          unless version == expected_version
            raise InvalidMigrationHistoryError.new(expected_version, version)
          end
          if version > CURRENT_SCHEMA_VERSION
            raise UnsupportedSchemaVersionError.new(version, CURRENT_SCHEMA_VERSION)
          end
          migration = SCHEMA_MIGRATIONS.find { |candidate| candidate.version == version }
          raise UnsupportedSchemaVersionError.new(version, CURRENT_SCHEMA_VERSION) unless migration
          unless migration.checksum == checksum
            raise MigrationChecksumMismatchError.new(version)
          end
        end
        history.map(&.[0])
      end

      private def apply_migration(connection : DB::Connection, migration : SchemaMigration)
        case migration.version
        when 1_i64
          migrate_core_schema(connection)
        when 2_i64
          migrate_query_schema(connection)
        when 3_i64
          migrate_outbox_schema(connection)
        when 4_i64
          migrate_shard_fencing_schema(connection)
        else
          raise UnsupportedSchemaVersionError.new(migration.version, CURRENT_SCHEMA_VERSION)
        end
      end

      private def migrate_core_schema(connection : DB::Connection)
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
        if table_exists?(connection, "durable_state") && !table_columns(connection, "durable_state").includes?("revision")
          migrate_legacy_state_store(connection)
        end
        create_state_store(connection)
      end

      private def migrate_query_schema(connection : DB::Connection)
        create_event_feed_table(connection)
        connection.exec(<<-SQL)
          CREATE TABLE IF NOT EXISTS projection_checkpoint (
            projection_name TEXT PRIMARY KEY,
            event_offset BIGINT NOT NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
          )
        SQL
        connection.exec(
          "CREATE INDEX IF NOT EXISTS idx_event_feed_persistence " +
          "ON event_feed (persistence_id, event_offset)"
        )
        existing = connection.query_one("SELECT COUNT(*) FROM event_feed", as: Int64)
        return unless existing == 0_i64
        rows = connection.query_all(
          "SELECT persistence_id, sequence_nr, manifest, payload FROM event_journal " +
          "ORDER BY created_at, persistence_id, sequence_nr",
          as: {String, Int64, String, String}
        )
        rows.each do |row|
          insert_event_feed(connection, row[0], row[1], row[2], row[3])
        end
      end

      private def migrate_outbox_schema(connection : DB::Connection)
        create_outbox_table(connection)
        connection.exec(
          "CREATE INDEX IF NOT EXISTS idx_persistence_outbox_pending " +
          "ON persistence_outbox (delivered, lease_until_epoch_ms, outbox_offset)"
        )
      end

      private def migrate_shard_fencing_schema(connection : DB::Connection)
        connection.exec(<<-SQL)
          CREATE TABLE IF NOT EXISTS shard_lease (
            cluster_name TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            shard_id BIGINT NOT NULL,
            owner TEXT NOT NULL,
            epoch BIGINT NOT NULL,
            lease_until_epoch_ms BIGINT NOT NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (cluster_name, entity_type, shard_id)
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
        connection.exec("ALTER TABLE event_journal RENAME TO event_journal_legacy")
        create_event_store(connection)
        connection.exec(<<-SQL)
          INSERT INTO event_journal (persistence_id, sequence_nr, manifest, payload, created_at)
          SELECT current.persistence_id,
                 (SELECT COUNT(*) FROM event_journal_legacy previous
                  WHERE previous.persistence_id = current.persistence_id AND previous.id <= current.id),
                 'legacy', current.payload, COALESCE(current.created_at, CURRENT_TIMESTAMP)
          FROM event_journal_legacy current
          ORDER BY current.id
        SQL
        connection.exec("DROP TABLE event_journal_legacy")
      end

      private def migrate_legacy_state_store(connection : DB::Connection)
        connection.exec("ALTER TABLE durable_state RENAME TO durable_state_legacy")
        create_state_store(connection)
        connection.exec(<<-SQL)
          INSERT INTO durable_state (persistence_id, revision, manifest, payload, deleted, updated_at)
          SELECT persistence_id, 1, 'legacy', payload, 0, COALESCE(updated_at, CURRENT_TIMESTAMP)
          FROM durable_state_legacy
        SQL
        connection.exec("DROP TABLE durable_state_legacy")
      end
    end
  end
end
