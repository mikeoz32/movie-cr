module Movie
  module Persistence
    record SchemaMigration, version : Int64, name : String, checksum : String

    SCHEMA_MIGRATIONS = [
      SchemaMigration.new(
        1_i64,
        "core-persistence-schema",
        Digest::SHA256.hexdigest("movie:persistence:migration:1:core-persistence-schema:v1")
      ),
      SchemaMigration.new(
        2_i64,
        "event-query-and-projection-schema",
        Digest::SHA256.hexdigest("movie:persistence:migration:2:event-query-and-projection-schema:v1")
      ),
      SchemaMigration.new(
        3_i64,
        "transactional-outbox-schema",
        Digest::SHA256.hexdigest("movie:persistence:migration:3:transactional-outbox-schema:v1")
      ),
      SchemaMigration.new(
        4_i64,
        "cluster-shard-fencing-schema",
        Digest::SHA256.hexdigest("movie:persistence:migration:4:cluster-shard-fencing-schema:v1")
      ),
    ]

    CURRENT_SCHEMA_VERSION = SCHEMA_MIGRATIONS.last.version

    class UnsupportedSchemaVersionError < Exception
      getter found_version : Int64
      getter supported_version : Int64

      def initialize(@found_version : Int64, @supported_version : Int64)
        super("Persistence schema version #{@found_version} is newer than supported version #{@supported_version}")
      end
    end

    class MigrationChecksumMismatchError < Exception
      getter version : Int64

      def initialize(@version : Int64)
        super("Persistence migration #{@version} checksum does not match this Movie build")
      end
    end

    class InvalidMigrationHistoryError < Exception
      getter expected_version : Int64
      getter found_version : Int64

      def initialize(@expected_version : Int64, @found_version : Int64)
        super("Persistence migration history expected version #{@expected_version}, found #{@found_version}")
      end
    end

    module SchemaBackend
      abstract def ensure_schema : Nil
      abstract def schema_version : Int64
    end
  end
end
