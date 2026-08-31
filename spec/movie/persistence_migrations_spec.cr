require "../spec_helper"
require "../../src/movie"
require "../../src/movie/persistence"
describe "Movie persistence production contracts" do
  describe "versioned schema migrations" do
    it "records ordered migrations once and reports the current version" do
      path = "/tmp/movie_migrations_#{UUID.random}.sqlite3"
      backend = Movie::Persistence::SQLiteBackend.new("sqlite3:#{path}")

      first = backend.connect
      first.ensure_event_store
      first.ensure_state_store
      first.schema_version.should eq(Movie::Persistence::CURRENT_SCHEMA_VERSION)
      first.close

      DB.open("sqlite3:#{path}") do |db|
        history = db.query_all(
          "SELECT version, name, checksum FROM movie_schema_migration ORDER BY version",
          as: {Int64, String, String}
        )
        history.map(&.[0]).should eq((1_i64..Movie::Persistence::CURRENT_SCHEMA_VERSION).to_a)
        history.each do |version, name, checksum|
          name.should_not be_empty
          checksum.should eq(Movie::Persistence::SCHEMA_MIGRATIONS[version.to_i - 1].checksum)
        end
      end

      second = backend.connect
      second.ensure_event_store
      second.schema_version.should eq(Movie::Persistence::CURRENT_SCHEMA_VERSION)
      second.close

      DB.open("sqlite3:#{path}") do |db|
        db.query_one("SELECT COUNT(*) FROM movie_schema_migration", as: Int64)
          .should eq(Movie::Persistence::CURRENT_SCHEMA_VERSION)
      end
    ensure
      first.try &.close
      second.try &.close
      File.delete(path) if path && File.exists?(path)
    end

    it "rejects a database created by a newer Movie schema" do
      path = "/tmp/movie_future_schema_#{UUID.random}.sqlite3"
      backend = Movie::Persistence::SQLiteBackend.new("sqlite3:#{path}")
      connection = backend.connect
      connection.ensure_event_store
      connection.close

      future_version = Movie::Persistence::CURRENT_SCHEMA_VERSION + 1
      DB.open("sqlite3:#{path}") do |db|
        db.exec(
          "INSERT INTO movie_schema_migration (version, name, checksum) VALUES (?, ?, ?)",
          future_version,
          "future",
          "unsupported"
        )
      end

      expect_raises(Movie::Persistence::UnsupportedSchemaVersionError) do
        next_connection = backend.connect
        begin
          next_connection.ensure_event_store
        ensure
          next_connection.close
        end
      end
    ensure
      connection.try &.close
      File.delete(path) if path && File.exists?(path)
    end

    it "rejects changed migration history" do
      path = "/tmp/movie_changed_schema_#{UUID.random}.sqlite3"
      backend = Movie::Persistence::SQLiteBackend.new("sqlite3:#{path}")
      connection = backend.connect
      connection.ensure_event_store
      connection.close

      DB.open("sqlite3:#{path}") do |db|
        db.exec("UPDATE movie_schema_migration SET checksum = 'changed' WHERE version = 1")
      end

      expect_raises(Movie::Persistence::MigrationChecksumMismatchError) do
        next_connection = backend.connect
        begin
          next_connection.ensure_event_store
        ensure
          next_connection.close
        end
      end
    ensure
      connection.try &.close
      File.delete(path) if path && File.exists?(path)
    end

    it "rejects migration history with a missing earlier version" do
      path = "/tmp/movie_holey_schema_#{UUID.random}.sqlite3"
      backend = Movie::Persistence::SQLiteBackend.new("sqlite3:#{path}")
      connection = backend.connect
      connection.ensure_event_store
      connection.close

      DB.open("sqlite3:#{path}") do |db|
        db.exec("DELETE FROM movie_schema_migration WHERE version = 1")
      end

      expect_raises(Movie::Persistence::InvalidMigrationHistoryError) do
        next_connection = backend.connect
        begin
          next_connection.ensure_event_store
        ensure
          next_connection.close
        end
      end
    ensure
      connection.try &.close
      File.delete(path) if path && File.exists?(path)
    end

    it "adopts and backfills an unversioned Epic 16 journal without rewriting events" do
      path = "/tmp/movie_adopt_schema_#{UUID.random}.sqlite3"
      DB.open("sqlite3:#{path}") do |db|
        db.exec(<<-SQL)
          CREATE TABLE event_journal (
            persistence_id TEXT NOT NULL,
            sequence_nr BIGINT NOT NULL,
            manifest TEXT NOT NULL,
            payload TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (persistence_id, sequence_nr)
          )
        SQL
        db.exec(
          "INSERT INTO event_journal (persistence_id, sequence_nr, manifest, payload) VALUES (?, ?, ?, ?)",
          "adopted",
          1_i64,
          "Added",
          "legacy-value"
        )
      end

      connection = Movie::Persistence::SQLiteBackend.new("sqlite3:#{path}").connect
      connection.ensure_schema
      connection.schema_version.should eq(Movie::Persistence::CURRENT_SCHEMA_VERSION)
      connection.load_events(Movie::Persistence::LoadEvents.new("adopted"))
        .map(&.payload)
        .should eq(["legacy-value"])
      connection.query_events(Movie::Persistence::QueryEvents.new(0_i64, 10, "adopted"))
        .events.map(&.payload)
        .should eq(["legacy-value"])
      connection.append_events(
        Movie::Persistence::AppendEvents.new(
          "adopted",
          1_i64,
          Movie::Persistence::OperationId.random,
          [Movie::Persistence::SerializedEvent.new("Added", "new-value")]
        )
      ).revision.should eq(2_i64)
    ensure
      connection.try &.close
      File.delete(path) if path && File.exists?(path)
    end
  end
end
