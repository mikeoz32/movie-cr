require "../spec_helper"
require "../../src/movie"
require "../../src/movie/persistence/postgres"

record PersistenceBackendCase, name : String, backend : Movie::Persistence::Backend

sqlite_contract_path = "/tmp/movie_backend_contract_#{UUID.random}.sqlite3"
backend_cases = [
  PersistenceBackendCase.new(
    "sqlite",
    Movie::Persistence::SQLiteBackend.new("sqlite3:#{sqlite_contract_path}")
  ),
]
if postgres_url = ENV["MOVIE_POSTGRES_TEST_URL"]?
  backend_cases << PersistenceBackendCase.new(
    "postgres",
    Movie::Persistence::PostgresBackend.new(postgres_url)
  )
end

Spec.after_suite do
  File.delete(sqlite_contract_path) if File.exists?(sqlite_contract_path)
end

backend_cases.each do |backend_case|
  describe "Movie persistence backend contract: #{backend_case.name}" do
    it "atomically appends, deduplicates, detects conflicts, and rejects stale writers" do
      persistence_id = "contract-events-#{UUID.random}"
      system = Movie::ActorSystem(Movie::SystemMessage).new(Movie::Behaviors(Movie::SystemMessage).same)
      pool = system.spawn(Movie::Persistence::ConnectionPool.behavior(backend_case.backend, 2))
      store = system.spawn(Movie::Persistence::EventStoreActor.new(pool))
      events = [
        Movie::Persistence::SerializedEvent.new("Added", "one"),
        Movie::Persistence::SerializedEvent.new("Added", "two"),
      ]
      operation_id = Movie::Persistence::OperationId.random

      written = system.ask(
        store,
        Movie::Persistence::AppendEvents.new(persistence_id, 0_i64, operation_id, events),
        Movie::Persistence::WriteResult,
        5.seconds
      ).await(5.seconds)
      written.should eq(Movie::Persistence::WriteResult.new(2_i64, false))

      duplicate = system.ask(
        store,
        Movie::Persistence::AppendEvents.new(persistence_id, 2_i64, operation_id, events),
        Movie::Persistence::WriteResult,
        5.seconds
      ).await(5.seconds)
      duplicate.should eq(Movie::Persistence::WriteResult.new(2_i64, true))

      expect_raises(Movie::Persistence::OperationConflictError) do
        system.ask(
          store,
          Movie::Persistence::AppendEvents.new(
            persistence_id,
            2_i64,
            operation_id,
            [Movie::Persistence::SerializedEvent.new("Added", "different")]
          ),
          Movie::Persistence::WriteResult,
          5.seconds
        ).await(5.seconds)
      end

      expect_raises(Movie::Persistence::ConcurrentWriteError) do
        system.ask(
          store,
          Movie::Persistence::AppendEvents.new(
            persistence_id,
            0_i64,
            Movie::Persistence::OperationId.random,
            [Movie::Persistence::SerializedEvent.new("Added", "stale")]
          ),
          Movie::Persistence::WriteResult,
          5.seconds
        ).await(5.seconds)
      end

      stored = system.ask(
        store,
        Movie::Persistence::LoadEvents.new(persistence_id),
        Array(Movie::Persistence::StoredEvent),
        5.seconds
      ).await(5.seconds)
      stored.map(&.sequence_nr).should eq([1_i64, 2_i64])
      stored.map(&.payload).should eq(["one", "two"])
    ensure
      system.try &.shutdown
    end

    it "keeps the newest snapshot and deletes it" do
      persistence_id = "contract-snapshot-#{UUID.random}"
      system = Movie::ActorSystem(Movie::SystemMessage).new(Movie::Behaviors(Movie::SystemMessage).same)
      pool = system.spawn(Movie::Persistence::ConnectionPool.behavior(backend_case.backend, 1))
      store = system.spawn(Movie::Persistence::EventStoreActor.new(pool))
      newest = Movie::Persistence::SnapshotRecord.new(5_i64, "Counter", "five")
      older = Movie::Persistence::SnapshotRecord.new(3_i64, "Counter", "three")

      [newest, older].each do |snapshot|
        system.ask(
          store,
          Movie::Persistence::SaveSnapshot.new(persistence_id, snapshot),
          Bool,
          5.seconds
        ).await(5.seconds).should be_true
      end
      loaded = system.ask(
        store,
        Movie::Persistence::LoadSnapshot.new(persistence_id),
        Movie::Persistence::SnapshotRecord?,
        5.seconds
      ).await(5.seconds)
      loaded.should eq(newest)

      system.ask(
        store,
        Movie::Persistence::DeleteSnapshot.new(persistence_id),
        Bool,
        5.seconds
      ).await(5.seconds).should be_true
      system.ask(
        store,
        Movie::Persistence::LoadSnapshot.new(persistence_id),
        Movie::Persistence::SnapshotRecord?,
        5.seconds
      ).await(5.seconds).should be_nil
    ensure
      system.try &.shutdown
    end

    it "persists durable revisions, deduplicates retries, and retains delete tombstones" do
      persistence_id = "contract-state-#{UUID.random}"
      system = Movie::ActorSystem(Movie::SystemMessage).new(Movie::Behaviors(Movie::SystemMessage).same)
      pool = system.spawn(Movie::Persistence::ConnectionPool.behavior(backend_case.backend, 2))
      store = system.spawn(Movie::Persistence::StateStoreActor.new(pool))
      save_id = Movie::Persistence::OperationId.random
      save = Movie::Persistence::SaveState.new(persistence_id, 0_i64, save_id, "Profile", "value")

      system.ask(store, save, Movie::Persistence::WriteResult, 5.seconds)
        .await(5.seconds)
        .should eq(Movie::Persistence::WriteResult.new(1_i64, false))
      system.ask(store, save, Movie::Persistence::WriteResult, 5.seconds)
        .await(5.seconds)
        .should eq(Movie::Persistence::WriteResult.new(1_i64, true))

      expect_raises(Movie::Persistence::OperationConflictError) do
        system.ask(
          store,
          Movie::Persistence::SaveState.new(persistence_id, 1_i64, save_id, "Profile", "different"),
          Movie::Persistence::WriteResult,
          5.seconds
        ).await(5.seconds)
      end

      delete = Movie::Persistence::DeleteState.new(
        persistence_id,
        1_i64,
        Movie::Persistence::OperationId.random
      )
      system.ask(store, delete, Movie::Persistence::WriteResult, 5.seconds)
        .await(5.seconds)
        .should eq(Movie::Persistence::WriteResult.new(2_i64, false))

      loaded = system.ask(
        store,
        Movie::Persistence::LoadState.new(persistence_id),
        Movie::Persistence::StateRecord?,
        5.seconds
      ).await(5.seconds).not_nil!
      loaded.revision.should eq(2_i64)
      loaded.deleted.should be_true
      loaded.payload.should be_nil
    ensure
      system.try &.shutdown
    end
  end
end
