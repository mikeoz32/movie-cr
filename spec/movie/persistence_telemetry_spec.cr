require "../spec_helper"
require "../../src/movie"
require "../../src/movie/persistence"
describe "Movie persistence production contracts" do
  describe "telemetry and health" do
    it "exposes non-blocking worker, queue, latency, failure, and conflict snapshots" do
      path = "/tmp/movie_telemetry_#{UUID.random}.sqlite3"
      config = Movie::Config.builder
        .set("persistence.db-path", path)
        .set("persistence.pool-size", 1)
        .set("persistence.io-queue-capacity", 4)
        .build
      system = Movie::ActorSystem(Movie::SystemMessage).new(
        Movie::Behaviors(Movie::SystemMessage).same,
        config
      )
      database = Movie::Database.get(system)
      pool = database.pool

      system.ask(pool, Movie::Persistence::EnsureEventStore.new, Bool, 5.seconds).await(5.seconds)
      event = Movie::Persistence::SerializedEvent.new("Added", "one")
      system.ask(
        pool,
        Movie::Persistence::AppendEvents.new(
          "telemetry-stream",
          0_i64,
          Movie::Persistence::OperationId.random,
          [event]
        ),
        Movie::Persistence::WriteResult,
        5.seconds
      ).await(5.seconds)

      expect_raises(Movie::Persistence::ConcurrentWriteError) do
        system.ask(
          pool,
          Movie::Persistence::AppendEvents.new(
            "telemetry-stream",
            0_i64,
            Movie::Persistence::OperationId.random,
            [event]
          ),
          Movie::Persistence::WriteResult,
          5.seconds
        ).await(5.seconds)
      end

      metrics = database.metrics
      metrics.enqueued.should be >= 3_i64
      metrics.completed.should be >= 2_i64
      metrics.failed.should eq(1_i64)
      metrics.conflicts.should eq(1_i64)
      metrics.total_latency.should be > Time::Span.zero
      metrics.max_latency.should be > Time::Span.zero
      metrics.queued.should eq(0_i64)
      metrics.in_flight.should eq(0_i64)

      health = database.health
      health.status.should eq(Movie::Persistence::HealthStatus::Healthy)
      health.workers.should eq(1_i64)
      health.available_workers.should eq(1_i64)
      health.queue_capacity.should eq(4_i64)
      health.queued.should eq(0_i64)
      health.last_error.not_nil!.should contain("Concurrent persistence write")

      readiness = database.readiness
      readiness.ready.should be_true
      readiness.backend.should eq("sqlite")
      readiness.schema_version.should eq(Movie::Persistence::CURRENT_SCHEMA_VERSION)
      readiness.error.should be_nil
    ensure
      system.try &.shutdown
      File.delete(path) if path && File.exists?(path)
    end
  end
end
