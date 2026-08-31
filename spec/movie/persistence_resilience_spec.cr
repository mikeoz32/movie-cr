require "../spec_helper"
require "../../src/movie"
require "../../src/movie/persistence"

module Movie
  class RetryConnectionLost < Exception
  end

  class RetryFailureProbe
    getter connections : Atomic(Int32)

    def initialize(failures : Int32)
      @remaining = Atomic(Int32).new(failures)
      @connections = Atomic(Int32).new(0)
    end

    def connect : Nil
      @connections.add(1)
    end

    def fail? : Bool
      loop do
        remaining = @remaining.get
        return false if remaining <= 0
        _, changed = @remaining.compare_and_set(remaining, remaining - 1)
        return true if changed
      end
    end
  end

  class RetrySQLiteConnection < Persistence::SQLiteBackendConnection
    def initialize(connection : DB::Connection, @probe : RetryFailureProbe)
      super(connection)
    end

    def load_events(message : Persistence::LoadEvents) : Array(Persistence::StoredEvent)
      raise RetryConnectionLost.new("transient connection loss") if @probe.fail?
      super
    end

    def connection_lost?(error : Exception) : Bool
      error.is_a?(RetryConnectionLost) || super
    end
  end

  class RetrySQLiteBackend < Persistence::Backend
    def initialize(@uri : String, getter @probe : RetryFailureProbe)
    end

    def name : String
      "retry-sqlite"
    end

    def connect : Persistence::BackendConnection
      @probe.connect
      RetrySQLiteConnection.new(DB.connect(@uri), @probe)
    end
  end
end

describe "Movie persistence production contracts" do
  describe "retry, backoff, and circuit breaking" do
    it "reconnects and retries a typed idempotent operation within policy bounds" do
      path = "/tmp/movie_retry_#{UUID.random}.sqlite3"
      probe = Movie::RetryFailureProbe.new(2)
      backend = Movie::RetrySQLiteBackend.new("sqlite3:#{path}", probe)
      telemetry = Movie::Persistence::Telemetry.new
      policy = Movie::Persistence::ResiliencePolicy.new(
        max_retries: 2,
        min_backoff: 1.millisecond,
        max_backoff: 2.milliseconds,
        circuit_failure_threshold: 5,
        circuit_reset_timeout: 1.second
      )
      worker = Movie::Persistence::ConnectionWorker.new(
        backend,
        "retry-worker",
        8,
        telemetry,
        policy
      )
      worker.execute(retryable: true) { |connection| connection.ensure_event_store }

      events = worker.execute(retryable: true) do |connection|
        connection.load_events(Movie::Persistence::LoadEvents.new("retry-stream"))
      end
      events.should be_empty
      probe.connections.get.should eq(3)
      telemetry.metrics.retries.should eq(2_i64)
      telemetry.metrics.reconnects.should eq(2_i64)
      telemetry.health.status.should eq(Movie::Persistence::HealthStatus::Healthy)
    ensure
      worker.try &.close
      File.delete(path) if path && File.exists?(path)
    end

    it "opens after consecutive connection failures and closes after a successful probe" do
      path = "/tmp/movie_circuit_#{UUID.random}.sqlite3"
      probe = Movie::RetryFailureProbe.new(2)
      backend = Movie::RetrySQLiteBackend.new("sqlite3:#{path}", probe)
      telemetry = Movie::Persistence::Telemetry.new
      policy = Movie::Persistence::ResiliencePolicy.new(
        max_retries: 0,
        min_backoff: Time::Span.zero,
        max_backoff: Time::Span.zero,
        circuit_failure_threshold: 2,
        circuit_reset_timeout: 10.milliseconds
      )
      worker = Movie::Persistence::ConnectionWorker.new(
        backend,
        "circuit-worker",
        8,
        telemetry,
        policy
      )
      worker.execute(retryable: true) { |connection| connection.ensure_event_store }
      request = Movie::Persistence::LoadEvents.new("circuit-stream")

      2.times do
        expect_raises(Movie::RetryConnectionLost) do
          worker.execute(retryable: true) { |connection| connection.load_events(request) }
        end
      end
      expect_raises(Movie::Persistence::CircuitOpenError) do
        worker.execute(retryable: true) { |connection| connection.load_events(request) }
      end
      telemetry.health.status.should eq(Movie::Persistence::HealthStatus::Unavailable)

      sleep 15.milliseconds
      worker.execute(retryable: true) { |connection| connection.load_events(request) }
        .should be_empty
      telemetry.metrics.circuit_opens.should eq(1_i64)
      telemetry.health.status.should eq(Movie::Persistence::HealthStatus::Healthy)
    ensure
      worker.try &.close
      File.delete(path) if path && File.exists?(path)
    end
  end
end
