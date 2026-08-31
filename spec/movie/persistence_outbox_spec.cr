require "../spec_helper"
require "../../src/movie"
require "../../src/movie/persistence"
describe "Movie persistence production contracts" do
  describe "transactional outbox" do
    it "commits, leases, retries, and acknowledges event outbox records idempotently" do
      path = "/tmp/movie_outbox_#{UUID.random}.sqlite3"
      connection = Movie::Persistence::SQLiteBackend.new("sqlite3:#{path}").connect
      connection.ensure_event_store
      operation_id = Movie::Persistence::OperationId.random
      outbox = Movie::Persistence::OutboxEntry.new(
        "message-1",
        "billing",
        "InvoiceRequested",
        %({"invoice":"one"})
      )
      append = Movie::Persistence::AppendEvents.new(
        "order-1",
        0_i64,
        operation_id,
        [Movie::Persistence::SerializedEvent.new("OrderPlaced", "one")],
        [outbox]
      )

      connection.append_events(append)
        .should eq(Movie::Persistence::WriteResult.new(1_i64, false))
      connection.append_events(append)
        .should eq(Movie::Persistence::WriteResult.new(1_i64, true))
      expect_raises(Movie::Persistence::OperationConflictError) do
        connection.append_events(
          Movie::Persistence::AppendEvents.new(
            "order-1",
            0_i64,
            operation_id,
            [
              Movie::Persistence::SerializedEvent.new("OrderPlaced", "one"),
              Movie::Persistence::SerializedEvent.new("message-1", "billing"),
              Movie::Persistence::SerializedEvent.new("InvoiceRequested", %({"invoice":"one"})),
            ]
          )
        )
      end

      lease = Movie::Persistence::ClaimOutbox.new(
        "dispatcher-a",
        10,
        Time.utc.to_unix_ms,
        Time.utc.to_unix_ms + 30_000_i64
      )
      claimed = connection.claim_outbox(lease)
      claimed.size.should eq(1)
      record = claimed.first
      record.message_id.should eq("message-1")
      record.persistence_id.should eq("order-1")
      record.operation_id.should eq(operation_id)
      record.destination.should eq("billing")
      record.attempts.should eq(0_i64)

      connection.claim_outbox(
        Movie::Persistence::ClaimOutbox.new(
          "dispatcher-b",
          10,
          lease.now_epoch_ms,
          lease.lease_until_epoch_ms
        )
      ).should be_empty

      connection.release_outbox(
        Movie::Persistence::ReleaseOutbox.new("dispatcher-a", "message-1", "temporary failure")
      ).should be_true
      retried = connection.claim_outbox(
        Movie::Persistence::ClaimOutbox.new(
          "dispatcher-b",
          10,
          lease.now_epoch_ms,
          lease.lease_until_epoch_ms
        )
      ).first
      retried.attempts.should eq(1_i64)
      retried.last_error.should eq("temporary failure")

      connection.acknowledge_outbox(
        Movie::Persistence::AcknowledgeOutbox.new("dispatcher-b", ["message-1"])
      ).should eq(1_i64)
      connection.acknowledge_outbox(
        Movie::Persistence::AcknowledgeOutbox.new("dispatcher-b", ["message-1"])
      ).should eq(0_i64)
      connection.claim_outbox(lease).should be_empty
    ensure
      connection.try &.close
      File.delete(path) if path && File.exists?(path)
    end

    it "commits durable state and its outbox record atomically" do
      path = "/tmp/movie_state_outbox_#{UUID.random}.sqlite3"
      connection = Movie::Persistence::SQLiteBackend.new("sqlite3:#{path}").connect
      connection.ensure_state_store
      operation_id = Movie::Persistence::OperationId.random
      outbox = Movie::Persistence::OutboxEntry.new(
        "state-success-message",
        "search",
        "ProfileChanged",
        "payload"
      )
      write = Movie::Persistence::SaveState.new(
        "profile-success",
        0_i64,
        operation_id,
        "Profile",
        "current",
        [outbox]
      )

      connection.save_state(write)
        .should eq(Movie::Persistence::WriteResult.new(1_i64, false))
      connection.save_state(write)
        .should eq(Movie::Persistence::WriteResult.new(1_i64, true))
      connection.load_state(Movie::Persistence::LoadState.new("profile-success"))
        .not_nil!.payload.should eq("current")
      claimed = connection.claim_outbox(
        Movie::Persistence::ClaimOutbox.for("state-dispatcher", 10)
      )
      claimed.map(&.message_id).should eq(["state-success-message"])
    ensure
      connection.try &.close
      File.delete(path) if path && File.exists?(path)
    end

    it "rolls back outbox records when the durable write fails" do
      path = "/tmp/movie_outbox_rollback_#{UUID.random}.sqlite3"
      connection = Movie::Persistence::SQLiteBackend.new("sqlite3:#{path}").connect
      connection.ensure_state_store
      outbox = Movie::Persistence::OutboxEntry.new(
        "state-message",
        "search",
        "ProfileChanged",
        "payload"
      )
      connection.save_state(
        Movie::Persistence::SaveState.new(
          "profile-1",
          0_i64,
          Movie::Persistence::OperationId.random,
          "Profile",
          "first"
        )
      )

      expect_raises(Movie::Persistence::ConcurrentWriteError) do
        connection.save_state(
          Movie::Persistence::SaveState.new(
            "profile-1",
            0_i64,
            Movie::Persistence::OperationId.random,
            "Profile",
            "stale",
            [outbox]
          )
        )
      end

      connection.claim_outbox(
        Movie::Persistence::ClaimOutbox.new(
          "dispatcher",
          10,
          Time.utc.to_unix_ms,
          Time.utc.to_unix_ms + 30_000_i64
        )
      ).should be_empty
    ensure
      connection.try &.close
      File.delete(path) if path && File.exists?(path)
    end
  end

  describe "projection and outbox runtime APIs" do
    it "checkpoints each processed event and resumes after a projection failure" do
      path = "/tmp/movie_projection_runner_#{UUID.random}.sqlite3"
      config = Movie::Config.builder.set("persistence.db-path", path).build
      system = Movie::ActorSystem(Movie::SystemMessage).new(
        Movie::Behaviors(Movie::SystemMessage).same,
        config
      )
      database = Movie::Database.get(system)
      system.ask(database.pool, Movie::Persistence::EnsureEventStore.new, Bool, 5.seconds).await(5.seconds)
      system.ask(
        database.pool,
        Movie::Persistence::AppendEvents.new(
          "projection-runner",
          0_i64,
          Movie::Persistence::OperationId.random,
          %w(one two three).map { |payload| Movie::Persistence::SerializedEvent.new("Added", payload) }
        ),
        Movie::Persistence::WriteResult,
        5.seconds
      ).await(5.seconds)

      runner = Movie::Persistence::ProjectionRunner.new(database, "runner", page_size: 2)
      processed = [] of String
      expect_raises(Exception, "projection failed") do
        runner.run_once do |event|
          raise "projection failed" if event.payload == "two"
          processed << event.payload
        end
      end
      processed.should eq(["one"])
      database.projection_offset("runner").should be > 0_i64

      result = runner.run_once { |event| processed << event.payload }
      processed.should eq(["one", "two", "three"])
      result.processed.should eq(2)
      result.has_more.should be_false
      database.projection_offset("runner").should eq(result.offset)
    ensure
      system.try &.shutdown
      File.delete(path) if path && File.exists?(path)
    end

    it "publishes effect outbox records through the leased dispatcher" do
      path = "/tmp/movie_outbox_dispatcher_#{UUID.random}.sqlite3"
      config = Movie::Config.builder.set("persistence.db-path", path).build
      system = Movie::ActorSystem(Movie::SystemMessage).new(
        Movie::Behaviors(Movie::SystemMessage).same,
        config
      )
      database = Movie::Database.get(system)
      system.ask(database.pool, Movie::Persistence::EnsureEventStore.new, Bool, 5.seconds).await(5.seconds)
      entry = Movie::Persistence::OutboxEntry.new("dispatch-1", "mail", "Welcome", "hello")
      effect = Movie::EventEffect(String, String).new(
        ["created"],
        Movie::Persistence::OperationId.random
      ).then_publish(entry)
      effect.outbox.should eq([entry])
      system.ask(
        database.pool,
        Movie::Persistence::AppendEvents.new(
          "dispatch-stream",
          0_i64,
          effect.operation_id.not_nil!,
          [Movie::Persistence::SerializedEvent.new("Created", "created")],
          effect.outbox
        ),
        Movie::Persistence::WriteResult,
        5.seconds
      ).await(5.seconds)

      delivered = [] of String
      dispatcher = Movie::Persistence::OutboxDispatcher.new(
        database,
        "dispatcher",
        batch_size: 10
      )
      result = dispatcher.run_once { |message| delivered << message.message_id }
      result.claimed.should eq(1)
      result.delivered.should eq(1)
      result.failed.should eq(0)
      delivered.should eq(["dispatch-1"])
      dispatcher.run_once { |_message| raise "must not redeliver" }.claimed.should eq(0)
    ensure
      system.try &.shutdown
      File.delete(path) if path && File.exists?(path)
    end
  end
end
