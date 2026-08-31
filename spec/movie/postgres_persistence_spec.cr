require "../spec_helper"
require "../../src/movie"
require "../../src/movie/persistence/postgres"

module Movie
  struct PgAdded
    include JSON::Serializable
    getter amount : Int32

    def initialize(@amount : Int32)
    end
  end

  struct PgCounterState
    include JSON::Serializable
    getter value : Int32

    def initialize(@value : Int32 = 0)
    end
  end

  struct PgAdd
    getter amount : Int32
    getter operation_id : Persistence::OperationId

    def initialize(@amount : Int32, @operation_id : Persistence::OperationId)
    end
  end

  struct PgGet
  end

  alias PgCounterCommand = PgAdd | PgGet

  class PgCounterBehavior < EventSourcedBehavior(PgCounterCommand, PgAdded, PgCounterState)
    protected def empty_state : PgCounterState
      PgCounterState.new
    end

    protected def apply_event(state : PgCounterState, event : PgAdded) : PgCounterState
      PgCounterState.new(state.value + event.amount)
    end

    protected def handle_command(
      state : PgCounterState,
      command : PgCounterCommand,
      ctx : ActorContext(PgCounterCommand),
    ) : EventEffect(PgAdded, PgCounterState)
      case command
      when PgAdd
        sender = ctx.sender
        persist(PgAdded.new(command.amount), command.operation_id).then_run do |current|
          Ask.reply_if_asked(sender, current.value)
        end
      when PgGet
        sender = ctx.sender
        none.then_run { |current| Ask.reply_if_asked(sender, current.value) }
      else
        none
      end
    end
  end

  # Simulates the only unsafe observation a client can make around a committed
  # database write: PostgreSQL committed it, but the connection disappeared
  # before the caller received the result.
  class CommittedWriteConnectionLost < Exception
  end

  class CommitDisconnectProbe
    def initialize
      @pending = Atomic(Bool).new(true)
    end

    def trigger? : Bool
      _, changed = @pending.compare_and_set(true, false)
      changed
    end
  end

  class CommitThenDisconnectPostgresConnection < Persistence::PostgresBackendConnection
    def initialize(connection : DB::Connection, @disconnect_after_commit : CommitDisconnectProbe)
      super(connection)
    end

    def append_events(message : Persistence::AppendEvents) : Persistence::WriteResult
      result = super
      if @disconnect_after_commit.trigger?
        raise CommittedWriteConnectionLost.new("connection dropped after commit")
      end
      result
    end

    def connection_lost?(error : Exception) : Bool
      error.is_a?(CommittedWriteConnectionLost) || super
    end
  end

  class CommitThenDisconnectPostgresBackend < Persistence::PostgresBackend
    getter connections : Atomic(Int32)

    def initialize(uri : String)
      @connections = Atomic(Int32).new(0)
      @disconnect_after_commit = CommitDisconnectProbe.new
      super(uri)
    end

    def connect : Persistence::BackendConnection
      @connections.add(1)
      CommitThenDisconnectPostgresConnection.new(DB.connect(uri), @disconnect_after_commit)
    end
  end
end

if postgres_url = ENV["MOVIE_POSTGRES_TEST_URL"]?
  describe Movie::Persistence::PostgresBackend do
    it "is selected from configuration and preserves the persistence contract" do
      prefix = "pg-contract-#{UUID.random}"
      config = Movie::Config.builder
        .set("persistence.backend", "postgres")
        .set("persistence.connection-uri", postgres_url)
        .set("persistence.pool-size", 2)
        .build
      system = Movie::ActorSystem(Movie::SystemMessage).new(
        Movie::Behaviors(Movie::SystemMessage).same,
        config
      )
      database = Movie::Database.get(system)
      database.backend_name.should eq("postgres")
      event_store = system.spawn(Movie::Persistence::EventStoreActor.new(database.pool))
      state_store = system.spawn(Movie::Persistence::StateStoreActor.new(database.pool))

      events = [
        Movie::Persistence::SerializedEvent.new("Added", "one"),
        Movie::Persistence::SerializedEvent.new("Added", "two"),
      ]
      operation_id = Movie::Persistence::OperationId.new("#{prefix}-append")
      append = Movie::Persistence::AppendEvents.new(prefix, 0_i64, operation_id, events)
      result = system.ask(event_store, append, Movie::Persistence::WriteResult, 5.seconds).await(5.seconds)
      result.should eq(Movie::Persistence::WriteResult.new(2_i64, false))

      duplicate = system.ask(
        event_store,
        Movie::Persistence::AppendEvents.new(prefix, 2_i64, operation_id, events),
        Movie::Persistence::WriteResult,
        5.seconds
      ).await(5.seconds)
      duplicate.should eq(Movie::Persistence::WriteResult.new(2_i64, true))

      stored = system.ask(
        event_store,
        Movie::Persistence::LoadEvents.new(prefix),
        Array(Movie::Persistence::StoredEvent),
        5.seconds
      ).await(5.seconds)
      stored.map(&.payload).should eq(["one", "two"])

      snapshot = Movie::Persistence::SnapshotRecord.new(2_i64, "Counter", "2")
      system.ask(
        event_store,
        Movie::Persistence::SaveSnapshot.new(prefix, snapshot),
        Bool,
        5.seconds
      ).await(5.seconds).should be_true
      loaded_snapshot = system.ask(
        event_store,
        Movie::Persistence::LoadSnapshot.new(prefix),
        Movie::Persistence::SnapshotRecord?,
        5.seconds
      ).await(5.seconds)
      loaded_snapshot.should eq(snapshot)

      state_id = "#{prefix}-state"
      save_id = Movie::Persistence::OperationId.new("#{prefix}-save")
      saved = system.ask(
        state_store,
        Movie::Persistence::SaveState.new(state_id, 0_i64, save_id, "Profile", "value"),
        Movie::Persistence::WriteResult,
        5.seconds
      ).await(5.seconds)
      saved.should eq(Movie::Persistence::WriteResult.new(1_i64, false))

      delete_id = Movie::Persistence::OperationId.new("#{prefix}-delete")
      deleted = system.ask(
        state_store,
        Movie::Persistence::DeleteState.new(state_id, 1_i64, delete_id),
        Movie::Persistence::WriteResult,
        5.seconds
      ).await(5.seconds)
      deleted.should eq(Movie::Persistence::WriteResult.new(2_i64, false))
      state = system.ask(
        state_store,
        Movie::Persistence::LoadState.new(state_id),
        Movie::Persistence::StateRecord?,
        5.seconds
      ).await(5.seconds).not_nil!
      state.revision.should eq(2_i64)
      state.deleted.should be_true
    ensure
      system.try &.shutdown
    end

    it "allows one writer across actor systems and exposes the committed stream to the other" do
      stream = "pg-contended-#{UUID.random}"
      first_system = Movie::ActorSystem(Movie::SystemMessage).new(Movie::Behaviors(Movie::SystemMessage).same)
      second_system = Movie::ActorSystem(Movie::SystemMessage).new(Movie::Behaviors(Movie::SystemMessage).same)
      first_pool = first_system.spawn(
        Movie::Persistence::ConnectionPool.behavior(Movie::Persistence::PostgresBackend.new(postgres_url), 1)
      )
      second_pool = second_system.spawn(
        Movie::Persistence::ConnectionPool.behavior(Movie::Persistence::PostgresBackend.new(postgres_url), 1)
      )
      first_store = first_system.spawn(Movie::Persistence::EventStoreActor.new(first_pool))
      second_store = second_system.spawn(Movie::Persistence::EventStoreActor.new(second_pool))
      event = Movie::Persistence::SerializedEvent.new("Added", "payload")
      outcomes = Channel(String).new(2)

      [
        {first_system, first_store},
        {second_system, second_store},
      ].each do |system, store|
        future = system.ask(
          store,
          Movie::Persistence::AppendEvents.new(
            stream,
            0_i64,
            Movie::Persistence::OperationId.random,
            [event]
          ),
          Movie::Persistence::WriteResult,
          5.seconds
        )
        spawn do
          begin
            future.await(5.seconds)
            outcomes.send("success")
          rescue error : Movie::Persistence::ConcurrentWriteError
            outcomes.send("concurrent")
          rescue error
            outcomes.send(error.class.name)
          end
        end
      end

      [outcomes.receive, outcomes.receive].sort.should eq(["concurrent", "success"])
      events = second_system.ask(
        second_store,
        Movie::Persistence::LoadEvents.new(stream),
        Array(Movie::Persistence::StoredEvent),
        5.seconds
      ).await(5.seconds)
      events.map(&.payload).should eq(["payload"])

      retry_stream = "#{stream}-retry"
      retry_operation = Movie::Persistence::OperationId.random
      retry_outcomes = Channel(Movie::Persistence::WriteResult).new(2)
      [
        {first_system, first_store},
        {second_system, second_store},
      ].each do |system, store|
        future = system.ask(
          store,
          Movie::Persistence::AppendEvents.new(retry_stream, 0_i64, retry_operation, [event]),
          Movie::Persistence::WriteResult,
          5.seconds
        )
        spawn { retry_outcomes.send(future.await(5.seconds)) }
      end
      retry_results = [retry_outcomes.receive, retry_outcomes.receive]
      retry_results.map(&.revision).should eq([1_i64, 1_i64])
      retry_results.count(&.duplicate).should eq(1)
    ensure
      first_system.try &.shutdown
      second_system.try &.shutdown
    end

    it "recovers an event-sourced entity on another actor system" do
      entity_id = "counter-#{UUID.random}"
      config = Movie::Config.builder
        .set("persistence.backend", "postgres")
        .set("persistence.connection-uri", postgres_url)
        .build

      first_system = Movie::ActorSystem(Movie::SystemMessage).new(
        Movie::Behaviors(Movie::SystemMessage).same,
        config
      )
      first_extension = Movie::EventSourcing.get(first_system)
      first_type = first_extension.register_entity(Movie::PgCounterBehavior, Movie::PgCounterCommand) do |id, store|
        Movie::PgCounterBehavior.new(id.persistence_id, store)
      end
      first_ref = first_extension.get_entity_ref(first_type.id(entity_id))
      first_system.ask(
        first_ref,
        Movie::PgAdd.new(3, Movie::Persistence::OperationId.random),
        Int32,
        5.seconds
      ).await(5.seconds).should eq(3)
      first_system.shutdown

      second_system = Movie::ActorSystem(Movie::SystemMessage).new(
        Movie::Behaviors(Movie::SystemMessage).same,
        config
      )
      second_extension = Movie::EventSourcing.get(second_system)
      second_type = second_extension.register_entity(Movie::PgCounterBehavior, Movie::PgCounterCommand) do |id, store|
        Movie::PgCounterBehavior.new(id.persistence_id, store)
      end
      second_ref = second_extension.get_entity_ref(second_type.id(entity_id))
      second_system.ask(second_ref, Movie::PgGet.new, Int32, 5.seconds).await(5.seconds).should eq(3)
    ensure
      first_system.try &.shutdown
      second_system.try &.shutdown
    end

    it "deduplicates an ambiguous committed write after reconnect" do
      stream = "pg-ambiguous-write-#{UUID.random}"
      operation_id = Movie::Persistence::OperationId.random
      request = Movie::Persistence::AppendEvents.new(
        stream,
        0_i64,
        operation_id,
        [Movie::Persistence::SerializedEvent.new("Added", "once")]
      )
      backend = Movie::CommitThenDisconnectPostgresBackend.new(postgres_url)
      worker = Movie::Persistence::ConnectionWorker.new(backend, "movie-pg-ambiguous-write")
      worker.execute { |connection| connection.ensure_event_store }

      expect_raises(Movie::CommittedWriteConnectionLost, "connection dropped after commit") do
        worker.execute { |connection| request.execute(connection) }
      end

      retried = worker.execute { |connection| request.execute(connection) }
      retried.should eq(Movie::Persistence::WriteResult.new(1_i64, true))
      backend.connections.get.should eq(2)

      events = worker.execute do |connection|
        Movie::Persistence::LoadEvents.new(stream).execute(connection)
      end
      events.map(&.payload).should eq(["once"])
    ensure
      worker.try &.close
    end

    it "fails the in-flight request and reconnects after PostgreSQL terminates the connection" do
      application_name = "movie_failover_#{UUID.random.to_s.gsub('-', '_')}"
      separator = postgres_url.includes?('?') ? '&' : '?'
      worker_url = "#{postgres_url}#{separator}application_name=#{application_name}"
      system = Movie::ActorSystem(Movie::SystemMessage).new(Movie::Behaviors(Movie::SystemMessage).same)
      pool = system.spawn(
        Movie::Persistence::ConnectionPool.behavior(Movie::Persistence::PostgresBackend.new(worker_url), 1)
      )
      store = system.spawn(Movie::Persistence::EventStoreActor.new(pool))

      system.ask(
        store,
        Movie::Persistence::LoadEvents.new("failover-probe"),
        Array(Movie::Persistence::StoredEvent),
        5.seconds
      ).await(5.seconds)

      DB.open(postgres_url) do |admin|
        admin.query_one(
          "SELECT pg_terminate_backend(pid) FROM pg_stat_activity " +
          "WHERE application_name = $1 AND pid <> pg_backend_pid() LIMIT 1",
          application_name,
          as: Bool
        ).should be_true
      end

      expect_raises(DB::ConnectionLost) do
        system.ask(
          store,
          Movie::Persistence::LoadEvents.new("failover-probe"),
          Array(Movie::Persistence::StoredEvent),
          5.seconds
        ).await(5.seconds)
      end

      recovered = system.ask(
        store,
        Movie::Persistence::LoadEvents.new("failover-probe"),
        Array(Movie::Persistence::StoredEvent),
        5.seconds
      ).await(5.seconds)
      recovered.should be_empty
    ensure
      system.try &.shutdown
    end

    it "rejects PostgreSQL configuration without a connection URI" do
      config = Movie::Config.builder
        .set("persistence.backend", "postgres")
        .build
      system = Movie::ActorSystem(Movie::SystemMessage).new(
        Movie::Behaviors(Movie::SystemMessage).same,
        config
      )

      expect_raises(
        Movie::Persistence::BackendConfigurationError,
        /persistence.connection-uri is required/
      ) do
        Movie::Database.get(system)
      end
    ensure
      system.try &.shutdown
    end

    it "adopts a populated Epic 16 journal in an isolated PostgreSQL schema" do
      schema_name = "movie_epic17_#{UUID.random.to_s.delete('-')}"
      admin = DB.connect(postgres_url)
      admin.exec("CREATE SCHEMA #{schema_name}")
      raw_connection = DB.connect(postgres_url)
      raw_connection.exec("SET search_path TO #{schema_name}")
      raw_connection.exec(<<-SQL)
        CREATE TABLE event_journal (
          persistence_id TEXT NOT NULL,
          sequence_nr BIGINT NOT NULL,
          manifest TEXT NOT NULL,
          payload TEXT NOT NULL,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (persistence_id, sequence_nr)
        )
      SQL
      raw_connection.exec(
        "INSERT INTO event_journal (persistence_id, sequence_nr, manifest, payload) VALUES ($1, $2, $3, $4)",
        "adopted-postgres",
        1_i64,
        "Added",
        "legacy-value"
      )
      connection = Movie::Persistence::PostgresBackendConnection.new(raw_connection)

      connection.ensure_schema
      connection.schema_version.should eq(Movie::Persistence::CURRENT_SCHEMA_VERSION)
      connection.query_events(
        Movie::Persistence::QueryEvents.new(0_i64, 10, "adopted-postgres")
      ).events.map(&.payload).should eq(["legacy-value"])
    ensure
      connection.try &.close
      raw_connection.try { |raw| raw.close unless raw.closed? }
      if admin && schema_name
        admin.exec("DROP SCHEMA IF EXISTS #{schema_name} CASCADE")
      end
      admin.try &.close
    end

    it "serializes event commits before PostgreSQL allocates global offsets" do
      persistence_id = "pg-commit-order-#{UUID.random}"
      connection = Movie::Persistence::PostgresBackend.new(postgres_url).connect
      connection.ensure_schema
      blocker = DB.connect(postgres_url)
      transaction = blocker.begin_transaction
      transaction.connection.query_each(
        "SELECT pg_advisory_xact_lock(#{Movie::Persistence::PostgresBackendConnection::EVENT_COMMIT_LOCK_ID})"
      ) { |row| row.read }
      completed = Channel(Exception?).new(1)

      spawn do
        begin
          connection.append_events(
            Movie::Persistence::AppendEvents.new(
              persistence_id,
              0_i64,
              Movie::Persistence::OperationId.random,
              [Movie::Persistence::SerializedEvent.new("Added", "ordered")]
            )
          )
          completed.send(nil)
        rescue error
          completed.send(error)
        end
      end

      select
      when early = completed.receive
        raise early if early
        fail("PostgreSQL append bypassed the global event commit lock")
      when timeout(50.milliseconds)
      end

      transaction.commit
      outcome = completed.receive
      raise outcome if outcome
    ensure
      transaction.try { |current| current.rollback unless current.closed? }
      blocker.try &.close
      connection.try &.close
    end

    it "supports migrations, query checkpoints, retention, and transactional outbox" do
      persistence_id = "pg-production-#{UUID.random}"
      projection_name = "pg-projection-#{persistence_id}"
      connection = Movie::Persistence::PostgresBackend.new(postgres_url).connect
      connection.ensure_schema
      connection.schema_version.should eq(Movie::Persistence::CURRENT_SCHEMA_VERSION)

      operation_id = Movie::Persistence::OperationId.random
      outbox = Movie::Persistence::OutboxEntry.new(
        "pg-outbox-#{UUID.random}",
        "billing",
        "InvoiceRequested",
        "payload"
      )
      connection.append_events(
        Movie::Persistence::AppendEvents.new(
          persistence_id,
          0_i64,
          operation_id,
          [
            Movie::Persistence::SerializedEvent.new("Added", "one"),
            Movie::Persistence::SerializedEvent.new("Added", "two"),
          ],
          [outbox]
        )
      ).revision.should eq(2_i64)

      page = connection.query_events(
        Movie::Persistence::QueryEvents.new(0_i64, 10, persistence_id)
      )
      page.events.map(&.payload).should eq(["one", "two"])
      connection.save_snapshot(
        Movie::Persistence::SaveSnapshot.new(
          persistence_id,
          Movie::Persistence::SnapshotRecord.new(2_i64, "Counter", "2")
        )
      )
      connection.save_projection_offset(
        Movie::Persistence::SaveProjectionOffset.new(projection_name, 0_i64)
      )
      expect_raises(Movie::Persistence::ProjectionBehindRetentionError) do
        connection.delete_events_to(
          Movie::Persistence::DeleteEventsTo.new(persistence_id, 2_i64)
        )
      end
      global_offset = 0_i64
      loop do
        global_page = connection.query_events(
          Movie::Persistence::QueryEvents.new(global_offset, 1_000)
        )
        global_offset = global_page.next_offset
        break unless global_page.has_more
      end
      connection.save_projection_offset(
        Movie::Persistence::SaveProjectionOffset.new(projection_name, global_offset)
      )
      connection.delete_events_to(
        Movie::Persistence::DeleteEventsTo.new(persistence_id, 2_i64)
      ).deleted_events.should eq(2_i64)

      claim = Movie::Persistence::ClaimOutbox.for("pg-dispatcher", 10)
      claimed = connection.claim_outbox(claim).select { |entry| entry.message_id == outbox.message_id }
      claimed.size.should eq(1)
      connection.acknowledge_outbox(
        Movie::Persistence::AcknowledgeOutbox.new("pg-dispatcher", [outbox.message_id])
      ).should eq(1_i64)
      connection.append_events(
        Movie::Persistence::AppendEvents.new(
          persistence_id,
          2_i64,
          operation_id,
          [
            Movie::Persistence::SerializedEvent.new("Added", "one"),
            Movie::Persistence::SerializedEvent.new("Added", "two"),
          ],
          [outbox]
        )
      ).duplicate.should be_true
    ensure
      if connection && projection_name
        begin
          connection.delete_projection_offset(
            Movie::Persistence::DeleteProjectionOffset.new(projection_name)
          )
        rescue
        end
      end
      connection.try &.close
    end
  end
end
