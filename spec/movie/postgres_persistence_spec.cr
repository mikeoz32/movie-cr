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
  end
end
