require "../spec_helper"
require "../../src/movie"
require "../../src/movie/persistence"

module Movie
  struct StoreProbeStart
  end

  alias StoreProbeMessage = StoreProbeStart
  STORE_TIMEOUT = 5.seconds

  class EventStoreProbe < AbstractBehavior(StoreProbeMessage)
    def initialize(
      @store : ActorRef(Movie::Persistence::EventStoreMessage),
      @promise : Promise(Array(Movie::Persistence::StoredEvent)),
    )
    end

    def receive(message : StoreProbeMessage, context : ActorContext(StoreProbeMessage))
      case message
      when StoreProbeStart
        events = [
          Movie::Persistence::SerializedEvent.new("Added", "a"),
          Movie::Persistence::SerializedEvent.new("Added", "b"),
        ]
        context.ask(@store, Movie::Persistence::AppendEvents.new("stream-1", 0_i64, events), Int64).await(STORE_TIMEOUT)
        events = context.ask(
          @store,
          Movie::Persistence::LoadEvents.new("stream-1", 0_i64),
          Array(Movie::Persistence::StoredEvent)
        ).await(STORE_TIMEOUT)
        @promise.try_success(events)
      end
      Behaviors(StoreProbeMessage).same
    end
  end

  class StateStoreProbe < AbstractBehavior(StoreProbeMessage)
    def initialize(
      @store : ActorRef(Movie::Persistence::StateStoreMessage),
      @promise : Promise(Movie::Persistence::StateRecord?),
    )
    end

    def receive(message : StoreProbeMessage, context : ActorContext(StoreProbeMessage))
      case message
      when StoreProbeStart
        context.ask(
          @store,
          Movie::Persistence::SaveState.new("entity-1", 0_i64, "Name", "payload"),
          Int64
        ).await(STORE_TIMEOUT)
        value = context.ask(
          @store,
          Movie::Persistence::LoadState.new("entity-1"),
          Movie::Persistence::StateRecord?
        ).await(STORE_TIMEOUT)
        @promise.try_success(value)
      end
      Behaviors(StoreProbeMessage).same
    end
  end

  class ConcurrentPoolProbe < AbstractBehavior(Persistence::ConnectionMessage)
    getter max_active : Atomic(Int32)

    def initialize
      @active = Atomic(Int32).new(0)
      @max_active = Atomic(Int32).new(0)
    end

    def receive(message : Persistence::ConnectionMessage, context : ActorContext(Persistence::ConnectionMessage))
      case message
      when Persistence::EnsureEventStore
        Ask.reply_if_asked(context.sender, true)
      when Persistence::AppendEvents
        sender = context.sender
        active = @active.add(1) + 1
        loop do
          maximum = @max_active.get
          break if maximum >= active
          _, changed = @max_active.compare_and_set(maximum, active)
          break if changed
        end
        spawn do
          sleep 50.milliseconds
          @active.sub(1)
          Ask.reply_if_asked(sender, message.expected_revision + message.events.size)
        end
      end
      Behaviors(Persistence::ConnectionMessage).same
    end
  end
end

describe "Movie persistence store actors" do
  it "allows independent journal requests to use the connection pool concurrently" do
    system = Movie::ActorSystem(Movie::SystemMessage).new(Movie::Behaviors(Movie::SystemMessage).same)
    pool_behavior = Movie::ConcurrentPoolProbe.new
    pool = system.spawn(pool_behavior)
    store = system.spawn(Movie::Persistence::EventStoreActor.new(pool))
    event = Movie::Persistence::SerializedEvent.new("Added", "payload")

    first = system.ask(store, Movie::Persistence::AppendEvents.new("a", 0_i64, [event]), Int64, 1.second)
    second = system.ask(store, Movie::Persistence::AppendEvents.new("b", 0_i64, [event]), Int64, 1.second)

    first.await(1.second).should eq(1_i64)
    second.await(1.second).should eq(1_i64)
    pool_behavior.max_active.get.should be >= 2
  ensure
    system.try &.shutdown
  end

  it "allows only one concurrent writer at the same expected revision" do
    path = "/tmp/movie_concurrent_journal_#{UUID.random}.sqlite3"
    system = Movie::ActorSystem(Movie::SystemMessage).new(Movie::Behaviors(Movie::SystemMessage).same)
    pool = system.spawn(Movie::Persistence::ConnectionPool.behavior("sqlite3:#{path}", 2))
    store = system.spawn(Movie::Persistence::EventStoreActor.new(pool))
    event = Movie::Persistence::SerializedEvent.new("Added", "payload")
    results = Channel(String).new(2)

    2.times do
      future = system.ask(
        store,
        Movie::Persistence::AppendEvents.new("contended", 0_i64, [event]),
        Int64,
        2.seconds
      )
      spawn do
        begin
          future.await(2.seconds)
          results.send("success")
        rescue error : Movie::Persistence::ConcurrentWriteError
          results.send("concurrent")
        rescue error
          results.send("#{error.class.name}: #{error.message}")
        end
      end
    end

    [results.receive, results.receive].sort.should eq(["concurrent", "success"])
  ensure
    system.try &.shutdown
    File.delete(path) if path && File.exists?(path)
  end

  it "migrates the pre-revision persistence schema without losing data" do
    path = "/tmp/movie_persistence_migration_#{UUID.random}.sqlite3"
    db_uri = "sqlite3:#{path}"
    DB.open(db_uri) do |db|
      db.exec(<<-SQL)
        CREATE TABLE event_journal (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          persistence_id TEXT NOT NULL,
          payload TEXT NOT NULL,
          created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
      SQL
      db.exec("INSERT INTO event_journal (persistence_id, payload) VALUES ('legacy-stream', 'one'), ('legacy-stream', 'two')")
      db.exec(<<-SQL)
        CREATE TABLE durable_state (
          persistence_id TEXT PRIMARY KEY,
          payload TEXT NOT NULL,
          updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
      SQL
      db.exec("INSERT INTO durable_state (persistence_id, payload) VALUES ('legacy-state', 'value')")
    end

    system = Movie::ActorSystem(Movie::SystemMessage).new(Movie::Behaviors(Movie::SystemMessage).same)
    pool = system.spawn(Movie::Persistence::ConnectionPool.behavior(db_uri, 1))
    event_store = system.spawn(Movie::Persistence::EventStoreActor.new(pool))
    state_store = system.spawn(Movie::Persistence::StateStoreActor.new(pool))

    events = system.ask(
      event_store,
      Movie::Persistence::LoadEvents.new("legacy-stream"),
      Array(Movie::Persistence::StoredEvent),
      2.seconds
    ).await(2.seconds)
    events.map(&.sequence_nr).should eq([1_i64, 2_i64])
    events.map(&.manifest).should eq(["legacy", "legacy"])
    events.map(&.payload).should eq(["one", "two"])

    state = system.ask(
      state_store,
      Movie::Persistence::LoadState.new("legacy-state"),
      Movie::Persistence::StateRecord?,
      2.seconds
    ).await(2.seconds)
    state.should_not be_nil
    state.not_nil!.revision.should eq(1_i64)
    state.not_nil!.manifest.should eq("legacy")
    state.not_nil!.payload.should eq("value")
  ensure
    system.try &.shutdown
    File.delete(path) if path && File.exists?(path)
  end

  it "propagates the original database error to ask callers" do
    path = "/tmp/movie_database_error_#{UUID.random}.sqlite3"
    db_uri = "sqlite3:#{path}"
    system = Movie::ActorSystem(Movie::SystemMessage).new(Movie::Behaviors(Movie::SystemMessage).same)
    pool = system.spawn(Movie::Persistence::ConnectionPool.behavior(db_uri, 1))

    expect_raises(SQLite3::Exception, /no such table/) do
      system.ask(
        pool,
        Movie::Persistence::DbExec.new("INSERT INTO missing_table (value) VALUES (1)"),
        Bool,
        1.second
      ).await(1.second)
    end
  ensure
    system.try &.shutdown
    File.delete(path) if path && File.exists?(path)
  end

  it "creates the configured database parent directory" do
    root = "/tmp/movie_database_#{UUID.random}"
    db_path = "#{root}/nested/movie.sqlite3"
    config = Movie::Config.builder
      .set("persistence.db-path", db_path)
      .build
    system = Movie::ActorSystem(Movie::SystemMessage).new(Movie::Behaviors(Movie::SystemMessage).same, config)
    database = Movie::Database.get(system)

    system.ask(database.pool, Movie::Persistence::DbExec.new("CREATE TABLE path_probe (id INTEGER)"), Bool, 2.seconds).await(2.seconds)
    File.exists?(db_path).should be_true
  ensure
    system.try &.shutdown
    File.delete(db_path) if db_path && File.exists?(db_path)
    nested = "#{root}/nested" if root
    Dir.delete(nested) if nested && Dir.exists?(nested)
    Dir.delete(root) if root && Dir.exists?(root)
  end

  it "appends and reads events through the event store actor" do
    path = "/tmp/movie_event_store_#{UUID.random}.sqlite3"
    db_uri = "sqlite3:#{path}"
    system = Movie::ActorSystem(Movie::SystemMessage).new(Movie::Behaviors(Movie::SystemMessage).same)

    pool = system.spawn(Movie::Persistence::ConnectionPool.behavior(db_uri, 1))
    store = system.spawn(Movie::Persistence::EventStoreActor.new(pool))

    promise = Movie::Promise(Array(Movie::Persistence::StoredEvent)).new
    probe = system.spawn(Movie::EventStoreProbe.new(store, promise))
    probe << Movie::StoreProbeStart.new

    events = promise.future.await(10.seconds)
    events.map(&.sequence_nr).should eq([1_i64, 2_i64])
    events.map(&.manifest).should eq(["Added", "Added"])
    events.map(&.payload).should eq(["a", "b"])

    error = expect_raises(Movie::Persistence::ConcurrentWriteError) do
      system.ask(
        store,
        Movie::Persistence::AppendEvents.new(
          "stream-1",
          0_i64,
          [Movie::Persistence::SerializedEvent.new("Added", "duplicate")]
        ),
        Int64,
        2.seconds
      ).await(2.seconds)
    end
    error.expected_revision.should eq(0_i64)
    error.actual_revision.should eq(2_i64)
  end

  it "saves and loads state through the state store actor" do
    path = "/tmp/movie_state_store_#{UUID.random}.sqlite3"
    db_uri = "sqlite3:#{path}"
    system = Movie::ActorSystem(Movie::SystemMessage).new(Movie::Behaviors(Movie::SystemMessage).same)

    pool = system.spawn(Movie::Persistence::ConnectionPool.behavior(db_uri, 1))
    store = system.spawn(Movie::Persistence::StateStoreActor.new(pool))

    promise = Movie::Promise(Movie::Persistence::StateRecord?).new
    probe = system.spawn(Movie::StateStoreProbe.new(store, promise))
    probe << Movie::StoreProbeStart.new

    value = promise.future.await(10.seconds)
    value.should_not be_nil
    value.not_nil!.revision.should eq(1_i64)
    value.not_nil!.manifest.should eq("Name")
    value.not_nil!.payload.should eq("payload")
  end
end
