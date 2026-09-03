require "../spec_helper"
require "../../src/movie"
require "../../src/movie/persistence"

private struct FencedAdded
  include JSON::Serializable
  getter amount : Int32

  def initialize(@amount : Int32)
  end
end

private struct FencedCounterState
  include JSON::Serializable
  getter value : Int32

  def initialize(@value : Int32 = 0)
  end
end

private struct FencedAdd
  getter amount : Int32
  getter operation_id : Movie::Persistence::OperationId

  def initialize(@amount : Int32, @operation_id : Movie::Persistence::OperationId)
  end
end

private struct FencedGet
  getter reply : Channel(Int32)

  def initialize(@reply : Channel(Int32))
  end
end

alias FencedCounterCommand = FencedAdd | FencedGet

private class FencedCounterBehavior < Movie::EventSourcedBehavior(
  FencedCounterCommand,
  FencedAdded,
  FencedCounterState,
)
  def initialize(
    persistence_id : String,
    store : Movie::Persistence::EventStoreClient,
    @failures : Channel(String),
  )
    super(persistence_id, store)
  end

  protected def empty_state : FencedCounterState
    FencedCounterState.new
  end

  protected def apply_event(state : FencedCounterState, event : FencedAdded) : FencedCounterState
    FencedCounterState.new(state.value + event.amount)
  end

  protected def handle_command(
    state : FencedCounterState,
    command : FencedCounterCommand,
    ctx : Movie::ActorContext(FencedCounterCommand),
  ) : Movie::EventEffect(FencedAdded, FencedCounterState)
    case command
    when FencedAdd
      persist(FencedAdded.new(command.amount), command.operation_id)
    when FencedGet
      none.then_run { |current| command.reply.send(current.value) }
    else
      none
    end
  end

  protected def on_persist_failure(error : Exception) : Nil
    @failures.send(error.class.name)
  end
end

private struct ClusteredNoop
  include JSON::Serializable
end

private class ClusteredNoopBehavior < Movie::EventSourcedBehavior(
  ClusteredNoop,
  FencedAdded,
  FencedCounterState,
)
  protected def empty_state : FencedCounterState
    FencedCounterState.new
  end

  protected def apply_event(state : FencedCounterState, event : FencedAdded) : FencedCounterState
    FencedCounterState.new(state.value + event.amount)
  end

  protected def handle_command(
    state : FencedCounterState,
    command : ClusteredNoop,
    ctx : Movie::ActorContext(ClusteredNoop),
  ) : Movie::EventEffect(FencedAdded, FencedCounterState)
    none
  end
end

describe "Movie persistence shard fencing" do
  it "exposes lease operations through the database extension" do
    path = "/tmp/movie_fence_extension_#{UUID.random}.sqlite3"
    config = Movie::Config.builder.set("persistence.db-path", path).build
    system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, config)
    database = Movie::Database.get(system)
    key = Movie::Persistence::ShardLeaseKey.new("orders", "Order", 1)

    token = database.acquire_shard_lease(key, "node-a", 5.seconds).not_nil!
    database.renew_shard_lease(token, 5.seconds).not_nil!.epoch.should eq(token.epoch)
    database.release_shard_lease(token).should be_true
  ensure
    system.try &.shutdown
    File.delete(path) if path && File.exists?(path)
  end

  it "injects a shard fence into an event-sourced entity store" do
    path = "/tmp/movie_fenced_entity_#{UUID.random}.sqlite3"
    config = Movie::Config.builder.set("persistence.db-path", path).build
    system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, config)
    failures = Channel(String).new(1)
    extension = Movie::EventSourcing.get(system)
    entity_type = extension.register_entity(FencedCounterBehavior, FencedCounterCommand) do |id, store|
      FencedCounterBehavior.new(id.persistence_id, store, failures)
    end
    database = Movie::Database.get(system)
    key = Movie::Persistence::ShardLeaseKey.new("orders", entity_type.name, 2)
    first = database.acquire_shard_lease(key, "node-a", 5.seconds).not_nil!
    ref = extension.get_entity_ref(entity_type.id("42"), first)
    ref << FencedAdd.new(1, Movie::Persistence::OperationId.random)
    current = Channel(Int32).new(1)
    ref << FencedGet.new(current)
    current.receive.should eq(1)

    database.release_shard_lease(first).should be_true
    second = database.acquire_shard_lease(key, "node-b", 5.seconds).not_nil!
    ref << FencedAdd.new(1, Movie::Persistence::OperationId.random)
    failures.receive.should eq(Movie::Persistence::StaleShardOwnerError.name)

    recovered = extension.get_entity_ref(entity_type.id("42"), second)
    recovered.should_not eq(ref)
    recovered << FencedGet.new(current)
    current.receive.should eq(1)
  ensure
    system.try &.shutdown
    File.delete(path) if path && File.exists?(path)
  end

  it "rejects clustered persistent entities on a process-local SQLite backend" do
    path = "/tmp/movie_sharding_sqlite_#{UUID.random}.sqlite3"
    config = Movie::Config.builder
      .set("name", "sqlite-sharding")
      .set("persistence.db-path", path)
      .build
    system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, config)
    system.enable_remoting("127.0.0.1", 0)
    system.enable_cluster
    persistence = Movie::EventSourcing.get(system)
    entity_type = persistence.register_entity(ClusteredNoopBehavior, ClusteredNoop) do |id, store|
      ClusteredNoopBehavior.new(id.persistence_id, store)
    end
    sharding = Movie::ClusterSharding.get(system)

    expect_raises(
      Movie::Cluster::ClusterShardingConfigurationError,
      "clustered persistent entities require the PostgreSQL backend"
    ) do
      sharding.init_event_sourced(entity_type, shard_count: 16)
    end
    expect_raises(
      Movie::Cluster::ClusterShardingConfigurationError,
      "clustered persistent entities require the PostgreSQL backend"
    ) do
      Movie::ClusterSingleton.get(system).init_event_sourced("sqlite", entity_type)
    end
  ensure
    system.try &.shutdown
    File.delete(path) if path && File.exists?(path)
  end

  it "acquires, renews, releases, and monotonically transfers a shard lease" do
    path = "/tmp/movie_fencing_#{UUID.random}.sqlite3"
    connection = Movie::Persistence::SQLiteBackend.new("sqlite3:#{path}").connect
    connection.ensure_schema
    key = Movie::Persistence::ShardLeaseKey.new("orders", "Order", 7)

    first = connection.acquire_shard_lease(
      Movie::Persistence::AcquireShardLease.new(key, "node-a", 5.seconds)
    ).not_nil!
    first.epoch.should eq(1_i64)
    connection.acquire_shard_lease(
      Movie::Persistence::AcquireShardLease.new(key, "node-b", 5.seconds)
    ).should be_nil

    renewed = connection.renew_shard_lease(
      Movie::Persistence::RenewShardLease.new(first, 5.seconds)
    ).not_nil!
    renewed.epoch.should eq(first.epoch)
    connection.release_shard_lease(
      Movie::Persistence::ReleaseShardLease.new(renewed)
    ).should be_true

    second = connection.acquire_shard_lease(
      Movie::Persistence::AcquireShardLease.new(key, "node-b", 5.seconds)
    ).not_nil!
    second.epoch.should eq(first.epoch + 1)
  ensure
    connection.try &.close
    File.delete(path) if path && File.exists?(path)
  end

  it "rejects a stale event writer in the same transaction as its append" do
    path = "/tmp/movie_stale_fence_#{UUID.random}.sqlite3"
    connection = Movie::Persistence::SQLiteBackend.new("sqlite3:#{path}").connect
    connection.ensure_schema
    key = Movie::Persistence::ShardLeaseKey.new("orders", "Order", 3)
    first = connection.acquire_shard_lease(
      Movie::Persistence::AcquireShardLease.new(key, "node-a", 5.seconds)
    ).not_nil!
    event = Movie::Persistence::SerializedEvent.new("Added", %({"amount":1}))
    connection.append_events(
      Movie::Persistence::AppendEvents.new(
        "Order:42",
        0_i64,
        Movie::Persistence::OperationId.random,
        [event],
        fence: first
      )
    ).revision.should eq(1_i64)

    connection.release_shard_lease(
      Movie::Persistence::ReleaseShardLease.new(first)
    ).should be_true
    second = connection.acquire_shard_lease(
      Movie::Persistence::AcquireShardLease.new(key, "node-b", 5.seconds)
    ).not_nil!

    expect_raises(Movie::Persistence::StaleShardOwnerError) do
      connection.save_snapshot(
        Movie::Persistence::SaveSnapshot.new(
          "Order:42",
          Movie::Persistence::SnapshotRecord.new(1_i64, "Order", %({"total":1})),
          fence: first
        )
      )
    end
    expect_raises(Movie::Persistence::StaleShardOwnerError) do
      connection.append_events(
        Movie::Persistence::AppendEvents.new(
          "Order:42",
          1_i64,
          Movie::Persistence::OperationId.random,
          [event],
          fence: first
        )
      )
    end
    connection.append_events(
      Movie::Persistence::AppendEvents.new(
        "Order:42",
        1_i64,
        Movie::Persistence::OperationId.random,
        [event],
        fence: second
      )
    ).revision.should eq(2_i64)
    snapshot = Movie::Persistence::SnapshotRecord.new(2_i64, "Order", %({"total":2}))
    connection.save_snapshot(
      Movie::Persistence::SaveSnapshot.new("Order:42", snapshot, fence: second)
    )
    expect_raises(Movie::Persistence::StaleShardOwnerError) do
      connection.delete_events_to(
        Movie::Persistence::DeleteEventsTo.new("Order:42", 1_i64, fence: first)
      )
    end
    connection.load_events(
      Movie::Persistence::LoadEvents.new("Order:42")
    ).size.should eq(2)
    expect_raises(Movie::Persistence::StaleShardOwnerError) do
      connection.delete_snapshot(
        Movie::Persistence::DeleteSnapshot.new("Order:42", fence: first)
      )
    end
    connection.load_snapshot(
      Movie::Persistence::LoadSnapshot.new("Order:42")
    ).should eq(snapshot)
  ensure
    connection.try &.close
    File.delete(path) if path && File.exists?(path)
  end

  it "fences durable-state writes and deletes after shard ownership changes" do
    path = "/tmp/movie_state_fence_#{UUID.random}.sqlite3"
    connection = Movie::Persistence::SQLiteBackend.new("sqlite3:#{path}").connect
    connection.ensure_schema
    key = Movie::Persistence::ShardLeaseKey.new("profiles", "Profile", 5)
    first = connection.acquire_shard_lease(
      Movie::Persistence::AcquireShardLease.new(key, "node-a", 5.seconds)
    ).not_nil!
    connection.save_state(
      Movie::Persistence::SaveState.new(
        "Profile:42",
        0_i64,
        Movie::Persistence::OperationId.random,
        "Profile",
        %({"name":"first"}),
        fence: first
      )
    ).revision.should eq(1_i64)
    connection.release_shard_lease(
      Movie::Persistence::ReleaseShardLease.new(first)
    ).should be_true
    second = connection.acquire_shard_lease(
      Movie::Persistence::AcquireShardLease.new(key, "node-b", 5.seconds)
    ).not_nil!

    expect_raises(Movie::Persistence::StaleShardOwnerError) do
      connection.delete_state(
        Movie::Persistence::DeleteState.new(
          "Profile:42",
          1_i64,
          Movie::Persistence::OperationId.random,
          fence: first
        )
      )
    end
    connection.delete_state(
      Movie::Persistence::DeleteState.new(
        "Profile:42",
        1_i64,
        Movie::Persistence::OperationId.random,
        fence: second
      )
    ).revision.should eq(2_i64)
  ensure
    connection.try &.close
    File.delete(path) if path && File.exists?(path)
  end
end
