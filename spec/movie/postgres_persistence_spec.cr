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
    include JSON::Serializable
    getter amount : Int32
    getter operation_id : Persistence::OperationId

    def initialize(@amount : Int32, @operation_id : Persistence::OperationId)
    end
  end

  struct PgGet
    include JSON::Serializable

    def initialize
    end
  end

  struct PgCounterReply
    include JSON::Serializable
    getter value : Int32

    def initialize(@value : Int32)
    end
  end

  struct PgShardedAdd
    include JSON::Serializable
    getter amount : Int32
    getter operation_id : Persistence::OperationId

    def initialize(@amount : Int32, @operation_id : Persistence::OperationId)
    end
  end

  struct PgShardedGet
    include JSON::Serializable

    def initialize
    end
  end

  alias PgCounterCommand = PgAdd | PgGet | PgShardedAdd | PgShardedGet

  class PgCounterBehavior < EventSourcedBehavior(PgCounterCommand, PgAdded, PgCounterState)
    def initialize(
      persistence_id : String,
      store : Persistence::EventStoreClient,
      @deliveries : Channel(Int32)? = nil,
    )
      super(persistence_id, store)
    end

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
      when PgShardedAdd
        sender = ctx.sender
        persist(PgAdded.new(command.amount), command.operation_id).then_run do |current|
          @deliveries.try &.send(current.value)
          Ask.reply_if_asked(sender, PgCounterReply.new(current.value))
        end
      when PgShardedGet
        sender = ctx.sender
        none.then_run { |current| Ask.reply_if_asked(sender, PgCounterReply.new(current.value)) }
      else
        none
      end
    end
  end

  struct PgProfileState
    include JSON::Serializable
    getter name : String

    def initialize(@name : String = "")
    end
  end

  struct PgSetProfile
    include JSON::Serializable
    getter name : String
    getter operation_id : Persistence::OperationId

    def initialize(@name : String, @operation_id : Persistence::OperationId)
    end
  end

  struct PgGetProfile
    include JSON::Serializable

    def initialize
    end
  end

  struct PgProfileReply
    include JSON::Serializable
    getter name : String

    def initialize(@name : String)
    end
  end

  alias PgProfileCommand = PgSetProfile | PgGetProfile

  class PgProfileBehavior < DurableStateBehavior(PgProfileCommand, PgProfileState)
    protected def empty_state : PgProfileState
      PgProfileState.new
    end

    protected def handle_command(
      state : PgProfileState,
      command : PgProfileCommand,
      ctx : ActorContext(PgProfileCommand),
    ) : DurableEffect(PgProfileState)
      sender = ctx.sender
      case command
      when PgSetProfile
        persist(PgProfileState.new(command.name), command.operation_id).then_run do |current|
          Ask.reply_if_asked(sender, PgProfileReply.new(current.name))
        end
      when PgGetProfile
        none.then_run { |current| Ask.reply_if_asked(sender, PgProfileReply.new(current.name)) }
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

private def wait_for_postgres_sharding(
  timeout_span : Time::Span = 10.seconds,
  &condition : -> Bool
) : Nil
  deadline = Time.instant + timeout_span
  until condition.call
    raise "PostgreSQL sharding condition was not met within #{timeout_span}" if Time.instant >= deadline
    sleep 10.milliseconds
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

    it "preserves tell order while a persistent shard waits for its lease" do
      suffix = UUID.random.to_s
      cluster_name = "pg-sharding-retry-#{suffix}"
      config = Movie::Config.builder
        .set("name", "pg-sharding-retry")
        .set("persistence.backend", "postgres")
        .set("persistence.connection-uri", postgres_url)
        .build
      system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, config)
      lease_system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, config)
      system.enable_remoting("127.0.0.1", 0)
      cluster = system.enable_cluster(Movie::Cluster::ClusterSettings.new(
        cluster_name: cluster_name,
        seed_nodes: [] of Movie::Address,
        join_retry_interval: 20.milliseconds,
        gossip_interval: 20.milliseconds,
        heartbeat_interval: 25.milliseconds,
        heartbeat_timeout: 500.milliseconds
      ))
      wait_for_postgres_sharding { cluster.up? && cluster.converged? }
      Movie::Remote::MessageRegistry.register(Movie::PgShardedAdd)

      deliveries = Channel(Int32).new(32)
      persistence = Movie::EventSourcing.get(system)
      persistent_type = persistence.register_entity(Movie::PgCounterBehavior, Movie::PgCounterCommand) do |id, store|
        Movie::PgCounterBehavior.new(id.persistence_id, store, deliveries)
      end
      sharding = Movie::ClusterSharding.get(system)
      sharded_type = sharding.init_event_sourced(
        persistent_type,
        shard_count: 1,
        lease_duration: 500.milliseconds,
        lease_renew_interval: 100.milliseconds
      )
      wait_for_postgres_sharding { sharding.allocations(sharded_type).size == 1 }

      lease_database = Movie::Database.get(lease_system)
      key = Movie::Persistence::ShardLeaseKey.new(cluster_name, persistent_type.name, 0)
      competing = lease_database.acquire_shard_lease(key, "competing-owner", 250.milliseconds).not_nil!
      ref = sharding.entity_ref_for(sharded_type, "ordered-#{suffix}")
      32.times do
        ref << Movie::PgShardedAdd.new(1, Movie::Persistence::OperationId.random)
      end
      wait_for_postgres_sharding { sharding.stats.lease_retries > 0_i64 }
      lease_database.release_shard_lease(competing).should be_true

      received = [] of Int32
      deadline = Time.instant + 2.seconds
      while received.size < 32
        remaining = deadline - Time.instant
        fail("timed out draining recovered shard retry queue: #{received.size}/32") if remaining <= Time::Span.zero
        select
        when value = deliveries.receive
          received << value
        when timeout(remaining)
          fail("timed out draining recovered shard retry queue: #{received.size}/32")
        end
      end
      received.should eq((1..32).to_a)
      sharding.stats.lease_retries.should be > 0_i64
    ensure
      system.try &.shutdown
      lease_system.try &.shutdown
    end

    it "bounds a shard retry queue by one shared lease deadline" do
      suffix = UUID.random.to_s
      cluster_name = "pg-sharding-deadline-#{suffix}"
      config = Movie::Config.builder
        .set("name", "pg-sharding-deadline")
        .set("persistence.backend", "postgres")
        .set("persistence.connection-uri", postgres_url)
        .build
      system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, config)
      lease_system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, config)
      system.enable_remoting("127.0.0.1", 0)
      cluster = system.enable_cluster(Movie::Cluster::ClusterSettings.new(
        cluster_name: cluster_name,
        seed_nodes: [] of Movie::Address,
        join_retry_interval: 20.milliseconds,
        gossip_interval: 20.milliseconds,
        heartbeat_interval: 25.milliseconds,
        heartbeat_timeout: 500.milliseconds
      ))
      wait_for_postgres_sharding { cluster.up? && cluster.converged? }
      Movie::Remote::MessageRegistry.register(Movie::PgShardedAdd)

      persistence = Movie::EventSourcing.get(system)
      persistent_type = persistence.register_entity(Movie::PgCounterBehavior, Movie::PgCounterCommand) do |id, store|
        Movie::PgCounterBehavior.new(id.persistence_id, store)
      end
      sharding = Movie::ClusterSharding.get(system)
      sharded_type = sharding.init_event_sourced(
        persistent_type,
        shard_count: 1,
        lease_duration: 60.milliseconds,
        lease_renew_interval: 20.milliseconds
      )
      wait_for_postgres_sharding { sharding.allocations(sharded_type).size == 1 }

      lease_database = Movie::Database.get(lease_system)
      key = Movie::Persistence::ShardLeaseKey.new(cluster_name, persistent_type.name, 0)
      competing = lease_database.acquire_shard_lease(key, "competing-owner", 2.seconds).not_nil!
      ref = sharding.entity_ref_for(sharded_type, "deadline-#{suffix}")
      20.times do
        ref << Movie::PgShardedAdd.new(1, Movie::Persistence::OperationId.random)
      end

      wait_for_postgres_sharding(1.second) do
        sharding.stats.rejected_envelopes >= 20_i64
      end
      lease_database.release_shard_lease(competing).should be_true
    ensure
      system.try &.shutdown
      lease_system.try &.shutdown
    end

    it "routes and relocates a fenced event-sourced entity through cluster sharding" do
      entity_id = "sharded-counter-#{UUID.random}"
      seed_config = Movie::Config.builder
        .set("name", "pg-sharding-seed")
        .set("persistence.backend", "postgres")
        .set("persistence.connection-uri", postgres_url)
        .build
      peer_config = Movie::Config.builder
        .set("name", "pg-sharding-peer")
        .set("persistence.backend", "postgres")
        .set("persistence.connection-uri", postgres_url)
        .build
      seed_system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, seed_config)
      peer_system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, peer_config)
      seed_remote = seed_system.enable_remoting("127.0.0.1", 0)
      peer_system.enable_remoting("127.0.0.1", 0)
      cluster_settings = ->(seeds : Array(Movie::Address)) do
        Movie::Cluster::ClusterSettings.new(
          cluster_name: "pg-sharding-spec",
          seed_nodes: seeds,
          join_retry_interval: 20.milliseconds,
          gossip_interval: 20.milliseconds,
          heartbeat_interval: 25.milliseconds,
          heartbeat_timeout: 500.milliseconds
        )
      end
      seed_cluster = seed_system.enable_cluster(cluster_settings.call([] of Movie::Address))
      peer_cluster = peer_system.enable_cluster(cluster_settings.call([seed_remote.address]))
      wait_for_postgres_sharding do
        seed_cluster.snapshot.members.count(&.status.up?) == 2 &&
          peer_cluster.snapshot.members.count(&.status.up?) == 2 &&
          seed_cluster.converged? && peer_cluster.converged?
      end
      Movie::Remote::MessageRegistry.register(Movie::PgShardedAdd)
      Movie::Remote::MessageRegistry.register(Movie::PgShardedGet)
      Movie::Remote::MessageRegistry.register(Movie::PgCounterReply)
      Movie::Remote::MessageRegistry.register(Movie::PgSetProfile)
      Movie::Remote::MessageRegistry.register(Movie::PgGetProfile)
      Movie::Remote::MessageRegistry.register(Movie::PgProfileReply)

      seed_persistence = Movie::EventSourcing.get(seed_system)
      seed_persistent_type = seed_persistence.register_entity(Movie::PgCounterBehavior, Movie::PgCounterCommand) do |id, store|
        Movie::PgCounterBehavior.new(id.persistence_id, store)
      end
      peer_persistence = Movie::EventSourcing.get(peer_system)
      peer_persistent_type = peer_persistence.register_entity(Movie::PgCounterBehavior, Movie::PgCounterCommand) do |id, store|
        Movie::PgCounterBehavior.new(id.persistence_id, store)
      end
      seed_sharding = Movie::ClusterSharding.get(seed_system)
      peer_sharding = Movie::ClusterSharding.get(peer_system)
      seed_type = seed_sharding.init_event_sourced(
        seed_persistent_type,
        shard_count: 16,
        lease_duration: 2.seconds,
        lease_renew_interval: 500.milliseconds
      )
      peer_type = peer_sharding.init_event_sourced(
        peer_persistent_type,
        shard_count: 16,
        lease_duration: 2.seconds,
        lease_renew_interval: 500.milliseconds
      )
      seed_states = Movie::DurableState.get(seed_system)
      seed_state_type = seed_states.register_entity(Movie::PgProfileBehavior, Movie::PgProfileCommand) do |id, store|
        Movie::PgProfileBehavior.new(id.persistence_id, store)
      end
      peer_states = Movie::DurableState.get(peer_system)
      peer_state_type = peer_states.register_entity(Movie::PgProfileBehavior, Movie::PgProfileCommand) do |id, store|
        Movie::PgProfileBehavior.new(id.persistence_id, store)
      end
      seed_profile_type = seed_sharding.init_durable_state(
        seed_state_type,
        shard_count: 16,
        lease_duration: 2.seconds,
        lease_renew_interval: 500.milliseconds
      )
      peer_profile_type = peer_sharding.init_durable_state(
        peer_state_type,
        shard_count: 16,
        lease_duration: 2.seconds,
        lease_renew_interval: 500.milliseconds
      )

      wait_for_postgres_sharding do
        seed_sharding.allocations(seed_type).size == 16 &&
          seed_sharding.allocations(seed_type) == peer_sharding.allocations(peer_type) &&
          seed_sharding.allocations(seed_profile_type).size == 16 &&
          seed_sharding.allocations(seed_profile_type) == peer_sharding.allocations(peer_profile_type)
      end

      selected_id = (0...1_000).map { |suffix| "#{entity_id}-#{suffix}" }.find do |candidate|
        shard_id = Movie::Cluster::StableHashPartitioner.new.shard_for(
          seed_type.name,
          candidate,
          16
        )
        seed_sharding.allocations(seed_type)[shard_id]? == peer_cluster.self_unique_address
      end.not_nil!
      ref = seed_sharding.entity_ref_for(seed_type, selected_id)
      ref.ask(
        Movie::PgShardedAdd.new(3, Movie::Persistence::OperationId.random),
        Movie::PgCounterReply,
        5.seconds
      ).await(5.seconds).value.should eq(3)
      profile_id = (0...1_000).map { |suffix| "profile-#{entity_id}-#{suffix}" }.find do |candidate|
        shard_id = Movie::Cluster::StableHashPartitioner.new.shard_for(
          seed_profile_type.name,
          candidate,
          16
        )
        seed_sharding.allocations(seed_profile_type)[shard_id]? == peer_cluster.self_unique_address
      end.not_nil!
      profile = seed_sharding.entity_ref_for(seed_profile_type, profile_id)
      profile.ask(
        Movie::PgSetProfile.new("Ada", Movie::Persistence::OperationId.random),
        Movie::PgProfileReply,
        5.seconds
      ).await(5.seconds).name.should eq("Ada")
      peer_sharding.local_entity_count.should eq(2)

      peer_cluster.leave.should be_true
      peer_cluster.await_removed(5.seconds)
      wait_for_postgres_sharding { peer_sharding.local_entity_count == 0 }
      wait_for_postgres_sharding do
        seed_cluster.snapshot.members.count(&.status.up?) == 1 &&
          seed_cluster.converged? &&
          seed_sharding.allocations(seed_type).values.all? { |owner| owner == seed_cluster.self_unique_address } &&
          seed_sharding.allocations(seed_profile_type).values.all? { |owner| owner == seed_cluster.self_unique_address }
      end
      ref.ask(
        Movie::PgShardedGet.new,
        Movie::PgCounterReply,
        5.seconds
      ).await(5.seconds).value.should eq(3)
      profile.ask(
        Movie::PgGetProfile.new,
        Movie::PgProfileReply,
        5.seconds
      ).await(5.seconds).name.should eq("Ada")
      seed_sharding.local_entity_count.should eq(2)
      peer_type.name.should eq(seed_type.name)
      peer_profile_type.name.should eq(seed_profile_type.name)
    ensure
      peer_system.try &.shutdown
      seed_system.try &.shutdown
    end

    it "serializes a fenced write before lease transfer" do
      suffix = UUID.random.to_s.delete('-')
      persistence_id = "pg-fence-race-#{suffix}"
      function_name = "movie_fence_pause_#{suffix}"
      trigger_name = "movie_fence_trigger_#{suffix}"
      advisory_lock = Random.rand(Int32::MAX).to_i64
      admin = DB.connect(postgres_url)
      old_connection = Movie::Persistence::PostgresBackend.new(postgres_url).connect
      new_connection = Movie::Persistence::PostgresBackend.new(postgres_url).connect
      old_connection.ensure_schema
      key = Movie::Persistence::ShardLeaseKey.new("fence-race", "Counter", 0)
      old_token = old_connection.acquire_shard_lease(
        Movie::Persistence::AcquireShardLease.new(key, "old-owner", 200.milliseconds)
      ).not_nil!
      admin.exec("SELECT pg_advisory_lock($1)", advisory_lock)
      admin.exec(<<-SQL)
        CREATE FUNCTION #{function_name}() RETURNS trigger AS $$
        BEGIN
          PERFORM pg_advisory_xact_lock(#{advisory_lock});
          RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
      SQL
      admin.exec(<<-SQL)
        CREATE TRIGGER #{trigger_name}
        BEFORE INSERT ON event_journal
        FOR EACH ROW WHEN (NEW.persistence_id = '#{persistence_id}')
        EXECUTE FUNCTION #{function_name}()
      SQL
      completions = Channel(String).new(2)

      spawn do
        old_connection.append_events(Movie::Persistence::AppendEvents.new(
          persistence_id,
          0_i64,
          Movie::Persistence::OperationId.random,
          [Movie::Persistence::SerializedEvent.new("Added", "old-write")],
          fence: old_token
        ))
        completions.send("write")
      rescue error
        completions.send("write-error:#{error.class.name}")
      end

      sleep 300.milliseconds
      spawn do
        token = new_connection.acquire_shard_lease(
          Movie::Persistence::AcquireShardLease.new(key, "new-owner", 1.second)
        )
        completions.send(token ? "transfer:#{token.epoch}" : "transfer-unavailable")
      rescue error
        completions.send("transfer-error:#{error.class.name}")
      end

      select
      when early = completions.receive
        fail("lease transfer completed before the fenced write: #{early}")
      when timeout(100.milliseconds)
      end

      admin.query_one("SELECT pg_advisory_unlock($1)", advisory_lock, as: Bool).should be_true
      completions.receive.should eq("write")
      completions.receive.should eq("transfer:#{old_token.epoch + 1}")
    ensure
      admin.try do |connection|
        connection.query_one("SELECT pg_advisory_unlock($1)", advisory_lock, as: Bool) rescue nil
        connection.exec("DROP TRIGGER IF EXISTS #{trigger_name} ON event_journal") if trigger_name
        connection.exec("DROP FUNCTION IF EXISTS #{function_name}()") if function_name
        connection.close
      end
      old_connection.try &.close
      new_connection.try &.close
    end

    it "serializes a fenced snapshot deletion before lease transfer" do
      suffix = UUID.random.to_s.delete('-')
      persistence_id = "pg-fence-delete-race-#{suffix}"
      function_name = "movie_fence_delete_pause_#{suffix}"
      trigger_name = "movie_fence_delete_trigger_#{suffix}"
      advisory_lock = Random.rand(Int32::MAX).to_i64
      admin = DB.connect(postgres_url)
      old_connection = Movie::Persistence::PostgresBackend.new(postgres_url).connect
      new_connection = Movie::Persistence::PostgresBackend.new(postgres_url).connect
      old_connection.ensure_schema
      key = Movie::Persistence::ShardLeaseKey.new("fence-delete-race", "Counter", 0)
      old_token = old_connection.acquire_shard_lease(
        Movie::Persistence::AcquireShardLease.new(key, "old-owner", 200.milliseconds)
      ).not_nil!
      old_connection.save_snapshot(
        Movie::Persistence::SaveSnapshot.new(
          persistence_id,
          Movie::Persistence::SnapshotRecord.new(1_i64, "Counter", "1"),
          fence: old_token
        )
      )
      admin.exec("SELECT pg_advisory_lock($1)", advisory_lock)
      admin.exec(<<-SQL)
        CREATE FUNCTION #{function_name}() RETURNS trigger AS $$
        BEGIN
          PERFORM pg_advisory_xact_lock(#{advisory_lock});
          RETURN OLD;
        END;
        $$ LANGUAGE plpgsql
      SQL
      admin.exec(<<-SQL)
        CREATE TRIGGER #{trigger_name}
        BEFORE DELETE ON snapshot_store
        FOR EACH ROW WHEN (OLD.persistence_id = '#{persistence_id}')
        EXECUTE FUNCTION #{function_name}()
      SQL
      completions = Channel(String).new(2)

      spawn do
        old_connection.delete_snapshot(
          Movie::Persistence::DeleteSnapshot.new(persistence_id, fence: old_token)
        )
        completions.send("delete")
      rescue error
        completions.send("delete-error:#{error.class.name}")
      end

      sleep 300.milliseconds
      spawn do
        token = new_connection.acquire_shard_lease(
          Movie::Persistence::AcquireShardLease.new(key, "new-owner", 1.second)
        )
        completions.send(token ? "transfer:#{token.epoch}" : "transfer-unavailable")
      rescue error
        completions.send("transfer-error:#{error.class.name}")
      end

      select
      when early = completions.receive
        fail("lease transfer completed before the fenced delete: #{early}")
      when timeout(100.milliseconds)
      end

      admin.query_one("SELECT pg_advisory_unlock($1)", advisory_lock, as: Bool).should be_true
      completions.receive.should eq("delete")
      completions.receive.should eq("transfer:#{old_token.epoch + 1}")
    ensure
      admin.try do |connection|
        connection.query_one("SELECT pg_advisory_unlock($1)", advisory_lock, as: Bool) rescue nil
        connection.exec("DROP TRIGGER IF EXISTS #{trigger_name} ON snapshot_store") if trigger_name
        connection.exec("DROP FUNCTION IF EXISTS #{function_name}()") if function_name
        connection.close
      end
      old_connection.try &.close
      new_connection.try &.close
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
