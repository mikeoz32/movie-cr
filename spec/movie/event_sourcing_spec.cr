require "../spec_helper"
require "../../src/movie"
require "../../src/movie/persistence"

module Movie
  struct CounterEvent
    include JSON::Serializable
    getter amount : Int32

    def initialize(@amount : Int32)
    end
  end

  struct CounterState
    include JSON::Serializable
    getter value : Int32

    def initialize(@value : Int32 = 0)
    end
  end

  struct Increment
    getter amount : Int32

    def initialize(@amount : Int32)
    end
  end

  struct IncrementMany
    getter amounts : Array(Int32)

    def initialize(@amounts : Array(Int32))
    end
  end

  struct StopCounter
  end

  struct GetValue
    getter reply_to : Movie::ActorRef(Int32)

    def initialize(@reply_to : Movie::ActorRef(Int32))
    end
  end

  alias CounterCommand = Increment | IncrementMany | GetValue | StopCounter

  class CounterBehavior < EventSourcedBehavior(CounterCommand, CounterEvent, CounterState)
    def empty_state : CounterState
      CounterState.new(0)
    end

    def apply_event(state : CounterState, event : CounterEvent) : CounterState
      CounterState.new(state.value + event.amount)
    end

    def handle_command(state : CounterState, command : CounterCommand, ctx : ActorContext(CounterCommand)) : EventEffect(CounterEvent, CounterState)
      case command
      when Increment
        persist(CounterEvent.new(command.amount))
      when IncrementMany
        persist_all(command.amounts.map { |amount| CounterEvent.new(amount) })
      when GetValue
        none.then_run { |current| command.reply_to << current.value }
      when StopCounter
        stop
      else
        none
      end
    end

    def deserialize_event(manifest : String, payload : String) : CounterEvent
      if manifest == "counter-v0"
        CounterEvent.new(JSON.parse(payload)["delta"].as_i.to_i)
      else
        super
      end
    end

    def snapshot_every : Int32?
      2
    end
  end

  class IntReceiver < AbstractBehavior(Int32)
    def initialize(@promise : Promise(Int32))
    end

    def receive(message : Int32, context : ActorContext(Int32))
      @promise.try_success(message)
      Behaviors(Int32).same
    end
  end

  class FailingEventStore < AbstractBehavior(Persistence::EventStoreMessage)
    def receive(message : Persistence::EventStoreMessage, context : ActorContext(Persistence::EventStoreMessage))
      case message
      when Persistence::LoadSnapshot
        Ask.reply_if_asked(context.sender, nil.as(Persistence::SnapshotRecord?))
      when Persistence::LoadEvents
        Ask.reply_if_asked(context.sender, [] of Persistence::StoredEvent)
      when Persistence::AppendEvents
        error = Persistence::ConcurrentWriteError.new(message.persistence_id, message.expected_revision, 7_i64)
        Ask.fail_if_asked(context.sender, error, Int64)
      when Persistence::SaveSnapshot, Persistence::DeleteSnapshot
        Ask.reply_if_asked(context.sender, true)
      end
      Behaviors(Persistence::EventStoreMessage).same
    end
  end

  class FailingEffectBehavior < EventSourcedBehavior(Increment, CounterEvent, CounterState)
    def initialize(
      store : Persistence::EventStoreClient,
      @failure : Promise(Exception),
      @callback : Promise(Int32),
    )
      super("failing-effect", store)
    end

    def empty_state : CounterState
      CounterState.new
    end

    def apply_event(state : CounterState, event : CounterEvent) : CounterState
      CounterState.new(state.value + event.amount)
    end

    def handle_command(state : CounterState, command : Increment, ctx : ActorContext(Increment)) : EventEffect(CounterEvent, CounterState)
      persist(CounterEvent.new(command.amount)).then_run { |current| @callback.try_success(current.value) }
    end

    def on_persist_failure(error : Exception)
      @failure.try_success(error)
    end
  end
end

describe Movie::EventSourcing do
  it "does not apply post-persist callbacks when storage rejects a write" do
    system = Movie::ActorSystem(Movie::SystemMessage).new(Movie::Behaviors(Movie::SystemMessage).same)
    store_ref = system.spawn(Movie::FailingEventStore.new)
    failure = Movie::Promise(Exception).new
    callback = Movie::Promise(Int32).new
    behavior = Movie::FailingEffectBehavior.new(Movie::Persistence::EventStoreClient.new(store_ref), failure, callback)
    entity = system.spawn(behavior)

    entity << Movie::Increment.new(1)

    error = failure.future.await(1.second)
    error.should be_a(Movie::Persistence::ConcurrentWriteError)
    expect_raises(Movie::FutureTimeout) { callback.future.await(100.milliseconds) }
  ensure
    system.try &.shutdown
  end

  it "replays events to recover state" do
    db_path = "/tmp/movie_event_sourcing_#{UUID.random}.sqlite3"
    config = Movie::Config.builder
      .set("persistence.db-path", db_path)
      .build

    system = Movie::ActorSystem(Movie::SystemMessage).new(Movie::Behaviors(Movie::SystemMessage).same, config)
    ext = Movie::EventSourcing.get(system)

    ext.register_entity(Movie::CounterBehavior) do |pid, store|
      Movie::CounterBehavior.new(pid.persistence_id, store)
    end

    counter = ext.get_entity_ref_as(Movie::CounterCommand, Movie::Persistence.id(Movie::CounterBehavior, "counter-1"))
      .as(Movie::ActorRef(Movie::CounterCommand))

    counter << Movie::IncrementMany.new([2, 3])

    promise = Movie::Promise(Int32).new
    receiver = system.spawn(Movie::IntReceiver.new(promise))
    counter << Movie::GetValue.new(receiver)

    value = promise.future.await(2.seconds)
    value.should eq(5)

    database = Movie::Database.get(system)
    snapshot = system.ask(
      database.pool,
      Movie::Persistence::LoadSnapshot.new("Movie::CounterBehavior:counter-1"),
      Movie::Persistence::SnapshotRecord?,
      2.seconds
    ).await(2.seconds)
    snapshot.should_not be_nil
    snapshot.not_nil!.sequence_nr.should eq(2_i64)

    system.ask(
      database.pool,
      Movie::Persistence::AppendEvents.new(
        "Movie::CounterBehavior:counter-1",
        2_i64,
        [Movie::Persistence::SerializedEvent.new("counter-v0", %({"delta":4}))]
      ),
      Int64,
      2.seconds
    ).await(2.seconds)

    counter.send_system(Movie::Restart.new(nil))
    sleep 50.milliseconds
    restarted_promise = Movie::Promise(Int32).new
    restarted_receiver = system.spawn(Movie::IntReceiver.new(restarted_promise))
    counter << Movie::GetValue.new(restarted_receiver)
    restarted_promise.future.await(2.seconds).should eq(9)

    first_actor_id = counter.id
    counter << Movie::StopCounter.new
    deadline = Time.instant + 1.second
    while system.context(first_actor_id) && Time.instant < deadline
      sleep 5.milliseconds
    end
    system.context(first_actor_id).should be_nil

    respawned = ext.get_entity_ref_as(Movie::CounterCommand, Movie::Persistence.id(Movie::CounterBehavior, "counter-1"))
    respawned.id.should_not eq(first_actor_id)
    respawned_promise = Movie::Promise(Int32).new
    respawned_receiver = system.spawn(Movie::IntReceiver.new(respawned_promise))
    respawned << Movie::GetValue.new(respawned_receiver)
    respawned_promise.future.await(2.seconds).should eq(9)

    system.ask(
      database.pool,
      Movie::Persistence::DbExec.new(
        "DELETE FROM event_journal WHERE persistence_id = ? AND sequence_nr <= ?",
        ["Movie::CounterBehavior:counter-1", 2_i64] of DB::Any
      ),
      Bool,
      2.seconds
    ).await(2.seconds)

    system2 = Movie::ActorSystem(Movie::SystemMessage).new(Movie::Behaviors(Movie::SystemMessage).same, config)
    ext2 = Movie::EventSourcing.get(system2)
    ext2.register_entity(Movie::CounterBehavior) do |pid, store|
      Movie::CounterBehavior.new(pid.persistence_id, store)
    end

    counter2 = ext2.get_entity_ref_as(Movie::CounterCommand, Movie::Persistence.id(Movie::CounterBehavior, "counter-1"))
      .as(Movie::ActorRef(Movie::CounterCommand))

    promise2 = Movie::Promise(Int32).new
    receiver2 = system2.spawn(Movie::IntReceiver.new(promise2))
    counter2 << Movie::GetValue.new(receiver2)

    value2 = promise2.future.await(2.seconds)
    value2.should eq(9)
  end
end
