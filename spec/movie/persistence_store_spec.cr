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
      @promise : Promise(Array(String))
    )
    end

    def receive(message : StoreProbeMessage, ctx : ActorContext(StoreProbeMessage))
      case message
      when StoreProbeStart
        ctx.ask(@store, Movie::Persistence::AppendEvent.new("stream-1", "a"), Int64).await(STORE_TIMEOUT)
        ctx.ask(@store, Movie::Persistence::AppendEvent.new("stream-1", "b"), Int64).await(STORE_TIMEOUT)
        events = ctx.ask(@store, Movie::Persistence::LoadEvents.new("stream-1"), Array(String)).await(STORE_TIMEOUT)
        @promise.try_success(events)
      end
      Behaviors(StoreProbeMessage).same
    end
  end

  class StateStoreProbe < AbstractBehavior(StoreProbeMessage)
    def initialize(
      @store : ActorRef(Movie::Persistence::StateStoreMessage),
      @promise : Promise(String?)
    )
    end

    def receive(message : StoreProbeMessage, ctx : ActorContext(StoreProbeMessage))
      case message
      when StoreProbeStart
        ctx.ask(@store, Movie::Persistence::SaveState.new("entity-1", "payload"), Bool).await(STORE_TIMEOUT)
        value = ctx.ask(@store, Movie::Persistence::LoadState.new("entity-1"), String?).await(STORE_TIMEOUT)
        @promise.try_success(value)
      end
      Behaviors(StoreProbeMessage).same
    end
  end
end

describe "Movie persistence store actors" do
  it "appends and reads events through the event store actor" do
    path = "/tmp/movie_event_store_#{UUID.random}.sqlite3"
    db_uri = "sqlite3:#{path}"
    system = Movie::ActorSystem(Movie::SystemMessage).new(Movie::Behaviors(Movie::SystemMessage).same)

    pool = system.spawn(Movie::Persistence::ConnectionPool.behavior(db_uri, 1))
    store = system.spawn(Movie::Persistence::EventStoreActor.new(pool))

    promise = Movie::Promise(Array(String)).new
    probe = system.spawn(Movie::EventStoreProbe.new(store, promise))
    probe << Movie::StoreProbeStart.new

    events = promise.future.await(10.seconds)
    events.should eq(["a", "b"])
  end

  it "saves and loads state through the state store actor" do
    path = "/tmp/movie_state_store_#{UUID.random}.sqlite3"
    db_uri = "sqlite3:#{path}"
    system = Movie::ActorSystem(Movie::SystemMessage).new(Movie::Behaviors(Movie::SystemMessage).same)

    pool = system.spawn(Movie::Persistence::ConnectionPool.behavior(db_uri, 1))
    store = system.spawn(Movie::Persistence::StateStoreActor.new(pool))

    promise = Movie::Promise(String?).new
    probe = system.spawn(Movie::StateStoreProbe.new(store, promise))
    probe << Movie::StoreProbeStart.new

    value = promise.future.await(10.seconds)
    value.should eq("payload")
  end
end
