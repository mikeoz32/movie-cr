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

  struct GetValue
    getter reply_to : Movie::ActorRef(Int32)
    def initialize(@reply_to : Movie::ActorRef(Int32))
    end
  end

  alias CounterCommand = Increment | GetValue

  class CounterBehavior < EventSourcedBehavior(CounterCommand, CounterEvent, CounterState)
    def empty_state : CounterState
      CounterState.new(0)
    end

    def apply_event(state : CounterState, event : CounterEvent) : CounterState
      CounterState.new(state.value + event.amount)
    end

    def handle_command(state : CounterState, command : CounterCommand, ctx : ActorContext(CounterCommand)) : Array(CounterEvent)
      case command
      when Increment
        [CounterEvent.new(command.amount)]
      when GetValue
        command.reply_to << state.value
        [] of CounterEvent
      else
        [] of CounterEvent
      end
    end
  end

  class IntReceiver < AbstractBehavior(Int32)
    def initialize(@promise : Promise(Int32))
    end

    def receive(message : Int32, ctx : ActorContext(Int32))
      @promise.try_success(message)
      Behaviors(Int32).same
    end
  end
end

describe Movie::EventSourcing do
  it "replays events to recover state" do
    db_path = "/tmp/movie_event_sourcing_#{UUID.random}.sqlite3"
    config = Movie::Config.builder
      .set("movie.persistence.db_path", db_path)
      .build

    system = Movie::ActorSystem(Movie::SystemMessage).new(Movie::Behaviors(Movie::SystemMessage).same, config)
    ext = Movie::EventSourcing.get(system)

    ext.register_entity(Movie::CounterBehavior) do |pid, store|
      Movie::CounterBehavior.new(pid.persistence_id, store)
    end

    counter = ext.get_entity_ref_as(Movie::CounterCommand, Movie::Persistence.id(Movie::CounterBehavior, "counter-1"))
      .as(Movie::ActorRef(Movie::CounterCommand))

    counter << Movie::Increment.new(2)
    counter << Movie::Increment.new(3)

    promise = Movie::Promise(Int32).new
    receiver = system.spawn(Movie::IntReceiver.new(promise))
    counter << Movie::GetValue.new(receiver)

    value = promise.future.await(2.seconds)
    value.should eq(5)

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
    value2.should eq(5)
  end
end
