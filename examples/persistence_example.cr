require "../src/movie"
require "../src/movie/persistence"

struct CounterAdded
  include JSON::Serializable
  getter amount : Int32

  def initialize(@amount : Int32)
  end
end

struct PersistentCounterState
  include JSON::Serializable
  getter value : Int32

  def initialize(@value : Int32 = 0)
  end
end

struct AddToCounter
  getter amount : Int32

  def initialize(@amount : Int32)
  end
end

struct ReadCounter
  getter reply_to : Movie::ActorRef(Int32)

  def initialize(@reply_to : Movie::ActorRef(Int32))
  end
end

alias PersistentCounterCommand = AddToCounter | ReadCounter

class PersistentCounter < Movie::EventSourcedBehavior(PersistentCounterCommand, CounterAdded, PersistentCounterState)
  protected def empty_state : PersistentCounterState
    PersistentCounterState.new
  end

  protected def apply_event(state : PersistentCounterState, event : CounterAdded) : PersistentCounterState
    PersistentCounterState.new(state.value + event.amount)
  end

  protected def handle_command(
    state : PersistentCounterState,
    command : PersistentCounterCommand,
    ctx : Movie::ActorContext(PersistentCounterCommand),
  ) : Movie::EventEffect(CounterAdded, PersistentCounterState)
    case command
    when AddToCounter
      persist(CounterAdded.new(command.amount))
    when ReadCounter
      none.then_run { |current| command.reply_to << current.value }
    else
      none
    end
  end

  protected def snapshot_every : Int32?
    1_000
  end
end

class CounterReply < Movie::AbstractBehavior(Int32)
  def initialize(@result : Movie::Promise(Int32))
  end

  def receive(message : Int32, context : Movie::ActorContext(Int32))
    @result.try_success(message)
    Movie::Behaviors(Int32).stopped
  end
end

config = Movie::Config.builder
  .set("name", "persistence-example")
  .set("persistence.db-path", "data/movie_example.sqlite3")
  .set("persistence.pool-size", 2)
  .build

system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, config)
events = Movie::EventSourcing.get(system)
events.register_entity(PersistentCounter) do |id, store|
  PersistentCounter.new(id.persistence_id, store)
end

counter = events.get_entity_ref_as(
  PersistentCounterCommand,
  Movie::Persistence.id(PersistentCounter, "example")
)
counter << AddToCounter.new(2)
counter << AddToCounter.new(3)

result = Movie::Promise(Int32).new
reply = system.spawn(CounterReply.new(result))
counter << ReadCounter.new(reply)
puts "Recovered counter value: #{result.future.await(5.seconds)}"

system.shutdown
