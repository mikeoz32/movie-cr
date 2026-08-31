require "../src/movie"
require "../src/movie/persistence/postgres"

struct PostgresCounterAdded
  include JSON::Serializable
  getter amount : Int32

  def initialize(@amount : Int32)
  end
end

struct PostgresCounterState
  include JSON::Serializable
  getter value : Int32

  def initialize(@value : Int32 = 0)
  end
end

struct AddPostgresCounter
  getter amount : Int32
  getter operation_id : Movie::Persistence::OperationId

  def initialize(@amount : Int32, @operation_id : Movie::Persistence::OperationId)
  end
end

struct ReadPostgresCounter
end

alias PostgresCounterCommand = AddPostgresCounter | ReadPostgresCounter

class PostgresCounter < Movie::EventSourcedBehavior(
  PostgresCounterCommand,
  PostgresCounterAdded,
  PostgresCounterState,
)
  protected def empty_state : PostgresCounterState
    PostgresCounterState.new
  end

  protected def apply_event(state : PostgresCounterState, event : PostgresCounterAdded) : PostgresCounterState
    PostgresCounterState.new(state.value + event.amount)
  end

  protected def handle_command(
    state : PostgresCounterState,
    command : PostgresCounterCommand,
    ctx : Movie::ActorContext(PostgresCounterCommand),
  ) : Movie::EventEffect(PostgresCounterAdded, PostgresCounterState)
    sender = ctx.sender
    case command
    when AddPostgresCounter
      persist(PostgresCounterAdded.new(command.amount), command.operation_id).then_run do |current|
        Movie::Ask.reply_if_asked(sender, current.value)
      end
    when ReadPostgresCounter
      none.then_run { |current| Movie::Ask.reply_if_asked(sender, current.value) }
    else
      none
    end
  end
end

postgres_url = ENV["MOVIE_POSTGRES_URL"]? ||
               raise "Set MOVIE_POSTGRES_URL to a postgres:// connection URI"
config = Movie::Config.builder
  .set("name", "postgres-persistence-example")
  .set("persistence.backend", "postgres")
  .set("persistence.connection-uri", postgres_url)
  .set("persistence.pool-size", 4)
  .build

system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, config)
events = Movie::EventSourcing.get(system)
counter_type = events.register_entity(PostgresCounter, PostgresCounterCommand) do |id, store|
  PostgresCounter.new(id.persistence_id, store)
end
counter = events.get_entity_ref(counter_type.id("shared-counter"))

operation_id = Movie::Persistence::OperationId.random
value = system.ask(
  counter,
  AddPostgresCounter.new(1, operation_id),
  Int32,
  5.seconds
).await(5.seconds)
puts "Persisted PostgreSQL counter value: #{value}"

system.shutdown
