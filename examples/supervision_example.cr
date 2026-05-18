require "../src/movie"

class FailingWorker < Movie::AbstractBehavior(Int32)
  def receive(message, context)
    raise "boom" if message == 1
    Movie::Behaviors(Int32).same
  end
end

one_for_one = Movie::SupervisionConfig.new(
  strategy: Movie::SupervisionStrategy::RESTART,
  scope: Movie::SupervisionScope::ONE_FOR_ONE,
  max_restarts: 2,
  within: 1.second,
  backoff_min: 20.milliseconds,
  backoff_max: 200.milliseconds,
  backoff_factor: 2.0,
  jitter: 0.1,
)

all_for_one = Movie::SupervisionConfig.new(
  strategy: Movie::SupervisionStrategy::RESTART,
  scope: Movie::SupervisionScope::ALL_FOR_ONE,
  max_restarts: 1,
  within: 200.milliseconds,
  backoff_min: 30.milliseconds,
  backoff_max: 500.milliseconds,
  backoff_factor: 2.0,
  jitter: 0.0,
)

main_behavior = Movie::Behaviors(Int32).setup do |context|
  worker = context.spawn(FailingWorker.new, Movie::RestartStrategy::RESTART, one_for_one)

  parent = context.spawn(Movie::Behaviors(Int32).same, Movie::RestartStrategy::RESTART, all_for_one)
  child_a = context.spawn(FailingWorker.new, Movie::RestartStrategy::RESTART, all_for_one)
  child_b = context.spawn(FailingWorker.new, Movie::RestartStrategy::RESTART, all_for_one)
  parent << 0 # keep parent alive

  # Send integers to main to route failures into the supervised children.
  Movie::Behaviors(Int32).receive do |message, ctx|
    case message
    when 1
      worker << 1
    when 2
      child_a << 1
    when 3
      child_b << 1
    end
    Movie::Behaviors(Int32).same
  end
end

# Root actor owns spawning; system-level config can stay default.
system = Movie::ActorSystem(Int32).new(main_behavior, Movie::RestartStrategy::RESTART)

# Trigger failures to see supervision in action
system << 1   # one-for-one worker fails and restarts with backoff
system << 2   # all-for-one: both children will be restarted on a sibling failure
