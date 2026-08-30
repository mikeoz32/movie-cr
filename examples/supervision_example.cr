require "../src/movie"

class FailingWorker < Movie::AbstractBehavior(Int32)
  def initialize(@name : String)
  end

  def receive(message, context)
    raise "boom" if message == 1
    Movie::Behaviors(Int32).same
  end

  def on_signal(signal : Movie::SystemMessage)
    puts "[#{@name}] restart requested" if signal.is_a?(Movie::PreRestart)
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

all_for_one_group = Movie::Behaviors(Int32).setup do |context|
  child_a = context.spawn(FailingWorker.new("group-a"), name: "group-a")
  child_b = context.spawn(FailingWorker.new("group-b"), name: "group-b")

  Movie::Behaviors(Int32).receive do |message, _ctx|
    child_a << 1 if message == 1
    child_b << 1 if message == 2
    Movie::Behaviors(Int32).same
  end
end

main_behavior = Movie::Behaviors(Int32).setup do |context|
  worker = context.spawn(FailingWorker.new("one-for-one"), name: "one-for-one")
  group = context.spawn(
    all_for_one_group,
    Movie::RestartStrategy::RESTART,
    all_for_one,
    "all-for-one"
  )

  # Send integers to main to route failures into the supervised children.
  Movie::Behaviors(Int32).receive do |message, _ctx|
    case message
    when 1
      worker << 1
    when 2
      group << 1
    when 3
      group << 2
    end
    Movie::Behaviors(Int32).same
  end
end

# A parent's supervision config controls its children. The root uses
# one-for-one; the nested group above uses all-for-one.
system = Movie::ActorSystem(Int32).new(
  main_behavior,
  Movie::RestartStrategy::RESTART,
  one_for_one
)

# Trigger failures to see supervision in action
system << 1 # one-for-one worker fails and restarts with backoff
system << 2 # all-for-one: both children will be restarted on a sibling failure

sleep 500.milliseconds
system.shutdown
