module Movie
  abstract class SystemMessage
  end

  PRE_START = PreStart.new().as(SystemMessage)
  POST_START = PostStart.new().as(SystemMessage)
  STOP = Stop.new().as(SystemMessage)
  PRE_STOP = PreStop.new().as(SystemMessage)
  POST_STOP = PostStop.new().as(SystemMessage)

  # Lifecycle Events
  class PreStart < SystemMessage
    # Sent when actor is about to start
    # actor should initialize resources
  end

  class PostStart < SystemMessage
    # Sent after actor successfully started
    # Actor is now in RUNNING state
  end

  class PreStop < SystemMessage
    # Sent when actor is about to stop
    # actor should begin cleanup
  end

  class PostStop < SystemMessage
    # Sent after actor is stopped
    # actor should release resources
  end

  class PreRestart < SystemMessage
    # Sent before actor restarts
    getter cause : Exception?
    def initialize(@cause : Exception?)
    end
  end

  class PostRestart < SystemMessage
    # Sent after actor is restarted
    # actor should reinitialize resources
  end

  # Actor Commands
  class Stop < SystemMessage
    # request actor to stop gracefully
  end

  class Terminate < SystemMessage
    # Force immediate termination
  end

  class Restart < SystemMessage
    # request actor to restart
    getter cause : Exception?
    def initialize(@cause : Exception?)
    end
  end

  enum RestartStrategy
    RESTART
    STOP
  end

  enum SupervisionStrategy
    RESTART
    STOP
    RESUME
    ESCALATE
  end

  enum SupervisionScope
    ONE_FOR_ONE
    ALL_FOR_ONE
  end

  struct SupervisionConfig
    getter strategy : SupervisionStrategy
    getter scope : SupervisionScope
    getter max_restarts : Int32
    getter within : Time::Span
    getter backoff_min : Time::Span
    getter backoff_max : Time::Span
    getter backoff_factor : Float64
    getter jitter : Float64

    def initialize(strategy : SupervisionStrategy = SupervisionStrategy::RESTART,
                   scope : SupervisionScope = SupervisionScope::ONE_FOR_ONE,
                   max_restarts : Int32 = 3,
                   within : Time::Span = 1.second,
                   backoff_min : Time::Span = 10.milliseconds,
                   backoff_max : Time::Span = 1.second,
                   backoff_factor : Float64 = 2.0,
                   jitter : Float64 = 0.0)
      @strategy = strategy
      @scope = scope
      @max_restarts = max_restarts
      @within = within
      @backoff_min = backoff_min
      @backoff_max = backoff_max
      @backoff_factor = backoff_factor
      @jitter = jitter
    end

    def self.default
      new
    end
  end

  # Supervision Commands
  class Watch < SystemMessage
    # request actor to watch another actor
    getter actor : ActorRefBase
    def initialize(@actor : ActorRefBase)
    end
  end

  class Unwatch < SystemMessage
    # request actor to stop watching another actor
    getter actor : ActorRefBase
    def initialize(@actor : ActorRefBase)
    end
  end

  # Supervision Events
  class Terminated < SystemMessage
    # Notification that watched actor terminated
    getter actor : ActorRefBase
    getter cause : Exception?
    def initialize(@actor : ActorRefBase, @cause : Exception? = nil)
    end
  end

  class Failed < SystemMessage
    # Notification that actor failed
    getter cause : Exception?
    getter actor : ActorRefBase
    def initialize(@actor : ActorRefBase, @cause : Exception?)
    end
  end

end
