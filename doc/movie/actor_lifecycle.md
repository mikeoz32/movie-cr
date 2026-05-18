# Movie Actor System - Actor Lifecycle Architecture

## Overview

**Movie** is a lightweight actor framework for Crystal that leverages the language's experimental execution contexts and fiber-based concurrency model. This document describes the complete architecture of the actor lifecycle, from creation to message processing.

## Core Concepts

### Actor Model Fundamentals

The Movie framework implements the classical actor model with the following principles:

1. **Isolation**: Each actor maintains its own state and cannot directly access another actor's state
2. **Asynchronous Communication**: Actors communicate exclusively through message passing
3. **Location Transparency**: Actors are referenced through `ActorRef`, abstracting their physical location
4. **Concurrency**: Multiple actors can execute concurrently based on dispatcher configuration

### Key Components

```
┌─────────────────────────────────────────────────────────────┐
│                        ActorSystem                          │
│  ┌───────────────────────────────────────────────────────┐ │
│  │                   ActorRegistry                       │ │
│  │  - Manages actor references and contexts             │ │
│  │  - Provides actor spawning capabilities              │ │
│  └───────────────────────────────────────────────────────┘ │
│  ┌───────────────────────────────────────────────────────┐ │
│  │                DispatcherRegistry                     │ │
│  │  - PinnedDispatcher (isolated thread)                │ │
│  │  - ParallelDispatcher (thread pool)                  │ │
│  │  - ConcurrentDispatcher (single thread, concurrent)  │ │
│  └───────────────────────────────────────────────────────┘ │
│  ┌───────────────────────────────────────────────────────┐ │
│  │                 MailboxManager                        │ │
│  │  - Creates and manages actor mailboxes               │ │
│  └───────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘

         │                    │                    │
         ▼                    ▼                    ▼
    
   ActorRef(T)         ActorContext(T)        Mailbox(T)
   ┌──────────┐        ┌──────────────┐      ┌────────────┐
   │ ID: Int  │───────▶│ Behavior     │◀─────│ Inbox      │
   │ System   │        │ Ref          │      │ System Q   │
   └──────────┘        │ Mailbox      │      │ Dispatcher │
                       │ System       │      └────────────┘
                       └──────────────┘
                              │
                              ▼
                    AbstractBehavior(T)
                    ┌──────────────────┐
                    │ receive(msg, ctx)│
                    │ on_signal(sys)   │
                    └──────────────────┘
```

## Actor Lifecycle Design

### Lifecycle States

An actor progresses through several states during its lifetime. This state machine ensures proper initialization, operation, and cleanup.

```
┌─────────────────────────────────────────────────────────────────┐
│                    ACTOR LIFECYCLE STATES                       │
└─────────────────────────────────────────────────────────────────┘

    [CREATED]
        │
        │ spawn()
        │
        ▼
    [STARTING] ──────────────┐
        │                    │ StartFailure
        │ PreStart           │
        │ Success            ▼
        │              [FAILED] ───► [TERMINATED]
        ▼                    ▲
    [RUNNING] ◄──────┐       │
        │            │       │
        │ Messages   │       │
        │ Processing │       │
        │            │       │
        ├────────────┘       │
        │                    │
        │ stop() / Error     │
        │                    │
        ▼                    │
    [STOPPING] ──────────────┤
        │                    │
        │ PostStop           │
        │ Cleanup            │
        │                    │
        ▼                    │
    [STOPPED] ───────────────┘
        │
        │ restart()
        │
        └──────► [RESTARTING] ──► [STARTING]
```

### State Descriptions

| State | Description | Valid Transitions | Message Processing |
|-------|-------------|------------------|-------------------|
| **CREATED** | Actor reference and context created, mailbox not yet initialized | STARTING | No |
| **STARTING** | PreStart hook executing, resources initializing | RUNNING, FAILED | No |
| **RUNNING** | Normal operation, processing messages | STOPPING, FAILED | Yes |
| **STOPPING** | Graceful shutdown in progress, PostStop executing | STOPPED | System messages only |
| **STOPPED** | Actor terminated, resources released | RESTARTING, TERMINATED | No |
| **FAILED** | Unrecoverable error occurred | TERMINATED, RESTARTING | No |
| **RESTARTING** | Supervisor-initiated restart | STARTING | No |
| **TERMINATED** | Final state, actor removed from registry | None | No |

### System Messages

System messages control the actor lifecycle and are processed with higher priority than user messages.

```crystal
module Movie
  abstract class SystemMessage
  end

  # Lifecycle Messages
  class PreStart < SystemMessage
    # Sent when actor is about to start
    # Actor should initialize resources
  end

  class PostStart < SystemMessage
    # Sent after actor successfully started
    # Actor is now in RUNNING state
  end

  class PreStop < SystemMessage
    # Sent when actor is about to stop
    # Actor should begin cleanup
  end

  class PostStop < SystemMessage
    # Sent after actor stopped
    # Actor should release all resources
  end

  class PreRestart < SystemMessage
    # Sent before actor restarts
    # Actor should prepare for restart
    getter cause : Exception?
    def initialize(@cause : Exception?)
    end
  end

  class PostRestart < SystemMessage
    # Sent after actor restarted
    # Actor should reinitialize
  end

  # Control Messages
  class Stop < SystemMessage
    # Request actor to stop gracefully
  end

  class Terminate < SystemMessage
    # Force immediate termination
  end

  class Restart < SystemMessage
    # Request actor to restart
    getter cause : Exception?
    def initialize(@cause : Exception?)
    end
  end

  # Supervision Messages
  class Watch < SystemMessage
    # Request to watch another actor
    getter target : ActorRefBase
    def initialize(@target : ActorRefBase)
    end
  end

  class Unwatch < SystemMessage
    # Stop watching an actor
    getter target : ActorRefBase
    def initialize(@target : ActorRefBase)
    end
  end

  class Terminated < SystemMessage
    # Notification that watched actor terminated
    getter actor : ActorRefBase
    getter cause : Exception?
    def initialize(@actor : ActorRefBase, @cause : Exception?)
    end
  end

  # Error Handling
  class Failure < SystemMessage
    # Actor encountered an error
    getter cause : Exception
    getter failed_message : Envelope?
    def initialize(@cause : Exception, @failed_message : Envelope? = nil)
    end
  end
end
```

### Lifecycle Event Flow

#### 1. Actor Creation and Startup

```
User Code                Registry              Context              Mailbox           Behavior
    │                        │                     │                    │                 │
    │──spawn(behavior)──────>│                     │                    │                 │
    │                        │──new ActorRef()───>│                    │                 │
    │                        │                     │                    │                 │
    │                        │──new Context()─────>│                    │                 │
    │                        │                     │                    │                 │
    │                        │──register()────────>│                    │                 │
    │                        │                     │                    │                 │
    │                        │                     │──start()──────────>│                 │
    │                        │                     │                    │                 │
    │                        │                     │<──mailbox created──│                 │
    │                        │                     │                    │                 │
    │                        │                     │──[PreStart]───────>│                 │
    │                        │                     │                    │──on_signal()──>│
    │                        │                     │                    │                 │
    │                        │                     │                    │<──initialize───│
    │                        │                     │                    │                 │
    │                        │                     │──[PostStart]──────>│                 │
    │                        │                     │                    │──on_signal()──>│
    │                        │                     │                    │                 │
    │                        │                     │   STATE = RUNNING  │                 │
    │<──ActorRef─────────────│                     │                    │                 │
    │                        │                     │                    │                 │
```

**Implementation Steps:**

1. **Spawn Request**: User calls `system.spawn(behavior)` or `context.spawn(behavior)`
2. **Reference Creation**: System generates unique ID and creates `ActorRef(T)`
3. **Context Creation**: `ActorContext` created with behavior and reference
4. **Registration**: Context registered in `ActorRegistry` with state = CREATED
5. **Mailbox Creation**: `context.start()` creates mailbox with dispatcher
6. **State Transition**: CREATED → STARTING
7. **PreStart Signal**: `PreStart` system message sent to mailbox
8. **Behavior Hook**: `behavior.on_signal(PreStart)` called for initialization
9. **Validation**: If initialization succeeds, continue; if fails, transition to FAILED
10. **PostStart Signal**: `PostStart` system message sent
11. **State Transition**: STARTING → RUNNING
12. **Return**: `ActorRef` returned to caller

#### 2. Normal Message Processing (RUNNING State)

```
Sender                   ActorRef              Context              Mailbox           Behavior
  │                         │                     │                    │                 │
  │──message──────────────>│                     │                    │                 │
  │                         │──tell()────────────>│                    │                 │
  │                         │                     │──enqueue()────────>│                 │
  │                         │                     │                    │                 │
  │                         │                     │                    │──schedule if not scheduled
  │                         │                     │                    │                 │
  │                         │                     │                    │──dispatch()────>│
  │                         │                     │                    │                 │
  │                         │                     │                    │──receive()─────>│
  │                         │                     │                    │                 │
  │                         │                     │                    │<──process───────│
  │                         │                     │                    │                 │
  │                         │                     │                    │──check queue───>│
  │                         │                     │                    │                 │
### Envelope senders & ask/ack pattern

- Every user message travels inside an `Envelope` that now carries an optional `sender : ActorRefBase?`. While a behavior handles a message it can retrieve the sender via `context.sender`.
- Reply helpers: `Movie::Ask.success(context.sender, value)` and `Movie::Ask.failure(context.sender, error, ResponseType)` ship values/errors back to the temporary listener tied to the original `ask` call.
- `ActorContext#ask(target, message, response_type : T.class = Nil, timeout : Time::Span? = nil)` materializes a temporary listener actor, sends `message` with that listener as the envelope sender, and returns a `Future(T)` that completes when the target replies. Default `response_type` is `Nil`, so simple ACKs look like:

```crystal
future = context.ask(order_actor, :reserve_stock)
future.await(200.milliseconds) # raises on FutureTimeout/Failure/Cancel
```

- To receive typed data, pass the expected response type:

```crystal
future = context.ask(payment_actor, :charge, Float64)
amount = future.await(500.milliseconds)
```

- A target actor only needs access to `context.sender` to acknowledge:

```crystal
class PaymentActor < Movie::AbstractBehavior(Symbol)
  def receive(message, context)
    case message
    when :charge
      charge = process_payment
      Movie::Ask.success(context.sender, charge)
    end
    Movie::Behaviors(Symbol).same
  end
end
```

- If the target terminates before replying, the future fails with `Movie::Ask::TargetTerminated`. Optional timeouts passed to `ask` force `FutureTimeout` and stop the temporary listener.
```

#### 3. Graceful Stop

```
User/System              ActorRef              Context              Mailbox           Behavior          Children
    │                        │                     │                    │                 │                 │
    │──stop()───────────────>│                     │                    │                 │                 │
    │                        │──[Stop]────────────>│                    │                 │                 │
    │                        │                     │──enqueue()────────>│                 │                 │
    │                        │                     │                    │                 │                 │
    │                        │                     │   STATE = STOPPING │                 │                 │
    │                        │                     │                    │                 │                 │
    │                        │                     │──reject user msgs──│                 │                 │
    │                        │                     │                    │                 │                 │
    │                        │                     │──finish current────│                 │                 │
    │                        │                     │                    │                 │                 │
    │                        │                     │──[PreStop]────────>│                 │                 │
    │                        │                     │                    │──on_signal()──>│                 │
    │                        │                     │                    │                 │                 │
    │                        │                     │                    │<──cleanup──────│                 │
    │                        │                     │                    │                 │                 │
    │                        │                     │──stop_children()───│                 │                 │
    │                        │                     │  (non-blocking)    │                 │                 │
    │                        │                     │                    │                 │                 │
    │                        │                     │──[Stop]────────────────────────────────────────────>│
    │                        │                     │──[Stop]────────────────────────────────────────────>│
    │                        │                     │──[Stop]────────────────────────────────────────────>│
    │                        │                     │  (to all children) │                 │                 │
    │                        │                     │                    │                 │                 │
    │                        │                     │  [Continue processing                │            [PreStop]
    │                        │                     │   SYSTEM messages] │                 │                 │
    │                        │                     │                    │                 │      [stop grandchildren]
    │                        │                     │                    │                 │                 │
    │                        │                     │                    │                 │      [WAIT for all]
    │                        │                     │                    │                 │                 │
    │                        │                     │                    │                 │            [PostStop]
    │                        │                     │                    │                 │                 │
    │                        │                     │                    │                 │          [TERMINATED]
    │                        │                     │                    │                 │                 │
    │                        │                     │<──[Terminated]─────────────────────────────────────────│
    │                        │                     │  system message    │                 │                 │
    │                        │                     │                    │                 │                 │
    │                        │                     │──track child count─│                 │                 │
    │                        │                     │  (decremented)     │                 │                 │
    │                        │                     │                    │                 │                 │
    │                        │                     │  [When all children│                 │                 │
    │                        │                     │   terminated]      │                 │                 │
    │                        │                     │                    │                 │                 │
    │                        │                     │──[PostStop]───────>│                 │                 │
    │                        │                     │                    │──on_signal()──>│                 │
    │                        │                     │                    │                 │                 │
    │                        │                     │   STATE = STOPPED  │                 │                 │
    │                        │                     │                    │                 │                 │
    │                        │──notify watchers───>│                    │                 │                 │
    │                        │                     │                    │                 │                 │
    │                        │──unregister()──────>│                    │                 │                 │
    │                        │                     │                    │                 │                 │
    │                        │   STATE = TERMINATED│                    │                 │                 │
    │                        │                     │                    │                 │                 │
```

**Stop Process:**

1. **Stop Request**: `actor.stop()` or `Stop` system message sent
2. **State Transition**: RUNNING → STOPPING
3. **Reject New Messages**: User messages rejected, only system messages accepted
4. **Reject User Messages**: User messages are rejected/sent to DeadLetter in STOPPING state
5. **Complete Current**: Finish processing current message
6. **PreStop Signal**: Send `PreStop` system message
7. **Cleanup Hook**: `behavior.on_signal(PreStop)` called
8. **Stop Children**: **NON-BLOCKING** - Send stop to all children, then continue processing system messages
   - Sends `Stop` system message to all children asynchronously
   - Children recursively stop their own children first (depth-first)
   - Parent continues to process system messages while waiting
   - Parent tracks count of active children
   - Children send `Terminated` system message to parent when done
   - Parent receives `Terminated` messages and decrements child counter
9. **Wait for Children**: Actor remains in STOPPING state processing system messages
   - Each `Terminated` message from child decrements counter
   - When child counter reaches zero, proceed to PostStop
   - Timeout mechanism can force proceed if children don't respond
10. **PostStop Signal**: Send `PostStop` system message (only after all children terminated)
11. **Release Resources**: Close connections, free memory
12. **State Transition**: STOPPING → STOPPED
13. **Notify Watchers**: Send `Terminated` message to all watchers
14. **Unregister**: Remove from `ActorRegistry`
15. **Final State**: STOPPED → TERMINATED

**Critical Guarantee**: A parent actor **never** completes its stop sequence (PostStop) before all of its children have fully terminated. This is achieved through asynchronous system message handling rather than blocking. The parent continues to be responsive to system events while waiting for children to stop.

#### 4. Error Handling and Restart

```
Behavior                Context              Supervisor           Mailbox
    │                      │                       │                  │
    │──exception thrown───>│                       │                  │
    │                      │──[Failure]───────────>│                  │
    │                      │                       │                  │
    │                      │   STATE = FAILED      │                  │
    │                      │                       │                  │
    │                      │                       │──decide strategy──
    │                      │                       │                  │
    │                      │<──[Restart]───────────│                  │
    │                      │                       │                  │
    │                      │   STATE = RESTARTING  │                  │
    │                      │                       │                  │
    │<──[PreRestart]───────│                       │                  │
    │                      │                       │                  │
    │──cleanup old state───│                       │                  │
    │                      │                       │                  │
    │                      │──[PostStop]──────────>│                  │
    │<──on_signal()────────│                       │                  │
    │                      │                       │                  │
    │                      │──clear mailbox───────────────────────────>│
    │                      │                       │                  │
    │                      │   STATE = STARTING    │                  │
    │                      │                       │                  │
    │<──[PreStart]─────────│                       │                  │
    │                      │                       │                  │
    │──initialize new state│                       │                  │
    │                      │                       │                  │
    │                      │──[PostStart]─────────>│                  │
    │                      │                       │                  │
    │                      │   STATE = RUNNING     │                  │
    │                      │                       │                  │
```

**Restart Process:**

1. **Failure Detection**: Exception caught in `receive()` or `on_signal()`
2. **Failure Message**: `Failure` system message created with exception
3. **State Transition**: RUNNING → FAILED
4. **Notify Supervisor**: If supervised, send `Failure` to parent
5. **Supervision Decision**: Supervisor decides: Restart, Stop, Escalate, or Resume
6. **PreRestart Signal**: Send `PreRestart(cause)` system message
7. **Cleanup**: `behavior.on_signal(PreRestart)` cleans up old state
8. **Stop Sequence**: Execute `PostStop` for complete cleanup
9. **Clear Mailbox**: Optionally clear or preserve messages
10. **State Transition**: FAILED → RESTARTING → STARTING
11. **Restart**: Execute normal startup sequence (PreStart, PostStart)
12. **State Transition**: STARTING → RUNNING

### Supervision Strategies

Supervision defines how parent actors handle child failures. The runtime exposes an action (`SupervisionStrategy`) and a scope (`SupervisionScope`), plus restart limits with backoff.

```crystal
module Movie
  enum SupervisionStrategy
    RESTART   # restart the failed child/group
    STOP      # stop permanently
    RESUME    # swallow the fault and continue
    ESCALATE  # propagate failure to parent
  end

  enum SupervisionScope
    ONE_FOR_ONE  # act on the failing child only
    ALL_FOR_ONE  # apply action to all siblings of the failing child
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

    def initialize(
      @strategy = SupervisionStrategy::RESTART,
      @scope = SupervisionScope::ONE_FOR_ONE,
      @max_restarts = 3,
      @within = 1.second,
      @backoff_min = 10.milliseconds,
      @backoff_max = 1.second,
      @backoff_factor = 2.0,
      @jitter = 0.0
    )
    end
  end
end
```

Backoff uses exponential growth (`backoff_min * backoff_factor^(attempt-1)`) clamped to `backoff_max`, with optional ±`jitter` scaling.

**Example (per-actor config):**

```crystal
require "./src/movie"

class FailingWorker < Movie::AbstractBehavior(Int32)
  def receive(message, context)
    raise "boom" if message == 1
    Movie::Behaviors(Int32).same
  end
end

# One-for-one with modest backoff
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

system = Movie::ActorSystem(Int32).new(Movie::Behaviors(Int32).same, Movie::RestartStrategy::RESTART, one_for_one)
worker = system.spawn(FailingWorker.new, Movie::RestartStrategy::RESTART, one_for_one)

# All-for-one supervising siblings with a slower backoff ramp
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

parent = system.spawn(Movie::Behaviors(Int32).same, Movie::RestartStrategy::RESTART, all_for_one)
child_a = system.spawn(FailingWorker.new, Movie::RestartStrategy::RESTART, all_for_one)
child_b = system.spawn(FailingWorker.new, Movie::RestartStrategy::RESTART, all_for_one)
```

**Supervision Decision Tree:**

```
Child Failure
    │
    ▼
Check Restart Count ──> Exceeded? ──> Stop Child
    │                      │
    │ No                   │ Yes
    ▼                      ▼
Apply Strategy      Escalate to Parent
    │
    ├──► Restart    ──> PreRestart → PostStop → PreStart → PostStart
    ├──► Resume     ──> Continue with next message
    ├──► Stop       ──> PreStop → PostStop → Terminate
    └──► Escalate   ──> Send Failure to parent supervisor
```

### Enhanced Context Structure

```crystal
module Movie
  class ActorContext(T) < AbstractActorContext
    enum State
      CREATED
      STARTING
      RUNNING
      STOPPING
      STOPPED
      FAILED
      RESTARTING
      TERMINATED
    end

    @state : State = State::CREATED
    @mailbox : Mailbox(T)?
    @children : Array(ActorRefBase) = [] of ActorRefBase
    @watchers : Array(ActorRefBase) = [] of ActorRefBase
    @watching : Array(ActorRefBase) = [] of ActorRefBase
    @supervision_config : SupervisionConfig
    @restart_count : Int32 = 0
    @restart_window_start : Time?

    def state : State
      @state
    end

    def start
      return if @state != State::CREATED
      
      @mailbox = @system.mailboxes.create(@system.dispatchers.default, self)
      transition_to(State::STARTING)
      
      # Send PreStart system message
      send_system_message(PreStart.new)
    end

    def stop
      return if [@state.stopping?, @state.stopped?, @state.terminated?].any?
      
      transition_to(State::STOPPING)
      send_system_message(Stop.new)
    end

    def restart(cause : Exception?)
      transition_to(State::RESTARTING)
      send_system_message(PreRestart.new(cause))
    end

    def watch(target : ActorRefBase)
      @watching << target
      target_context = @system.context(target.id)
      target_context.add_watcher(self.@ref) if target_context
    end

    def unwatch(target : ActorRefBase)
      @watching.delete(target)
      target_context = @system.context(target.id)
      target_context.remove_watcher(self.@ref) if target_context
    end

    protected def add_watcher(watcher : ActorRefBase)
      @watchers << watcher
    end

    protected def remove_watcher(watcher : ActorRefBase)
      @watchers.delete(watcher)
    end

    protected def notify_watchers
      cause = @last_failure
      @watchers.each do |watcher|
        watcher_context = @system.context(watcher.id)
        watcher_context.send_system_message(Terminated.new(@ref, cause)) if watcher_context
      end
    end

    protected def transition_to(new_state : State)
      old_state = @state
      @state = new_state
      puts "Actor #{@ref.id}: #{old_state} → #{new_state}"
    end

    protected def send_system_message(message : SystemMessage)
      raise "Mailbox not initialized" unless @mailbox
      @mailbox.as(Mailbox(T)).send_system(message)
    end

    def on_message(envelope : Envelope(T))
      return unless @state.running?
      
      begin
        @behavior.receive(envelope.message, self)
      rescue ex : Exception
        handle_failure(ex, envelope)
      end
    end

    def on_system_message(message : SystemMessage)
      case message
      when PreStart
        handle_pre_start
      when PostStart
        handle_post_start
      when Stop
        handle_stop
      when PreStop
        handle_pre_stop
      when PostStop
        handle_post_stop
      when PreRestart
        handle_pre_restart(message.cause)
      when PostRestart
        handle_post_restart
      when Terminated
        handle_child_terminated(message)
      when Failure
        handle_child_failure(message)
      else
        @behavior.on_signal(message)
      end
    end

    private def handle_pre_start
      begin
        @behavior.on_signal(PreStart.new)
        send_system_message(PostStart.new)
      rescue ex : Exception
        transition_to(State::FAILED)
        @last_failure = ex
        stop
      end
    end

    private def handle_post_start
      transition_to(State::RUNNING)
      @behavior.on_signal(PostStart.new)
    end

    private def handle_stop
      send_system_message(PreStop.new)
    end

    private def handle_pre_stop
      @behavior.on_signal(PreStop.new)
      
      # Asynchronously send stop to all children (non-blocking)
      initiate_children_stop
      
      # PostStop will be triggered when all children have terminated
      # (handled by handle_child_terminated)
    end

    private def handle_post_stop
      @behavior.on_signal(PostStop.new)
      transition_to(State::STOPPED)
      notify_watchers
      @system.unregister(@ref.id)
      transition_to(State::TERMINATED)
    end

    private def handle_pre_restart(cause : Exception?)
      @behavior.on_signal(PreRestart.new(cause))
      send_system_message(PostStop.new)
    end

    private def handle_post_restart
      clear_mailbox
      transition_to(State::STARTING)
      send_system_message(PreStart.new)
    end

    private def handle_failure(ex : Exception, envelope : Envelope?)
      @last_failure = ex
      transition_to(State::FAILED)
      
      # Notify parent supervisor if exists
      if @parent
        failure_msg = Failure.new(ex, envelope)
        parent_context = @system.context(@parent.id)
        parent_context.send_system_message(failure_msg) if parent_context
      else
        # No supervisor, stop the actor
        stop
      end
    end

    private def handle_child_failure(failure : Failure)
      # Supervision logic
      strategy = supervision_decision(failure)
      
      case strategy
      when SupervisionStrategy::Restart
        child_context = find_child_context(failure)
        child_context.restart(failure.cause) if child_context
      when SupervisionStrategy::Stop
        child_context = find_child_context(failure)
        child_context.stop if child_context
      when SupervisionStrategy::Resume
        # Let child continue
      when SupervisionStrategy::Escalate
        # Re-throw to parent
        handle_failure(failure.cause, nil)
      end
    end

    private def handle_child_terminated(terminated : Terminated)
      @children.delete(terminated.actor)
      @watching.delete(terminated.actor)
      
      # If we're stopping, track child termination
      if @state.stopping?
        @stopping_children.delete(terminated.actor) if @stopping_children
        
        # Check if all children have now terminated
        check_and_complete_stop
      end
      
      # Notify behavior
      @behavior.on_signal(terminated)
    end

    private def supervision_decision(failure : Failure) : SupervisionStrategy
      # Check restart limits
      now = Time.utc
      @restart_window_start ||= now
      
      if (now - @restart_window_start.as(Time)) > @supervision_config.time_window
        @restart_count = 0
        @restart_window_start = now
      end
      
      @restart_count += 1
      
      if @restart_count > @supervision_config.max_restarts
        SupervisionStrategy::Stop
      else
        @supervision_config.strategy
      end
    end

    private def clear_mailbox
      @mailbox.as(Mailbox(T)).clear if @mailbox
    end
  end
end
```

### Enhanced Mailbox Structure

```crystal
module Movie
  class Mailbox(T)
    @scheduled = false
    @processing = false

    def send_system(message : SystemMessage)
      envelope = Envelope(SystemMessage).new(message, NullActorRef.new)
      @system.enqueue(envelope)
      schedule_dispatch unless @scheduled
    end

    def dispatch
      @processing = true
      
      # System messages have priority
      while !@system.empty?
        message = @system.dequeue
        @context.on_system_message(message.message) if message
      end
      
      # Process user messages only if in RUNNING state
      if @context.state.running?
        message = @inbox.dequeue
        @context.on_message(message) if message
      end
      
      @processing = false
      @scheduled = false
      
      # Reschedule if more messages
      if !@inbox.empty? || !@system.empty?
        schedule_dispatch
      end
    end

    def clear
      while !@inbox.empty?
        @inbox.dequeue
      end
    end

    private def schedule_dispatch
      return if @scheduled || @processing
      @dispatcher.dispatch(self)
      @scheduled = true
    end
  end
end
```

## Detailed Spawn Process

### Overview

The spawn process is the entry point for creating new actors in the system. It can be initiated from the `ActorSystem` itself (for top-level actors) or from within an actor's context (for child actors). This section provides a comprehensive breakdown of the spawn operation.

### Spawn Entry Points

```crystal
# 1. Spawn from ActorSystem (top-level actor)
actor_ref = system.spawn(MyBehavior.create())

# 2. Spawn from Actor Context (child actor)
class ParentBehavior < AbstractBehavior(Message)
  def receive(message, context)
    child_ref = context.spawn(ChildBehavior.create())
  end
end
```

### Spawn Process Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                         ACTOR SPAWN PROCESS                         │
└─────────────────────────────────────────────────────────────────────┘

User/Parent Actor
    │
    │ spawn(behavior)
    ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Step 1: ENTRY POINT VALIDATION                                      │
│  - Validate behavior is not nil                                     │
│  - Check system is initialized                                      │
│  - Verify registry is available                                     │
└─────────────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Step 2: ID GENERATION                                               │
│  - Atomic increment: @id_generator.add(1)                           │
│  - Thread-safe operation                                            │
│  - Guaranteed uniqueness                                            │
│  - Example: 1, 2, 3, 4, ... N                                      │
└─────────────────────────────────────────────────────────────────────┘
    │
    │ Generated ID: Int32
    ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Step 3: ACTOR REFERENCE CREATION                                    │
│  ActorRef(T).new(@system)                                           │
│                                                                     │
│  Creates:                                                           │
│    - ActorRef(T) with type safety                                   │
│    - @id = generated ID                                             │
│    - @system = reference to ActorSystem                             │
│                                                                     │
│  Purpose:                                                           │
│    - Handle for sending messages                                    │
│    - Location transparency                                          │
│    - Type-safe message protocol                                     │
└─────────────────────────────────────────────────────────────────────┘
    │
    │ ActorRef(T)
    ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Step 4: ACTOR CONTEXT CREATION                                      │
│  ActorContext(T).new(behavior, ref, system)                         │
│                                                                     │
│  Creates:                                                           │
│    - @behavior = user-provided behavior                             │
│    - @ref = actor reference                                         │
│    - @system = actor system reference                               │
│    - @state = State::CREATED                                        │
│    - @mailbox = nil (not yet created)                               │
│    - @children = [] (empty children list)                           │
│    - @watchers = [] (empty watchers list)                           │
│    - @watching = [] (empty watching list)                           │
│    - @supervision_config = default config                           │
│    - @restart_count = 0                                             │
│    - @last_failure = nil                                            │
│                                                                     │
│  Purpose:                                                           │
│    - Container for actor state and behavior                         │
│    - Manages actor lifecycle                                        │
│    - Handles message processing                                     │
│    - Supervises child actors                                        │
└─────────────────────────────────────────────────────────────────────┘
    │
    │ ActorContext(T)
    ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Step 5: REGISTRY REGISTRATION                                       │
│  @actors[ref.id] = context                                          │
│                                                                     │
│  Synchronized with mutex:                                           │
│    @mutex.synchronize do                                            │
│      @actors[ref.id] = context                                      │
│    end                                                              │
│                                                                     │
│  Registry State:                                                    │
│    Key: Int32 (actor ID)                                            │
│    Value: AbstractActorContext                                      │
│                                                                     │
│  Purpose:                                                           │
│    - Enable message routing by ID                                   │
│    - Track all active actors                                        │
│    - Support actor lookup operations                                │
│    - Enable system-wide operations (shutdown, etc.)                 │
└─────────────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Step 6: PARENT-CHILD RELATIONSHIP (if spawned from context)         │
│                                                                     │
│  If spawned from actor context:                                     │
│    - Add child ref to parent's @children array                      │
│    - Set @parent reference in child context                         │
│    - Inherit supervision config (optional)                          │
│                                                                     │
│  If spawned from system:                                            │
│    - No parent relationship                                         │
│    - Top-level actor                                                │
│    - Reports to system only                                         │
└─────────────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Step 7: START INVOCATION                                            │
│  context.start()                                                    │
│                                                                     │
│  State Transition: CREATED → STARTING                               │
└─────────────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Step 8: MAILBOX CREATION                                            │
│  @mailbox = @system.mailboxes.create(dispatcher, self)             │
│                                                                     │
│  Creates:                                                           │
│    - Mailbox(T) instance                                            │
│    - @inbox = Queue(Envelope(T)).new                                │
│    - @system = Queue(Envelope(SystemMessage)).new                   │
│    - @dispatcher = default or custom dispatcher                     │
│    - @context = reference to ActorContext                           │
│    - @scheduled = false                                             │
│    - @processing = false                                            │
│                                                                     │
│  Dispatcher Selection:                                              │
│    - Default: ParallelDispatcher (thread pool)                      │
│    - Can be overridden per actor                                    │
│    - Retrieved from DispatcherRegistry                              │
│                                                                     │
│  Purpose:                                                           │
│    - Message queue management                                       │
│    - Scheduling coordination                                        │
│    - Priority handling (system vs user messages)                    │
└─────────────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Step 9: PRESTART SIGNAL                                             │
│  send_system_message(PreStart.new)                                  │
│                                                                     │
│  Process:                                                           │
│    1. Create PreStart system message                                │
│    2. Wrap in Envelope(SystemMessage)                               │
│    3. Enqueue to @system queue in mailbox                           │
│    4. Schedule mailbox dispatch                                     │
│                                                                     │
│  Dispatcher executes:                                               │
│    - Spawn fiber in execution context                               │
│    - Call mailbox.dispatch()                                        │
│    - Dequeue PreStart message                                       │
│    - Invoke context.on_system_message(PreStart)                     │
│                                                                     │
│  Behavior Hook:                                                     │
│    - behavior.on_signal(PreStart) called                            │
│    - Actor performs initialization                                  │
│    - Open connections, load resources, etc.                         │
│                                                                     │
│  Error Handling:                                                    │
│    - If PreStart throws exception:                                  │
│      * State → FAILED                                               │
│      * Execute stop sequence                                        │
│      * Notify supervisor (if exists)                                │
│                                                                     │
│  On Success:                                                        │
│    - Enqueue PostStart message                                      │
└─────────────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Step 10: POSTSTART SIGNAL                                           │
│  send_system_message(PostStart.new)                                 │
│                                                                     │
│  Process:                                                           │
│    - Dequeued and processed after PreStart                          │
│    - behavior.on_signal(PostStart) called                           │
│    - Final initialization steps                                     │
│                                                                     │
│  State Transition: STARTING → RUNNING                               │
│                                                                     │
│  Actor is now:                                                      │
│    - Fully initialized                                              │
│    - Ready to receive user messages                                 │
│    - Able to spawn children                                         │
│    - Can send messages to other actors                              │
└─────────────────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Step 11: RETURN ACTOR REFERENCE                                     │
│  return ref : ActorRef(T)                                           │
│                                                                     │
│  Caller receives:                                                   │
│    - Type-safe actor reference                                      │
│    - Can send messages via << operator                              │
│    - Can stop actor via ref.stop()                                  │
│    - Can watch actor via ref.watch()                                │
│                                                                     │
│  Actor state:                                                       │
│    - State = RUNNING                                                │
│    - Registered in ActorRegistry                                    │
│    - Mailbox initialized and empty                                  │
│    - Ready for operation                                            │
└─────────────────────────────────────────────────────────────────────┘
    │
    ▼
[Actor is operational and processing messages]
```

### Detailed Component Interactions

```
┌──────────────┐      ┌──────────────┐      ┌──────────────┐
│    Caller    │      │ ActorSystem  │      │   Registry   │
└──────┬───────┘      └──────┬───────┘      └──────┬───────┘
       │                     │                     │
       │ spawn(behavior)     │                     │
       │────────────────────>│                     │
       │                     │                     │
       │                     │ next_id()           │
       │                     │─────────┐           │
       │                     │         │ Atomic    │
       │                     │<────────┘ increment │
       │                     │                     │
       │                     │ new ActorRef(id)    │
       │                     │─────────┐           │
       │                     │         │           │
       │                     │<────────┘           │
       │                     │                     │
       │                     │ new Context(behavior, ref)
       │                     │─────────┐           │
       │                     │         │           │
       │                     │<────────┘           │
       │                     │                     │
       │                     │ register(id, ctx)   │
       │                     │────────────────────>│
       │                     │                     │
       │                     │                     │ [mutex lock]
       │                     │                     │ @actors[id] = ctx
       │                     │                     │ [mutex unlock]
       │                     │                     │
       │                     │<────────────────────│
       │                     │                     │
       │                     │ context.start()     │
       │                     │─────────┐           │
       │                     │         │           │
       │                     │         ▼           │
       │                     │  ┌────────────┐     │
       │                     │  │  Mailbox   │     │
       │                     │  │  Manager   │     │
       │                     │  └─────┬──────┘     │
       │                     │        │            │
       │                     │        │ create_mailbox()
       │                     │        │            │
       │                     │        ▼            │
       │                     │  ┌────────────┐     │
       │                     │  │ Dispatcher │     │
       │                     │  │  Registry  │     │
       │                     │  └─────┬──────┘     │
       │                     │        │            │
       │                     │        │ get_default()
       │                     │        │            │
       │                     │<───────┘            │
       │                     │                     │
       │                     │ [PreStart message]  │
       │                     │─────────┐           │
       │                     │         ▼           │
       │                     │    ┌─────────┐      │
       │                     │    │ Mailbox │      │
       │                     │    └────┬────┘      │
       │                     │         │           │
       │                     │         │ enqueue() │
       │                     │         │ schedule()│
       │                     │         ▼           │
       │                     │    ┌──────────┐     │
       │                     │    │Dispatcher│     │
       │                     │    └────┬─────┘     │
       │                     │         │           │
       │                     │         │ spawn fiber
       │                     │         │ dispatch()│
       │                     │         ▼           │
       │                     │    [Behavior.on_signal(PreStart)]
       │                     │         │           │
       │                     │         │ [initialization]
       │                     │         │           │
       │                     │         ▼           │
       │                     │    [PostStart message]
       │                     │         │           │
       │                     │         ▼           │
       │                     │    [State = RUNNING]│
       │                     │                     │
       │<────────────────────│                     │
       │   ActorRef(T)       │                     │
       │                     │                     │
```

### Spawn Method Signatures

```crystal
module Movie
  # ActorSystem spawn (top-level actor)
  class ActorSystem(T) < AbstractActorSystem
    def spawn(behavior : AbstractBehavior) : ActorRef
      raise "System not initialized" unless @registry
      @registry.as(ActorRegistry).spawn(behavior)
    end
  end

  # ActorContext spawn (child actor)
  class ActorContext(T) < AbstractActorContext
    def spawn(behavior : AbstractBehavior(U)) : ActorRef(U) forall U
      raise "System not initialized" unless @system
      
      # Spawn through system
      child_ref = @system.spawn(behavior)
      
      # Establish parent-child relationship
      @children << child_ref
      
      # Set parent reference in child context
      child_context = @system.context(child_ref.id)
      child_context.as(ActorContext(U)).set_parent(@ref) if child_context
      
      child_ref
    end
  end

  # ActorRegistry spawn (internal)
  class ActorRegistry
    def spawn(behavior : AbstractBehavior(T)) : ActorRef(T) forall T
      raise "System not initialized" unless @system
      
      # Step 1: Generate ID
      ref = ActorRef(T).new(@system.as(ActorSystem))
      
      # Step 2: Create context
      context = ActorContext(T).new(behavior, ref, @system.as(AbstractActorSystem))
      
      # Step 3: Register
      @mutex.synchronize do
        @actors[ref.id] = context
      end
      
      # Step 4: Start
      context.start
      
      # Step 5: Return reference
      ref
    end
  end
end
```

### Spawn Variants and Options

```crystal
# Basic spawn
actor = system.spawn(MyBehavior.new)

# Spawn with custom dispatcher
actor = system.spawn(MyBehavior.new, dispatcher: "pinned")

# Spawn with supervision config
actor = system.spawn(
  MyBehavior.new,
  supervision: SupervisionConfig.new(
    strategy: SupervisionStrategy::Restart,
    max_restarts: 5,
    time_window: 1.minute
  )
)

# Spawn with custom mailbox size
actor = system.spawn(
  MyBehavior.new,
  mailbox_size: 1000
)

# Deferred spawn (lazy initialization)
deferred = Behaviors.setup do |context|
  # Initialize based on runtime conditions
  if condition
    BehaviorA.new
  else
    BehaviorB.new
  end
end

actor = system.spawn(deferred)
```

### Thread Safety During Spawn

The spawn process involves multiple shared resources that must be synchronized:

1. **ID Generation**: Uses `Atomic(Int32)` for lock-free increment
   ```crystal
   @id_generator.add(1)  # Atomic operation, no lock needed
   ```

2. **Registry Access**: Protected by mutex
   ```crystal
   @mutex.synchronize do
     @actors[ref.id] = context
   end
   ```

3. **Parent's Children List**: Synchronized in context
   ```crystal
   @children_mutex.synchronize do
     @children << child_ref
   end
   ```

4. **Mailbox Creation**: Thread-safe factory pattern
   ```crystal
   # Each mailbox has its own queues with their own mutexes
   mailbox = Mailbox.new(dispatcher, context)
   ```

### Spawn Failure Handling

```
spawn(behavior)
    │
    ▼
Try: ID Generation
    │
    ├─ Success ────────────────┐
    │                          │
    └─ Failure ───> Exception  │
                                │
                                ▼
                    Try: Context Creation
                                │
                    ├─ Success ────────────────┐
                    │                          │
                    └─ Failure ───> Exception  │
                                                │
                                                ▼
                                    Try: Registration
                                                │
                                    ├─ Success ────────────────┐
                                    │                          │
                                    └─ Failure ───> Rollback   │
                                                    Cleanup    │
                                                    Exception  │
                                                                │
                                                                ▼
                                                    Try: Start (PreStart)
                                                                │
                                                    ├─ Success ─────> Return Ref
                                                    │
                                                    └─ Failure ───> Unregister
                                                                    Stop Actor
                                                                    Exception
```

**Failure Recovery:**
- If spawn fails before registration: No cleanup needed
- If spawn fails after registration: Actor is unregistered
- If PreStart fails: Actor is stopped and removed from registry
- Caller receives exception and can retry

### Memory Layout After Spawn

```
Heap Memory Layout:
┌────────────────────────────────────────────────────────────┐
│ ActorSystem                                                │
│  ├─ @registry: ActorRegistry                               │
│  │   └─ @actors: Hash(Int32 => ActorContext)               │
│  │       └─ [ID] => ┐                                      │
│  ├─ @dispatchers: DispatcherRegistry                       │
│  └─ @mailboxes: MailboxManager                             │
└─────────────────────┬──────────────────────────────────────┘
                      │
                      ▼
┌────────────────────────────────────────────────────────────┐
│ ActorContext(T)                                            │
│  ├─ @behavior: MyBehavior  ◄───────────────┐               │
│  ├─ @ref: ActorRef(T)                      │               │
│  ├─ @system: ActorSystem                   │               │
│  ├─ @state: State::RUNNING                 │               │
│  ├─ @mailbox: Mailbox(T) ───┐              │               │
│  ├─ @children: Array(ActorRefBase)         │               │
│  ├─ @parent: ActorRefBase?                 │               │
│  └─ @supervision_config                    │               │
└────────────────────────┬───────────────────┼───────────────┘
                         │                   │
                         ▼                   │
┌────────────────────────────────────────────┼───────────────┐
│ Mailbox(T)                                 │               │
│  ├─ @inbox: Queue(Envelope(T))             │               │
│  ├─ @system: Queue(Envelope(SystemMessage))│               │
│  ├─ @dispatcher: Dispatcher                │               │
│  ├─ @context: ActorContext(T) ─────────────┘               │
│  ├─ @scheduled: Bool                                       │
│  └─ @processing: Bool                                      │
└────────────────────────────────────────────────────────────┘

Stack/Fiber Memory:
┌────────────────────────────────────────────────────────────┐
│ User Code Fiber                                            │
│  └─ actor_ref: ActorRef(T)  (references heap context)      │
└────────────────────────────────────────────────────────────┘
┌────────────────────────────────────────────────────────────┐
│ Dispatcher Fiber (for PreStart)                            │
│  └─ Executing behavior.on_signal(PreStart)                 │
└────────────────────────────────────────────────────────────┘
```

### Performance Characteristics of Spawn

| Operation | Time Complexity | Space Complexity | Notes |
|-----------|----------------|------------------|-------|
| ID Generation | O(1) | O(1) | Atomic increment |
| ActorRef Creation | O(1) | O(1) | Simple struct allocation |
| Context Creation | O(1) | O(1) | Object allocation with fields |
| Registry Registration | O(1) | O(1) | Hash map insert with mutex |
| Mailbox Creation | O(1) | O(1) | Queue initialization |
| PreStart Dispatch | O(1) | O(1) | Fiber spawn + enqueue |
| **Total Spawn** | **O(1)** | **O(1)** | All operations constant time |

**Throughput Considerations:**
- Spawn is fast: typically microseconds
- Registry mutex is brief bottleneck
- PreStart execution is async (doesn't block spawn)
- Can spawn thousands of actors per second

### Best Practices for Spawning

1. **Spawn Strategy:**
   ```crystal
   # Good: Spawn actors lazily when needed
   def receive(message, context)
     worker = context.spawn(Worker.new)
     worker << message
   end
   
   # Bad: Spawn excessive actors upfront
   def pre_start
     1000.times { context.spawn(Worker.new) }  # May exhaust resources
   end
   ```

2. **Initialization in PreStart:**
   ```crystal
   class MyBehavior < AbstractBehavior(Message)
     def on_signal(signal)
       case signal
       when PreStart
         # Open database connections
         @db = Database.connect(...)
         # Load configuration
         @config = load_config()
       end
     end
   end
   ```

3. **Handle Spawn Failures:**
   ```crystal
   begin
     actor = system.spawn(MyBehavior.new)
   rescue ex : Exception
     puts "Failed to spawn actor: #{ex.message}"
     # Retry or use fallback strategy
   end
   ```

4. **Parent-Child Hierarchy:**
   ```crystal
   class Supervisor < AbstractBehavior(Command)
     def pre_start
       # Spawn children during initialization
       @workers = 10.times.map do
         context.spawn(Worker.new)
       end.to_a
     end
   end
   ```

## Actor Cleanup and Registry Removal

### Overview

When an actor stops (gracefully or due to failure), proper cleanup is essential to prevent memory leaks and resource exhaustion. The cleanup process involves removing the actor from the registry, notifying watchers, releasing resources, and deallocating memory.

### Cleanup Trigger Points

Actors can be removed from the registry through several paths:

1. **Graceful Stop**: Explicitly requested via `actor.stop()`
2. **System Shutdown**: All actors stopped during `system.shutdown()`
3. **Failure**: Actor failed and supervisor decided to stop (not restart)
4. **Parent Termination**: Parent stopped, children must also stop
5. **Self-Termination**: Actor completes its work and stops itself

### Registry Removal Process

```
┌─────────────────────────────────────────────────────────────────────┐
│                    ACTOR CLEANUP & REMOVAL PROCESS                  │
└─────────────────────────────────────────────────────────────────────┘

[Actor in STOPPING state]
        │
        │ PostStop completed
        ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Step 1: STATE TRANSITION                                            │
│   State: STOPPING → STOPPED                                         │
│                                                                     │
│   Actor no longer processes messages                                │
│   Mailbox is frozen                                                 │
│   Behavior cleanup completed                                        │
└─────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Step 2: NOTIFY WATCHERS                                             │
│   For each watcher in @watchers:                                    │
│     - Create Terminated(actor_ref, cause) message                   │
│     - Send to watcher's mailbox                                     │
│     - Watcher can react to termination                              │
│                                                                     │
│   Purpose:                                                          │
│     - Allow dependent actors to handle termination                  │
│     - Enable cleanup of cross-actor resources                       │
│     - Support supervision and monitoring patterns                   │
└─────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Step 3: STOP CHILD ACTORS (NON-BLOCKING)                            │
│   For each child in @children:                                      │
│     1. Send Stop system message                                     │
│     2. Track child in @stopping_children set                        │
│     3. Return immediately (non-blocking)                            │
│                                                                     │
│   Recursive Process:                                                │
│     - Children stop their children first (depth-first)              │
│     - Bottom-up termination tree via system messages                │
│     - Parent continues processing system messages                   │
│                                                                     │
│   Asynchronous Waiting:                                             │
│     - Parent receives Terminated system messages from children      │
│     - Each Terminated message removes child from tracking set       │
│     - When @stopping_children becomes empty → proceed to PostStop   │
│                                                                     │
│   Timeout Handling:                                                 │
│     - Start timeout timer when stop initiated                       │
│     - If timeout expires before all children terminated             │
│     - Force terminate remaining children                            │
│     - Proceed to PostStop                                           │
│                                                                     │
│   Guarantee:                                                        │
│     - Parent never proceeds to PostStop before children terminate   │
│     - Parent remains responsive to system messages while waiting    │
│     - Prevents orphaned actors                                      │
│     - Ensures clean resource hierarchy                              │
└─────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Step 4: CLEANUP WATCHES                                             │
│   For each watched actor in @watching:                              │
│     - Send Unwatch message to watched actor                         │
│     - Remove self from watched actor's @watchers list               │
│                                                                     │
│   Purpose:                                                          │
│     - Prevent memory leaks from stale watchers                      │
│     - Clean bidirectional watch relationships                       │
└─────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Step 5: DRAIN MAILBOX                                               │
│   Process remaining messages based on strategy:                     │
│                                                                     │
│   Option A: Drop All                                                │
│     - Discard all messages in inbox                                 │
│     - Fast shutdown                                                 │
│     - Messages lost                                                 │
│                                                                     │
│   Option B: DeadLetter                                              │
│     - Send messages to DeadLetter queue                             │
│     - System can log/monitor dropped messages                       │
│     - Useful for debugging                                          │
│                                                                     │
│   Option C: Redirect                                                │
│     - Forward messages to another actor                             │
│     - Preserve message semantics                                    │
│     - Used for graceful handoff                                     │
│                                                                     │
│   System messages are always discarded                              │
└─────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Step 6: RELEASE MAILBOX                                             │
│   - Clear inbox queue                                               │
│   - Clear system message queue                                      │
│   - Unlink from dispatcher                                          │
│   - Set @mailbox = nil in context                                   │
│                                                                     │
│   Dispatcher Effects:                                               │
│     - No more fiber spawns for this mailbox                         │
│     - Pending dispatch operations become no-ops                     │
└─────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Step 7: UNREGISTER FROM PARENT                                      │
│   If actor has parent:                                              │
│     1. Locate parent context                                        │
│     2. Remove self from parent's @children array                    │
│     3. Synchronized with parent's mutex                             │
│                                                                     │
│   Purpose:                                                          │
│     - Clean parent-child relationship                               │
│     - Prevent parent from trying to supervise stopped actor         │
└─────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Step 8: UNREGISTER FROM SYSTEM                                      │
│   @system.unregister(actor_id)                                      │
│                                                                     │
│   Registry Operation:                                               │
│     @mutex.synchronize do                                           │
│       context = @actors.delete(actor_id)                            │
│       @terminated_count += 1                                        │
│     end                                                             │
│                                                                     │
│   Effects:                                                          │
│     - Actor no longer reachable via ID lookup                       │
│     - Message sends to this actor will fail                         │
│     - Actor ID can be reused (after wrap-around)                    │
└─────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Step 9: FINAL STATE TRANSITION                                      │
│   State: STOPPED → TERMINATED                                       │
│                                                                     │
│   TERMINATED is terminal state:                                     │
│     - No further state transitions                                  │
│     - Actor is functionally dead                                    │
│     - Waiting for garbage collection                                │
└─────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Step 10: MEMORY DEALLOCATION                                        │
│   Garbage collector will reclaim:                                   │
│     - ActorContext object                                           │
│     - Behavior instance and its state                               │
│     - Mailbox and queues                                            │
│     - Message envelopes                                             │
│                                                                     │
│   ActorRef may still exist:                                         │
│     - Held by other actors                                          │
│     - Becomes "dead letter" reference                               │
│     - Sends to it fail gracefully                                   │
│                                                                     │
│   Timing:                                                           │
│     - Immediate: Registry removal                                   │
│     - Delayed: GC reclaims memory when no references remain         │
└─────────────────────────────────────────────────────────────────────┘
        │
        ▼
[Actor fully terminated and removed]
```

### Detailed Component Interactions During Cleanup

```
Actor Context        Registry         Watchers         Children         Mailbox
      │                 │                 │                │               │
      │ [STOPPING]      │                 │                │               │
      │                 │                 │                │               │
      │─PostStop────────│                 │                │               │
      │                 │                 │                │               │
      │ [STOPPED]       │                 │                │               │
      │                 │                 │                │               │
      │─notify()───────────────────────>│                │               │
      │                 │                 │                │               │
      │                 │           [Terminated msg]       │               │
      │                 │                 │                │               │
      │─stop_children()─────────────────────────────────>│               │
      │                 │                 │                │               │
      │                 │                 │          [Stop msgs]           │
      │                 │                 │                │               │
      │<─children stopped────────────────────────────────│               │
      │                 │                 │                │               │
      │─cleanup_watches()───────────────>│                │               │
      │                 │                 │                │               │
      │                 │           [Unwatch msgs]         │               │
      │                 │                 │                │               │
      │─drain_mailbox()───────────────────────────────────────────────>│
      │                 │                 │                │               │
      │                 │                 │                │         [clear queues]
      │                 │                 │                │               │
      │─unregister()──>│                 │                │               │
      │                 │                 │                │               │
      │                 │ [mutex lock]    │                │               │
      │                 │ delete(id)      │                │               │
      │                 │ [mutex unlock]  │                │               │
      │                 │                 │                │               │
      │ [TERMINATED]    │                 │                │               │
      │                 │                 │                │               │
      │────────────────────────────────> [GC eligible] <──────────────────│
```

### Registry Unregister Implementation

```crystal
module Movie
  class ActorRegistry
    @actors : Hash(Int32, AbstractActorContext)
    @mutex : Mutex
    @terminated_count : Atomic(Int64)

    def unregister(id : Int32) : AbstractActorContext?
      context = nil
      
      @mutex.synchronize do
        context = @actors.delete(id)
        @terminated_count.add(1) if context
      end
      
      # Log termination
      if context
        puts "Actor #{id} unregistered (total terminated: #{@terminated_count.get})"
      else
        puts "Warning: Attempted to unregister non-existent actor #{id}"
      end
      
      context
    end

    def stop_all(timeout : Time::Span = 5.seconds)
      deadline = Time.utc + timeout
      
      # Get snapshot of all actors
      actor_ids = @mutex.synchronize { @actors.keys }
      
      # Send stop to all actors
      actor_ids.each do |id|
        context = self[id]?
        context.stop if context
      end
      
      # Wait for all to stop
      while active_count > 0 && Time.utc < deadline
        sleep 0.1
      end
    end

    def terminate_all
      # Force terminate any remaining actors
      @mutex.synchronize do
        @actors.each_value do |context|
          context.force_terminate
        end
        @actors.clear
      end
    end

    def active_count : Int32
      @mutex.synchronize { @actors.size }
    end

    def terminated_count : Int64
      @terminated_count.get
    end

    def exists?(id : Int32) : Bool
      @mutex.synchronize { @actors.has_key?(id) }
    end
  end
end
```

### ActorContext Cleanup Methods

```crystal
module Movie
  class ActorContext(T) < AbstractActorContext
    def stop
      return if [@state.stopping?, @state.stopped?, @state.terminated?].any?
      
      transition_to(State::STOPPING)
      
      # Start stop timeout timer
      @stop_deadline = Time.utc + @stop_timeout
      
      send_system_message(PreStop.new)
      
      # The rest happens asynchronously through system message handling
    end

    def force_terminate
      # Immediate termination without cleanup
      transition_to(State::TERMINATED)
      cleanup_resources
      @system.unregister(@ref.id)
    end

    private def complete_stop_sequence
      # Called when all children have terminated
      # Triggered by last child's Terminated message
      
      # Step 1: Transition to STOPPED
      transition_to(State::STOPPED)
      
      # Step 2: Notify watchers
      notify_watchers
      
      # Step 3: Cleanup watches
      cleanup_watches
      
      # Step 4: Drain mailbox
      drain_mailbox
      
      # Step 5: Release mailbox
      release_mailbox
      
      # Step 6: Unregister from parent
      unregister_from_parent
      
      # Step 7: Unregister from system
      @system.unregister(@ref.id)
      
      # Step 8: Final transition
      transition_to(State::TERMINATED)
    end

    private def notify_watchers
      cause = @last_failure
      @watchers.each do |watcher|
        begin
          watcher_context = @system.context(watcher.id)
          if watcher_context
            terminated_msg = Terminated.new(@ref, cause)
            watcher_context.send_system_message(terminated_msg)
          end
        rescue ex : Exception
          puts "Failed to notify watcher #{watcher.id}: #{ex.message}"
        end
      end
      @watchers.clear
    end

    private def initiate_children_stop
      return check_and_complete_stop if @children.empty?
      
      puts "Actor #{@ref.id}: Initiating stop for #{@children.size} children..."
      
      # Track children that are being stopped
      @stopping_children = @children.to_set
      
      # Send stop to all children (non-blocking)
      @children.each do |child|
        begin
          child_context = @system.context(child.id)
          if child_context && !child_context.state.terminated?
            child_context.stop
          else
            # Child already terminated, remove from tracking
            @stopping_children.delete(child)
          end
        rescue ex : Exception
          puts "Failed to stop child #{child.id}: #{ex.message}"
          @stopping_children.delete(child)
        end
      end
      
      # Check if all children already stopped
      check_and_complete_stop
    end
    
    private def check_and_complete_stop
      # Check if all children have terminated
      if @stopping_children.empty? || @stopping_children.nil?
        # All children stopped, proceed to PostStop
        send_system_message(PostStop.new)
      elsif @stop_deadline && Time.utc >= @stop_deadline.as(Time)
        # Timeout expired, force terminate remaining children
        puts "Actor #{@ref.id}: Stop timeout, force terminating #{@stopping_children.size} children"
        @stopping_children.each do |child|
          child_context = @system.context(child.id)
          child_context.force_terminate if child_context
        end
        @stopping_children.clear
        send_system_message(PostStop.new)
      end
      # Otherwise, keep waiting for Terminated messages
    end

    private def cleanup_watches
      @watching.each do |watched|
        begin
          watched_context = @system.context(watched.id)
          watched_context.remove_watcher(@ref) if watched_context
        rescue ex : Exception
          puts "Failed to cleanup watch #{watched.id}: #{ex.message}"
        end
      end
      @watching.clear
    end

    private def drain_mailbox
      return unless @mailbox
      
      case @mailbox_drain_strategy
      when :drop
        @mailbox.as(Mailbox(T)).clear
      when :deadletter
        @mailbox.as(Mailbox(T)).send_to_deadletter(@system.deadletter)
      when :redirect
        @mailbox.as(Mailbox(T)).redirect_to(@redirect_target) if @redirect_target
      end
    end

    private def release_mailbox
      @mailbox = nil
    end

    private def unregister_from_parent
      return unless @parent
      
      parent_context = @system.context(@parent.id)
      parent_context.remove_child(@ref) if parent_context
      @parent = nil
    end

    protected def remove_child(child_ref : ActorRefBase)
      @children.delete(child_ref)
    end

    private def cleanup_resources
      # Behavior-specific cleanup
      begin
        @behavior.on_signal(PostStop.new)
      rescue ex : Exception
        puts "Error in PostStop: #{ex.message}"
      end
      
      # Clear references
      @children.clear
      @watchers.clear
      @watching.clear
      @mailbox = nil
    end
  end
end
```

### Message Sending to Stopped Actors

When attempting to send a message to a stopped actor, the system should handle it gracefully:

```crystal
module Movie
  class ActorRef(T) < ActorRefBase
    def <<(message : T)
      context = @system.context(@id)
      
      if context.nil?
        # Actor not found in registry
        handle_dead_letter(message)
        return
      end
      
      ctx = context.as(ActorContext(T))
      
      # Check if actor can receive messages
      unless ctx.state.running?
        handle_dead_letter(message)
        return
      end
      
      # Normal send
      ctx << message
    end

    private def handle_dead_letter(message : T)
      # Send to dead letter queue
      @system.deadletter << DeadLetter.new(
        message: message,
        sender: nil,
        recipient: @id
      )
    end
  end
end
```

### DeadLetter Queue

The DeadLetter queue collects messages that couldn't be delivered:

```crystal
module Movie
  record DeadLetter,
    message : Any,
    sender : ActorRefBase?,
    recipient : Int32,
    timestamp : Time = Time.utc

  class DeadLetterQueue
    @queue : Channel(DeadLetter)
    @max_size : Int32
    
    def initialize(@max_size = 10000)
      @queue = Channel(DeadLetter).new(@max_size)
      spawn_processor
    end

    def <<(letter : DeadLetter)
      @queue.send(letter) rescue nil  # Drop if full
    end

    private def spawn_processor
      spawn do
        loop do
          letter = @queue.receive
          log_dead_letter(letter)
        end
      end
    end

    private def log_dead_letter(letter : DeadLetter)
      puts "[DeadLetter] Message #{letter.message.class} " \
           "to actor #{letter.recipient} at #{letter.timestamp}"
    end
  end

  class AbstractActorSystem
    getter deadletter : DeadLetterQueue = DeadLetterQueue.new
  end
end
```

### System Shutdown Process

Complete system shutdown with proper cleanup:

```crystal
module Movie
  class ActorSystem(T) < AbstractActorSystem
    def shutdown(timeout : Time::Span = 30.seconds)
      puts "Initiating system shutdown..."
      deadline = Time.utc + timeout
      
      # Phase 1: Stop all user actors
      @registry.as(ActorRegistry).stop_all(timeout / 2)
      
      # Phase 2: Wait for graceful stop
      while @registry.as(ActorRegistry).active_count > 0 && Time.utc < deadline
        sleep 0.1
      end
      
      # Phase 3: Force terminate remaining
      if @registry.as(ActorRegistry).active_count > 0
        puts "Force terminating #{@registry.as(ActorRegistry).active_count} actors"
        @registry.as(ActorRegistry).terminate_all
      end
      
      # Phase 4: Shutdown dispatchers
      @dispatchers.shutdown
      
      # Phase 5: Final cleanup
      @deadletter.close
      
      puts "System shutdown complete. Terminated #{@registry.as(ActorRegistry).terminated_count} actors"
    end
  end
end
```

### Memory Leak Prevention

Key strategies to prevent memory leaks:

1. **Reference Cleanup:**
   ```crystal
   # Always clear collections during cleanup
   @children.clear
   @watchers.clear
   @watching.clear
   ```

2. **Circular Reference Breaking:**
   ```crystal
   # Break parent-child cycles
   @parent = nil
   # Break behavior-context cycles
   @mailbox = nil
   ```

3. **Watch Relationship Cleanup:**
   ```crystal
   # Bidirectional cleanup
   watched_actor.remove_watcher(self)
   @watching.delete(watched_actor)
   ```

4. **Registry Removal:**
   ```crystal
   # Remove from registry to allow GC
   @system.unregister(@ref.id)
   ```

### Cleanup Best Practices
### Best Practices

1. **Resource Release in PostStop:**
   ```crystal
   def on_signal(signal)
     case signal
     when PostStop
       @database.close if @database
       @file.close if @file
       @connection.close if @connection
     end
   end
   ```

2. **Graceful Shutdown Period:**
   ```crystal
   # Give actors time to cleanup (including waiting for children)
   # Timeout should account for nested actor hierarchies
   system.shutdown(timeout: 30.seconds)
   ```

3. **Actor Hierarchy Stop Sequence:**
   ```
   Example hierarchy:
   
   Supervisor
      ├── Worker 1
      │   ├── SubWorker 1a
      │   └── SubWorker 1b
      └── Worker 2
   
   Stop sequence when Supervisor.stop() is called:
   
   Time    Supervisor           Worker 1             SubWorker 1a    SubWorker 1b    Worker 2
   ──────────────────────────────────────────────────────────────────────────────────────────
   T0      [PreStop]            [RUNNING]            [RUNNING]       [RUNNING]       [RUNNING]
   T1      └─send stop(W1)─────>[PreStop]            [RUNNING]       [RUNNING]       [RUNNING]
   T2      └─send stop(W2)──────────────────────────────────────────────────────────>[PreStop]
   T3      [STOPPING]           └─send stop(S1a)────>[PreStop]       [RUNNING]       │
   T4      │process sys msgs    │                    │               [RUNNING]       │
   T5      │                    └─send stop(S1b)─────────────────────>[PreStop]      │
   T6      │                    [STOPPING]           [PostStop]      │               │
   T7      │                    │process sys msgs    [TERMINATED]    │               │
   T8      │                    │                    └──Terminated──>│               │
   T9      │                    │                                    [PostStop]      │
   T10     │                    │                                    [TERMINATED]    │
   T11     │                    │                                    └──Terminated──>│
   T12     │                    │                                                    [PostStop]
   T13     │                    [PostStop]                                           [TERMINATED]
   T14     │                    [TERMINATED]                                         └──Terminated──>
   T15     │                    └──Terminated────────────────────────────────────────────────────>│
   T16     │<──Terminated(W2)────────────────────────────────────────────────────────────────────┘
   T17     │<──Terminated(W1)───┘
   T18     [all children done]
   T19     [PostStop]
   T20     [TERMINATED]
   
   Key Points:
   - Supervisor sends stop to ALL children immediately (T1-T2)
   - Supervisor enters STOPPING state, processes system messages (T3+)
   - Children stop recursively (depth-first)
   - Children send Terminated system messages to parent when done (T15-T17)
   - Parent tracks child termination via system messages
   - Parent only proceeds to PostStop after receiving ALL Terminated messages (T18-T19)
   - Bottom-up termination ensures no orphaned actors
   - NON-BLOCKING: Parent can process system events while waiting
   ```

4. **Child Actor Lifecycle Guarantee:**
   ```crystal
   # Parent ALWAYS waits for children during stop
   # This is automatic - no special handling needed
   # Parent remains responsive to system messages while waiting
   parent.stop  # Non-blocking, but won't complete until all children stopped
   ```

5. **Monitor DeadLetters:**
   ```crystal
   # Log and alert on excessive dead letters
   if system.deadletter.count > 1000
     alert("High dead letter count!")
   end
   ```

6. **Handle Stop in Tests:**
   ```crystal
   it "cleans up actors with children" do
     parent = system.spawn(ParentBehavior.new)
     # Parent spawns children internally
     
     parent.stop
     sleep 0.1
     
     # Both parent and children should be gone
     system.registry.exists?(parent.id).should be_false
   end
   ```

7. **Actor Behavior During STOPPING State:**
   ```crystal
   # In STOPPING state, actors:
   # - Reject user messages (sent to DeadLetter)
   # - Continue processing system messages (Terminated, etc.)
   # - Can still respond to supervision events
   
   def on_message(envelope)
     # User messages automatically rejected in STOPPING state
     return unless @state.running?
     @behavior.receive(envelope.message, self)
   end
   
   def on_system_message(message)
     # System messages processed even in STOPPING state
     case message
     when Terminated
       handle_child_terminated(message)
     # ... other system messages
     end
   end
   ```

8. **Avoid Long-Running Operations in PreStop:**
   ```crystal
   # Good: Quick cleanup, let children stop asynchronously
   def on_signal(signal)
     case signal
     when PreStop
       @flag = false  # Signal background tasks to stop
       # Children will stop in parallel, not blocked by parent's cleanup
     end
   end
   ```

### Performance Impact of Cleanup

| Operation | Time Complexity | Notes |
|-----------|----------------|-------|
| Notify Watchers | O(w) | w = number of watchers |
| Initiate Children Stop | O(c) | c = children, send stop asynchronously |
| Wait for Children (async) | O(1) per Terminated msg | Event-driven, non-blocking |
| Stop Children (Recursive) | O(d) messages | d = depth, parallel stopping |
| Cleanup Watches | O(w) | w = watched actors |
| Drain Mailbox | O(m) | m = messages in queue |
| Unregister | O(1) | Hash map delete with mutex |
| **Total Cleanup** | **O(c + w + m)** | Linear, children stop in parallel |

**Hierarchy Impact:**
- Shallow hierarchy (depth 1-2): Children stop in parallel, very fast
- Deep hierarchy (depth 5+): Each level stops concurrently with siblings
- Wide hierarchy (many children): All children stop in parallel
- Async waiting eliminates blocking delays
- Actual time = max(child stop time) not sum(child stop times)
- Timeout only applies if children don't respond, not cumulative

**Optimization Tips:**
- Limit number of watchers per actor
- Use shallow actor hierarchies
- Process messages during operation (avoid large queues)
- Use appropriate timeouts for stop operations

## Actor Lifecycle Phases

### 1. System Initialization

The actor system serves as the runtime environment for all actors. It must be initialized with a root behavior that serves as the entry point.

**Initialization Sequence:**

```crystal
# Step 1: Create the root behavior
main_behavior = Main.create()

# Step 2: Initialize the actor system
system = ActorSystem(MainMessage).new(main_behavior)
```

**Internal Process:**

1. `ActorRegistry` is created to manage all actors
2. `DispatcherRegistry` is initialized with default dispatcher (ParallelDispatcher)
3. `MailboxManager` is created to manage mailboxes
4. Root actor is spawned automatically
5. Atomic ID generator is initialized (starting at 1)

**Key Objects Created:**
- `ActorSystem(T)`: The main system container
- `ActorRegistry`: Maps actor IDs to contexts
- `DispatcherRegistry`: Manages execution context dispatchers
- `MailboxManager`: Factory for mailboxes

### 2. Actor Spawning

Actors are created through the `spawn` method, which can be called from the system or from within another actor's context.

**Spawning Sequence:**

```crystal
# From system
child = system.spawn(Child.create(parent))

# From actor context (inside receive method)
child = context.spawn(Child.create())
```

**Internal Process:**

1. **Reference Creation** (`ActorRef.new`):
   - Generates unique ID using atomic increment
   - Stores reference to the actor system
   - Type-safe reference (`ActorRef(T)`) ensures correct message types

2. **Context Creation** (`ActorContext.new`):
   - Associates behavior with the actor reference
   - Links to the parent actor system
   - Behavior holds actor state and message handling logic

3. **Registration**:
   - Context is registered in `ActorRegistry` with its ID
   - Thread-safe registration using mutex

4. **Mailbox Initialization** (`context.start`):
   - Mailbox is created by `MailboxManager`
   - Associated with default dispatcher (or custom if specified)
   - Two queues are initialized:
     - `inbox`: User messages queue
     - `system`: System messages queue (for lifecycle events)
   - Scheduled flag is set to `false`

5. **Reference Return**:
   - `ActorRef(T)` is returned to caller
   - Actor is now ready to receive messages

**State After Spawning:**
```
ActorRef(T) → ActorContext(T) → Mailbox(T) → Dispatcher
    ↓               ↓                ↓
   ID          Behavior         Queues (empty)
```

### 3. Message Sending

Messages are sent to actors through their `ActorRef` using the `<<` operator or `tell` method.

**Message Sending Flow:**

```crystal
actor_ref << MyMessage.new("data")
```

**Internal Process:**

1. **Reference Resolution**:
   - `ActorRef.<<` looks up context from system registry
   - Retrieves `ActorContext(T)` by ID
   - Thread-safe lookup with mutex

2. **Context Routing**:
   - `ActorContext.tell` is invoked
   - Message is wrapped in `Envelope` with sender information
   - Envelope contains: message payload + sender reference

3. **Mailbox Enqueue**:
   - `Mailbox.send` enqueues envelope to inbox
   - Thread-safe enqueue operation
   - Queue size counter is incremented

4. **Scheduling Decision**:
   - If mailbox is NOT already scheduled (`@scheduled == false`):
     - Mailbox is dispatched to its dispatcher
     - `@scheduled` flag is set to `true`
   - If already scheduled:
     - Message waits in queue
     - No additional dispatch (prevents duplicate scheduling)

**Message States:**
- **Enqueued**: Message is in the mailbox inbox queue
- **Scheduled**: Mailbox is registered with dispatcher for execution
- **Processing**: Message is being processed by behavior
- **Completed**: Message handling finished

### 4. Message Dispatching

Dispatchers are responsible for scheduling mailbox execution in appropriate execution contexts.

**Dispatcher Types:**

| Dispatcher | Execution Context | Use Case | Thread Model |
|------------|-------------------|----------|--------------|
| `PinnedDispatcher` | Isolated | Single-threaded actor (e.g., UI, state machines) | 1 thread dedicated |
| `ParallelDispatcher` | Parallel | High-throughput actors | Thread pool (24 threads) |
| `ConcurrentDispatcher` | Concurrent | Cooperative multitasking | Single thread, fiber-based |

**Dispatch Process:**

```crystal
dispatcher.dispatch(mailbox)
```

**Internal Process:**

1. **Fiber Spawning**:
   - Dispatcher spawns a new fiber in its execution context
   - Fiber execution model depends on dispatcher type:
     - **Isolated**: Runs in dedicated thread
     - **Parallel**: Scheduled in thread pool
     - **Concurrent**: Scheduled cooperatively

2. **Mailbox Execution**:
   - `mailbox.dispatch` is called within fiber
   - Isolated execution per dispatch cycle

### 5. Message Processing

The core of the actor lifecycle is message processing within the mailbox dispatch cycle.

**Processing Sequence:**

```crystal
mailbox.dispatch()
```

**Internal Process:**

1. **System Messages First**:
   - `@system` queue is dequeued
   - Each system message invokes `context.on_system_message`
   - System messages include lifecycle signals, supervision, etc.

2. **User Messages**:
   - `@inbox` queue is dequeued
   - Each message invokes `context.on_message`
   - Message envelope is unpacked

3. **Behavior Invocation**:
   - `behavior.receive(message, context)` is called
   - Behavior has access to:
     - Message payload
     - Actor context (for spawning children, etc.)
     - Sender reference (from envelope)

4. **Rescheduling Decision**:
   - `@scheduled` flag is set to `false`
   - If inbox still has messages (`@inbox.size > 0`):
     - Mailbox is dispatched again
     - `@scheduled` is set to `true`
   - Otherwise:
     - Mailbox becomes idle
     - Waits for next message

**Processing Guarantees:**
- **Sequential**: Messages are processed one at a time per actor
- **Ordered**: Messages from same sender are processed in order
- **Non-blocking**: Long-running tasks should not block the actor
- **Isolated**: Each actor processes independently

### 6. Behavior Execution

The behavior defines the actor's logic and state. It processes messages and can spawn child actors.

**Behavior Interface:**

```crystal
abstract class AbstractBehavior(T)
  def receive(message : T, context)
    # Process message
    # Access/modify actor state
    # Send messages to other actors
    # Spawn child actors
  end

  def on_signal(signal : SystemMessage)
    # Handle lifecycle events
  end
end
```

**Behavior Responsibilities:**

1. **State Management**: Maintain actor's private state
2. **Message Handling**: Implement business logic for each message type
3. **Child Management**: Spawn and manage child actors
4. **Communication**: Send messages to other actors via their references

**Example Flow:**

```crystal
class Child < AbstractBehavior(String)
  def initialize(@parent : ActorRef(MainMessage))
  end

  def receive(message, context)
    # Process message
    result = process(message)
    
    # Send result to parent
    @parent << MainMessage.new(message: result)
    
    # Spawn child if needed
    grandchild = context.spawn(GrandChild.create())
  end
end
```

### 7. Actor Hierarchy

Actors can form hierarchies through parent-child relationships, typically for supervision patterns.

**Hierarchy Structure:**

```
ActorSystem (Root)
    │
    ├─── Main Actor
    │     │
    │     ├─── Child Actor 1
    │     │     └─── Grandchild Actor
    │     │
    │     └─── Child Actor 2
    │
    └─── Another Top-Level Actor
```

**Parent-Child Relationship:**

- Parent spawns children using `context.spawn`
- Children typically hold reference to parent
- Communication flows through message passing
- All actors registered in same `ActorRegistry`

## Data Structures

### Queue Implementation

The framework uses a custom lock-free queue implementation for mailboxes.

**Queue Structure:**

```
QueueNode(T) → QueueNode(T) → QueueNode(T) → nil
    ↑                             ↑
  @first                        @last
```

**Operations:**

- **Enqueue**: O(1) - Append to tail with mutex
- **Dequeue**: O(1) - Remove from head with mutex
- **Size Tracking**: Maintained atomically

**Thread Safety:**

- Mutex-based synchronization
- Separate lock per queue instance
- No deadlocks (single lock per operation)

### Envelope

Messages are wrapped in envelopes to carry metadata.

```crystal
class Envelope(T)
  @message : T           # Actual message payload
  @sender : ActorRefBase # Reference to sender
end
```

**Purpose:**
- Sender identification for request-response patterns
- Future: Priority, timestamps, routing metadata
- Type safety through generic parameter

## Execution Model

### Fiber-Based Concurrency

Movie leverages Crystal's fiber system for lightweight concurrency.

**Execution Contexts:**

1. **Isolated** (`Fiber::ExecutionContext::Isolated`):
   - Single dedicated OS thread
   - No concurrent execution
   - Deterministic scheduling
   - Use case: State machines, sequential processing

2. **Parallel** (`Fiber::ExecutionContext::Parallel`):
   - Thread pool (configurable, default 24)
   - True parallelism on multi-core systems
   - Higher throughput
   - Use case: I/O bound operations, CPU-intensive tasks

3. **Concurrent** (`Fiber::ExecutionContext::Concurrent`):
   - Single OS thread
   - Cooperative multitasking
   - Low overhead
   - Use case: Many lightweight actors

### Scheduling Model

```
Message Arrives → Mailbox.send() → Check @scheduled
                                          │
                        ┌─────────────────┴─────────────────┐
                        │                                   │
                    @scheduled == true              @scheduled == false
                        │                                   │
                  Enqueue only                      Enqueue + Dispatch
                        │                                   │
                        └─────────────────┬─────────────────┘
                                          │
                                          ▼
                                Dispatcher.dispatch(mailbox)
                                          │
                                          ▼
                               spawn fiber { mailbox.dispatch() }
                                          │
                                          ▼
                            Process messages in inbox/system queues
                                          │
                                          ▼
                                 Set @scheduled = false
                                          │
                    ┌─────────────────────┴─────────────────┐
                    │                                       │
              Inbox empty                            Inbox has messages
                    │                                       │
              Become idle                          Reschedule dispatch
```

## Concurrency & Thread Safety

### Thread-Safe Components

1. **ActorRegistry**:
   - Mutex-protected actor map
   - Thread-safe context lookup
   - Safe concurrent spawning

2. **Queue**:
   - Mutex per queue instance
   - Atomic size counter
   - Safe enqueue/dequeue

3. **Mailbox**:
   - Scheduled flag prevents duplicate dispatch
   - Separate system and user message queues
   - Lock-free scheduling decision

4. **ID Generator**:
   - Atomic(Int32) for unique IDs
   - Lock-free increment
   - Guaranteed uniqueness

### Synchronization Patterns

**Actor Isolation**: Each actor processes messages sequentially, eliminating race conditions within actor state.

**Message Queue**: Synchronizes multiple senders through mutex-protected enqueue.

**Dispatcher**: Execution context handles fiber scheduling and thread coordination.

## Lifecycle Implementation Checklist

To implement the complete actor lifecycle, the following components need to be added:

### 1. State Management
- [ ] Add `State` enum to `ActorContext`
- [ ] Implement state transition methods
- [ ] Add state validation for operations
- [ ] Track state history for debugging

### 2. System Messages
- [x] Define `SystemMessage` base class (exists)
- [ ] Implement lifecycle messages (PreStart, PostStart, PreStop, PostStop, etc.)
- [ ] Implement supervision messages (Watch, Unwatch, Terminated, Failure)
- [ ] Add system message queue priority handling

### 3. Lifecycle Hooks
- [ ] Implement `on_signal` handler in `ActorContext`
- [ ] Add lifecycle method dispatch
- [ ] Handle exceptions in lifecycle hooks
- [ ] Add default implementations in `AbstractBehavior`

### 4. Graceful Shutdown
- [ ] Implement `stop()` method in `ActorContext` and `ActorRef`
- [ ] Add PreStop/PostStop signal handling
- [ ] Implement message queue draining strategy
- [ ] Add timeout for stop operations

### 5. Supervision
- [ ] Add parent-child relationship tracking
- [ ] Implement `SupervisionConfig` and strategies
- [ ] Add restart counter and time window tracking
- [ ] Implement supervision decision logic
- [ ] Handle escalation to parent supervisor

### 6. Watch/Unwatch
- [ ] Implement watcher registration
- [ ] Track watched actors
- [ ] Send `Terminated` messages to watchers
- [ ] Clean up watch relationships on termination

### 7. Restart Logic
- [ ] Implement restart state transitions
- [ ] Add PreRestart/PostRestart handling
- [ ] Clear mailbox on restart (configurable)
- [ ] Preserve/restore actor state (optional)
- [ ] Handle restart failures

### 8. Error Handling
- [ ] Catch exceptions in message processing
- [ ] Create `Failure` system messages
- [ ] Propagate failures to supervisor
- [ ] Handle uncaught exceptions
- [ ] Add deadletter queue for failed messages

### 9. Registry Management
- [ ] Add unregister method to `ActorRegistry`
- [ ] Clean up terminated actors
- [ ] Prevent message sending to terminated actors
- [ ] Add actor lifecycle event logging

### 10. Testing Infrastructure
- [ ] Add lifecycle state assertions
- [ ] Test each state transition
- [ ] Test supervision strategies
- [ ] Test graceful shutdown
- [ ] Test restart scenarios
- [ ] Test error propagation

## Integration Points

### Behavior Interface Enhancement

```crystal
abstract class AbstractBehavior(T)
  # Existing
  def receive(message : T, context : ActorContext)
  end

  # Lifecycle hooks to implement
  def on_signal(signal : SystemMessage)
    case signal
    when PreStart
      pre_start
    when PostStart
      post_start
    when PreStop
      pre_stop
    when PostStop
      post_stop
    when PreRestart
      pre_restart(signal.cause)
    when PostRestart
      post_restart
    when Terminated
      child_terminated(signal.actor, signal.cause)
    end
  end

  # Optional lifecycle hooks (default empty implementations)
  def pre_start
  end

  def post_start
  end

  def pre_stop
  end

  def post_stop
  end

  def pre_restart(cause : Exception?)
  end

  def post_restart
  end

  def child_terminated(child : ActorRefBase, cause : Exception?)
  end
end
```

### ActorRef Enhancement

```crystal
class ActorRef(T) < ActorRefBase
  def stop
    context = @system.context(@id)
    return if context.nil?
    context.as(ActorContext(T)).stop
  end

  def watch(watcher : ActorRef)
    context = @system.context(@id)
    return if context.nil?
    context.as(ActorContext(T)).watch(watcher)
  end

  def unwatch(watcher : ActorRef)
    context = @system.context(@id)
    return if context.nil?
    context.as(ActorContext(T)).unwatch(watcher)
  end

  def state : ActorContext::State?
    context = @system.context(@id)
    return nil if context.nil?
    context.as(ActorContext(T)).state
  end
end
```

### ActorSystem Enhancement

```crystal
class ActorSystem(T) < AbstractActorSystem
  def shutdown(timeout : Time::Span = 5.seconds)
    # Stop all actors gracefully
    @registry.as(ActorRegistry).stop_all(timeout)
    
    # Wait for all actors to terminate
    deadline = Time.utc + timeout
    
    while @registry.as(ActorRegistry).active_count > 0 && Time.utc < deadline
      sleep 0.1
    end
    
    # Force terminate remaining actors
    @registry.as(ActorRegistry).terminate_all
  end

  def unregister(id : Int32)
    @registry.as(ActorRegistry).unregister(id)
  end
end
```

## Performance Characteristics

### Time Complexity

| Operation | Complexity | Notes |
|-----------|------------|-------|
| Spawn Actor | O(1) | ID generation + registration |
| Send Message | O(1) | Enqueue operation |
| Lookup Actor | O(1) | Hash map lookup |
| Process Message | O(n) | n = messages in mailbox |
| Dispatch Mailbox | O(1) | Fiber spawn |

### Space Complexity

- **Per Actor**: O(1) - Context, behavior, mailbox
- **Per Message**: O(1) - Envelope + payload
- **Registry**: O(n) - n = number of actors
- **Queue**: O(m) - m = messages in mailbox

### Throughput Characteristics

- **Parallel Dispatcher**: High throughput, scales with cores
- **Concurrent Dispatcher**: Lower overhead, good for many actors
- **Pinned Dispatcher**: Deterministic, predictable latency

## Design Patterns

### 1. Request-Response Pattern

```crystal
# Requester sends message with self reference
requester << ResponseMessage.new(data, self_ref)

# Responder sends back via envelope.sender
def receive(message, context)
  result = process(message)
  message.sender << Result.new(result)
end
```

### 2. Router Pattern

```crystal
class Router < AbstractBehavior(Work)
  def initialize
    @workers = spawn_workers(10)
    @index = 0
  end

  def receive(message, context)
    worker = @workers[@index % @workers.size]
    worker << message
    @index += 1
  end
end
```

### 3. State Machine Pattern

```crystal
class StateMachine < AbstractBehavior(Event)
  def initialize
    @state = :idle
  end

  def receive(event, context)
    @state = case {@state, event}
    when {:idle, :start} then :running
    when {:running, :pause} then :paused
    when {:paused, :resume} then :running
    else @state
    end
  end
end
```

## Best Practices

### 1. Actor Design

- **Single Responsibility**: Each actor should have one clear purpose
- **Immutable Messages**: Message data should be immutable
- **Non-Blocking**: Avoid blocking operations in receive method
- **State Encapsulation**: Keep state private to actor

### 2. Message Design

- **Type Safety**: Use strongly-typed messages (struct or class)
- **Small Messages**: Keep message payloads small
- **Immutability**: Messages should not be modified after sending
- **Serializable**: Consider future distribution requirements

### 3. Hierarchy Design

- **Shallow Trees**: Avoid deeply nested hierarchies
- **Supervision**: Parent actors should supervise children (future feature)
- **Lifecycle Management**: Parent responsible for child lifecycle
- **Error Isolation**: Failures should not cascade up hierarchy

### 4. Performance Optimization

- **Dispatcher Selection**: Choose appropriate dispatcher for use case
- **Batch Processing**: Process multiple messages when possible
- **Backpressure**: Implement flow control for high-throughput scenarios
- **Resource Pooling**: Reuse expensive resources across messages

## Future Enhancements

### 1. Supervision

Implement supervision strategies for fault tolerance:
- **Restart**: Restart failed child actor
- **Resume**: Continue processing after failure
- **Stop**: Terminate failed actor
- **Escalate**: Propagate failure to parent

### 2. Actor Lifecycle Hooks

Add lifecycle callback methods:
- `pre_start`: Called before actor processes first message
- `post_stop`: Called after actor stops
- `pre_restart`: Called before restart
- `post_restart`: Called after restart

### 3. Message Priority

Support message prioritization:
- High-priority system messages
- User-defined priority levels
- Priority queue implementation

### 4. Remote Actors

Enable distributed actor systems:
- Network-transparent actor references
- Serialization/deserialization
- Remote dispatcher
- Cluster management

### 5. Backpressure

Implement flow control mechanisms:
- Mailbox size limits
- Sender blocking on full mailbox
- Adaptive dispatch strategies

### 6. Monitoring & Metrics

Add observability features:
- Message throughput metrics
- Mailbox depth monitoring
- Actor lifecycle events
- Performance profiling

## Conclusion

The Movie actor framework provides a solid foundation for building concurrent, distributed systems in Crystal. By leveraging fibers, execution contexts, and the actor model, it enables:

- **Isolation**: Each actor maintains independent state
- **Concurrency**: Multiple actors execute simultaneously
- **Scalability**: System grows by adding more actors
- **Fault Tolerance**: Errors are contained within actors (with future supervision)
- **Simplicity**: Message passing simplifies concurrent programming

The lifecycle from spawning to message processing is well-defined, with clear separation of concerns between system components. The framework is designed for extensibility, with future enhancements planned for supervision, remote actors, and advanced scheduling strategies.

---

**Document Version**: 1.0  
**Framework Version**: Movie (Ametist 0.1.0)  
**Last Updated**: 2024  
**Author**: System Architecture Documentation