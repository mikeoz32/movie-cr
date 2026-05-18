module Movie
  abstract class AbstractActorContext
    abstract def ref : ActorRefBase
  end

  class ActorContext(T) < AbstractActorContext
    getter log : Log
    getter supervision_config : SupervisionConfig
    getter path : ActorPath?
    getter system : AbstractActorSystem

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
    @ref : ActorRefBase
    @restart_strategy : RestartStrategy
    @supervision_config : SupervisionConfig
    @path : ActorPath?

    @children : Array(ActorRefBase) = [] of ActorRefBase
    @watching : Array(ActorRefBase) = [] of ActorRefBase
    @watchers : Array(ActorRefBase) = [] of ActorRefBase
    @pending_children : Array(ActorRefBase) = [] of ActorRefBase
    @pending_terminations : Int32 = 0
    @pre_stop_completed : Bool = false
    @post_stop_sent : Bool = false
    @restart_counters : Hash(Int32 | Symbol, NamedTuple(count: Int32, started_at: Time::Span)) = {} of Int32 | Symbol => NamedTuple(count: Int32, started_at: Time::Span)
    @current_sender : ActorRefBase? = nil

    def initialize(
      behavior : AbstractBehavior(T),
      ref : ActorRef(T),
      @system : AbstractActorSystem,
      restart_strategy : RestartStrategy,
      supervision_config : SupervisionConfig = SupervisionConfig.default,
      @path : ActorPath? = nil
    )
      @ref = ref.as(ActorRefBase)
      @behavior = behavior
      @active_behavior = behavior
      @restart_strategy = restart_strategy
      @supervision_config = supervision_config
      @log = Log.for(@ref.id.to_s)
    end

    def state
      @state
    end

    def ref : ActorRef(T)
      @ref.as(ActorRef(T))
    end

    def sender : ActorRefBase?
      @current_sender
    end

    # Pipe a future result to a target actor as Pipe::Success or Pipe::Failure.
    def pipe(future : Future(U), reply_to : ActorRef(Pipe::Message(U))) : Nil forall U
      future.on_success { |value| reply_to << Pipe::Success(U).new(value) }
      future.on_failure { |error| reply_to << Pipe::Failure(U).new(error) }
      future.on_cancel { reply_to << Pipe::Failure(U).new(FutureCancelled.new) }
    end

    # Pipe a future result to a target actor using custom success/failure mappers.
    def pipe(
      future : Future(U),
      reply_to : ActorRef(V),
      success : U -> V,
      failure : Exception -> V
    ) : Nil forall U, V
      future.on_success { |value| reply_to << success.call(value) }
      future.on_failure { |error| reply_to << failure.call(error) }
      future.on_cancel { reply_to << failure.call(FutureCancelled.new) }
    end

    # Retrieve a system extension via its ExtensionId (Akka-style).
    # Example: ctx.extension(Movie::Remote::Remoting)
    def extension(id : ExtensionId(U)) : U forall U
      id.get(@system)
    end

    def start(internal = false)
      return if @state != State::CREATED

      dispatcher = internal ? @system.dispatchers.internal : @system.dispatchers.default

      @mailbox = @system.mailboxes.create(dispatcher, self)

      transition_to(State::STARTING)

      send_system_message(PRE_START)
    end

    def stop
      return if [@state.stopped?, @state.failed?, @state.terminated?, @state == State::STOPPING].any?

      send_system_message(STOP)
    end


    # Spawns a child actor under this actor's supervision.
    # If name is provided, the child gets a hierarchical path: {parent_path}/{name}
    # If name is nil, generates a unique name based on actor ID.
    def spawn(
      behavior : AbstractBehavior(U),
      restart_strategy : RestartStrategy = @restart_strategy,
      supervision_config : SupervisionConfig = @supervision_config,
      name : String? = nil
    ) : ActorRef(U) forall U
      raise "System not initialized" unless @system

      # Build child path from parent path
      child_path = if parent_path = @path
        child_name = name || "$#{@system.next_id}"
        parent_path / child_name
      else
        nil
      end

      # Create the child ref and context
      ref = ActorRef(U).new(@system, child_path)
      context = ActorContext(U).new(behavior, ref, @system, restart_strategy, supervision_config, child_path)

      # Register in system registry
      @system.register_context(ref.id, context)

      # Register path
      if p = child_path
        @system.path_registry.register(ref, p)
      end

      context.start
      attach_child(ref)
      ref
    end

    def attach_child(child : ActorRef(U)) forall U
      @children << child unless @children.includes?(child)
      if child_ctx = @system.context(child.id)
        child_ctx.as(ActorContext(U)).register_watcher(@ref)
      end
      watch child
    end

    def watch(actor : ActorRef(U)) forall U
      return if @watching.includes?(actor)

      @watching << actor
      actor.send_system(Watch.new(@ref).as(SystemMessage))
    end

    def ask(target : ActorRef(M), message : M, response_type : T.class = Nil, timeout : Time::Span? = nil) : Future(T) forall M, T
      state = Movie::Ask::AskState(T).new(Promise(T).new)

      listener_behavior = Behaviors(Movie::Ask::Response(T)).setup do |listener_context|
        listener_context.watch(target)
        Movie::Ask::ListenerBehavior(T).new(state, target.as(ActorRefBase))
      end

      listener = spawn(listener_behavior, RestartStrategy::STOP, SupervisionConfig.default)
      listener_ref = listener.as(ActorRef(Movie::Ask::Response(T)))

      target.tell_from(listener_ref.as(ActorRefBase), message)

      if timeout
        timer_handle = @system.scheduler.schedule_once(timeout) do
          if state.promise.future.pending?
            state.promise.try_failure(FutureTimeout.new)
            listener.send_system(STOP)
          end
        end
        state.timer_handle = timer_handle
      end

      state.promise.future
    end

    protected def register_watcher(actor : ActorRefBase)
      @watchers << actor unless @watchers.includes?(actor)
    end

    def mailbox=(mailbox : Mailbox(T))
      @mailbox = mailbox
    end

    def tell(message : T)
      deliver(message, @ref.as(ActorRefBase))
    end

    def << (message : T)
      tell message
    end

    def deliver(message : T, sender : ActorRefBase?)
      raise "Mailbox not initialized" unless @mailbox
      mbox = @mailbox.as(Mailbox(T))
      mbox << Envelope(T).new(message.as(T), sender || @system.dead_letters)
    end

    def send_system_message(message : SystemMessage)
      raise "Mailbox not initialized" unless @mailbox
      mbox = @mailbox.as(Mailbox(T))
      mbox.send_system(Envelope(SystemMessage).new(message, @ref))
    end

    def on_message(message : Envelope(T))
      if @state == State::STOPPING || @state == State::STOPPED || @state == State::FAILED || @state == State::TERMINATED || @state == State::RESTARTING
        log.warn { "Dropping message #{message.message.inspect} from #{message.sender} in state #{@state}" }
        return
      end
      log.debug { "Actor #{@ref} received message #{message.message}" }
      log.debug { "Current state: #{@state}" }
      previous_sender = @current_sender
      @current_sender = message.sender
      new_behavior = @active_behavior.receive(message.message, self)
      if new_behavior.is_a?(AbstractBehavior(T))
        @active_behavior = resolve_behavior(new_behavior)
        @behavior = @active_behavior
      end
    rescue ex : Exception
      log.error(exception: ex) { "Error handling message" }
      notify_for_failure(ex)
      transition_to(State::FAILED)
      apply_restart_strategy(ex)
    ensure
      @current_sender = previous_sender
    end

    def on_system_message(message : Envelope(SystemMessage))
      # TODO: handle system messages
      case message.message
      when PRE_START
        handle_pre_start
      when POST_START
        handle_post_start
      when STOP
        handle_stop
      when PRE_STOP
        handle_pre_stop
      when POST_STOP
        handle_post_stop
      when Watch
        watcher = message.message.as(Watch).actor
        unless @watchers.includes?(watcher)
          @watchers << watcher
        end
      when Unwatch
        unwatcher = message.message.as(Unwatch).actor
        unless @watchers.includes?(unwatcher)
          return
        end
        @watchers.delete(unwatcher)
      when Failed
        handle_failed(message.message.as(Failed))
      when Terminated
        handle_terminated(message.message.as(Terminated))
      when Restart
        handle_restart(message.message.as(Restart))
      else
        # Unknown system message - send to dead letters or log
      end
    end

    protected def handle_failed(message : Failed)
      failed_actor = message.actor
      cause = message.cause
      return unless @children.any? { |child| child.id == failed_actor.id }
      attempt, exceeded = track_restart(failed_actor, cause)
      return if exceeded
      case @supervision_config.scope
      when SupervisionScope::ONE_FOR_ONE
        apply_supervision_action(failed_actor, cause, @supervision_config.strategy, attempt)
      when SupervisionScope::ALL_FOR_ONE
        @children.each do |child|
          apply_supervision_action(child, cause, @supervision_config.strategy, attempt)
        end
      end
    end

    private def track_restart(failed_actor : ActorRefBase, cause : Exception?) : {Int32, Bool}
      key = supervision_key(failed_actor)
      now = Time.monotonic
      entry = @restart_counters[key]?
      if entry
        elapsed = now - entry[:started_at]
        if elapsed > @supervision_config.within
          entry = {count: 0, started_at: now}
        end
      else
        entry = {count: 0, started_at: now}
      end

      entry = {count: entry[:count] + 1, started_at: entry[:started_at]}
      @restart_counters[key] = entry

      if entry[:count] > @supervision_config.max_restarts
        handle_restart_limit_exceeded(failed_actor, cause)
        {entry[:count], true}
      else
        {entry[:count], false}
      end
    end

    private def supervision_key(failed_actor : ActorRefBase) : Int32 | Symbol
      case @supervision_config.scope
      when SupervisionScope::ONE_FOR_ONE
        failed_actor.id
      when SupervisionScope::ALL_FOR_ONE
        :all_for_one
      else
        :all_for_one
      end
    end

    private def handle_restart_limit_exceeded(failed_actor : ActorRefBase, cause : Exception?)
      case @supervision_config.scope
      when SupervisionScope::ONE_FOR_ONE
        failed_actor.send_system(STOP)
      when SupervisionScope::ALL_FOR_ONE
        @children.each do |child|
          child.send_system(STOP)
        end
      end
      escalate_failure(failed_actor, cause)
    end

    protected def apply_supervision_action(actor : ActorRefBase, cause : Exception?, strategy : SupervisionStrategy, attempt : Int32)
      case strategy
      when SupervisionStrategy::RESTART
        delay = compute_backoff_delay(attempt)
        sleep delay if delay > Time::Span.zero
        actor.send_system(Restart.new(cause).as(SystemMessage))
      when SupervisionStrategy::STOP
        actor.send_system(STOP)
      when SupervisionStrategy::RESUME
        resume_actor(actor)
      when SupervisionStrategy::ESCALATE
        escalate_failure(actor, cause)
      end
    end

    protected def resume_actor(actor : ActorRefBase)
      # Resume keeps the actor running without restart; placeholder for state reset if needed.
    end

    protected def escalate_failure(actor : ActorRefBase, cause : Exception?)
      @watchers.each do |watcher|
        watcher.send_system(Failed.new(actor, cause).as(SystemMessage))
      end
    end

    private def compute_backoff_delay(attempt : Int32) : Time::Span
      base = @supervision_config.backoff_min * (@supervision_config.backoff_factor ** (attempt - 1))
      clamped = {base, @supervision_config.backoff_max}.min
      if @supervision_config.jitter > 0.0
        j = @supervision_config.jitter
        factor = 1.0 + (Random::DEFAULT.rand * 2.0 * j - j)
        clamped *= factor
      end
      if clamped < Time::Span.zero
        Time::Span.zero
      elsif clamped > @supervision_config.backoff_max
        @supervision_config.backoff_max
      else
        clamped
      end
    end

    protected def resolve_behavior(behavior : AbstractBehavior(T)) : AbstractBehavior(T)
      case behavior
      when SameBehavior(T)
        @active_behavior
      when DeferredBehavior(T)
        behavior.defer(self)
      when StoppedBehavior(T)
        stop
        behavior
      else
        behavior
      end
    end

    protected def notify_for_termination
      @watchers.each do |watcher|
        watcher.send_system(Terminated.new(@ref).as(SystemMessage))
      end
    end

    protected def notify_for_failure(ex : Exception)
      @watchers.each do |watcher|
        watcher.send_system(Failed.new(@ref, ex).as(SystemMessage))
      end
    end

    protected def apply_restart_strategy(ex : Exception)
      case @restart_strategy
      when RestartStrategy::RESTART
        # Supervision pipeline handles restart (with backoff). We only stop explicitly when configured to do so.
      when RestartStrategy::STOP
        send_system_message(STOP)
      end
    end

    protected def handle_pre_start
      @active_behavior = resolve_behavior(@behavior)
      @behavior = @active_behavior
      @active_behavior.on_signal(PRE_START)
      send_system_message(POST_START)
    rescue ex : Exception
      transition_to(State::FAILED)
      # @last_failure = ex
    end

    protected def handle_post_start
      transition_to(State::RUNNING)
      @behavior.on_signal(POST_START)
    end

    protected def handle_stop
      return if @state == State::STOPPING
      transition_to(State::STOPPING)
      @pre_stop_completed = false
      @post_stop_sent = false
      initiate_children_stop
      send_system_message(PRE_STOP)
      finalize_stop_if_ready
    end

    protected def handle_pre_stop
      @active_behavior.on_signal(PRE_STOP)
      @pre_stop_completed = true
      finalize_stop_if_ready
    end

    protected def handle_post_stop
      @active_behavior.on_signal(POST_STOP)
      transition_to(State::STOPPED)
      notify_for_termination
    end

    protected def handle_terminated(message : Terminated)
      actor = message.actor
      @watching.reject! { |ref| ref.id == actor.id }
      was_child = @children.any? { |ref| ref.id == actor.id }
      @children.reject! { |ref| ref.id == actor.id }

      if @pending_terminations > 0 && @pending_children.includes?(actor)
        @pending_children.delete(actor)
        @pending_terminations -= 1
        # STDERR.puts "actor=#{@ref.id} pending_terminations=#{@pending_terminations} pre_stop_completed=#{@pre_stop_completed}" if ENV["DEBUG_STOP"]?
      end

      @active_behavior.on_signal(message)

      # Deregister terminated children to prevent memory leaks
      if was_child
        @system.deregister(actor.id)
      end

      finalize_stop_if_ready
    end

    protected def handle_restart(message : Restart)
      transition_to(State::RESTARTING)
      cause = message.cause
      @active_behavior.on_signal(PreRestart.new(cause))
      @active_behavior.on_signal(POST_STOP)
      if mb = @mailbox
        mb.purge_inbox
      end
      @state = State::STARTING
      handle_pre_start
      handle_post_start
    end

    protected def initiate_children_stop
      @pending_children = @children.dup
      @pending_terminations = @pending_children.size
      STDERR.puts "actor=#{@ref.id} children=#{@pending_terminations}" if ENV["DEBUG_STOP"]?
      @pending_children.each do |child|
        child.send_system(STOP)
      end
    end

    protected def finalize_stop_if_ready
      STDERR.puts "actor=#{@ref.id} check finalize: pending=#{@pending_terminations} pre=#{@pre_stop_completed} post_sent=#{@post_stop_sent}" if ENV["DEBUG_STOP"]?
      return if @post_stop_sent
      return unless @pre_stop_completed
      return unless @pending_terminations == 0

      STDERR.puts "actor=#{@ref.id} finalize_stop" if ENV["DEBUG_STOP"]?
      @post_stop_sent = true
      send_system_message(POST_STOP)
    end

    protected def transition_to(new_state : State)
      log.debug { "Actor #{@ref} transitioning from #{@state} to #{new_state}" }
      @state = new_state
    end
  end
end
