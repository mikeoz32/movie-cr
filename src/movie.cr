require "fiber"
require "log"
require "uuid"
require "./queue"

require "./movie/config"
require "./movie/path"
require "./movie/behavior"
require "./movie/mailbox"
require "./movie/context"
require "./movie/extension_id"
require "./movie/system"
require "./movie/future"
require "./movie/scheduler"
require "./movie/ask"
require "./movie/pipe"
require "./movie/streams_typed"

module Movie
  class ActorUnavailableError < Exception
  end

  class ActorSystemShuttingDownError < Exception
  end

  class ExtensionStartError < Exception
  end

  # Movie is an actor framework for Crystal
  # It is possible to build actors due to experimental execution contexts in language
  # Will hope that Crystal will not be deprecated in future
  #
  # Each actor is represented by its behavior, which is a class that defines the actor's state and behavior.
  # Internally actors are instances of ActorContext.
  # Actors could be referenced with their ActorRef.
  # Actors communicate with each other by sending messages.
  # Each actor has a mailbox where messages are queued and processed. Mailboxes are dispatched by dispatchers.
  # Dispatchers are responsible for scheduling mailboxes to be executed in specific execution context.
  #
  # TODO: implement envelopes for messages to handle metadata and priority?. (sender, priority, time_sent, time_received)

  alias PoolExecutionContext = Fiber::ExecutionContext::Parallel

  abstract class Dispatcher
    def initialize(execution_context : Fiber::ExecutionContext)
      @execution_context = execution_context
    end

    def dispatch(mailbox : Mailbox)
      @execution_context.spawn do
        mailbox.dispatch
      end
    end

    # Spawns a fiber in this dispatcher's execution context to run the given block.
    # Used by the Scheduler for timer callbacks and other deferred execution.
    def execute(&block : -> Nil)
      @execution_context.spawn do
        block.call
      end
    end
  end

  class PinnedDispatcher < Dispatcher
    # Uses isolated execution context, so only one actor can be executed in by this dispatcher
    # in single thread
    def initialize
      super(Fiber::ExecutionContext::Isolated.new)
    end
  end

  class ParallelDispatcher < Dispatcher
    # uses parallel execution context, so multiple actors can be executed in parallel in different threads
    def initialize
      super(Fiber::ExecutionContext::Parallel.new "pd-1", 24)
    end
  end

  class ConcurrentDispatcher < Dispatcher
    # uses concurrent execution context, so multiple actors can be executed concurrently in single thread
    def initialize
      super(Fiber::ExecutionContext::Concurrent.new("cd-1"))
    end
  end

  abstract class ActorRefBase
    @id : Int32
    @path : ActorPath?

    getter id : Int32
    getter path : ActorPath?

    def initialize(id : Int32, @path : ActorPath? = nil)
      @id = id
    end

    # Sets the path for this actor ref (used during initialization)
    def path=(@path : ActorPath?)
    end

    abstract def send_system(message : SystemMessage)
  end

  class ActorRef(T) < ActorRefBase
    @system : AbstractActorSystem

    def initialize(@system : AbstractActorSystem, path : ActorPath? = nil)
      super(@system.next_id, path)
    end

    def <<(message : T)
      tell_from(@system.dead_letters, message)
    end

    def tell_from(sender : ActorRefBase?, message : T)
      context = @system.context @id
      raise "Context not found" if context.nil?
      context.as(ActorContext(T)).deliver(message, sender)
    end

    def send_system(message : SystemMessage)
      context = @system.context @id
      return if context.nil? # Actor already stopped and deregistered
      context.as(ActorContext(T)).send_system_message(message)
    end

    def ask(message : T, response_type : R.class = Nil, timeout : Time::Span? = nil) : Future(R) forall R
      state = Movie::Ask::AskState(R).new(Promise(R).new, self.as(ActorRefBase))
      listener_behavior = Behaviors(Movie::Ask::Response(R)).setup do |_listener_context|
        Movie::Ask::ListenerBehavior(R).new(state, self.as(ActorRefBase))
      end

      listener = @system.spawn(listener_behavior, RestartStrategy::STOP, SupervisionConfig.default)
      listener_ref = listener.as(ActorRef(Movie::Ask::Response(R)))
      state.listener = listener_ref.as(ActorRefBase)
      listener_context = @system.context(listener.id).as(ActorContext(Movie::Ask::Response(R)))
      listener_context.watch(self)
      begin
        tell_from(listener_ref.as(ActorRefBase), message)
      rescue ex : Exception
        state.promise.try_failure(Movie::Ask::TargetTerminated.new(self.as(ActorRefBase)))
        state.stop_listener
        return state.promise.future
      end

      if timeout
        timer_handle = @system.scheduler.schedule_once(timeout) do
          if state.promise.future.pending?
            state.promise.try_failure(FutureTimeout.new)
            state.stop_listener
          end
        end
        state.timer_handle = timer_handle
      end

      state.promise.future
    end
  end

  struct RootGuardianMessage
  end

  class RootGuardian < AbstractBehavior(RootGuardianMessage)
    def receive(message : RootGuardianMessage, context : ActorContext(RootGuardianMessage)) : AbstractBehavior(RootGuardianMessage)
      Behaviors(RootGuardianMessage).same
    end

    def on_signal(signal : SystemMessage)
    end
  end

  struct UserGuardianMessage
  end

  class UserGuardian < AbstractBehavior(UserGuardianMessage)
    def receive(message : UserGuardianMessage, context : ActorContext(UserGuardianMessage)) : AbstractBehavior(UserGuardianMessage)
      Behaviors(UserGuardianMessage).same
    end

    def on_signal(signal : SystemMessage)
    end
  end

  struct SystemGuardianMessage
  end

  class SystemGuardian < AbstractBehavior(SystemGuardianMessage)
    def receive(message : SystemGuardianMessage, context : ActorContext(SystemGuardianMessage)) : AbstractBehavior(SystemGuardianMessage)
      Behaviors(SystemGuardianMessage).same
    end

    def on_signal(signal : SystemMessage)
    end
  end

  struct DeadLetter
  end

  class DeadLetters < AbstractBehavior(DeadLetter)
    def receive(message, context)
      Movie::Behaviors(DeadLetter).same
    end
  end

  class ActorRegistry
    @system : AbstractActorSystem?
    @root : ActorRef(RootGuardianMessage)?
    @user_guardian : ActorRef(UserGuardianMessage)?
    @system_guardian : ActorRef(SystemGuardianMessage)?
    @dead_letters : ActorRef(DeadLetter)?
    @default_supervision_config : SupervisionConfig = SupervisionConfig.default

    getter user_guardian : ActorRef(UserGuardianMessage)?
    getter system_guardian : ActorRef(SystemGuardianMessage)?
    getter dead_letters : ActorRef(DeadLetter)?

    def initialize
      @actors = {} of Int32 => AbstractActorContext
      @mutex = Mutex.new
    end

    def start
      raise "System not initialized" unless @system
      system = @system.as(AbstractActorSystem)

      # Create root with path "/"
      root_path = ActorPath.new(system.address, [] of String)
      root_context = create_actor_context(RootGuardian.new, RestartStrategy::RESTART, @default_supervision_config, root_path, start_immediately: false)
      root_ref = root_context.ref
      @root = root_ref

      # Create system guardian with path "/system"
      system_path = ActorPath.new(system.address, ["system"])
      system_context = create_actor_context(SystemGuardian.new, RestartStrategy::RESTART, @default_supervision_config, system_path, start_immediately: false)

      # Create dead letters with path "/system/dead_letters"
      dead_letters_path = system_path / "dead_letters"
      dead_letters_context = create_actor_context(DeadLetters.new, RestartStrategy::RESTART, @default_supervision_config, dead_letters_path, start_immediately: false)

      # Create user guardian with path "/user"
      user_path = ActorPath.new(system.address, ["user"])
      user_context = create_actor_context(UserGuardian.new, RestartStrategy::RESTART, @default_supervision_config, user_path, start_immediately: false)

      root_context.attach_child(system_context.ref, notify_child: false)
      system_context.attach_child(dead_letters_context.ref, notify_child: false)
      root_context.attach_child(user_context.ref, notify_child: false)
      @user_guardian = user_context.ref
      @system_guardian = system_context.ref
      @dead_letters = dead_letters_context.ref

      root_context.start
      system_context.start
      dead_letters_context.start
      user_context.start
    end

    def root_guardian : ActorRef(RootGuardianMessage)?
      @root
    end

    protected def create_actor_context(
      behavior : AbstractBehavior(T),
      restart_strategy : RestartStrategy = RestartStrategy::RESTART,
      supervision_config : SupervisionConfig = @default_supervision_config,
      path : ActorPath? = nil,
      *,
      start_immediately : Bool = true,
    ) : ActorContext(T) forall T
      raise "System not initialized" unless @system
      system = @system.as(AbstractActorSystem)
      ref = ActorRef(T).new(system, path)
      context = ActorContext(T).new(behavior, ref, system, restart_strategy, supervision_config, path)
      @mutex.synchronize do
        @actors[ref.id] = context
      end
      begin
        # Register in path registry if path is provided.
        if p = path
          system.path_registry.register(ref, p)
        end
      rescue ex
        @mutex.synchronize { @actors.delete(ref.id) }
        raise ex
      end
      context.start if start_immediately
      context
    end

    def system=(system : AbstractActorSystem?) forall T
      @system = system
    end

    def supervision_config=(config : SupervisionConfig)
      @default_supervision_config = config
    end

    # Spawns an actor under the user guardian
    # If name is provided, the actor gets path /user/{name}
    # If name is nil, generates a unique name based on actor ID
    def spawn(
      behavior : AbstractBehavior(T),
      restart_strategy : RestartStrategy = RestartStrategy::RESTART,
      supervision_config : SupervisionConfig = @default_supervision_config,
      name : String? = nil,
    ) : ActorRef(T) forall T
      raise "System not initialized" unless @system
      raise "Root guardian not initialized" if @user_guardian.nil?

      system = @system.as(AbstractActorSystem)
      user_guardian = @user_guardian.as(ActorRef(UserGuardianMessage))
      user_context = context(user_guardian.id)
      raise "User guardian context not found" unless user_context

      # Build path: /user/{name}
      user_path = user_guardian.path || ActorPath.new(system.address, ["user"])
      actor_name = name || "$#{system.next_id}"
      child_path = user_path / actor_name

      child = create_actor_context(behavior, restart_strategy, supervision_config, child_path, start_immediately: false)
      begin
        user_context.as(ActorContext(UserGuardianMessage)).attach_child(child.ref, notify_child: false)
      rescue ex
        system.deregister(child.ref.id)
        raise ex
      end
      child.start
      child.ref
    end

    # Spawns an actor under a specific parent path (for hierarchical spawning from ActorContext)
    def spawn_child(
      behavior : AbstractBehavior(T),
      restart_strategy : RestartStrategy = RestartStrategy::RESTART,
      supervision_config : SupervisionConfig = @default_supervision_config,
      name : String? = nil,
      parent_path : ActorPath? = nil,
    ) : ActorRef(T) forall T
      raise "System not initialized" unless @system
      system = @system.as(AbstractActorSystem)

      # Build child path from parent path
      child_path = if parent_path
                     actor_name = name || "$#{system.next_id}"
                     parent_path / actor_name
                   else
                     nil
                   end

      create_actor_context(behavior, restart_strategy, supervision_config, child_path)
    end

    def context(id : Int32)
      @mutex.synchronize do
        @actors[id]?
      end
    end

    def [](id : Int32)
      context(id)
    end

    def register_context(id : Int32, context : AbstractActorContext)
      @mutex.synchronize do
        @actors[id] = context
      end
    end

    def rebind_paths(address : Address)
      raise "System not initialized" unless @system
      system = @system.as(AbstractActorSystem)

      snapshots = [] of NamedTuple(context: AbstractActorContext, elements: Array(String))
      @mutex.synchronize do
        @actors.each_value do |context|
          path = context.path
          next unless path
          next unless path.address.system == address.system

          snapshots << {context: context, elements: path.elements.dup}
        end
      end

      snapshots.each do |snapshot|
        rebound_path = ActorPath.new(address, snapshot[:elements])
        snapshot[:context].rebind_path(rebound_path)
        system.path_registry.register(snapshot[:context].ref, rebound_path)
      end
    end

    def deregister(id : Int32)
      @mutex.synchronize do
        @actors.delete(id)
      end
    end

    def empty? : Bool
      @mutex.synchronize do
        @actors.empty?
      end
    end
  end

  class DispatcherRegistry
    def initialize
      @dispatchers = {} of String => Dispatcher
      @default_dispatcher = nil.as(Dispatcher?)
      @internal_dispatcher = nil.as(Dispatcher?)
      @scheduler_dispatcher = nil.as(Dispatcher?)
      @mutex = Mutex.new
    end

    def register(name : String, dispatcher : Dispatcher)
      @mutex.synchronize { @dispatchers[name] = dispatcher }
    end

    def get(name : String)
      @mutex.synchronize do
        dispatcher = @dispatchers[name]
        dispatcher || raise "Dispatcher not found: #{name}"
        dispatcher
      end
    end

    def default
      # ||= is a safe way to initialize a variable only once
      # if there is no default dispatcher, create one and register it
      # return default or return default = new
      @mutex.synchronize do
        @dispatchers["default"] ||= begin
          @default_dispatcher ||= ParallelDispatcher.new
        end
      end
    end

    def internal
      @mutex.synchronize do
        @dispatchers["internal"] ||= begin
          @internal_dispatcher ||= ParallelDispatcher.new
        end
      end
    end

    # Dedicated dispatcher for scheduler/timers.
    # Can be overridden by registering a "scheduler" dispatcher before first use.
    def scheduler
      @mutex.synchronize do
        @dispatchers["scheduler"] ||= begin
          @scheduler_dispatcher ||= ConcurrentDispatcher.new
        end
      end
    end
  end

  # PathRegistry for local actor path lookups (moved from Remote module for unified access)
  # Uses normalized keys (system:path) to allow lookups regardless of protocol/host/port.
  class PathRegistry
    @path_to_id : Hash(String, Int32)
    @id_to_path : Hash(Int32, ActorPath)
    @mutex : Mutex

    def initialize
      @path_to_id = {} of String => Int32
      @id_to_path = {} of Int32 => ActorPath
      @mutex = Mutex.new
    end

    # Creates a normalized key from system name and path elements.
    # This allows matching local and remote paths that refer to the same actor.
    # Example: "server-system:/user/ping" matches both:
    #   - movie://server-system/user/ping
    #   - movie.tcp://server-system@127.0.0.1:9000/user/ping
    private def normalize_key(path : ActorPath) : String
      elements_str = path.elements.empty? ? "" : "/" + path.elements.join("/")
      "#{path.address.system}:#{elements_str}"
    end

    private def normalize_key(path_str : String) : String
      path = ActorPath.parse(path_str)
      normalize_key(path)
    end

    def register(ref : ActorRefBase, path : ActorPath)
      @mutex.synchronize do
        key = normalize_key(path)
        if existing_id = @path_to_id[key]?
          raise ArgumentError.new("Actor path #{path} is already registered by actor #{existing_id}") unless existing_id == ref.id
        end
        if old_path = @id_to_path[ref.id]?
          old_key = normalize_key(old_path)
          @path_to_id.delete(old_key) if old_key != key && @path_to_id[old_key]? == ref.id
        end
        @path_to_id[key] = ref.id
        @id_to_path[ref.id] = path
      end
    end

    def unregister(ref : ActorRefBase)
      @mutex.synchronize do
        if path = @id_to_path.delete(ref.id)
          key = normalize_key(path)
          @path_to_id.delete(key) if @path_to_id[key]? == ref.id
        end
      end
    end

    def unregister(id : Int32)
      @mutex.synchronize do
        if path = @id_to_path.delete(id)
          key = normalize_key(path)
          @path_to_id.delete(key) if @path_to_id[key]? == id
        end
      end
    end

    def resolve(path : ActorPath) : Int32?
      @mutex.synchronize do
        @path_to_id[normalize_key(path)]?
      end
    end

    def resolve(path_str : String) : Int32?
      @mutex.synchronize do
        @path_to_id[normalize_key(path_str)]?
      end
    end

    def path_for(ref : ActorRefBase) : ActorPath?
      @mutex.synchronize do
        @id_to_path[ref.id]?
      end
    end

    def path_for(id : Int32) : ActorPath?
      @mutex.synchronize do
        @id_to_path[id]?
      end
    end

    def registered?(path : ActorPath) : Bool
      @mutex.synchronize do
        @path_to_id.has_key?(normalize_key(path))
      end
    end

    def size : Int32
      @mutex.synchronize do
        @id_to_path.size
      end
    end

    def clear
      @mutex.synchronize do
        @path_to_id.clear
        @id_to_path.clear
      end
    end
  end

  # Base class for ActorSystem extensions.
  # Extensions provide additional functionality to the actor system
  # (e.g., remoting, clustering, persistence, metrics).
  #
  # Extensions are:
  # - Singletons per ActorSystem (one instance per extension type)
  # - Lazily initialized on first access
  # - Automatically stopped when the system shuts down
  #
  # Example:
  #   class MyExtension < Movie::Extension
  #     def initialize(@system : Movie::AbstractActorSystem)
  #     end
  #
  #     def start
  #       # Initialize extension
  #     end
  #
  #     def stop
  #       # Cleanup resources
  #     end
  #   end
  #
  #   # Register and use
  #   ext = MyExtension.new(system)
  #   system.register_extension(ext)
  #   system.extension(MyExtension)  # => MyExtension instance
  #
  #   # Akka-style access with ExtensionId:
  #   class MyExtId < Movie::ExtensionId(MyExtension)
  #     def create(system : Movie::AbstractActorSystem)
  #       MyExtension.new(system)
  #     end
  #   end
  #
  #   MyExtId.get(system) # => MyExtension instance
  #
  abstract class Extension
    # Called when the extension is registered with the system.
    # Override to perform initialization.
    def start
    end

    # Called when the system is shutting down.
    # Override to cleanup resources (close connections, stop services, etc.)
    abstract def stop
  end

  # Registry for managing extensions within an ActorSystem.
  class ExtensionRegistry
    @extensions : Hash(String, Extension)
    @starting : Hash(String, Channel(Nil))
    @closed : Bool
    @mutex : Mutex

    def initialize
      @extensions = {} of String => Extension
      @starting = {} of String => Channel(Nil)
      @closed = false
      @mutex = Mutex.new
    end

    # Registers an extension instance.
    # The extension's class name is used as the key.
    def register(extension : Extension)
      key = extension.class.name
      registry_closed = @mutex.synchronize { @closed }
      if registry_closed
        extension.stop
        raise ActorSystemShuttingDownError.new("extension registry is stopped")
      end

      started = extension.start
      if started == false
        extension.stop
        raise ExtensionStartError.new("Extension #{key} failed to start")
      end

      rejection = @mutex.synchronize do
        if @closed
          ActorSystemShuttingDownError.new("extension registry is stopped").as(Exception)
        elsif @extensions.has_key?(key)
          Exception.new("Extension #{key} is already registered")
        else
          @extensions[key] = extension
          nil
        end
      end
      return unless rejection

      extension.stop
      raise rejection
    end

    # Creates, starts, and registers an extension once. Concurrent callers wait
    # for startup to finish and never observe a partially started instance.
    def get_or_register(type : T.class, &factory : -> Extension) : T forall T
      key = T.name
      loop do
        existing = nil.as(Extension?)
        start_signal = nil.as(Channel(Nil)?)
        starts_extension = false
        registry_closed = false

        @mutex.synchronize do
          if @closed
            registry_closed = true
          elsif ext = @extensions[key]?
            existing = ext
          elsif signal = @starting[key]?
            start_signal = signal
          else
            start_signal = Channel(Nil).new
            @starting[key] = start_signal.not_nil!
            starts_extension = true
          end
        end

        raise ActorSystemShuttingDownError.new("extension registry is stopped") if registry_closed
        return existing.as(T) if existing

        unless starts_extension
          start_signal.not_nil!.receive?
          next
        end

        extension = nil.as(Extension?)
        begin
          extension = factory.call
          started = extension.start
          if started == false
            raise ExtensionStartError.new("Extension #{key} failed to start")
          end

          published = @mutex.synchronize do
            @starting.delete(key)
            unless @closed
              @extensions[key] = extension
              true
            else
              false
            end
          end
          start_signal.not_nil!.close
          unless published
            raise ActorSystemShuttingDownError.new("extension registry stopped during extension startup")
          end
          return extension.as(T)
        rescue ex
          begin
            extension.try &.stop
          rescue stop_error
            Log.for(self.class).error(exception: stop_error) { "Failed to stop extension #{key} after startup failure" }
          end
          @mutex.synchronize { @starting.delete(key) }
          start_signal.not_nil!.close
          raise ex
        end
      end
    end

    # Compatibility overload for callers that already constructed an instance.
    def get_or_register(type : T.class, extension : Extension) : T forall T
      get_or_register(T) { extension }
    end

    # Returns an extension by type, or nil if not registered.
    def get(type : T.class) : T? forall T
      key = T.name
      @mutex.synchronize do
        if ext = @extensions[key]?
          ext.as(T)
        end
      end
    end

    # Returns an extension by type, raising if not found.
    def get!(type : T.class) : T forall T
      get(T) || raise "Extension #{T.name} not registered"
    end

    # Returns true if an extension of the given type is registered.
    def registered?(type : T.class) : Bool forall T
      key = T.name
      @mutex.synchronize do
        @extensions.has_key?(key)
      end
    end

    # Stops all registered extensions.
    def stop_all
      registered_extensions = @mutex.synchronize do
        @closed = true
        current_extensions = @extensions.values
        @extensions.clear
        current_extensions
      end

      registered_extensions.each(&.stop)
    end

    # Returns all registered extensions.
    def all : Array(Extension)
      @mutex.synchronize do
        @extensions.values
      end
    end
  end

  abstract class AbstractActorSystem
    @id_generator : Atomic(Int32) = Atomic(Int32).new(1)
    @registry : ActorRegistry?
    @scheduler : Scheduler?
    @scheduler_mutex : Mutex = Mutex.new
    @spawn_admission_mutex : Mutex = Mutex.new
    @spawn_admission_closed : Bool = false
    @actor_dispatch_mutex : Mutex = Mutex.new
    @active_actor_fibers : Hash(UInt64, Int32) = {} of UInt64 => Int32
    @path_registry : PathRegistry = PathRegistry.new
    @extensions : ExtensionRegistry = ExtensionRegistry.new

    # The unique name of this actor system
    getter name : String = "default"

    # The address of this actor system (local by default, remote when remoting is enabled)
    getter address : Address = Address.local("default")

    # Registry for actor paths (both local and remote lookups)
    getter path_registry : PathRegistry

    # Registry for system extensions
    getter extensions : ExtensionRegistry

    getter dispatchers : DispatcherRegistry = DispatcherRegistry.new
    getter mailboxes : MailboxManager = MailboxManager.new

    def next_id
      @id_generator.add(1)
    end

    def context(id : Int32)
      @registry.as(ActorRegistry)[id] if @registry
    end

    def shutting_down? : Bool
      false
    end

    def with_spawn_admission(&block)
      @spawn_admission_mutex.synchronize do
        raise ActorSystemShuttingDownError.new("Actor system is shutting down") if @spawn_admission_closed
        yield
      end
    end

    def close_spawn_admission : Nil
      @spawn_admission_mutex.synchronize { @spawn_admission_closed = true }
    end

    def actor_dispatch_enter : Nil
      fiber_id = Fiber.current.object_id
      @actor_dispatch_mutex.synchronize do
        @active_actor_fibers[fiber_id] = (@active_actor_fibers[fiber_id]? || 0) + 1
      end
    end

    def actor_dispatch_leave : Nil
      fiber_id = Fiber.current.object_id
      @actor_dispatch_mutex.synchronize do
        if count = @active_actor_fibers[fiber_id]?
          if count > 1
            @active_actor_fibers[fiber_id] = count - 1
          else
            @active_actor_fibers.delete(fiber_id)
          end
        end
      end
    end

    def actor_dispatching_on_current_fiber? : Bool
      @actor_dispatch_mutex.synchronize { @active_actor_fibers.has_key?(Fiber.current.object_id) }
    end

    def deregister(id : Int32)
      @registry.as(ActorRegistry).deregister(id) if @registry
      @path_registry.unregister(id)
    end

    def dead_letters : ActorRefBase
      @registry.as(ActorRegistry).dead_letters || raise "Dead letters not initialized"
    end

    # Registers an actor context in the system registry
    def register_context(id : Int32, context : AbstractActorContext)
      @registry.as(ActorRegistry).register_context(id, context) if @registry
    end

    # Returns the system scheduler for scheduling timers and delayed tasks.
    # Uses a dedicated scheduler dispatcher for execution.
    def scheduler : Scheduler
      @scheduler_mutex.synchronize do
        @scheduler ||= Scheduler.new(dispatchers.scheduler)
      end
    end

    # Returns a config instance (empty for systems without config).
    def config : Config
      Config.empty
    end

    # Registers an extension with this actor system.
    # The extension will be started immediately and stopped when the system shuts down.
    def register_extension(extension : Extension)
      @extensions.register(extension)
    end

    # Returns an extension by type, or nil if not registered.
    # Example: system.extension(Remote::RemoteExtension)
    def extension(type : T.class) : T? forall T
      @extensions.get(T)
    end

    # Returns an extension via an ExtensionId (Akka-style lazy access).
    # Example: Movie::Remote::Remoting.get(system)
    def extension(id : ExtensionId(T)) : T forall T
      id.get(self)
    end

    # Returns an extension by type, raising if not found.
    def extension!(type : T.class) : T forall T
      @extensions.get!(T)
    end

    # Enables remoting on this actor system.
    # Returns the RemoteExtension for configuring remote communication.
    def enable_remoting(host : String, port : Int32, stripe_count : Int32 = Remote::StripedConnectionPool::DEFAULT_STRIPE_COUNT) : Remote::RemoteExtension
      @extensions.get_or_register(Remote::RemoteExtension) do
        Remote::RemoteExtension.new(self, host, port, stripe_count)
      end
    end

    # Publishes the actual address selected by a remoting extension and rebinds
    # every existing local actor path to it.
    def publish_remoting_address(address : Address) : Nil
      @address = address
      @registry.try &.rebind_paths(address)
    end

    # Returns the remote extension if remoting is enabled.
    # Convenience method - equivalent to extension(Remote::RemoteExtension)
    def remote : Remote::RemoteExtension?
      @extensions.get(Remote::RemoteExtension)
    end

    # Returns true if remoting is enabled.
    def remoting_enabled? : Bool
      @extensions.registered?(Remote::RemoteExtension)
    end

    # Unified actor lookup - returns local or remote actor ref based on path.
    # For local paths (matching this system's address), returns the local ActorRef.
    # For remote paths, returns a RemoteActorRef that transparently handles serialization.
    #
    # Example:
    #   ref = system.actor_for("movie.tcp://my-system@localhost:9000/user/worker", MyMessage)
    #   ref << MyMessage.new  # Works for both local and remote
    #
    def actor_for(path : ActorPath, type : T.class) : ActorRefBase forall T
      if local_path?(path)
        # Local lookup
        actor_id = @path_registry.resolve(path)
        raise "Actor not found for path: #{path}" unless actor_id
        ctx = context(actor_id)
        raise "Actor context not found for path: #{path}" unless ctx
        # Return the actor's ref (we can't verify the type at runtime)
        ctx.ref
      else
        # Remote - need remoting enabled
        remote_ext = remote
        raise "Remoting not enabled - cannot create remote ref for: #{path}" unless remote_ext
        remote_ext.actor_ref(path, T)
      end
    end

    # Convenience overload that parses a path string.
    # Supports multiple formats:
    #   - Full URI: "movie://system/user/ping" or "movie.tcp://system@host:port/user/ping"
    #   - Absolute path: "/user/ping" → auto-prepends local system address
    #   - Relative path: "user/ping" → auto-prepends local system address
    def actor_for(path_str : String, type : T.class) : ActorRefBase forall T
      full_path = normalize_path_string(path_str)
      actor_for(ActorPath.parse(full_path), T)
    end

    # Normalizes a path string to a full URI.
    # - Full URI (movie://... or movie.tcp://...) → returns as-is
    # - Absolute path (/user/ping) → prepends local system address
    # - Relative path (user/ping) → prepends local system address with /
    protected def normalize_path_string(path_str : String) : String
      if path_str.includes?("://")
        # Already a full URI
        path_str
      elsif path_str.starts_with?("/")
        # Absolute path - prepend local address
        "#{@address}#{path_str}"
      else
        # Relative path - prepend local address with /
        "#{@address}/#{path_str}"
      end
    end

    # Checks if a path refers to this local system.
    def local_path?(path : ActorPath) : Bool
      path_addr = path.address
      # Local if: address is explicitly local, or matches our system's address
      path_addr.local? || path_addr == @address || (
        path_addr.system == @name &&
          path_addr.host == @address.host &&
          path_addr.port == @address.port
      )
    end

    # Convenience method to lookup an actor under /user/{name}
    def user_actor(name : String, type : T.class) : ActorRefBase forall T
      actor_for("/user/#{name}", T)
    end

    # Convenience method to lookup an actor under /system/{name}
    def system_actor(name : String, type : T.class) : ActorRefBase forall T
      actor_for("/system/#{name}", T)
    end

    # Spawns an actor under the user guardian using registry defaults.
    # Available on AbstractActorSystem so extensions can spawn helper actors
    # without depending on a concrete root message type.
    def spawn(
      behavior : AbstractBehavior(U),
      restart_strategy : RestartStrategy = RestartStrategy::RESTART,
      name : String? = nil,
    ) : ActorRef(U) forall U
      raise "System not initialized" unless @registry
      with_spawn_admission do
        @registry.as(ActorRegistry).spawn(behavior, restart_strategy, name: name)
      end
    end

    # Spawns an actor under the user guardian with an explicit supervision config.
    def spawn(
      behavior : AbstractBehavior(U),
      restart_strategy : RestartStrategy,
      supervision_config : SupervisionConfig,
      name : String? = nil,
    ) : ActorRef(U) forall U
      raise "System not initialized" unless @registry
      with_spawn_admission do
        @registry.as(ActorRegistry).spawn(behavior, restart_strategy, supervision_config, name)
      end
    end

    # Spawns a system actor under /system/{name}
    # System actors are internal framework actors (scheduler, dead letters, etc.)
    def spawn_system_actor(
      behavior : AbstractBehavior(T),
      name : String,
      restart_strategy : RestartStrategy = RestartStrategy::RESTART,
      supervision_config : SupervisionConfig = SupervisionConfig.default,
    ) : ActorRef(T) forall T
      raise "System guardian not initialized" if @registry.system_guardian.nil?

      with_spawn_admission do
        system_guardian = @registry.system_guardian.as(ActorRef(SystemGuardianMessage))
        system_context = context(system_guardian.id)
        raise "System guardian context not found" unless system_context

        # Build path: /system/{name}
        system_path = system_guardian.path || ActorPath.new(@address, ["system"])
        child_path = system_path / name

        ref = ActorRef(T).new(self, child_path)
        child_context = ActorContext(T).new(behavior, ref, self, restart_strategy, supervision_config, child_path)
        @registry.register_context(ref.id, child_context)
        begin
          @path_registry.register(ref, child_path)
        rescue ex
          deregister(ref.id)
          raise ex
        end
        system_context.as(ActorContext(SystemGuardianMessage)).attach_child(ref, notify_child: false)
        child_context.start
        ref
      end
    end
  end

  class ActorSystem(T) < AbstractActorSystem
    @restart_strategy : RestartStrategy = RestartStrategy::RESTART
    @supervision_config : SupervisionConfig = SupervisionConfig.default
    @config : Config = Config.empty
    @shutdown_mutex : Mutex = Mutex.new
    @shutdown_finalize_mutex : Mutex = Mutex.new
    @shutdown_started : Bool = false
    @shutdown_completed : Bool = false

    def shutting_down? : Bool
      @shutdown_mutex.synchronize { @shutdown_started }
    end

    # Creates an ActorSystem with explicit parameters.
    def self.new(
      main_behavior : AbstractBehavior(T),
      restart_strategy : RestartStrategy = RestartStrategy::RESTART,
      supervision_config : SupervisionConfig = SupervisionConfig.default,
      name : String = "actor-system-#{UUID.random.to_s[0..7]}",
    ) forall T
      registry = ActorRegistry.new
      system = ActorSystem(T).allocate
      system.initialize(main_behavior, registry, restart_strategy, supervision_config, name, Config.empty)
      registry.start
      system.bootstrap_main
      system.auto_enable_remoting
      system
    end

    # Creates an ActorSystem from a Config.
    # Config values override defaults. See ActorSystemConfig for available paths.
    #
    # Example:
    #   config = Movie::Config.load("movie.yml", Movie::ActorSystemConfig.default)
    #   system = Movie::ActorSystem.new(main_behavior, config)
    #
    def self.new(
      main_behavior : AbstractBehavior(T),
      config : Config,
    ) forall T
      # Merge with defaults
      full_config = config.with_fallback(ActorSystemConfig.default)

      # Extract values from config
      name = full_config.get_string(ActorSystemConfig::NAME, "")
      name = "actor-system-#{UUID.random.to_s[0..7]}" if name.empty?

      restart_strategy = ActorSystemConfig.restart_strategy(full_config)
      supervision_config = ActorSystemConfig.supervision_config(full_config)

      registry = ActorRegistry.new
      system = ActorSystem(T).allocate
      system.initialize(main_behavior, registry, restart_strategy, supervision_config, name, full_config)
      registry.start
      system.bootstrap_main
      system.auto_enable_remoting
      system
    end

    @root : ActorRef(T)?
    @main_behavior : AbstractBehavior(T)?

    # Returns the configuration used to create this system.
    getter config : Config

    protected def initialize(
      main_behavior : AbstractBehavior(T),
      registry : ActorRegistry,
      restart_strategy : RestartStrategy,
      supervision_config : SupervisionConfig,
      name : String,
      config : Config,
    ) forall T
      @name = name
      @address = Address.local(name)
      @config = config
      registry.system = self
      registry.supervision_config = supervision_config
      @registry = registry
      @main_behavior = main_behavior
      @restart_strategy = restart_strategy
      @supervision_config = supervision_config
    end

    # Automatically enables remoting if configured.
    protected def auto_enable_remoting
      return if @config.empty?
      return unless @config.get_bool(ActorSystemConfig::REMOTING_ENABLED, false)

      host = @config.get_string(ActorSystemConfig::REMOTING_HOST, "127.0.0.1")
      port = @config.get_int(ActorSystemConfig::REMOTING_PORT, 2552)
      stripe_count = @config.get_int(ActorSystemConfig::REMOTING_STRIPE_COUNT, Remote::StripedConnectionPool::DEFAULT_STRIPE_COUNT)
      enable_remoting(host, port, stripe_count)
    end

    protected def bootstrap_main
      behavior = @main_behavior || raise "Main behavior not initialized"
      @root ||= spawn(behavior, @restart_strategy, @supervision_config)
    end

    def <<(message)
      if root = @root
        root << message
      end
    end

    def ask(message : T, response_type : R.class = Nil, timeout : Time::Span? = nil) : Future(R) forall R
      root = @root || raise "System not initialized"
      state = Movie::Ask::AskState(R).new(Promise(R).new, root.as(ActorRefBase))

      listener_behavior = Behaviors(Movie::Ask::Response(R)).setup do |_listener_context|
        Movie::Ask::ListenerBehavior(R).new(state, root.as(ActorRefBase))
      end

      listener = spawn(listener_behavior, RestartStrategy::STOP, SupervisionConfig.default)
      listener_ref = listener.as(ActorRef(Movie::Ask::Response(R)))
      state.listener = listener_ref.as(ActorRefBase)
      listener_context = context(listener.id).as(ActorContext(Movie::Ask::Response(R)))
      listener_context.watch(root)

      begin
        root.tell_from(listener_ref.as(ActorRefBase), message)
      rescue ex : Exception
        state.promise.try_failure(Movie::Ask::TargetTerminated.new(root.as(ActorRefBase)))
        state.stop_listener
        return state.promise.future
      end

      if timeout
        timer_handle = scheduler.schedule_once(timeout) do
          if state.promise.future.pending?
            state.promise.try_failure(FutureTimeout.new)
            state.stop_listener
          end
        end
        state.timer_handle = timer_handle
      end

      state.promise.future
    end

    # Ask a specific actor and receive a response, similar to ActorContext#ask.
    def ask(target : ActorRef(M), message : M, response_type : R.class = Nil, timeout : Time::Span? = nil) : Future(R) forall M, R
      state = Movie::Ask::AskState(R).new(Promise(R).new, target.as(ActorRefBase))

      listener_behavior = Behaviors(Movie::Ask::Response(R)).setup do |_listener_context|
        Movie::Ask::ListenerBehavior(R).new(state, target.as(ActorRefBase))
      end

      listener = spawn(listener_behavior, RestartStrategy::STOP, SupervisionConfig.default)
      listener_ref = listener.as(ActorRef(Movie::Ask::Response(R)))
      state.listener = listener_ref.as(ActorRefBase)
      listener_context = context(listener.id).as(ActorContext(Movie::Ask::Response(R)))
      listener_context.watch(target)

      begin
        target.tell_from(listener_ref.as(ActorRefBase), message)
      rescue ex : Exception
        state.promise.try_failure(Movie::Ask::TargetTerminated.new(target.as(ActorRefBase)))
        state.stop_listener
        return state.promise.future
      end

      if timeout
        timer_handle = scheduler.schedule_once(timeout) do
          if state.promise.future.pending?
            state.promise.try_failure(FutureTimeout.new)
            state.stop_listener
          end
        end
        state.timer_handle = timer_handle
      end

      state.promise.future
    end

    def shutdown(timeout : Time::Span = 5.seconds) : Nil
      root_guardian = nil.as(ActorRef(RootGuardianMessage)?)
      should_initiate_shutdown = false

      @shutdown_mutex.synchronize do
        return if @shutdown_completed

        unless @shutdown_started
          @shutdown_started = true
          close_spawn_admission
          should_initiate_shutdown = true
          root_guardian = @registry.as(ActorRegistry).root_guardian if @registry
        end
      end

      if should_initiate_shutdown
        root_guardian.try &.send_system(STOP)
      end

      if actor_dispatching_on_current_fiber?
        spawn do
          begin
            complete_shutdown(timeout)
          rescue ex : Exception
            Log.error(exception: ex) { "Actor-side shutdown failed" }
          end
        end
        return
      end

      complete_shutdown(timeout)
    end

    private def complete_shutdown(timeout : Time::Span) : Nil
      @shutdown_finalize_mutex.synchronize do
        return if @shutdown_mutex.synchronize { @shutdown_completed }

        wait_for_shutdown(timeout)
        begin
          # Let actors use extensions and scheduler during PreStop/PostStop.
          @extensions.stop_all
        ensure
          @scheduler.try &.stop
        end

        @shutdown_mutex.synchronize do
          @shutdown_completed = true
        end
        @root = nil
      end
    end

    # Spawns an actor under the user guardian.
    # If name is provided, the actor gets path /user/{name}
    def spawn(
      behavior : AbstractBehavior(U),
      restart_strategy : RestartStrategy = RestartStrategy::RESTART,
      supervision_config : SupervisionConfig = @supervision_config,
      name : String? = nil,
    ) : ActorRef(U) forall U
      raise "System not initialized" unless @registry
      with_spawn_admission do
        @registry.as(ActorRegistry).spawn(behavior, restart_strategy, supervision_config, name)
      end
    end

    # Internal: spawns a child actor under a parent's path
    def spawn_child(
      behavior : AbstractBehavior(U),
      restart_strategy : RestartStrategy,
      supervision_config : SupervisionConfig,
      name : String?,
      parent_path : ActorPath?,
    ) : ActorRef(U) forall U
      raise "System not initialized" unless @registry
      with_spawn_admission do
        @registry.as(ActorRegistry).spawn_child(behavior, restart_strategy, supervision_config, name, parent_path)
      end
    end

    private def wait_for_shutdown(timeout : Time::Span) : Nil
      registry = @registry
      return unless registry

      deadline = Time.instant + timeout
      actor_registry = registry.as(ActorRegistry)

      until actor_registry.empty?
        raise "ActorSystem shutdown timed out after #{timeout}" if Time.instant >= deadline
        sleep 10.milliseconds
      end
    end
  end
end

# Typed stream blueprints use ActorSystem extensions for runtime ownership.
require "./movie/streams/core"
require "./movie/streams/testkit"

# Require executor after base extension types are defined
require "./movie/executor"

# Require remote module after base types are defined
require "./movie/remote"

record MainMessage, message : String
