require "./server"
require "./connection"
require "./connection_pool"
require "./remote_actor_ref"
require "./wire_envelope"
require "./message_registry"
require "../path"

module Movie::Remote
  # RemoteExtension is the main coordinator for the remoting system.
  # It manages the server, connections, and provides methods for remote communication.
  #
  # Usage:
  #   extension = system.enable_remoting("127.0.0.1", 9000)
  #   # or
  #   Movie::Remote::Remoting.get(system)
  #
  class RemoteExtension < Movie::Extension
    Log = ::Log.for(self)

    getter address : Address
    getter stripe_count : Int32

    @server : Server
    @pools : Hash(String, StripedConnectionPool)
    @pools_mutex : Mutex
    @system : Movie::AbstractActorSystem

    # Delegate to system's path registry for unified actor lookup
    def path_registry : Movie::PathRegistry
      @system.path_registry
    end

    def initialize(
      @system : Movie::AbstractActorSystem,
      bind_host : String,
      bind_port : Int32,
      @stripe_count : Int32 = StripedConnectionPool::DEFAULT_STRIPE_COUNT
    )
      system_name = @system.name
      @address = Address.remote(system_name, bind_host, bind_port)
      @pools = {} of String => StripedConnectionPool
      @pools_mutex = Mutex.new

      @server = Server.new(
        system: @system,
        host: bind_host,
        port: bind_port,
        path_registry: @system.path_registry,
        on_message: ->(envelope : WireEnvelope, conn : InboundConnection) {
          handle_incoming_message(envelope, conn)
        }
      )
    end

    # Starts the remote extension (server).
    def start : Bool
      @server.start
    end

    # Stops the remote extension.
    def stop
      @server.stop
      @pools_mutex.synchronize do
        @pools.each_value(&.close)
        @pools.clear
      end
    end

    # Returns the actual bound port.
    def local_port : Int32
      @server.local_port
    end

    # Gets or creates a connection pool to the given address.
    def pool_for(address : Address) : StripedConnectionPool
      key = address.to_s

      @pools_mutex.synchronize do
        if existing = @pools[key]?
          return existing if existing.connected?
          # Pool exists but is disconnected - remove it
          @pools.delete(key)
        end
      end

      pool = StripedConnectionPool.new(
        address: address,
        path_registry: path_registry,
        system: @system,
        stripe_count: @stripe_count,
        on_message: ->(envelope : WireEnvelope) {
          handle_incoming_message(envelope, nil)
        }
      )

      if pool.connect
        # Send handshake on first stripe
        handshake = WireEnvelope.handshake(@system.name, @address.to_s)
        pool.stripe(0).send(handshake)

        @pools_mutex.synchronize do
          @pools[key] = pool
        end
      end

      pool
    end

    # Legacy: single connection interface (uses first stripe)
    def connect(address : Address) : Connection
      pool_for(address).stripe(0)
    end

    # Legacy: single connection getter
    def connection_for(address : Address) : Connection?
      pool = pool_for(address)
      pool.connected? ? pool.stripe(0) : nil
    end

    # Creates a remote actor reference for the given path and type.
    # Uses the striped connection pool for parallel sending.
    def actor_ref(path : ActorPath, type : T.class) : RemoteActorRef(T) forall T
      pool = pool_for(path.address)

      RemoteActorRef(T).new(
        system: @system,
        pool: pool,
        target_path: path,
        path_registry: path_registry
      )
    end

    # Creates a remote actor reference from a path string.
    def actor_ref(path_str : String, type : T.class) : RemoteActorRef(T) forall T
      path = ActorPath.parse(path_str)
      actor_ref(path, type)
    end

    # Registers a local actor with its path for remote access.
    def register_actor(ref : Movie::ActorRefBase, path : ActorPath)
      path_registry.register(ref, path)
    end

    # Unregisters a local actor.
    def unregister_actor(ref : Movie::ActorRefBase)
      path_registry.unregister(ref)
    end

    # Generates a path for a new actor under the user guardian.
    def path_for_actor(name : String) : ActorPath
      ActorPath.new(@address, ["user", name])
    end

    # Returns statistics for all connection pools.
    def pool_stats : Array(PoolStats)
      @pools_mutex.synchronize do
        @pools.values.map(&.stats)
      end
    end

    private def handle_incoming_message(envelope : WireEnvelope, conn : InboundConnection?)
      case envelope.kind
      when .user_message?, .ask_request?
        deliver_to_local(envelope, conn)
      when .system_message?
        deliver_system_to_local(envelope)
      when .heartbeat?
        # Ignore heartbeats (could log or track connection health)
      else
        Log.warn { "Unknown envelope kind: #{envelope.kind}" }
      end
    end

    private def deliver_to_local(envelope : WireEnvelope, conn : InboundConnection?)
      target_path_str = envelope.target_path
      actor_id = path_registry.resolve(target_path_str)

      unless actor_id
        Log.warn { "No local actor found for path: #{target_path_str}" }
        return
      end

      context = @system.context(actor_id)
      unless context
        Log.warn { "No context found for actor ID: #{actor_id}" }
        return
      end

      # Deserialize the message
      begin
        wrapper = MessageRegistry.deserialize(envelope.message_type, envelope.payload)
        message = wrapper.value

        # For ask requests, we need to handle the response
        if envelope.kind.ask_request? && envelope.correlation_id
          # TODO: Set up response handling
          # For now, just deliver the message
        end

        # Deliver to the actor's mailbox
        # Note: We can't directly call context.deliver because we don't know the type T
        # This is a limitation - we need to use a type-erased delivery mechanism
        deliver_typed_message(context, message, envelope.sender_path, conn, envelope.correlation_id)

      rescue ex
        Log.error { "Failed to deserialize message: #{ex.message}" }
      end
    end

    private def deliver_typed_message(
      context : Movie::AbstractActorContext,
      message : JSON::Serializable,
      sender_path : String?,
      conn : InboundConnection?,
      correlation_id : String?
    )
      # This is a workaround for Crystal's type system
      # We store a callback on the context that can handle generic delivery
      # For now, we'll log a warning since full type-erased delivery requires
      # additional infrastructure
      Log.debug { "Received message for actor, type: #{message.class}" }

      # TODO: Implement type-erased message delivery
      # This would require either:
      # 1. A message handler registry on each actor
      # 2. Using Crystal's Proc with union types
      # 3. A dedicated remote message mailbox
    end

    private def deliver_system_to_local(envelope : WireEnvelope)
      target_path_str = envelope.target_path
      actor_id = path_registry.resolve(target_path_str)

      unless actor_id
        Log.warn { "No local actor found for system message path: #{target_path_str}" }
        return
      end

      context = @system.context(actor_id)
      unless context
        Log.warn { "No context found for actor ID: #{actor_id}" }
        return
      end

      # Deserialize and deliver system message
      system_message = deserialize_system_message(envelope.message_type, envelope.payload)
      if system_message
        # System messages can be delivered via send_system on the ref
        # But we need to get the ref from the context
        # For now, log that we received it
        Log.debug { "Received system message for actor: #{envelope.message_type}" }
      end
    end

    private def deserialize_system_message(type : String, payload : JSON::Any) : Movie::SystemMessage?
      case type
      when "Movie::Stop"
        Movie::STOP
      when "Movie::PreStart"
        Movie::PRE_START
      when "Movie::PostStart"
        Movie::POST_START
      when "Movie::PreStop"
        Movie::PRE_STOP
      when "Movie::PostStop"
        Movie::POST_STOP
      else
        Log.warn { "Unknown system message type: #{type}" }
        nil
      end
    end
  end
end
