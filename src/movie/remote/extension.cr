require "./server"
require "./connection"
require "./connection_pool"
require "./remote_actor_ref"
require "./wire_envelope"
require "./message_registry"
require "../path"

module Movie::Remote
  record RemoteAskFailurePayload, error_class : String, message : String do
    include JSON::Serializable
  end

  private record EmptyRemoteAskPayload do
    include JSON::Serializable
  end

  class RemoteAskResponseSenderRef < Movie::ActorRefBase
    ASK_FAILURE_TAG   = "__movie_remote_ask_failure__"
    ASK_CANCELLED_TAG = "__movie_remote_ask_cancelled__"

    def initialize(@connection : InboundConnection, @correlation_id : String, path : ActorPath?)
      super(0, path)
    end

    def reply_success(value : JSON::Serializable) : Nil
      tag, payload = MessageRegistry.prepare(value)
      send_response(WireEnvelope.ask_response(
        target_path: path.try(&.to_s) || "",
        message_type: tag,
        payload: payload,
        correlation_id: @correlation_id
      ))
    end

    def reply_failure(error : Exception) : Nil
      payload = JsonPayload.wrap(
        RemoteAskFailurePayload.new(
          error_class: error.class.name,
          message: error.message || ""
        )
      )
      send_response(WireEnvelope.ask_response(
        target_path: path.try(&.to_s) || "",
        message_type: ASK_FAILURE_TAG,
        payload: payload,
        correlation_id: @correlation_id
      ))
    end

    def reply_cancelled : Nil
      send_response(WireEnvelope.ask_response(
        target_path: path.try(&.to_s) || "",
        message_type: ASK_CANCELLED_TAG,
        payload: JsonPayload.wrap(EmptyRemoteAskPayload.new),
        correlation_id: @correlation_id
      ))
    end

    def send_system(message : Movie::SystemMessage)
      raise "Remote ask response system messages are not supported yet"
    end

    private def send_response(envelope : WireEnvelope) : Nil
      unless @connection.send(envelope)
        ::Log.for(self.class).warn { "Failed to send remote ask response for correlation #{@correlation_id}" }
      end
    end
  end

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

    private class RemoteSystemRef < Movie::ActorRefBase
      def initialize(@path_registry : Movie::PathRegistry, @connection : InboundConnection?, path : ActorPath)
        super(0, path)
      end

      def send_system(message : Movie::SystemMessage)
        path = self.path || raise RemoteDeliveryError.new("Remote system ref is missing a path")
        connection = @connection || raise RemoteDeliveryError.new("No remote connection is available for #{path}")
        tag, payload = SystemMessageCodec.serialize(message, @path_registry)

        unless connection.send(WireEnvelope.system_message(
                 target_path: path.to_s,
                 message_type: tag,
                 payload: payload
               ))
          Log.warn { "Failed to send system message to #{path}" }
        end
      end

      def ==(other : Movie::ActorRefBase) : Bool
        path = self.path
        other_path = other.path
        return false unless path && other_path

        path == other_path
      end

      def hash(hasher)
        if path = self.path
          path.hash(hasher)
        else
          id.hash(hasher)
        end
      end
    end

    getter address : Address
    getter stripe_count : Int32
    getter node_uid : String
    getter settings : AssociationSettings

    @server : Server
    @pools : Hash(String, StripedConnectionPool)
    @pools_mutex : Mutex
    @system : Movie::AbstractActorSystem
    @settings : AssociationSettings

    # Delegate to system's path registry for unified actor lookup
    def path_registry : Movie::PathRegistry
      @system.path_registry
    end

    def initialize(
      @system : Movie::AbstractActorSystem,
      bind_host : String,
      bind_port : Int32,
      @stripe_count : Int32 = StripedConnectionPool::DEFAULT_STRIPE_COUNT,
      @settings : AssociationSettings = AssociationSettings.new,
    )
      system_name = @system.name
      @address = Address.remote(system_name, bind_host, bind_port)
      @node_uid = UUID.random.to_s
      @pools = {} of String => StripedConnectionPool
      @pools_mutex = Mutex.new

      @server = Server.new(
        system: @system,
        host: bind_host,
        port: bind_port,
        path_registry: @system.path_registry,
        local_address: -> { @address },
        node_uid: @node_uid,
        settings: @settings,
        on_message: ->(envelope : WireEnvelope, conn : InboundConnection) {
          handle_incoming_message(envelope, conn)
        },
        on_disconnect: ->(conn : InboundConnection) {
          handle_inbound_disconnect(conn)
        }
      )
    end

    # Starts the remote extension (server).
    def start : Bool
      started = @server.start
      if started
        @address = Address.remote(@address.system, @address.host.not_nil!, @server.local_port)
        @system.publish_remoting_address(@address)
      end
      started
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

      stale_pool = nil.as(StripedConnectionPool?)
      @pools_mutex.synchronize do
        if existing = @pools[key]?
          return existing unless existing.stopped?
          # Pool exists but is disconnected - remove it
          stale_pool = @pools.delete(key)
        end
      end
      stale_pool.try &.close

      pool = StripedConnectionPool.new(
        address: address,
        path_registry: path_registry,
        system: @system,
        local_address: -> { @address },
        node_uid: @node_uid,
        stripe_count: @stripe_count,
        settings: @settings,
        on_message: ->(envelope : WireEnvelope) {
          handle_incoming_message(envelope, nil)
        }
      )

      pool.connect

      @pools_mutex.synchronize do
        if existing = @pools[key]?
          unless existing.stopped?
            pool.close
            return existing
          end
          existing.close
        end
        @pools[key] = pool
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
        deliver_system_to_local(envelope, conn)
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
        wrapper = MessageRegistry.deserialize(envelope.message_type, envelope.payload_data)
        deliver_typed_message(context, wrapper, remote_sender_for(envelope.sender_path, envelope.correlation_id, conn, envelope.kind.ask_request?))
      rescue ex
        Log.error { "Failed to deserialize message: #{ex.message}" }
      end
    rescue ex
      Log.error { "Failed to route remote message to #{target_path_str}: #{ex.message}" }
    end

    private def deliver_typed_message(
      context : Movie::AbstractActorContext,
      wrapper : MessageWrapper,
      sender : Movie::ActorRefBase?,
    )
      wrapper.deliver_to(context, sender)
    end

    private def remote_sender_for(
      sender_path : String?,
      correlation_id : String?,
      conn : InboundConnection?,
      ask_request : Bool,
    ) : Movie::ActorRefBase?
      path = parse_sender_path(sender_path)

      if ask_request && correlation_id && conn
        RemoteAskResponseSenderRef.new(conn, correlation_id, path)
      else
        path ? RemoteSystemRef.new(path_registry, nil, path) : nil
      end
    end

    private def parse_sender_path(sender_path : String?) : ActorPath?
      return nil unless sender_path

      begin
        ActorPath.parse(sender_path)
      rescue ex
        Log.warn { "Failed to parse remote sender path #{sender_path}: #{ex.message}" }
        nil
      end
    end

    private def deliver_system_to_local(envelope : WireEnvelope, conn : InboundConnection?)
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

      system_message = deserialize_system_message(envelope.message_type, envelope.payload_data, conn)
      if connection = conn
        case system_message
        when Movie::Watch
          connection.track_watch(context.ref, system_message.actor)
        when Movie::Unwatch
          connection.untrack_watch(context.ref, system_message.actor)
        end
      end
      context.ref.send_system(system_message)
    rescue ex : RemoteUnsupportedSystemMessageError
      Log.error { "Unsupported remote system message #{envelope.message_type}: #{ex.message}" }
    rescue ex
      Log.error(exception: ex) { "Failed to deliver remote system message #{envelope.message_type}" }
    end

    private def deserialize_system_message(type : String, payload : JsonPayload, conn : InboundConnection?) : Movie::SystemMessage
      case type
      when "Movie::Stop"
        Movie::STOP
      when "Movie::Watch"
        Movie::Watch.new(remote_system_ref_from_payload(payload, conn))
      when "Movie::Unwatch"
        Movie::Unwatch.new(remote_system_ref_from_payload(payload, conn))
      when "Movie::Terminated"
        Movie::Terminated.new(remote_system_ref_from_payload(payload, nil))
      when "Movie::Failed"
        failed = RemoteFailedSystemPayload.from_json(payload.json_source)
        cause = if failed.error_class.empty? && failed.message.empty?
                  nil
                else
                  RemoteSystemFailureError.new(failed.error_class, failed.message)
                end
        Movie::Failed.new(remote_system_ref_from_payload(payload, nil), cause)
      else
        raise RemoteUnsupportedSystemMessageError.new(
          "Incoming remote system message #{type} is not supported"
        )
      end
    end

    private def remote_system_ref_from_payload(payload : JsonPayload, conn : InboundConnection?) : Movie::ActorRefBase
      actor_path = payload["actor_path"]?.try(&.as_s)
      raise RemoteUnsupportedSystemMessageError.new("Remote system message is missing actor_path") unless actor_path

      RemoteSystemRef.new(path_registry, conn, ActorPath.parse(actor_path))
    rescue ex : ArgumentError
      raise RemoteUnsupportedSystemMessageError.new("Invalid remote actor path #{actor_path}: #{ex.message}")
    end

    private def handle_inbound_disconnect(connection : InboundConnection) : Nil
      connection.drain_watches.each do |(target, watcher)|
        target.send_system(Movie::Unwatch.new(watcher).as(Movie::SystemMessage))
      rescue ex
        Log.warn { "Failed to purge remote watch after association loss: #{ex.message}" }
      end
    end
  end

  class RemoteSystemFailureError < Exception
    getter remote_class : String

    def initialize(@remote_class : String, message : String)
      super("#{remote_class}: #{message}")
    end
  end
end
