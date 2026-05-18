require "./wire_envelope"
require "./message_registry"
require "./connection"
require "./connection_pool"
require "../path"

module Movie::Remote
  # RemoteActorRef provides a type-safe reference to an actor in a remote system.
  # Messages sent through this reference are serialized and transmitted over TCP.
  # Uses striped connection pools for parallel sending while preserving per-actor ordering.
  class RemoteActorRef(T) < Movie::ActorRefBase
    Log = ::Log.for(self)

    getter target_path : ActorPath

    @pool : StripedConnectionPool
    @path_registry : Movie::PathRegistry
    @dedicated_connection : Connection

    def initialize(
      @system : Movie::AbstractActorSystem,
      @pool : StripedConnectionPool,
      @target_path : ActorPath,
      @path_registry : Movie::PathRegistry
    )
      super(@system.next_id)
      # Get consistent connection for this actor (preserves message ordering)
      @dedicated_connection = @pool.connection_for(@target_path)
    end

    # Sends a message to the remote actor (fire-and-forget).
    def <<(message : T)
      tell_from(nil, message)
    end

    # Sends a message to the remote actor with sender information.
    def tell_from(sender : Movie::ActorRefBase?, message : T)
      sender_path_str = if sender
        @path_registry.path_for(sender).try(&.to_s)
      else
        nil
      end

      tag, payload = MessageRegistry.serialize(message)

      envelope = WireEnvelope.user_message(
        target_path: @target_path.to_s,
        message_type: tag,
        payload: payload,
        sender_path: sender_path_str
      )

      # Use dedicated connection for ordering guarantee
      unless @dedicated_connection.send(envelope)
        Log.warn { "Failed to send message to #{@target_path}" }
      end
    end

    # Sends a message without ordering guarantee (uses round-robin for load balancing).
    def tell_unordered(message : T)
      tag, payload = MessageRegistry.serialize(message)

      envelope = WireEnvelope.user_message(
        target_path: @target_path.to_s,
        message_type: tag,
        payload: payload
      )

      # Use round-robin for maximum throughput
      unless @pool.send_round_robin(envelope)
        Log.warn { "Failed to send unordered message to #{@target_path}" }
      end
    end

    # Sends a system message to the remote actor.
    def send_system(message : Movie::SystemMessage)
      tag, payload = serialize_system_message(message)

      envelope = WireEnvelope.system_message(
        target_path: @target_path.to_s,
        message_type: tag,
        payload: payload
      )

      unless @dedicated_connection.send(envelope)
        Log.warn { "Failed to send system message to #{@target_path}" }
      end
    end

    # Performs an ask (request-reply) to the remote actor.
    # Returns a Future that will be completed with the response.
    def ask(message : T, response_type : R.class, timeout : Time::Span = 30.seconds) : Movie::Future(R) forall R
      correlation_id = UUID.random.to_s
      promise = Movie::Promise(R).new

      tag, payload = MessageRegistry.serialize(message)

      envelope = WireEnvelope.ask_request(
        target_path: @target_path.to_s,
        message_type: tag,
        payload: payload,
        correlation_id: correlation_id
      )

      # Register the pending ask before sending
      response_channel = @dedicated_connection.register_pending_ask(correlation_id)

      unless @dedicated_connection.send(envelope)
        @dedicated_connection.remove_pending_ask(correlation_id)
        promise.failure(RemoteDeliveryError.new("Failed to send ask to #{@target_path}"))
        return promise.future
      end

      # Spawn a fiber to wait for the response with timeout
      spawn do
        select
        when response = response_channel.receive?
          if response
            begin
              wrapper = MessageRegistry.deserialize(response.message_type, response.payload)
              result = wrapper.unwrap(R)
              promise.success(result)
            rescue ex
              promise.failure(ex)
            end
          else
            # Channel was closed (connection lost)
            promise.failure(RemoteDeliveryError.new("Connection closed while waiting for response"))
          end
        when timeout(timeout)
          @dedicated_connection.remove_pending_ask(correlation_id)
          promise.failure(Movie::FutureTimeout.new("Ask timed out after #{timeout}"))
        end
      end

      promise.future
    end

    # Returns the connection being used for this actor ref.
    def connection : Connection
      @dedicated_connection
    end

    private def serialize_system_message(message : Movie::SystemMessage) : {String, JSON::Any}
      # System messages need special handling since they're not user-registered types
      case message
      when Movie::Stop
        {"Movie::Stop", JSON::Any.new({} of String => JSON::Any)}
      when Movie::PreStart
        {"Movie::PreStart", JSON::Any.new({} of String => JSON::Any)}
      when Movie::PostStart
        {"Movie::PostStart", JSON::Any.new({} of String => JSON::Any)}
      when Movie::PreStop
        {"Movie::PreStop", JSON::Any.new({} of String => JSON::Any)}
      when Movie::PostStop
        {"Movie::PostStop", JSON::Any.new({} of String => JSON::Any)}
      when Movie::Watch
        # Watch contains an actor ref - we need to serialize the path
        watch = message.as(Movie::Watch)
        actor_path = @path_registry.path_for(watch.actor)
        payload = JSON::Any.new({"actor_path" => JSON::Any.new(actor_path.try(&.to_s) || "")})
        {"Movie::Watch", payload}
      when Movie::Unwatch
        unwatch = message.as(Movie::Unwatch)
        actor_path = @path_registry.path_for(unwatch.actor)
        payload = JSON::Any.new({"actor_path" => JSON::Any.new(actor_path.try(&.to_s) || "")})
        {"Movie::Unwatch", payload}
      when Movie::Terminated
        terminated = message.as(Movie::Terminated)
        actor_path = @path_registry.path_for(terminated.actor)
        payload = JSON::Any.new({"actor_path" => JSON::Any.new(actor_path.try(&.to_s) || "")})
        {"Movie::Terminated", payload}
      else
        # Generic fallback - just use the class name
        {message.class.name, JSON::Any.new({} of String => JSON::Any)}
      end
    end
  end

  # Error raised when a message cannot be delivered to a remote actor.
  class RemoteDeliveryError < Exception
  end
end
