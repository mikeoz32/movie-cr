require "json"
require "./json_payload"

module Movie::Remote
  # Base class for deserializers to work around Crystal's generic type limitations.
  abstract class MessageDeserializer
    abstract def deserialize(payload : JsonPayload) : MessageWrapper
    abstract def deserialize(pull : JSON::PullParser) : MessageWrapper
  end

  # Wrapper for deserialized messages.
  abstract class MessageWrapper
    abstract def value : JSON::Serializable
    abstract def deliver_to(context : ::Movie::AbstractActorContext, sender : ::Movie::ActorRefBase?) : Nil

    def unwrap(type : T.class) : T forall T
      value.as(T)
    end
  end

  class TypedMessageWrapper(T) < MessageWrapper
    def initialize(@value : T)
    end

    def value : JSON::Serializable
      @value.as(JSON::Serializable)
    end

    def deliver_to(context : ::Movie::AbstractActorContext, sender : ::Movie::ActorRefBase?) : Nil
      context.deliver_serializable(@value, sender)
    end
  end

  # Holds a registered message that was decoded directly while parsing its
  # wire envelope. Normal delivery consumes the wrapper without parsing JSON a
  # second time; dynamic payload access can still serialize it on demand.
  private class DecodedMessagePayload < JsonPayload
    getter tag : String
    getter wrapper : MessageWrapper

    def initialize(@tag : String, @wrapper : MessageWrapper)
    end

    def to_json(json : JSON::Builder) : Nil
      @wrapper.value.to_json(json)
    end

    def json_source : String | IO
      io = IO::Memory.new
      @wrapper.value.to_json(io)
      io.rewind
    end
  end

  # Typed deserializer for a specific message type.
  class TypedDeserializer(T) < MessageDeserializer
    def deserialize(payload : JsonPayload) : MessageWrapper
      TypedMessageWrapper(T).new(T.from_json(payload.json_source))
    end

    def deserialize(pull : JSON::PullParser) : MessageWrapper
      TypedMessageWrapper(T).new(T.new(pull))
    end
  end

  class MalformedMessagePayloadError < Exception
    getter message_type : String

    def initialize(@message_type : String, cause : Exception)
      super("Malformed payload for #{message_type}: #{cause.message}", cause)
    end
  end

  # MessageRegistry manages serialization and deserialization of message types.
  # Message types must be registered before they can be sent over the wire.
  class MessageRegistry
    PAYLOAD_DECODER = ->(tag : String, pull : JSON::PullParser) do
      MessageRegistry.decode_payload(tag, pull).as(JsonPayload)
    end

    @@deserializers = {} of String => MessageDeserializer
    @@type_to_tag = {} of String => String
    @@tag_to_type = {} of String => String
    @@mutex = Mutex.new

    # Registers a message type with optional custom tag.
    # The type must include JSON::Serializable.
    #
    # Example:
    #   MessageRegistry.register(MyMessage)
    #   MessageRegistry.register(MyMessage, "custom-tag")
    macro register(type, tag = nil)
      {% tag_value = tag || type.stringify %}
      ::Movie::Remote::MessageRegistry.register_type(
        {{ tag_value }},
        {{ type }}.name,
        ::Movie::Remote::TypedDeserializer({{ type }}).new
      )
    end

    # Internal method to register type handlers.
    def self.register_type(
      tag : String,
      type_name : String,
      deserializer : MessageDeserializer,
    )
      @@mutex.synchronize do
        if existing_type = @@tag_to_type[tag]?
          raise ArgumentError.new("Message tag #{tag} is already registered for #{existing_type}") unless existing_type == type_name
        end
        if existing_tag = @@type_to_tag[type_name]?
          raise ArgumentError.new("Message type #{type_name} is already registered with tag #{existing_tag}") unless existing_tag == tag
        end
        @@deserializers[tag] = deserializer
        @@type_to_tag[type_name] = tag
        @@tag_to_type[tag] = type_name
      end
    end

    # Returns the tag for a type, or the type name if not registered.
    def self.tag_for(type_name : String) : String
      @@mutex.synchronize do
        @@type_to_tag[type_name]? || type_name
      end
    end

    # Prepares a message for direct wire serialization without materializing
    # an intermediate String or JSON DOM.
    def self.prepare(message : T) : {String, JsonPayload} forall T
      type_name = T.name
      tag = @@mutex.synchronize { @@type_to_tag[type_name]? } || type_name
      {tag, SerializableJsonPayload(T).new(message).as(JsonPayload)}
    end

    def self.prepare(message : JSON::Serializable) : {String, JsonPayload}
      type_name = message.class.name
      tag = @@mutex.synchronize { @@type_to_tag[type_name]? } || type_name
      {tag, SerializableJsonPayload(JSON::Serializable).new(message).as(JsonPayload)}
    end

    # Compatibility API for callers that explicitly require a JSON DOM.
    # Remoting hot paths use .prepare and never materialize this value.
    def self.serialize(message : T) : {String, JSON::Any} forall T
      tag, payload = prepare(message)
      {tag, payload.to_any}
    end

    def self.serialize(message : JSON::Serializable) : {String, JSON::Any}
      tag, payload = prepare(message)
      {tag, payload.to_any}
    end

    # Returns the reusable callback injected into connection-owned frame
    # decoders. Public/stateless FrameCodec decoding does not use the registry.
    def self.payload_decoder : JsonPayloadDecoder
      PAYLOAD_DECODER
    end

    # Consumes a registered payload directly from the envelope pull parser.
    # Unknown tags retain the raw/lazy compatibility path.
    def self.decode_payload(tag : String, pull : JSON::PullParser) : JsonPayload
      deserializer = @@mutex.synchronize { @@deserializers[tag]? }
      return RawJsonPayload.new(pull.read_raw) unless deserializer

      begin
        DecodedMessagePayload.new(tag, deserializer.deserialize(pull))
      rescue ex
        raise MalformedMessagePayloadError.new(tag, ex)
      end
    end

    # Deserializes a message from its tag and JSON payload.
    # Raises if the tag is not registered.
    def self.deserialize(tag : String, payload : JsonPayload) : MessageWrapper
      if decoded = payload.as?(DecodedMessagePayload)
        raise ArgumentError.new("Decoded payload tag #{decoded.tag} does not match #{tag}") unless decoded.tag == tag
        return decoded.wrapper
      end

      deserializer = @@mutex.synchronize { @@deserializers[tag]? }
      raise "No deserializer registered for tag: #{tag}" unless deserializer

      deserializer.deserialize(payload)
    end

    def self.deserialize(tag : String, payload : JSON::Any) : MessageWrapper
      deserialize(tag, AnyJsonPayload.new(payload))
    end

    # Checks if a tag is registered.
    def self.registered?(tag : String) : Bool
      @@mutex.synchronize { @@deserializers.has_key?(tag) }
    end

    # Returns all registered tags.
    def self.registered_tags : Array(String)
      @@mutex.synchronize { @@deserializers.keys }
    end

    # Clears all registrations (useful for testing).
    def self.clear
      @@mutex.synchronize do
        @@deserializers.clear
        @@type_to_tag.clear
        @@tag_to_type.clear
      end
    end
  end
end
