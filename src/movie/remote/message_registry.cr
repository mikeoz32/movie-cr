require "json"

module Movie::Remote
  # Base class for deserializers to work around Crystal's generic type limitations.
  abstract class MessageDeserializer
    abstract def deserialize(json : JSON::Any) : MessageWrapper
  end

  # Wrapper for deserialized messages.
  class MessageWrapper
    getter value : JSON::Serializable

    def initialize(@value : JSON::Serializable)
    end

    def unwrap(type : T.class) : T forall T
      @value.as(T)
    end
  end

  # Typed deserializer for a specific message type.
  class TypedDeserializer(T) < MessageDeserializer
    def deserialize(json : JSON::Any) : MessageWrapper
      MessageWrapper.new(T.from_json(json.to_json))
    end
  end

  # MessageRegistry manages serialization and deserialization of message types.
  # Message types must be registered before they can be sent over the wire.
  class MessageRegistry
    @@deserializers = {} of String => MessageDeserializer
    @@type_to_tag = {} of String => String
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
      deserializer : MessageDeserializer
    )
      @@mutex.synchronize do
        @@deserializers[tag] = deserializer
        @@type_to_tag[type_name] = tag
      end
    end

    # Returns the tag for a type, or the type name if not registered.
    def self.tag_for(type_name : String) : String
      @@mutex.synchronize do
        @@type_to_tag[type_name]? || type_name
      end
    end

    # Serializes a message, returning {tag, json_payload}.
    # The type must include JSON::Serializable.
    def self.serialize(message : T) : {String, JSON::Any} forall T
      type_name = T.name
      tag = @@mutex.synchronize { @@type_to_tag[type_name]? } || type_name
      json = JSON.parse(message.to_json)
      {tag, json}
    end

    # Deserializes a message from its tag and JSON payload.
    # Raises if the tag is not registered.
    def self.deserialize(tag : String, json : JSON::Any) : MessageWrapper
      deserializer = @@mutex.synchronize { @@deserializers[tag]? }
      raise "No deserializer registered for tag: #{tag}" unless deserializer

      deserializer.deserialize(json)
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
      end
    end
  end
end
