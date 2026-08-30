require "json"

module Movie::Remote
  alias JsonPayloadDecoder = Proc(String, JSON::PullParser, JsonPayload)

  # A JSON value that can write itself directly to a builder.
  #
  # Outbound payloads retain the original serializable value instead of
  # materializing a String and parsing it into a JSON DOM. Decoded payloads
  # retain one raw JSON value and only build JSON::Any for dynamic access.
  abstract class JsonPayload
    abstract def to_json(json : JSON::Builder) : Nil
    abstract def json_source : String | IO

    def to_any : JSON::Any
      JSON.parse(json_source)
    end

    def [](key : String) : JSON::Any
      to_any[key]
    end

    def []?(key : String) : JSON::Any?
      to_any[key]?
    end

    def self.wrap(payload : JsonPayload) : JsonPayload
      payload
    end

    def self.wrap(payload : JSON::Any) : JsonPayload
      AnyJsonPayload.new(payload)
    end

    def self.wrap(payload : T) : JsonPayload forall T
      SerializableJsonPayload(T).new(payload)
    end
  end

  class AnyJsonPayload < JsonPayload
    def initialize(@value : JSON::Any)
    end

    def to_json(json : JSON::Builder) : Nil
      @value.to_json(json)
    end

    def json_source : String | IO
      io = IO::Memory.new
      @value.to_json(io)
      io.rewind
    end

    def to_any : JSON::Any
      @value
    end
  end

  class RawJsonPayload < JsonPayload
    @materialized : JSON::Any?

    def initialize(@raw : String)
      @materialized = nil
    end

    def to_json(json : JSON::Builder) : Nil
      json.raw(@raw)
    end

    def json_source : String | IO
      @raw
    end

    def to_any : JSON::Any
      @materialized ||= JSON.parse(@raw)
    end
  end

  class SerializableJsonPayload(T) < JsonPayload
    @materialized : JSON::Any?

    def initialize(@value : T)
      @materialized = nil
    end

    def to_json(json : JSON::Builder) : Nil
      @value.to_json(json)
    end

    def json_source : String | IO
      io = IO::Memory.new
      @value.to_json(io)
      io.rewind
    end

    def to_any : JSON::Any
      @materialized ||= JSON.parse(json_source)
    end
  end
end
