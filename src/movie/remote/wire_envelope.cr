require "json"
require "./json_payload"

module Movie::Remote
  private record HandshakePayload, system : String, address : String do
    include JSON::Serializable
  end

  private record EmptyPayload do
    include JSON::Serializable
  end

  # WireEnvelope is the envelope used for serializing messages over the wire.
  # It preserves decoded payloads as raw JSON and writes outbound serializable
  # values directly to JSON::Builder.
  struct WireEnvelope
    # The kind of message being sent.
    enum Kind
      USER_MESSAGE   # Regular user message to an actor
      SYSTEM_MESSAGE # System message (watch, stop, etc.)
      ASK_REQUEST    # Request part of ask pattern
      ASK_RESPONSE   # Response part of ask pattern
      HANDSHAKE      # Connection handshake
      HEARTBEAT      # Keep-alive heartbeat
    end

    property kind : Kind
    property correlation_id : String?
    property sender_path : String?
    property target_path : String
    property message_type : String
    property timestamp : Int64

    @payload_data : JsonPayload

    getter payload_data : JsonPayload

    def initialize(
      @kind : Kind,
      @target_path : String,
      @message_type : String,
      payload : P,
      @correlation_id : String? = nil,
      @sender_path : String? = nil,
      @timestamp : Int64 = Time.utc.to_unix_ms,
    ) forall P
      @payload_data = JsonPayload.wrap(payload)
    end

    def initialize(pull : JSON::PullParser)
      kind = nil.as(Kind?)
      correlation_id = nil.as(String?)
      sender_path = nil.as(String?)
      target_path = nil.as(String?)
      message_type = nil.as(String?)
      payload_data = nil.as(JsonPayload?)
      timestamp = nil.as(Int64?)

      pull.read_object do |key|
        case key
        when "kind"
          kind = Kind.parse(pull.read_string)
        when "correlation_id"
          correlation_id = pull.read_string_or_null
        when "sender_path"
          sender_path = pull.read_string_or_null
        when "target_path"
          target_path = pull.read_string
        when "message_type"
          message_type = pull.read_string
        when "payload"
          payload_data = RawJsonPayload.new(pull.read_raw)
        when "timestamp"
          timestamp = pull.read_int
        else
          pull.skip
        end
      end

      @kind = kind || pull.raise("Missing JSON attribute: kind")
      @correlation_id = correlation_id
      @sender_path = sender_path
      @target_path = target_path || pull.raise("Missing JSON attribute: target_path")
      @message_type = message_type || pull.raise("Missing JSON attribute: message_type")
      @payload_data = payload_data || pull.raise("Missing JSON attribute: payload")
      @timestamp = timestamp || pull.raise("Missing JSON attribute: timestamp")
    end

    # Dynamic payload access is materialized lazily. Normal registered message
    # delivery uses payload_data and does not build JSON::Any.
    def payload : JSON::Any
      @payload_data.to_any
    end

    def payload=(value : JSON::Any) : JSON::Any
      @payload_data = AnyJsonPayload.new(value)
      value
    end

    def to_json(json : JSON::Builder) : Nil
      json.object do
        json.field("kind") { @kind.to_json(json) }
        if correlation_id = @correlation_id
          json.field("correlation_id", correlation_id)
        end
        if sender_path = @sender_path
          json.field("sender_path", sender_path)
        end
        json.field("target_path", @target_path)
        json.field("message_type", @message_type)
        json.field("payload") { @payload_data.to_json(json) }
        json.field("timestamp", @timestamp)
      end
    end

    # Creates a user message envelope.
    def self.user_message(
      target_path : String,
      message_type : String,
      payload : P,
      sender_path : String? = nil,
    ) : WireEnvelope forall P
      new(
        kind: Kind::USER_MESSAGE,
        target_path: target_path,
        message_type: message_type,
        payload: payload,
        sender_path: sender_path
      )
    end

    # Creates a system message envelope.
    def self.system_message(
      target_path : String,
      message_type : String,
      payload : P,
      sender_path : String? = nil,
    ) : WireEnvelope forall P
      new(
        kind: Kind::SYSTEM_MESSAGE,
        target_path: target_path,
        message_type: message_type,
        payload: payload,
        sender_path: sender_path
      )
    end

    # Creates an ask request envelope.
    def self.ask_request(
      target_path : String,
      message_type : String,
      payload : P,
      correlation_id : String,
      sender_path : String? = nil,
    ) : WireEnvelope forall P
      new(
        kind: Kind::ASK_REQUEST,
        target_path: target_path,
        message_type: message_type,
        payload: payload,
        correlation_id: correlation_id,
        sender_path: sender_path
      )
    end

    # Creates an ask response envelope.
    def self.ask_response(
      target_path : String,
      message_type : String,
      payload : P,
      correlation_id : String,
    ) : WireEnvelope forall P
      new(
        kind: Kind::ASK_RESPONSE,
        target_path: target_path,
        message_type: message_type,
        payload: payload,
        correlation_id: correlation_id
      )
    end

    # Creates a handshake envelope.
    def self.handshake(system_name : String, address : String) : WireEnvelope
      new(
        kind: Kind::HANDSHAKE,
        target_path: "",
        message_type: "handshake",
        payload: HandshakePayload.new(system_name, address)
      )
    end

    # Creates a heartbeat envelope.
    def self.heartbeat : WireEnvelope
      new(
        kind: Kind::HEARTBEAT,
        target_path: "",
        message_type: "heartbeat",
        payload: EmptyPayload.new
      )
    end
  end
end
