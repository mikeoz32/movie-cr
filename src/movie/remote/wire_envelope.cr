require "json"

module Movie::Remote
  # WireEnvelope is the envelope used for serializing messages over the wire.
  # It contains metadata about the message type and payload.
  struct WireEnvelope
    include JSON::Serializable

    # The kind of message being sent.
    enum Kind
      USER_MESSAGE    # Regular user message to an actor
      SYSTEM_MESSAGE  # System message (watch, stop, etc.)
      ASK_REQUEST     # Request part of ask pattern
      ASK_RESPONSE    # Response part of ask pattern
      HANDSHAKE       # Connection handshake
      HEARTBEAT       # Keep-alive heartbeat
    end

    # The kind of this envelope.
    property kind : Kind

    # Correlation ID for request-reply patterns (ask).
    # Used to match responses to their requests.
    property correlation_id : String?

    # The path of the sending actor (nil for system/anonymous senders).
    property sender_path : String?

    # The path of the target actor.
    property target_path : String

    # The type tag of the serialized message (for deserialization).
    property message_type : String

    # The serialized message payload as JSON.
    property payload : JSON::Any

    # Timestamp when the message was sent (epoch milliseconds).
    property timestamp : Int64

    def initialize(
      @kind : Kind,
      @target_path : String,
      @message_type : String,
      @payload : JSON::Any,
      @correlation_id : String? = nil,
      @sender_path : String? = nil,
      @timestamp : Int64 = Time.utc.to_unix_ms
    )
    end

    # Creates a user message envelope.
    def self.user_message(
      target_path : String,
      message_type : String,
      payload : JSON::Any,
      sender_path : String? = nil
    ) : WireEnvelope
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
      payload : JSON::Any,
      sender_path : String? = nil
    ) : WireEnvelope
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
      payload : JSON::Any,
      correlation_id : String,
      sender_path : String? = nil
    ) : WireEnvelope
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
      payload : JSON::Any,
      correlation_id : String
    ) : WireEnvelope
      new(
        kind: Kind::ASK_RESPONSE,
        target_path: target_path,
        message_type: message_type,
        payload: payload,
        correlation_id: correlation_id
      )
    end

    # Creates a handshake envelope.
    def self.handshake(
      system_name : String,
      address : String
    ) : WireEnvelope
      payload = JSON::Any.new({
        "system" => JSON::Any.new(system_name),
        "address" => JSON::Any.new(address)
      })
      new(
        kind: Kind::HANDSHAKE,
        target_path: "",
        message_type: "handshake",
        payload: payload
      )
    end

    # Creates a heartbeat envelope.
    def self.heartbeat : WireEnvelope
      new(
        kind: Kind::HEARTBEAT,
        target_path: "",
        message_type: "heartbeat",
        payload: JSON::Any.new({} of String => JSON::Any)
      )
    end
  end
end
