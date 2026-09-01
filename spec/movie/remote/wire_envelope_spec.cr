require "../../spec_helper"
require "../../../src/movie/remote/wire_envelope"

describe Movie::Remote::WireEnvelope do
  describe ".user_message" do
    it "creates a user message envelope" do
      payload = JSON::Any.new({"data" => JSON::Any.new("hello")})
      env = Movie::Remote::WireEnvelope.user_message(
        target_path: "movie://sys/user/actor1",
        message_type: "MyMessage",
        payload: payload,
        sender_path: "movie://sys/user/sender"
      )

      env.kind.should eq(Movie::Remote::WireEnvelope::Kind::USER_MESSAGE)
      env.target_path.should eq("movie://sys/user/actor1")
      env.message_type.should eq("MyMessage")
      env.sender_path.should eq("movie://sys/user/sender")
      env.correlation_id.should be_nil
    end
  end

  describe ".system_message" do
    it "creates a system message envelope" do
      payload = JSON::Any.new({} of String => JSON::Any)
      env = Movie::Remote::WireEnvelope.system_message(
        target_path: "movie://sys/user/actor1",
        message_type: "Stop",
        payload: payload
      )

      env.kind.should eq(Movie::Remote::WireEnvelope::Kind::SYSTEM_MESSAGE)
      env.message_type.should eq("Stop")
    end
  end

  describe ".ask_request" do
    it "creates an ask request envelope" do
      payload = JSON::Any.new({"query" => JSON::Any.new("test")})
      env = Movie::Remote::WireEnvelope.ask_request(
        target_path: "movie://sys/user/actor1",
        message_type: "Query",
        payload: payload,
        correlation_id: "corr-123",
        sender_path: "movie://sys/user/asker"
      )

      env.kind.should eq(Movie::Remote::WireEnvelope::Kind::ASK_REQUEST)
      env.correlation_id.should eq("corr-123")
    end
  end

  describe ".ask_response" do
    it "creates an ask response envelope" do
      payload = JSON::Any.new({"result" => JSON::Any.new(42_i64)})
      env = Movie::Remote::WireEnvelope.ask_response(
        target_path: "movie://sys/user/asker",
        message_type: "QueryResult",
        payload: payload,
        correlation_id: "corr-123"
      )

      env.kind.should eq(Movie::Remote::WireEnvelope::Kind::ASK_RESPONSE)
      env.correlation_id.should eq("corr-123")
    end
  end

  describe ".handshake" do
    it "creates a handshake envelope" do
      env = Movie::Remote::WireEnvelope.handshake(
        system_name: "my-system",
        address: "movie.tcp://my-system@127.0.0.1:2552"
      )

      env.kind.should eq(Movie::Remote::WireEnvelope::Kind::HANDSHAKE)
      env.payload["system"].as_s.should eq("my-system")
      env.payload["address"].as_s.should eq("movie.tcp://my-system@127.0.0.1:2552")
    end

    it "carries a signed versioned association identity" do
      handshake = Movie::Remote::AssociationHandshake.create(
        system: "my-system",
        address: "movie.tcp://my-system@127.0.0.1:2552",
        node_uid: "node-1",
        association_id: "association-1",
        shared_secret: "cluster-secret"
      )

      envelope = Movie::Remote::WireEnvelope.handshake(handshake)
      decoded = Movie::Remote::WireEnvelope.from_json(envelope.to_json)
      payload = Movie::Remote::AssociationHandshake.from_json(decoded.payload_data.json_source)

      payload.protocol_version.should eq(Movie::Remote::PROTOCOL_VERSION)
      payload.capabilities.should contain(Movie::Remote::CAPABILITY_CONTROL_ACKS)
      payload.authenticated?("cluster-secret").should be_true
      payload.authenticated?("wrong-secret").should be_false
    end

    it "creates explicit handshake acknowledgements and rejections" do
      ack = Movie::Remote::WireEnvelope.handshake_ack(
        Movie::Remote::AssociationHandshake.create(
          system: "server",
          address: "movie.tcp://server@127.0.0.1:2552",
          node_uid: "server-node",
          association_id: "association-1"
        )
      )
      rejection = Movie::Remote::WireEnvelope.handshake_reject("unsupported protocol version")

      ack.kind.should eq(Movie::Remote::WireEnvelope::Kind::HANDSHAKE_ACK)
      rejection.kind.should eq(Movie::Remote::WireEnvelope::Kind::HANDSHAKE_REJECT)
      rejection.payload["reason"].as_s.should eq("unsupported protocol version")
    end

    it "creates handshake confirmation and ready frames" do
      confirmation = Movie::Remote::AssociationConfirmation.new(
        association_id: "association-1",
        node_uid: "client-node",
        client_nonce: "client-nonce",
        server_nonce: "server-nonce",
        auth_proof: "proof"
      )

      confirm = Movie::Remote::WireEnvelope.handshake_confirm(confirmation)
      ready = Movie::Remote::WireEnvelope.handshake_ready("association-1")

      confirm.kind.handshake_confirm?.should be_true
      ready.kind.handshake_ready?.should be_true
    end
  end

  describe ".heartbeat" do
    it "creates a heartbeat envelope" do
      env = Movie::Remote::WireEnvelope.heartbeat

      env.kind.should eq(Movie::Remote::WireEnvelope::Kind::HEARTBEAT)
      env.message_type.should eq("heartbeat")
    end
  end

  describe "JSON serialization" do
    it "serializes and deserializes" do
      payload = JSON::Any.new({"data" => JSON::Any.new("test")})
      env = Movie::Remote::WireEnvelope.user_message(
        target_path: "movie://sys/user/actor1",
        message_type: "MyMessage",
        payload: payload,
        sender_path: "movie://sys/user/sender"
      )

      json = env.to_json
      parsed = Movie::Remote::WireEnvelope.from_json(json)

      parsed.kind.should eq(env.kind)
      parsed.target_path.should eq(env.target_path)
      parsed.message_type.should eq(env.message_type)
      parsed.sender_path.should eq(env.sender_path)
      parsed.payload["data"].as_s.should eq("test")
    end

    it "preserves timestamp" do
      payload = JSON::Any.new({} of String => JSON::Any)
      before = Time.utc.to_unix_ms
      env = Movie::Remote::WireEnvelope.user_message(
        target_path: "movie://sys/user/actor1",
        message_type: "MyMessage",
        payload: payload
      )
      after = Time.utc.to_unix_ms

      env.timestamp.should be >= before
      env.timestamp.should be <= after
    end
  end
end
