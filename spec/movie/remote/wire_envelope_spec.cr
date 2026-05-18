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
