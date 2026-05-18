require "../../spec_helper"
require "../../../src/movie"

# Integration test message types
record EchoRequest, message : String do
  include JSON::Serializable
end

record EchoResponse, message : String do
  include JSON::Serializable
end

describe "Movie Remote Integration" do
  describe "ActorSystem with remoting" do
    it "creates an actor system with a name" do
      system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "test-system"
      )
      system.name.should eq("test-system")
      system.address.system.should eq("test-system")
      system.address.local?.should be_true
    end

    it "enables remoting on an actor system" do
      system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "remote-test"
      )

      # Use port 0 to let OS assign an available port
      extension = system.enable_remoting("127.0.0.1", 0)

      extension.should_not be_nil
      system.remoting_enabled?.should be_true
      system.remote.should eq(extension)

      # Address should be updated
      system.address.remote?.should be_true
      system.address.host.should eq("127.0.0.1")

      # Clean up
      extension.stop
    end

    it "can get actual bound port when using port 0" do
      system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "port-test"
      )

      extension = system.enable_remoting("127.0.0.1", 0)
      port = extension.local_port

      port.should be > 0
      port.should be < 65536

      extension.stop
    end

    it "creates remote actor references" do
      # Create system with remoting
      system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "ref-test"
      )
      extension = system.enable_remoting("127.0.0.1", 0)

      # Create a remote ref to a hypothetical actor on another system
      remote_path = Movie::ActorPath.parse("movie.tcp://other-system@127.0.0.1:9999/user/actor1")
      remote_ref = extension.actor_ref(remote_path, String)

      remote_ref.should be_a(Movie::Remote::RemoteActorRef(String))
      remote_ref.target_path.should eq(remote_path)

      extension.stop
    end
  end

  describe "Address and ActorPath integration" do
    it "creates local address from system" do
      system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "local-sys"
      )

      addr = system.address
      addr.protocol.should eq("movie")
      addr.system.should eq("local-sys")
      addr.local?.should be_true
    end

    it "updates address when enabling remoting" do
      system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "remote-sys"
      )

      system.address.local?.should be_true

      extension = system.enable_remoting("127.0.0.1", 0)

      system.address.remote?.should be_true
      system.address.protocol.should eq("movie.tcp")
      system.address.host.should eq("127.0.0.1")

      extension.stop
    end
  end

  describe "PathRegistry integration" do
    it "registers and resolves actor paths" do
      registry = Movie::PathRegistry.new
      address = Movie::Address.local("test-system")

      # Create a mock ref-like object for testing
      # In real usage, this would be an actual ActorRef
      system = Movie::ActorSystem(String).new(
        Movie::Behaviors(String).same,
        name: "registry-test"
      )

      actor = system.spawn(Movie::Behaviors(String).same)
      path = Movie::ActorPath.new(address, ["user", "test-actor"])

      registry.register(actor, path)

      # Resolve by path
      resolved_id = registry.resolve(path)
      resolved_id.should eq(actor.id)

      # Resolve by path string
      resolved_id2 = registry.resolve(path.to_s)
      resolved_id2.should eq(actor.id)

      # Get path for ref
      found_path = registry.path_for(actor)
      found_path.should eq(path)
    end
  end

  describe "MessageRegistry roundtrip" do
    it "serializes and deserializes custom message types" do
      # Register within test to avoid issues with other tests clearing registry
      Movie::Remote::MessageRegistry.register(EchoRequest)
      Movie::Remote::MessageRegistry.register(EchoResponse)

      original = EchoRequest.new(message: "Hello, World!")
      tag, json = Movie::Remote::MessageRegistry.serialize(original)

      tag.should eq("EchoRequest")

      wrapper = Movie::Remote::MessageRegistry.deserialize(tag, json)
      restored = wrapper.unwrap(EchoRequest)

      restored.message.should eq(original.message)
    end
  end

  describe "WireEnvelope creation" do
    it "creates user message envelopes with actor paths" do
      payload = JSON.parse(%({"message": "test"}))

      envelope = Movie::Remote::WireEnvelope.user_message(
        target_path: "movie.tcp://sys@host:1234/user/actor",
        message_type: "EchoRequest",
        payload: payload,
        sender_path: "movie.tcp://other@host:5678/user/sender"
      )

      envelope.kind.should eq(Movie::Remote::WireEnvelope::Kind::USER_MESSAGE)
      envelope.target_path.should eq("movie.tcp://sys@host:1234/user/actor")
      envelope.sender_path.should eq("movie.tcp://other@host:5678/user/sender")
    end
  end
end
