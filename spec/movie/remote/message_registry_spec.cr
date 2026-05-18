require "../../spec_helper"
require "../../../src/movie/remote/message_registry"

# Test message type
record TestMessage, value : String do
  include JSON::Serializable
end

record AnotherMessage, count : Int32 do
  include JSON::Serializable
end

describe Movie::Remote::MessageRegistry do
  before_each do
    Movie::Remote::MessageRegistry.clear
  end

  describe ".register" do
    it "registers a message type" do
      Movie::Remote::MessageRegistry.register(TestMessage)
      Movie::Remote::MessageRegistry.registered?("TestMessage").should be_true
    end

    it "registers with custom tag" do
      Movie::Remote::MessageRegistry.register(TestMessage, "custom-tag")
      Movie::Remote::MessageRegistry.registered?("custom-tag").should be_true
    end
  end

  describe ".serialize" do
    it "serializes a registered message" do
      Movie::Remote::MessageRegistry.register(TestMessage)

      msg = TestMessage.new(value: "hello")
      tag, json = Movie::Remote::MessageRegistry.serialize(msg)

      tag.should eq("TestMessage")
      json["value"].as_s.should eq("hello")
    end

    it "serializes unregistered types using type name" do
      # AnotherMessage is not registered
      msg = AnotherMessage.new(count: 42)
      tag, json = Movie::Remote::MessageRegistry.serialize(msg)

      tag.should eq("AnotherMessage")
      json["count"].as_i.should eq(42)
    end
  end

  describe ".deserialize" do
    it "deserializes a registered message" do
      Movie::Remote::MessageRegistry.register(TestMessage)

      json = JSON.parse(%({"value": "world"}))
      wrapper = Movie::Remote::MessageRegistry.deserialize("TestMessage", json)

      wrapper.value.should be_a(TestMessage)
      wrapper.unwrap(TestMessage).value.should eq("world")
    end

    it "raises for unregistered tag" do
      expect_raises(Exception, /No deserializer registered/) do
        json = JSON.parse(%({}))
        Movie::Remote::MessageRegistry.deserialize("UnknownType", json)
      end
    end
  end

  describe ".registered_tags" do
    it "returns all registered tags" do
      Movie::Remote::MessageRegistry.register(TestMessage, "tag1")
      Movie::Remote::MessageRegistry.register(AnotherMessage, "tag2")

      tags = Movie::Remote::MessageRegistry.registered_tags
      tags.should contain("tag1")
      tags.should contain("tag2")
    end
  end

  describe "roundtrip" do
    it "serializes and deserializes correctly" do
      Movie::Remote::MessageRegistry.register(TestMessage)

      original = TestMessage.new(value: "roundtrip-test")
      tag, json = Movie::Remote::MessageRegistry.serialize(original)
      wrapper = Movie::Remote::MessageRegistry.deserialize(tag, json)

      restored = wrapper.unwrap(TestMessage)
      restored.value.should eq(original.value)
    end
  end
end
