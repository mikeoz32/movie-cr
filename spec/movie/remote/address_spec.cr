require "../../spec_helper"
require "../../../src/movie/path"

describe Movie::Address do
  describe ".local" do
    it "creates a local address" do
      addr = Movie::Address.local("my-system")
      addr.protocol.should eq("movie")
      addr.system.should eq("my-system")
      addr.host.should be_nil
      addr.port.should be_nil
      addr.local?.should be_true
      addr.remote?.should be_false
    end
  end

  describe ".remote" do
    it "creates a remote address" do
      addr = Movie::Address.remote("my-system", "127.0.0.1", 2552)
      addr.protocol.should eq("movie.tcp")
      addr.system.should eq("my-system")
      addr.host.should eq("127.0.0.1")
      addr.port.should eq(2552)
      addr.local?.should be_false
      addr.remote?.should be_true
    end
  end

  describe ".parse" do
    it "parses a local address" do
      addr = Movie::Address.parse("movie://my-system")
      addr.protocol.should eq("movie")
      addr.system.should eq("my-system")
      addr.local?.should be_true
    end

    it "parses a remote address" do
      addr = Movie::Address.parse("movie.tcp://my-system@127.0.0.1:2552")
      addr.protocol.should eq("movie.tcp")
      addr.system.should eq("my-system")
      addr.host.should eq("127.0.0.1")
      addr.port.should eq(2552)
      addr.remote?.should be_true
    end

    it "raises on invalid URI" do
      expect_raises(ArgumentError) do
        Movie::Address.parse("invalid")
      end
    end
  end

  describe "#to_s" do
    it "serializes local address" do
      addr = Movie::Address.local("my-system")
      addr.to_s.should eq("movie://my-system")
    end

    it "serializes remote address" do
      addr = Movie::Address.remote("my-system", "127.0.0.1", 2552)
      addr.to_s.should eq("movie.tcp://my-system@127.0.0.1:2552")
    end
  end

  describe "JSON serialization" do
    it "serializes and deserializes local address" do
      addr = Movie::Address.local("my-system")
      json = addr.to_json
      parsed = Movie::Address.from_json(json)
      parsed.should eq(addr)
    end

    it "serializes and deserializes remote address" do
      addr = Movie::Address.remote("my-system", "127.0.0.1", 2552)
      json = addr.to_json
      parsed = Movie::Address.from_json(json)
      parsed.should eq(addr)
    end
  end

  describe "#==" do
    it "compares equal addresses" do
      a1 = Movie::Address.remote("sys", "host", 1234)
      a2 = Movie::Address.remote("sys", "host", 1234)
      a1.should eq(a2)
    end

    it "compares different addresses" do
      a1 = Movie::Address.remote("sys", "host", 1234)
      a2 = Movie::Address.remote("sys", "host", 5678)
      a1.should_not eq(a2)
    end
  end
end

describe Movie::ActorPath do
  describe ".root" do
    it "creates a root path" do
      addr = Movie::Address.local("my-system")
      path = Movie::ActorPath.root(addr)
      path.address.should eq(addr)
      path.elements.should be_empty
      path.root?.should be_true
      path.name.should eq("")
    end
  end

  describe "#/" do
    it "creates child paths" do
      addr = Movie::Address.local("my-system")
      root = Movie::ActorPath.root(addr)
      user = root / "user"
      actor = user / "actor1"

      user.elements.should eq(["user"])
      user.name.should eq("user")

      actor.elements.should eq(["user", "actor1"])
      actor.name.should eq("actor1")
    end
  end

  describe "#parent" do
    it "returns parent path" do
      addr = Movie::Address.local("my-system")
      path = Movie::ActorPath.new(addr, ["user", "actor1"])
      parent = path.parent
      parent.should_not be_nil
      parent.not_nil!.elements.should eq(["user"])
    end

    it "returns nil for root path" do
      addr = Movie::Address.local("my-system")
      root = Movie::ActorPath.root(addr)
      root.parent.should be_nil
    end
  end

  describe ".parse" do
    it "parses local path with elements" do
      path = Movie::ActorPath.parse("movie://my-system/user/actor1")
      path.address.system.should eq("my-system")
      path.address.local?.should be_true
      path.elements.should eq(["user", "actor1"])
    end

    it "parses remote path with elements" do
      path = Movie::ActorPath.parse("movie.tcp://my-system@127.0.0.1:2552/user/actor1")
      path.address.system.should eq("my-system")
      path.address.host.should eq("127.0.0.1")
      path.address.port.should eq(2552)
      path.elements.should eq(["user", "actor1"])
    end

    it "parses path without elements" do
      path = Movie::ActorPath.parse("movie://my-system")
      path.address.system.should eq("my-system")
      path.elements.should be_empty
    end
  end

  describe "#to_s" do
    it "serializes path with elements" do
      addr = Movie::Address.local("my-system")
      path = Movie::ActorPath.new(addr, ["user", "actor1"])
      path.to_s.should eq("movie://my-system/user/actor1")
    end

    it "serializes root path" do
      addr = Movie::Address.local("my-system")
      path = Movie::ActorPath.root(addr)
      path.to_s.should eq("movie://my-system")
    end

    it "serializes remote path" do
      addr = Movie::Address.remote("my-system", "127.0.0.1", 2552)
      path = Movie::ActorPath.new(addr, ["user", "actor1"])
      path.to_s.should eq("movie.tcp://my-system@127.0.0.1:2552/user/actor1")
    end
  end

  describe "JSON serialization" do
    it "serializes and deserializes" do
      addr = Movie::Address.remote("my-system", "127.0.0.1", 2552)
      path = Movie::ActorPath.new(addr, ["user", "actor1"])
      json = path.to_json
      parsed = Movie::ActorPath.from_json(json)
      parsed.should eq(path)
    end
  end

  describe "roundtrip" do
    it "parse(to_s) roundtrips" do
      addr = Movie::Address.remote("sys", "host", 1234)
      path = Movie::ActorPath.new(addr, ["a", "b", "c"])
      roundtripped = Movie::ActorPath.parse(path.to_s)
      roundtripped.should eq(path)
    end
  end
end
