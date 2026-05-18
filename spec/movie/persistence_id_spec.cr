require "../spec_helper"
require "../../src/movie"
require "../../src/movie/persistence"

module Movie
  class DummyBehavior < AbstractBehavior(Int32)
    def receive(message, ctx)
      Behaviors(Int32).same
    end
  end
end

describe Movie::Persistence::Id do
  it "builds persistence_id from type and entity_id" do
    id = Movie::Persistence::Id.new("Counter", "c1")
    id.entity_type.should eq("Counter")
    id.entity_id.should eq("c1")
    id.persistence_id.should eq("Counter:c1")
  end

  it "uses class name in helper" do
    id = Movie::Persistence.id(Movie::DummyBehavior, "x1")
    id.entity_type.should eq(Movie::DummyBehavior.name)
    id.persistence_id.should eq("#{Movie::DummyBehavior.name}:x1")
  end

  it "builds deterministic entity name from id" do
    id = Movie::Persistence::Id.new("Test", "abc-123")
    Movie::Persistence.entity_name(id).should eq("entity-test-abc-123")
  end
end
