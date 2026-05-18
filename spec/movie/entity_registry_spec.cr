require "../spec_helper"
require "../../src/movie"
require "../../src/movie/persistence"

module Movie
  struct PathRequest
    getter reply_to : Movie::ActorRef(String)

    def initialize(@reply_to : Movie::ActorRef(String))
    end
  end

  class PathActor < AbstractBehavior(PathRequest)
    def receive(message, ctx)
      path_str = ctx.path.try(&.to_s) || ""
      message.reply_to << path_str
      Behaviors(PathRequest).same
    end
  end

  class StringReceiver < AbstractBehavior(String)
    def initialize(@promise : Promise(String))
    end

    def receive(message, ctx)
      @promise.try_success(message)
      Behaviors(String).same
    end
  end
end

describe Movie::Persistence::EntityRegistry do
  it "spawns entities once and keeps them as children" do
    system = Movie::ActorSystem(Movie::SystemMessage).new(Movie::Behaviors(Movie::SystemMessage).same)
    registry = system.spawn(Movie::Persistence::EntityRegistry.new)

    spawn_proc = ->(ctx : Movie::ActorContext(Movie::Persistence::RegistryMessage), id : Movie::Persistence::Id) do
      ctx.spawn(Movie::PathActor.new).as(Movie::ActorRefBase)
    end

    pid = Movie::Persistence::Id.new("Test", "entity-1")
    ref1 = system.ask(registry, Movie::Persistence::GetEntity.new(pid, spawn_proc), Movie::ActorRefBase, 1.second).await(1.second)
    ref2 = system.ask(registry, Movie::Persistence::GetEntity.new(pid, spawn_proc), Movie::ActorRefBase, 1.second).await(1.second)

    ref1.id.should eq(ref2.id)

    promise = Movie::Promise(String).new
    receiver = system.spawn(Movie::StringReceiver.new(promise))
    ref1.as(Movie::ActorRef(Movie::PathRequest)) << Movie::PathRequest.new(receiver)
    path = promise.future.await(1.second)

    registry_path = registry.path.try(&.to_s) || ""
    path.starts_with?(registry_path).should be_true
  end

  it "uses deterministic child names based on persistence id" do
    system = Movie::ActorSystem(Movie::SystemMessage).new(Movie::Behaviors(Movie::SystemMessage).same)
    registry = system.spawn(Movie::Persistence::EntityRegistry.new)

    spawn_proc = ->(ctx : Movie::ActorContext(Movie::Persistence::RegistryMessage), id : Movie::Persistence::Id) do
      ctx.spawn(Movie::PathActor.new, name: Movie::Persistence.entity_name(id)).as(Movie::ActorRefBase)
    end

    pid = Movie::Persistence::Id.new("TestType", "entity-42")
    ref = system.ask(registry, Movie::Persistence::GetEntity.new(pid, spawn_proc), Movie::ActorRefBase, 1.second).await(1.second)

    promise = Movie::Promise(String).new
    receiver = system.spawn(Movie::StringReceiver.new(promise))
    ref.as(Movie::ActorRef(Movie::PathRequest)) << Movie::PathRequest.new(receiver)
    path = promise.future.await(1.second)

    path.includes?(Movie::Persistence.entity_name(pid)).should be_true
  end
end
