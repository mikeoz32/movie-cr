require "../spec_helper"
require "../../src/movie"
require "../../src/movie/persistence"

module Movie
  struct NameState
    include JSON::Serializable
    getter name : String

    def initialize(@name : String = "")
    end
  end

  struct SetName
    getter name : String
    def initialize(@name : String)
    end
  end

  struct GetName
    getter reply_to : Movie::ActorRef(String)
    def initialize(@reply_to : Movie::ActorRef(String))
    end
  end

  alias NameCommand = SetName | GetName

  class NameBehavior < DurableStateBehavior(NameCommand, NameState)
    def empty_state : NameState
      NameState.new("")
    end

    def handle_command(state : NameState, command : NameCommand, ctx : ActorContext(NameCommand)) : NameState?
      case command
      when SetName
        NameState.new(command.name)
      when GetName
        command.reply_to << state.name
        nil
      end
    end
  end

  class StringReceiver < AbstractBehavior(String)
    def initialize(@promise : Promise(String))
    end

    def receive(message : String, ctx : ActorContext(String))
      @promise.try_success(message)
      Behaviors(String).same
    end
  end
end

describe Movie::DurableState do
  it "loads and persists state by persistence id" do
    db_path = "/tmp/movie_durable_state_#{UUID.random}.sqlite3"
    config = Movie::Config.builder
      .set("movie.persistence.db_path", db_path)
      .build

    system = Movie::ActorSystem(Movie::SystemMessage).new(Movie::Behaviors(Movie::SystemMessage).same, config)
    ext = Movie::DurableState.get(system)

    ext.register_entity(Movie::NameBehavior) do |pid, store|
      Movie::NameBehavior.new(pid.persistence_id, store)
    end

    name_ref = ext.get_entity_ref_as(Movie::NameCommand, Movie::Persistence.id(Movie::NameBehavior, "name-1"))

    name_ref << Movie::SetName.new("alice")

    promise = Movie::Promise(String).new
    receiver = system.spawn(Movie::StringReceiver.new(promise))
    name_ref << Movie::GetName.new(receiver)
    value = promise.future.await(2.seconds)
    value.should eq("alice")

    system2 = Movie::ActorSystem(Movie::SystemMessage).new(Movie::Behaviors(Movie::SystemMessage).same, config)
    ext2 = Movie::DurableState.get(system2)
    ext2.register_entity(Movie::NameBehavior) do |pid, store|
      Movie::NameBehavior.new(pid.persistence_id, store)
    end

    name_ref2 = ext2.get_entity_ref_as(Movie::NameCommand, Movie::Persistence.id(Movie::NameBehavior, "name-1"))

    promise2 = Movie::Promise(String).new
    receiver2 = system2.spawn(Movie::StringReceiver.new(promise2))
    name_ref2 << Movie::GetName.new(receiver2)
    value2 = promise2.future.await(2.seconds)
    value2.should eq("alice")
  end
end
