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
    getter operation_id : Persistence::OperationId

    def initialize(@name : String, @operation_id : Persistence::OperationId = Persistence::OperationId.random)
    end
  end

  struct GetName
    getter reply_to : Movie::ActorRef(String)

    def initialize(@reply_to : Movie::ActorRef(String))
    end
  end

  struct DeleteName
    getter operation_id : Persistence::OperationId

    def initialize(@operation_id : Persistence::OperationId = Persistence::OperationId.random)
    end
  end

  alias NameCommand = SetName | GetName | DeleteName

  class NameBehavior < DurableStateBehavior(NameCommand, NameState)
    def empty_state : NameState
      NameState.new("")
    end

    def handle_command(state : NameState, command : NameCommand, ctx : ActorContext(NameCommand)) : DurableEffect(NameState)
      case command
      when SetName
        persist(NameState.new(command.name), command.operation_id)
      when GetName
        none.then_run { |current| command.reply_to << current.name }
      when DeleteName
        delete(command.operation_id)
      else
        none
      end
    end
  end

  class StringReceiver < AbstractBehavior(String)
    def initialize(@promise : Promise(String))
    end

    def receive(message : String, context : ActorContext(String))
      @promise.try_success(message)
      Behaviors(String).same
    end
  end
end

describe Movie::DurableState do
  it "loads and persists state by persistence id" do
    db_path = "/tmp/movie_durable_state_#{UUID.random}.sqlite3"
    config = Movie::Config.builder
      .set("persistence.db-path", db_path)
      .build

    system = Movie::ActorSystem(Movie::SystemMessage).new(Movie::Behaviors(Movie::SystemMessage).same, config)
    ext = Movie::DurableState.get(system)

    name_type = ext.register_entity(Movie::NameBehavior, Movie::NameCommand) do |pid, store|
      Movie::NameBehavior.new(pid.persistence_id, store)
    end

    name_ref = ext.get_entity_ref(name_type.id("name-1"))

    name_ref << Movie::SetName.new("alice")

    promise = Movie::Promise(String).new
    receiver = system.spawn(Movie::StringReceiver.new(promise))
    name_ref << Movie::GetName.new(receiver)
    value = promise.future.await(2.seconds)
    value.should eq("alice")

    database = Movie::Database.get(system)
    system.ask(
      database.pool,
      Movie::Persistence::SaveState.new(
        "Movie::NameBehavior:name-1",
        1_i64,
        Movie::Persistence::OperationId.new("external-name-update"),
        "name-v0",
        Movie::NameState.new("bob").to_json
      ),
      Movie::Persistence::WriteResult,
      2.seconds
    ).await(2.seconds)

    name_ref.send_system(Movie::Restart.new(nil))
    sleep 50.milliseconds
    restarted_promise = Movie::Promise(String).new
    restarted_receiver = system.spawn(Movie::StringReceiver.new(restarted_promise))
    name_ref << Movie::GetName.new(restarted_receiver)
    restarted_promise.future.await(2.seconds).should eq("bob")

    name_ref << Movie::DeleteName.new
    deleted_promise = Movie::Promise(String).new
    deleted_receiver = system.spawn(Movie::StringReceiver.new(deleted_promise))
    name_ref << Movie::GetName.new(deleted_receiver)
    deleted_promise.future.await(2.seconds).should eq("")

    tombstone = system.ask(
      database.pool,
      Movie::Persistence::LoadState.new("Movie::NameBehavior:name-1"),
      Movie::Persistence::StateRecord?,
      2.seconds
    ).await(2.seconds)
    tombstone.should_not be_nil
    tombstone.not_nil!.revision.should eq(3_i64)
    tombstone.not_nil!.deleted.should be_true

    stale = expect_raises(Movie::Persistence::ConcurrentWriteError) do
      system.ask(
        database.pool,
        Movie::Persistence::SaveState.new(
          "Movie::NameBehavior:name-1",
          2_i64,
          Movie::Persistence::OperationId.new("stale-state-update"),
          "name-v0",
          Movie::NameState.new("stale").to_json
        ),
        Movie::Persistence::WriteResult,
        2.seconds
      ).await(2.seconds)
    end
    stale.actual_revision.should eq(3_i64)

    system2 = Movie::ActorSystem(Movie::SystemMessage).new(Movie::Behaviors(Movie::SystemMessage).same, config)
    ext2 = Movie::DurableState.get(system2)
    name_type2 = ext2.register_entity(Movie::NameBehavior, Movie::NameCommand) do |pid, store|
      Movie::NameBehavior.new(pid.persistence_id, store)
    end

    name_ref2 = ext2.get_entity_ref(name_type2.id("name-1"))

    promise2 = Movie::Promise(String).new
    receiver2 = system2.spawn(Movie::StringReceiver.new(promise2))
    name_ref2 << Movie::GetName.new(receiver2)
    value2 = promise2.future.await(2.seconds)
    value2.should eq("")
  end
end
