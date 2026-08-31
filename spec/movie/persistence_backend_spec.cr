require "../spec_helper"
require "../../src/movie"
require "../../src/movie/persistence"

module Movie
  class ProbeConnectionLost < Exception
  end

  class ProbeBackendConnection < Persistence::BackendConnection
    def initialize(@fail_first_read : Bool)
      @closed = false
    end

    def ensure_event_store : Nil
    end

    def ensure_state_store : Nil
    end

    def append_events(message : Persistence::AppendEvents) : Persistence::WriteResult
      Persistence::WriteResult.new(message.expected_revision + message.events.size, false)
    end

    def load_events(message : Persistence::LoadEvents) : Array(Persistence::StoredEvent)
      if @fail_first_read
        @fail_first_read = false
        raise ProbeConnectionLost.new("connection dropped")
      end
      [Persistence::StoredEvent.new(1_i64, "probe", "payload")]
    end

    def save_snapshot(message : Persistence::SaveSnapshot) : Nil
    end

    def load_snapshot(message : Persistence::LoadSnapshot) : Persistence::SnapshotRecord?
      nil
    end

    def delete_snapshot(message : Persistence::DeleteSnapshot) : Nil
    end

    def save_state(message : Persistence::SaveState) : Persistence::WriteResult
      Persistence::WriteResult.new(message.expected_revision + 1, false)
    end

    def load_state(message : Persistence::LoadState) : Persistence::StateRecord?
      nil
    end

    def delete_state(message : Persistence::DeleteState) : Persistence::WriteResult
      Persistence::WriteResult.new(message.expected_revision + 1, false)
    end

    def connection_lost?(error : Exception) : Bool
      error.is_a?(ProbeConnectionLost)
    end

    def close : Nil
      @closed = true
    end
  end

  class ProbeBackend < Persistence::Backend
    getter connections : Atomic(Int32)

    def initialize
      @connections = Atomic(Int32).new(0)
    end

    def name : String
      "probe"
    end

    def connect : Persistence::BackendConnection
      number = @connections.add(1) + 1
      ProbeBackendConnection.new(number == 1)
    end
  end
end

describe "Movie persistence backend SPI" do
  it "rejects an unregistered backend by name" do
    expect_raises(Movie::Persistence::UnknownBackendError, /not registered: missing/) do
      Movie::Persistence::BackendRegistry.build("missing", Movie::Config.empty)
    end
  end

  it "executes storage requests without a SQL driver and reconnects after connection loss" do
    backend = Movie::ProbeBackend.new
    system = Movie::ActorSystem(Movie::SystemMessage).new(Movie::Behaviors(Movie::SystemMessage).same)
    pool = system.spawn(Movie::Persistence::ConnectionPool.behavior(backend, 1))

    expect_raises(Movie::ProbeConnectionLost, "connection dropped") do
      system.ask(
        pool,
        Movie::Persistence::LoadEvents.new("probe-stream"),
        Array(Movie::Persistence::StoredEvent),
        1.second
      ).await(1.second)
    end

    events = system.ask(
      pool,
      Movie::Persistence::LoadEvents.new("probe-stream"),
      Array(Movie::Persistence::StoredEvent),
      1.second
    ).await(1.second)

    events.map(&.payload).should eq(["payload"])
    backend.connections.get.should eq(2)
  ensure
    system.try &.shutdown
  end
end
