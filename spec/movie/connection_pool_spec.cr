require "../spec_helper"
require "../../src/movie"
require "../../src/movie/persistence"

module Movie
  struct PoolProbeStart
  end

  alias PoolProbeMessage = PoolProbeStart

  class FakeConnection < AbstractBehavior(Persistence::ConnectionMessage)
    def initialize(@id : String)
    end

    def receive(message, ctx)
      case message
      when Persistence::DbQueryString
        Movie::Ask.reply_if_asked(ctx.sender, @id.as(String?))
      end
      Behaviors(Persistence::ConnectionMessage).same
    end
  end

  class PoolProbe < AbstractBehavior(PoolProbeMessage)
    def initialize(
      @pool : ActorRef(Persistence::ConnectionMessage),
      @promise : Promise(Array(String?))
    )
    end

    def receive(message, ctx)
      case message
      when PoolProbeStart
        first = ctx.ask(@pool, Persistence::DbQueryString.new("SELECT 1"), String?).await(1.second)
        second = ctx.ask(@pool, Persistence::DbQueryString.new("SELECT 1"), String?).await(1.second)
        @promise.try_success([first, second])
      end
      Behaviors(PoolProbeMessage).same
    end
  end
end

describe Movie::Persistence::ConnectionPool do
  it "routes queries in round-robin order" do
    system = Movie::ActorSystem(Movie::SystemMessage).new(Movie::Behaviors(Movie::SystemMessage).same)
    c1 = system.spawn(Movie::FakeConnection.new("c1"))
    c2 = system.spawn(Movie::FakeConnection.new("c2"))

    pool = system.spawn(Movie::Persistence::ConnectionPool.new([c1, c2]))

    promise = Movie::Promise(Array(String?)).new
    probe = system.spawn(Movie::PoolProbe.new(pool, promise))
    probe << Movie::PoolProbeStart.new
    results = promise.future.await(2.seconds)

    results[0].should eq("c1")
    results[1].should eq("c2")
  end
end
