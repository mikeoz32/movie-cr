require "../../spec_helper"
require "../../../src/movie"

private struct SingletonPing
  include JSON::Serializable

  getter value : String

  def initialize(@value : String)
  end
end

private struct SingletonPong
  include JSON::Serializable

  getter value : String

  def initialize(@value : String)
  end
end

private struct SingletonOtherPing
  include JSON::Serializable

  getter value : String

  def initialize(@value : String)
  end
end

private class SingletonOtherProbe < Movie::AbstractBehavior(SingletonOtherPing)
  def receive(
    message : SingletonOtherPing,
    context : Movie::ActorContext(SingletonOtherPing),
  )
    Movie::Ask.reply_if_asked(context.sender, SingletonPong.new(message.value))
    Movie::Behaviors(SingletonOtherPing).same
  end
end

private class SingletonProbe < Movie::AbstractBehavior(SingletonPing)
  def initialize(
    @node : String,
    @started : Channel(String),
    @deliveries : Channel(String),
    @blocked : Channel(Nil)? = nil,
    @release : Channel(Nil)? = nil,
    @stopped : Channel(String)? = nil,
  )
  end

  def on_signal(signal : Movie::SystemMessage)
    @started.send(@node) if signal.is_a?(Movie::PreStart)
    @stopped.try &.send(@node) if signal.is_a?(Movie::PostStop)
  end

  def receive(message : SingletonPing, context : Movie::ActorContext(SingletonPing))
    value = "#{@node}:#{message.value}"
    @deliveries.send(value)
    if message.value == "block"
      @blocked.try &.send(nil)
      @release.try &.receive
    end
    Movie::Ask.reply_if_asked(context.sender, SingletonPong.new(value))
    Movie::Behaviors(SingletonPing).same
  end
end

describe Movie::ClusterSingletonExtension do
  it "places a singleton only on a required role and routes from another node" do
    Movie::Remote::MessageRegistry.register(SingletonPing)
    Movie::Remote::MessageRegistry.register(SingletonPong)
    seed_system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, name: "singleton-role-seed")
    worker_system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, name: "singleton-role-worker")
    begin
      seed_remote = seed_system.enable_remoting("127.0.0.1", 0)
      worker_system.enable_remoting("127.0.0.1", 0)
      settings = ->(seeds : Array(Movie::Address), roles : Array(String)) do
        Movie::Cluster::ClusterSettings.new(
          cluster_name: "singleton-role",
          seed_nodes: seeds,
          roles: roles,
          join_retry_interval: 20.milliseconds,
          gossip_interval: 20.milliseconds,
          heartbeat_interval: 25.milliseconds,
          heartbeat_timeout: 500.milliseconds
        )
      end
      seed_cluster = seed_system.enable_cluster(settings.call([] of Movie::Address, ["frontend"]))
      worker_cluster = worker_system.enable_cluster(settings.call([seed_remote.address], ["singleton"]))
      wait_for_singleton do
        seed_cluster.snapshot.members.count(&.status.up?) == 2 &&
          worker_cluster.snapshot.members.count(&.status.up?) == 2 &&
          seed_cluster.converged? && worker_cluster.converged?
      end

      started = Channel(String).new(2)
      deliveries = Channel(String).new(1)
      seed_singleton = Movie::ClusterSingleton.get(seed_system)
      worker_singleton = Movie::ClusterSingleton.get(worker_system)
      seed_ref = seed_singleton.init("role-jobs", SingletonPing, roles: ["singleton"]) do
        SingletonProbe.new("seed", started, deliveries)
      end
      worker_singleton.init("role-jobs", SingletonPing, roles: ["singleton"]) do
        SingletonProbe.new("worker", started, deliveries)
      end

      started.receive.should eq("worker")
      wait_for_singleton { seed_ref.owner == worker_cluster.self_unique_address }
      seed_ref.locally_owned?.should be_false
      response = seed_ref.ask(SingletonPing.new("remote"), SingletonPong, 2.seconds).await(2.seconds)
      response.value.should eq("worker:remote")
      deliveries.receive.should eq("worker:remote")
    ensure
      worker_system.shutdown
      seed_system.shutdown
    end
  end

  it "rejects a cross-node singleton message type mismatch" do
    Movie::Remote::MessageRegistry.register(SingletonPing)
    Movie::Remote::MessageRegistry.register(SingletonOtherPing)
    Movie::Remote::MessageRegistry.register(SingletonPong)
    seed_system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, name: "a-singleton-type-seed")
    peer_system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, name: "z-singleton-type-peer")
    begin
      seed_remote = seed_system.enable_remoting("127.0.0.1", 0)
      peer_system.enable_remoting("127.0.0.1", 0)
      settings = ->(seeds : Array(Movie::Address), roles : Array(String)) do
        Movie::Cluster::ClusterSettings.new(
          cluster_name: "singleton-type-mismatch",
          seed_nodes: seeds,
          roles: roles,
          join_retry_interval: 20.milliseconds,
          gossip_interval: 20.milliseconds,
          heartbeat_interval: 25.milliseconds,
          heartbeat_timeout: 500.milliseconds
        )
      end
      seed_cluster = seed_system.enable_cluster(settings.call([] of Movie::Address, ["singleton"]))
      peer_cluster = peer_system.enable_cluster(settings.call([seed_remote.address], ["proxy"]))
      wait_for_singleton do
        seed_cluster.snapshot.members.count(&.status.up?) == 2 &&
          peer_cluster.snapshot.members.count(&.status.up?) == 2 &&
          seed_cluster.converged? && peer_cluster.converged?
      end

      started = Channel(String).new(1)
      deliveries = Channel(String).new(1)
      seed_ref = Movie::ClusterSingleton.get(seed_system).init(
        "typed-service",
        SingletonPing,
        roles: ["singleton"]
      ) do
        SingletonProbe.new("seed", started, deliveries)
      end
      wait_for_singleton { !seed_ref.owner.nil? }
      peer_singleton = Movie::ClusterSingleton.get(peer_system)
      peer_ref = peer_singleton.init(
        "typed-service",
        SingletonOtherPing,
        roles: ["singleton"]
      ) do
        SingletonOtherProbe.new
      end

      error = expect_raises(Movie::Remote::RemoteAskError) do
        peer_ref.ask(
          SingletonOtherPing.new("wrong-type"),
          SingletonPong,
          1.second
        ).await(1.second)
      end
      error.remote_class.should eq(Movie::Cluster::ClusterShardingConfigurationError.name)
      wait_for_singleton { peer_singleton.stats.routing_rejections == 1_i64 }
    ensure
      peer_system.shutdown
      seed_system.shutdown
    end
  end

  it "drains the old singleton and preserves buffered order on graceful handoff" do
    Movie::Remote::MessageRegistry.register(SingletonPing)
    Movie::Remote::MessageRegistry.register(SingletonPong)
    seed_system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, name: "a-singleton-handoff")
    peer_system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, name: "z-singleton-handoff")
    released = false
    release = Channel(Nil).new(1)
    begin
      seed_remote = seed_system.enable_remoting("127.0.0.1", 0)
      peer_system.enable_remoting("127.0.0.1", 0)
      settings = ->(seeds : Array(Movie::Address)) do
        Movie::Cluster::ClusterSettings.new(
          cluster_name: "singleton-handoff",
          seed_nodes: seeds,
          join_retry_interval: 20.milliseconds,
          gossip_interval: 20.milliseconds,
          heartbeat_interval: 25.milliseconds,
          heartbeat_timeout: 500.milliseconds
        )
      end
      seed_cluster = seed_system.enable_cluster(settings.call([] of Movie::Address))
      peer_cluster = peer_system.enable_cluster(settings.call([seed_remote.address]))
      wait_for_singleton do
        seed_cluster.snapshot.members.count(&.status.up?) == 2 &&
          peer_cluster.snapshot.members.count(&.status.up?) == 2 &&
          seed_cluster.converged? && peer_cluster.converged?
      end

      started = Channel(String).new(4)
      deliveries = Channel(String).new(16)
      blocked = Channel(Nil).new(1)
      seed_singleton = Movie::ClusterSingleton.get(seed_system)
      peer_singleton = Movie::ClusterSingleton.get(peer_system)
      seed_ref = seed_singleton.init("handoff", SingletonPing) do
        SingletonProbe.new("seed", started, deliveries, blocked, release)
      end
      peer_ref = peer_singleton.init("handoff", SingletonPing) do
        SingletonProbe.new("peer", started, deliveries)
      end

      started.receive.should eq("seed")
      seed_ref << SingletonPing.new("block")
      deliveries.receive.should eq("seed:block")
      blocked.receive
      seed_cluster.leave.should be_true
      handoff_deadline = Time.instant + 5.seconds
      until seed_singleton.handoff_in_progress?("handoff") ||
            peer_singleton.handoff_in_progress?("handoff")
        if Time.instant >= handoff_deadline
          fail(
            "singleton handoff did not start; " \
            "seed_owner=#{seed_ref.owner}; peer_owner=#{peer_ref.owner}; " \
            "seed_members=#{seed_cluster.snapshot.members}; " \
            "peer_members=#{peer_cluster.snapshot.members}; " \
            "seed_handoffs=#{Movie::ClusterSharding.get(seed_system).handoffs_in_progress}; " \
            "peer_handoffs=#{Movie::ClusterSharding.get(peer_system).handoffs_in_progress}"
          )
        end
        sleep 5.milliseconds
      end
      10.times { |index| peer_ref << SingletonPing.new(index.to_s) }
      release.send(nil)
      released = true

      started.receive.should eq("peer")
      received = Array(String).new(10) { deliveries.receive }
      received.should eq(Array(String).new(10) { |index| "peer:#{index}" })
      wait_for_singleton { peer_ref.owner == peer_cluster.self_unique_address }
      peer_singleton.stats.tells.should eq(10_i64)
      peer_singleton.stats.shared_sharding_handoffs_completed.should be >= 1_i64
      seed_cluster.await_removed(5.seconds)
    ensure
      release.send(nil) unless released
      peer_system.shutdown
      seed_system.shutdown
    end
  end

  it "stops the singleton when the final eligible role leaves" do
    Movie::Remote::MessageRegistry.register(SingletonPing)
    seed_system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, name: "singleton-ineligible-seed")
    worker_system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, name: "singleton-ineligible-worker")
    begin
      seed_remote = seed_system.enable_remoting("127.0.0.1", 0)
      worker_system.enable_remoting("127.0.0.1", 0)
      settings = ->(seeds : Array(Movie::Address), roles : Array(String)) do
        Movie::Cluster::ClusterSettings.new(
          cluster_name: "singleton-final-role",
          seed_nodes: seeds,
          roles: roles,
          join_retry_interval: 20.milliseconds,
          gossip_interval: 20.milliseconds,
          heartbeat_interval: 25.milliseconds,
          heartbeat_timeout: 500.milliseconds
        )
      end
      seed_cluster = seed_system.enable_cluster(settings.call([] of Movie::Address, ["frontend"]))
      worker_cluster = worker_system.enable_cluster(settings.call([seed_remote.address], ["singleton"]))
      wait_for_singleton do
        seed_cluster.snapshot.members.count(&.status.up?) == 2 &&
          worker_cluster.snapshot.members.count(&.status.up?) == 2 &&
          seed_cluster.converged? && worker_cluster.converged?
      end

      started = Channel(String).new(2)
      stopped = Channel(String).new(1)
      deliveries = Channel(String).new(1)
      seed_singleton = Movie::ClusterSingleton.get(seed_system)
      worker_singleton = Movie::ClusterSingleton.get(worker_system)
      seed_ref = seed_singleton.init("role-exhausted", SingletonPing, roles: ["singleton"]) do
        SingletonProbe.new("seed", started, deliveries)
      end
      worker_singleton.init("role-exhausted", SingletonPing, roles: ["singleton"]) do
        SingletonProbe.new("worker", started, deliveries, stopped: stopped)
      end

      started.receive.should eq("worker")
      worker_cluster.leave.should be_true
      worker_cluster.await_removed(5.seconds)
      stopped.receive.should eq("worker")
      wait_for_singleton { seed_ref.owner.nil? }
      rejected_before = seed_singleton.stats.shared_sharding_rejected_envelopes
      expect_raises(Movie::Cluster::NoShardOwnerError) do
        seed_ref.ask(
          SingletonPing.new("unowned"),
          SingletonPong,
          1.second
        ).await(1.second)
      end
      wait_for_singleton do
        stats = seed_singleton.stats
        stats.routing_rejections == 1_i64 &&
          stats.shared_sharding_rejected_envelopes > rejected_before
      end
    ensure
      worker_system.shutdown
      seed_system.shutdown
    end
  end
end

private def wait_for_singleton(
  timeout_span : Time::Span = 5.seconds,
  &condition : -> Bool
) : Nil
  deadline = Time.instant + timeout_span
  until condition.call
    raise "singleton condition was not met within #{timeout_span}" if Time.instant >= deadline
    sleep 5.milliseconds
  end
end

describe Movie::ClusterSingletonExtension do
  it "eagerly activates one local singleton and routes through its typed proxy" do
    Movie::Remote::MessageRegistry.register(SingletonPing)
    Movie::Remote::MessageRegistry.register(SingletonPong)
    system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, name: "singleton-local")
    begin
      system.enable_remoting("127.0.0.1", 0)
      cluster = system.enable_cluster
      singleton = Movie::ClusterSingleton.get(system)
      started = Channel(String).new(2)
      stopped = Channel(String).new(2)
      deliveries = Channel(String).new(2)
      ref = singleton.init(
        "jobs",
        SingletonPing,
        activation_interval: 20.milliseconds,
        activation_timeout: 1.second
      ) { SingletonProbe.new("local", started, deliveries, stopped: stopped) }

      same_ref = singleton.init(
        "jobs",
        SingletonPing,
        activation_interval: 20.milliseconds,
        activation_timeout: 1.second
      ) { SingletonProbe.new("unused", started, deliveries) }
      same_ref.same?(ref).should be_true
      expect_raises(Movie::Cluster::ClusterSingletonConfigurationError) do
        singleton.init(
          "jobs",
          SingletonPing,
          roles: ["other"],
          activation_interval: 20.milliseconds,
          activation_timeout: 1.second
        ) { SingletonProbe.new("unused", started, deliveries) }
      end
      expect_raises(ArgumentError) do
        singleton.init("", SingletonPing) { SingletonProbe.new("unused", started, deliveries) }
      end

      started.receive.should eq("local")
      ref.owner.should eq(cluster.self_unique_address)
      response = ref.ask(SingletonPing.new("ping"), SingletonPong, 1.second).await(1.second)
      response.value.should eq("local:ping")
      deliveries.receive.should eq("local:ping")

      ref.send_system(Movie::STOP)
      stopped.receive.should eq("local")
      started.receive.should eq("local")
      second = ref.ask(SingletonPing.new("again"), SingletonPong, 1.second).await(1.second)
      second.value.should eq("local:again")
      deliveries.receive.should eq("local:again")
      wait_for_singleton { singleton.stats.reactivations == 1_i64 }
      stats = singleton.stats
      stats.registrations.should eq(1_i64)
      stats.activations.should eq(1_i64)
      stats.asks.should eq(2_i64)
      stats.stop_requests.should eq(1_i64)
      stats.activation_failures.should eq(0_i64)
    ensure
      system.shutdown
    end
  end
end
