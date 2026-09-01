require "../../spec_helper"
require "../../../src/movie"

private def wait_for_cluster(timeout_span : Time::Span = 3.seconds, &block : -> Bool) : Nil
  deadline = Time.instant + timeout_span
  until yield
    fail "cluster condition was not met within #{timeout_span}" if Time.instant >= deadline
    sleep 5.milliseconds
  end
end

private class RemoteStopOrderProbe < Movie::Extension
  def initialize(@events : Array(String), @mutex : Mutex)
  end

  def stop
    @mutex.synchronize { @events << "remoting" }
  end
end

private class ClusterStopOrderProbe < Movie::Extension
  def initialize(@events : Array(String), @mutex : Mutex)
  end

  def stop
    @mutex.synchronize { @events << "cluster" }
  end
end

describe Movie::ExtensionRegistry do
  it "stops dependent extensions in reverse registration order" do
    events = [] of String
    mutex = Mutex.new
    registry = Movie::ExtensionRegistry.new
    registry.register(RemoteStopOrderProbe.new(events, mutex))
    registry.register(ClusterStopOrderProbe.new(events, mutex))

    registry.stop_all
    events.should eq(["cluster", "remoting"])
  end
end

describe Movie::Cluster::ClusterExtension do
  it "requires remoting and starts one idempotent seed extension" do
    unbound = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "cluster-unbound")
    expect_raises(Movie::Cluster::ClusterConfigurationError, /remoting/) do
      unbound.enable_cluster
    end
    unbound.shutdown(1.second)

    system = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "cluster-seed")
    system.enable_remoting("127.0.0.1", 0, 1)
    settings = Movie::Cluster::ClusterSettings.new(roles: ["backend", "backend"])

    begin
      first = system.enable_cluster(settings)
      second = system.enable_cluster(settings)
      second.should be(first)
      system.cluster.should be(first)
      first.up?.should be_true
      first.self_member.status.up?.should be_true
      first.self_member.roles.should eq(["backend"])
      first.snapshot.leader.should eq(first.self_unique_address)

      daemon_path = Movie::ActorPath.new(system.address, ["system", Movie::Cluster::ClusterExtension::DAEMON_NAME])
      system.path_registry.resolve(daemon_path).should_not be_nil
    ensure
      system.shutdown(1.second)
    end
  end

  it "joins a static seed and converges an idempotent two-node membership" do
    seed_system = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "seed-node")
    seed_remote = seed_system.enable_remoting("127.0.0.1", 0, 1)
    seed = seed_system.enable_cluster(Movie::Cluster::ClusterSettings.new(roles: ["seed"]))

    joining_system = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "joining-node")
    joining_system.enable_remoting("127.0.0.1", 0, 1)
    joining = joining_system.enable_cluster(Movie::Cluster::ClusterSettings.new(
      seed_nodes: [seed_remote.address],
      roles: ["worker"],
      join_retry_interval: 20.milliseconds
    ))

    begin
      joining.await_up(3.seconds)
      wait_for_cluster do
        seed.snapshot.members.count(&.status.up?) == 2 &&
          joining.snapshot.members.count(&.status.up?) == 2
      end

      expected_leader = {seed.self_unique_address, joining.self_unique_address}.min
      seed.snapshot.leader.should eq(expected_leader)
      joining.snapshot.leader.should eq(expected_leader)
      joining.self_member.roles.should eq(["worker"])
      seed.self_member.roles.should eq(["seed"])

      joining.join(seed_remote.address)
      sleep 50.milliseconds
      seed.snapshot.members.count { |member| member.unique_address == joining.self_unique_address }.should eq(1)
    ensure
      joining_system.shutdown(1.second)
      seed_system.shutdown(1.second)
    end
  end

  it "gossips a transitive join to convergence across three nodes" do
    common = {
      join_retry_interval: 20.milliseconds,
      gossip_interval:     20.milliseconds,
      gossip_fanout:       3,
    }
    first_system = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "a-cluster-node")
    first_remote = first_system.enable_remoting("127.0.0.1", 0, 1)
    first = first_system.enable_cluster(Movie::Cluster::ClusterSettings.new(**common, roles: ["seed"]))

    second_system = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "b-cluster-node")
    second_remote = second_system.enable_remoting("127.0.0.1", 0, 1)
    second = second_system.enable_cluster(Movie::Cluster::ClusterSettings.new(
      **common,
      seed_nodes: [first_remote.address]
    ))
    second.await_up(3.seconds)

    third_system = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "c-cluster-node")
    third_system.enable_remoting("127.0.0.1", 0, 1)
    third = third_system.enable_cluster(Movie::Cluster::ClusterSettings.new(
      **common,
      seed_nodes: [second_remote.address]
    ))

    begin
      third.await_up(3.seconds)
      wait_for_cluster(5.seconds) do
        [first, second, third].all? do |cluster|
          cluster.snapshot.members.count(&.status.up?) == 3 && cluster.converged?
        end
      end

      expected_leader = [first, second, third].map(&.self_unique_address).min
      [first, second, third].each do |cluster|
        cluster.snapshot.leader.should eq(expected_leader)
        cluster.stats.gossip_rounds.should be > 0
        cluster.stats.gossip_received.should be > 0
      end
    ensure
      third_system.shutdown(1.second)
      second_system.shutdown(1.second)
      first_system.shutdown(1.second)
    end
  end

  it "converges a graceful leave without downing another member" do
    seed_system = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "a-leave-seed")
    seed_remote = seed_system.enable_remoting("127.0.0.1", 0, 1)
    settings = Movie::Cluster::ClusterSettings.new(
      join_retry_interval: 20.milliseconds,
      gossip_interval: 20.milliseconds,
      gossip_fanout: 2,
      heartbeat_interval: 20.milliseconds,
      heartbeat_timeout: 120.milliseconds
    )
    seed = seed_system.enable_cluster(settings)

    leaving_system = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "b-leaving-node")
    leaving_system.enable_remoting("127.0.0.1", 0, 1)
    leaving = leaving_system.enable_cluster(Movie::Cluster::ClusterSettings.new(
      seed_nodes: [seed_remote.address],
      join_retry_interval: 20.milliseconds,
      gossip_interval: 20.milliseconds,
      gossip_fanout: 2,
      heartbeat_interval: 20.milliseconds,
      heartbeat_timeout: 120.milliseconds
    ))

    begin
      leaving.await_up(3.seconds)
      wait_for_cluster { seed.snapshot.members.count(&.status.up?) == 2 }
      expect_raises(Movie::Cluster::ClusterConfigurationError, /shared secret/) do
        leaving.down(seed.self_unique_address)
      end
      leaving.leave.should be_true
      wait_for_cluster(3.seconds) do
        seed.snapshot.member(leaving.self_unique_address).nil? &&
          leaving.snapshot.member(leaving.self_unique_address).nil?
      end
      seed.snapshot.members.should eq([seed.self_member])
    ensure
      leaving_system.shutdown(1.second)
      seed_system.shutdown(1.second)
    end
  end

  it "keeps an unreachable member until manual down and accepts a new UID at the same address" do
    seed_system = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "a-down-seed")
    seed_remote = seed_system.enable_remoting("127.0.0.1", 0, 1)
    seed = seed_system.enable_cluster(Movie::Cluster::ClusterSettings.new(
      join_retry_interval: 20.milliseconds,
      gossip_interval: 20.milliseconds,
      heartbeat_interval: 20.milliseconds,
      heartbeat_timeout: 100.milliseconds
    ))

    failed_system = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "b-restarted-node")
    failed_remote = failed_system.enable_remoting("127.0.0.1", 0, 1)
    failed_port = failed_remote.local_port
    failed = failed_system.enable_cluster(Movie::Cluster::ClusterSettings.new(
      seed_nodes: [seed_remote.address],
      join_retry_interval: 20.milliseconds,
      gossip_interval: 20.milliseconds,
      heartbeat_interval: 20.milliseconds,
      heartbeat_timeout: 100.milliseconds
    ))
    restarted_system = nil.as(Movie::ActorSystem(String)?)

    begin
      failed.await_up(3.seconds)
      wait_for_cluster { seed.snapshot.members.count(&.status.up?) == 2 }
      old_incarnation = failed.self_unique_address
      failed_system.shutdown(1.second)

      wait_for_cluster(3.seconds) { seed.snapshot.unreachable.includes?(old_incarnation) }
      seed.snapshot.member(old_incarnation).not_nil!.status.up?.should be_true
      sleep 150.milliseconds
      seed.snapshot.member(old_incarnation).not_nil!.status.up?.should be_true

      seed.down(old_incarnation).should be_true
      wait_for_cluster(3.seconds) { seed.snapshot.member(old_incarnation).nil? }

      restarted_system = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "b-restarted-node")
      restarted_system.enable_remoting("127.0.0.1", failed_port, 1)
      restarted = restarted_system.enable_cluster(Movie::Cluster::ClusterSettings.new(
        seed_nodes: [seed_remote.address],
        join_retry_interval: 20.milliseconds,
        gossip_interval: 20.milliseconds,
        heartbeat_interval: 20.milliseconds,
        heartbeat_timeout: 100.milliseconds
      ))
      restarted.await_up(3.seconds)
      restarted.self_unique_address.should_not eq(old_incarnation)
      wait_for_cluster(3.seconds) { seed.snapshot.members.count(&.status.up?) == 2 }
      seed.snapshot.member(restarted.self_unique_address).not_nil!.status.up?.should be_true
      seed.stats.heartbeat_timeouts.should be > 0
    ensure
      restarted_system.try &.shutdown(1.second)
      failed_system.shutdown(1.second) rescue nil
      seed_system.shutdown(1.second)
    end
  end
end
