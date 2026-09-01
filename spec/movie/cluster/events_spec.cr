require "../../spec_helper"
require "../../../src/movie"

private class ClusterEventProbe < Movie::AbstractBehavior(Movie::Cluster::ClusterEvent)
  def initialize(@events : Channel(Movie::Cluster::ClusterEvent))
  end

  def receive(message : Movie::Cluster::ClusterEvent, context : Movie::ActorContext(Movie::Cluster::ClusterEvent))
    @events.send(message)
    Movie::Behaviors(Movie::Cluster::ClusterEvent).same
  end
end

private def wait_for_cluster_event_condition(timeout_span : Time::Span = 3.seconds, &block : -> Bool) : Nil
  deadline = Time.instant + timeout_span
  until yield
    fail "cluster event condition was not met within #{timeout_span}" if Time.instant >= deadline
    sleep 5.milliseconds
  end
end

private def drain_cluster_events(channel : Channel(Movie::Cluster::ClusterEvent)) : Array(Movie::Cluster::ClusterEvent)
  events = [] of Movie::Cluster::ClusterEvent
  loop do
    select
    when event = channel.receive
      events << event
    when timeout(100.milliseconds)
      return events
    end
  end
end

describe Movie::Cluster::ClusterEvent do
  it "publishes initial state, member transitions, and leader changes without gossip duplicates" do
    seed_system = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "z-event-seed")
    seed_remote = seed_system.enable_remoting("127.0.0.1", 0, 1)
    seed = seed_system.enable_cluster(Movie::Cluster::ClusterSettings.new(
      join_retry_interval: 20.milliseconds,
      gossip_interval: 20.milliseconds,
      heartbeat_interval: 20.milliseconds,
      heartbeat_timeout: 200.milliseconds
    ))
    events = Channel(Movie::Cluster::ClusterEvent).new(32)
    subscriber = seed_system.spawn(ClusterEventProbe.new(events), name: "cluster-events")
    seed.subscribe(subscriber)

    joining_system = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "a-event-joiner")
    joining_system.enable_remoting("127.0.0.1", 0, 1)
    joining = joining_system.enable_cluster(Movie::Cluster::ClusterSettings.new(
      seed_nodes: [seed_remote.address],
      join_retry_interval: 20.milliseconds,
      gossip_interval: 20.milliseconds,
      heartbeat_interval: 20.milliseconds,
      heartbeat_timeout: 200.milliseconds
    ))

    begin
      joining.await_up(3.seconds)
      wait_for_cluster_event_condition { seed.snapshot.members.count(&.status.up?) == 2 }
      joining.leave.should be_true
      wait_for_cluster_event_condition { seed.snapshot.member(joining.self_unique_address).nil? }
      sleep 100.milliseconds

      received = drain_cluster_events(events)
      kinds = received.map(&.kind)
      kinds.should contain(Movie::Cluster::ClusterEvent::Kind::CurrentState)
      kinds.should contain(Movie::Cluster::ClusterEvent::Kind::MemberJoined)
      kinds.should contain(Movie::Cluster::ClusterEvent::Kind::MemberUp)
      kinds.should contain(Movie::Cluster::ClusterEvent::Kind::MemberLeaving)
      kinds.should contain(Movie::Cluster::ClusterEvent::Kind::MemberExiting)
      kinds.should contain(Movie::Cluster::ClusterEvent::Kind::MemberRemoved)
      kinds.count(Movie::Cluster::ClusterEvent::Kind::MemberJoined).should eq(1)
      kinds.count(Movie::Cluster::ClusterEvent::Kind::MemberUp).should eq(1)
      received.select(&.kind.leader_changed?).any? do |event|
        event.leader == joining.self_unique_address
      end.should be_true
      received.first.snapshot.not_nil!.members.size.should eq(1)
      seed.stats.subscribers.should eq(1)

      seed.unsubscribe(subscriber)
      seed.stats.subscribers.should eq(0)
    ensure
      joining_system.shutdown(1.second)
      seed_system.shutdown(1.second)
    end
  end

  it "publishes reachability changes without removing the member" do
    seed_system = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "a-reachability-seed")
    seed_remote = seed_system.enable_remoting("127.0.0.1", 0, 1)
    seed = seed_system.enable_cluster(Movie::Cluster::ClusterSettings.new(
      join_retry_interval: 20.milliseconds,
      gossip_interval: 20.milliseconds,
      heartbeat_interval: 20.milliseconds,
      heartbeat_timeout: 100.milliseconds
    ))
    events = Channel(Movie::Cluster::ClusterEvent).new(32)
    subscriber = seed_system.spawn(ClusterEventProbe.new(events), name: "reachability-events")
    seed.subscribe(subscriber)

    peer_system = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "b-reachability-peer")
    peer_system.enable_remoting("127.0.0.1", 0, 1)
    peer = peer_system.enable_cluster(Movie::Cluster::ClusterSettings.new(
      seed_nodes: [seed_remote.address],
      join_retry_interval: 20.milliseconds,
      gossip_interval: 20.milliseconds,
      heartbeat_interval: 20.milliseconds,
      heartbeat_timeout: 100.milliseconds
    ))

    begin
      peer.await_up(3.seconds)
      wait_for_cluster_event_condition { seed.snapshot.members.count(&.status.up?) == 2 }
      peer_uid = peer.self_unique_address
      drain_cluster_events(events)
      peer_system.shutdown(1.second)

      wait_for_cluster_event_condition { seed.snapshot.unreachable.includes?(peer_uid) }
      received = drain_cluster_events(events)
      unreachable = received.find(&.kind.unreachable_member?)
      unreachable.should_not be_nil
      unreachable.not_nil!.member.not_nil!.unique_address.should eq(peer_uid)
      seed.snapshot.member(peer_uid).not_nil!.status.up?.should be_true
    ensure
      peer_system.shutdown(1.second) rescue nil
      seed_system.shutdown(1.second)
    end
  end
end
