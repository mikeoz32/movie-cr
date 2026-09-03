require "../../spec_helper"
require "../../../src/movie"

private struct ReceptionistPing
  include JSON::Serializable

  getter value : String

  def initialize(@value : String)
  end
end

private struct ReceptionistOtherPing
  include JSON::Serializable

  getter value : String

  def initialize(@value : String)
  end
end

private struct ReceptionistPong
  include JSON::Serializable

  getter node : String
  getter value : String

  def initialize(@node : String, @value : String)
  end
end

private class ReceptionistService < Movie::AbstractBehavior(ReceptionistPing)
  def initialize(@node : String)
  end

  def receive(
    message : ReceptionistPing,
    context : Movie::ActorContext(ReceptionistPing),
  )
    Movie::Ask.reply_if_asked(
      context.sender,
      ReceptionistPong.new(@node, message.value)
    )
    Movie::Behaviors(ReceptionistPing).same
  end
end

private class ReceptionistOtherService < Movie::AbstractBehavior(ReceptionistOtherPing)
  def receive(
    message : ReceptionistOtherPing,
    context : Movie::ActorContext(ReceptionistOtherPing),
  )
    Movie::Behaviors(ReceptionistOtherPing).same
  end
end

private class ReceptionistListingProbe < Movie::AbstractBehavior(
  Movie::Cluster::ReceptionistListing(ReceptionistPing),
)
  def initialize(@listings : Channel(Array(String)))
  end

  def receive(
    message : Movie::Cluster::ReceptionistListing(ReceptionistPing),
    context : Movie::ActorContext(Movie::Cluster::ReceptionistListing(ReceptionistPing)),
  )
    @listings.send(message.services.map(&.path.to_s))
    Movie::Behaviors(Movie::Cluster::ReceptionistListing(ReceptionistPing)).same
  end
end

private def wait_for_receptionist(
  timeout_span : Time::Span = 5.seconds,
  &condition : -> Bool
) : Nil
  deadline = Time.instant + timeout_span
  until condition.call
    raise "receptionist condition was not met within #{timeout_span}" if Time.instant >= deadline
    sleep 5.milliseconds
  end
end

private record ReceptionistClusterPair,
  seed_system : Movie::ActorSystem(Nil),
  peer_system : Movie::ActorSystem(Nil),
  seed_cluster : Movie::Cluster::ClusterExtension,
  peer_cluster : Movie::Cluster::ClusterExtension

private def start_receptionist_pair(
  seed_name : String,
  peer_name : String,
  cluster_name : String,
) : ReceptionistClusterPair
  seed_system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, name: seed_name)
  peer_system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, name: peer_name)
  begin
    seed_remote = seed_system.enable_remoting("127.0.0.1", 0)
    peer_system.enable_remoting("127.0.0.1", 0)
    settings = ->(seeds : Array(Movie::Address)) do
      Movie::Cluster::ClusterSettings.new(
        cluster_name: cluster_name,
        seed_nodes: seeds,
        join_retry_interval: 20.milliseconds,
        gossip_interval: 20.milliseconds,
        heartbeat_interval: 25.milliseconds,
        heartbeat_timeout: 500.milliseconds
      )
    end
    seed_cluster = seed_system.enable_cluster(settings.call([] of Movie::Address))
    peer_cluster = peer_system.enable_cluster(settings.call([seed_remote.address]))
    wait_for_receptionist do
      seed_cluster.snapshot.members.count(&.status.up?) == 2 &&
        peer_cluster.snapshot.members.count(&.status.up?) == 2 &&
        seed_cluster.converged? && peer_cluster.converged?
    end
    ReceptionistClusterPair.new(seed_system, peer_system, seed_cluster, peer_cluster)
  rescue error
    peer_system.shutdown
    seed_system.shutdown
    raise error
  end
end

describe Movie::ClusterReceptionistExtension do
  it "provides typed local discovery, subscriptions, and termination cleanup" do
    Movie::Remote::MessageRegistry.register(ReceptionistPing)
    Movie::Remote::MessageRegistry.register(ReceptionistPong)
    system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, name: "receptionist-local")
    begin
      system.enable_remoting("127.0.0.1", 0)
      system.enable_cluster
      receptionist = Movie::ClusterReceptionist.get(system)
      key = Movie::Cluster::ServiceKey(ReceptionistPing).new("workers")
      listings = Channel(Array(String)).new(4)
      subscriber = system.spawn(ReceptionistListingProbe.new(listings), name: "listing-probe")

      receptionist.subscribe(key, subscriber)
      listings.receive.should be_empty

      worker = system.spawn(ReceptionistService.new("local"), name: "worker")
      receptionist.register(key, worker).should be_true
      receptionist.register(key, worker).should be_false
      listings.receive.should eq([worker.path.not_nil!.to_s])

      listing = receptionist.find(key)
      listing.services.size.should eq(1)
      listed = listing.services
      listed.clear
      receptionist.find(key).services.size.should eq(1)
      reply = listing.services.first.ask(
        ReceptionistPing.new("ping"),
        ReceptionistPong,
        1.second
      ).await(1.second)
      reply.should eq(ReceptionistPong.new("local", "ping"))

      other_key = Movie::Cluster::ServiceKey(ReceptionistOtherPing).new("workers")
      other = system.spawn(ReceptionistOtherService.new, name: "other-worker")
      expect_raises(Movie::Cluster::ReceptionistConfigurationError) do
        receptionist.register(other_key, other)
      end

      worker.send_system(Movie::STOP)
      listings.receive.should be_empty
      receptionist.find(key).services.should be_empty
      receptionist.deregister(key, worker).should be_false
      receptionist.unsubscribe(key, subscriber).should be_true

      stats = receptionist.stats
      stats.registrations.should eq(1_i64)
      stats.deregistrations.should eq(1_i64)
      stats.local_registrations.should eq(0)
      stats.subscribers.should eq(0)
    ensure
      system.shutdown
    end
  end

  it "rejects remote state atomically without poisoning service-key types" do
    system = Movie::ActorSystem(Nil).new(
      Movie::Behaviors(Nil).same,
      name: "receptionist-atomic-state"
    )
    begin
      system.enable_remoting("127.0.0.1", 0)
      cluster = system.enable_cluster
      receptionist = Movie::ClusterReceptionist.get(system)
      conflicting_key = Movie::Cluster::ServiceKey(ReceptionistOtherPing).new("z-conflict")
      receptionist.find(conflicting_key).should be_empty

      owner = cluster.self_unique_address
      registrations = [
        Movie::Cluster::ServiceRegistration.new(
          "a-fresh",
          ReceptionistPing.name,
          Movie::ActorPath.new(owner.address, ["user", "fresh"]),
          owner
        ),
        Movie::Cluster::ServiceRegistration.new(
          "z-conflict",
          ReceptionistPing.name,
          Movie::ActorPath.new(owner.address, ["user", "conflict"]),
          owner
        ),
      ]
      rejected_before = receptionist.stats.protocol_rejections

      receptionist.handle_envelope(
        Movie::Cluster::ReceptionistEnvelope.new(
          cluster.settings.cluster_name,
          owner,
          1_i64,
          registrations
        ),
        Movie::ActorPath.new(
          owner.address,
          ["system", Movie::ClusterReceptionistExtension::DAEMON_NAME]
        ),
        owner.address,
        owner.node_uid
      )

      receptionist.stats.protocol_rejections.should eq(rejected_before + 1)
      receptionist.find(
        Movie::Cluster::ServiceKey(ReceptionistOtherPing).new("a-fresh")
      ).should be_empty
    ensure
      system.shutdown
    end
  end

  it "replicates reachable services and removes them after graceful leave" do
    Movie::Remote::MessageRegistry.register(ReceptionistPing)
    Movie::Remote::MessageRegistry.register(ReceptionistPong)
    pair = start_receptionist_pair(
      "receptionist-seed",
      "receptionist-peer",
      "receptionist-cluster"
    )
    begin
      seed_receptionist = Movie::ClusterReceptionist.get(pair.seed_system)
      peer_receptionist = Movie::ClusterReceptionist.get(pair.peer_system)
      key = Movie::Cluster::ServiceKey(ReceptionistPing).new("cluster-workers")
      worker = pair.seed_system.spawn(ReceptionistService.new("seed"), name: "worker")
      seed_receptionist.register(key, worker).should be_true

      wait_for_receptionist { peer_receptionist.find(key).services.size == 1 }
      service = peer_receptionist.find(key).services.first
      service.path.should eq(worker.path)
      reply = service.ask(
        ReceptionistPing.new("remote"),
        ReceptionistPong,
        2.seconds
      ).await(2.seconds)
      reply.should eq(ReceptionistPong.new("seed", "remote"))

      attacker = pair.peer_system.spawn(
        Movie::Behaviors(Movie::Cluster::ReceptionistEnvelope).same,
        name: "receptionist-attacker"
      )
      target = pair.peer_system.remote.not_nil!.actor_ref(
        Movie::ActorPath.new(
          pair.seed_cluster.self_unique_address.address,
          ["system", Movie::ClusterReceptionistExtension::DAEMON_NAME]
        ),
        Movie::Cluster::ReceptionistEnvelope
      )
      rejected_before = seed_receptionist.stats.protocol_rejections
      target.tell_from(attacker, Movie::Cluster::ReceptionistEnvelope.new(
        "receptionist-cluster",
        pair.peer_cluster.self_unique_address,
        1_i64,
        [] of Movie::Cluster::ServiceRegistration
      ))
      wait_for_receptionist do
        seed_receptionist.stats.protocol_rejections > rejected_before
      end

      pair.seed_cluster.leave.should be_true
      pair.seed_cluster.await_removed(5.seconds)
      wait_for_receptionist { peer_receptionist.find(key).services.empty? }
      peer_receptionist.stats.purged_nodes.should be >= 1_i64
    ensure
      pair.peer_system.shutdown
      pair.seed_system.shutdown
    end
  end

  it "rejects incompatible remote service-key state" do
    Movie::Remote::MessageRegistry.register(ReceptionistPing)
    Movie::Remote::MessageRegistry.register(ReceptionistOtherPing)
    pair = start_receptionist_pair(
      "receptionist-type-seed",
      "receptionist-type-peer",
      "receptionist-type-mismatch"
    )
    begin
      seed_receptionist = Movie::ClusterReceptionist.get(pair.seed_system)
      peer_receptionist = Movie::ClusterReceptionist.get(pair.peer_system)
      ping_key = Movie::Cluster::ServiceKey(ReceptionistPing).new("typed-workers")
      other_key = Movie::Cluster::ServiceKey(ReceptionistOtherPing).new("typed-workers")
      peer_receptionist.find(other_key).should be_empty
      worker = pair.seed_system.spawn(ReceptionistService.new("seed"), name: "typed-worker")
      rejected_before = peer_receptionist.stats.protocol_rejections

      seed_receptionist.register(ping_key, worker).should be_true
      wait_for_receptionist do
        peer_receptionist.stats.protocol_rejections > rejected_before
      end
      peer_receptionist.find(other_key).should be_empty
    ensure
      pair.peer_system.shutdown
      pair.seed_system.shutdown
    end
  end
end
