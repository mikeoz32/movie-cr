require "../src/movie"

struct GreetingRequest
  include JSON::Serializable

  getter name : String

  def initialize(@name : String)
  end
end

struct GreetingReply
  include JSON::Serializable

  getter message : String

  def initialize(@message : String)
  end
end

class GreetingEntity < Movie::AbstractBehavior(GreetingRequest)
  def initialize(@entity_id : String)
  end

  def receive(message : GreetingRequest, context : Movie::ActorContext(GreetingRequest))
    Movie::Ask.reply_if_asked(
      context.sender,
      GreetingReply.new("hello #{message.name} from #{@entity_id}")
    )
    Movie::Behaviors(GreetingRequest).same
  end
end

private def await_cluster(nodes : Array(Movie::Cluster::ClusterExtension)) : Nil
  deadline = Time.instant + 10.seconds
  until nodes.all? { |node| node.snapshot.members.count(&.status.up?) == nodes.size && node.converged? }
    raise "cluster did not converge" if Time.instant >= deadline
    sleep 20.milliseconds
  end
end

Movie::Remote::MessageRegistry.register(GreetingRequest)
Movie::Remote::MessageRegistry.register(GreetingReply)

seed_system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, name: "sharding-seed")
peer_system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, name: "sharding-peer")

begin
  seed_remote = seed_system.enable_remoting("127.0.0.1", 0)
  peer_system.enable_remoting("127.0.0.1", 0)
  seed_cluster = seed_system.enable_cluster(Movie::Cluster::ClusterSettings.new(
    cluster_name: "greetings",
    roles: ["backend"]
  ))
  peer_cluster = peer_system.enable_cluster(Movie::Cluster::ClusterSettings.new(
    cluster_name: "greetings",
    seed_nodes: [seed_remote.address],
    roles: ["backend"]
  ))
  await_cluster([seed_cluster, peer_cluster])

  allocation = Movie::Cluster::RoleAwareAllocation.new(
    Movie::Cluster::RendezvousAllocation.new,
    ["backend"]
  )
  rebalance = Movie::Cluster::RateLimitedRebalance.new(max_concurrent: 2)
  seed_sharding = Movie::ClusterSharding.get(seed_system)
  peer_sharding = Movie::ClusterSharding.get(peer_system)
  seed_type = seed_sharding.init(
    "Greeting",
    GreetingRequest,
    shard_count: 32,
    allocation: allocation,
    rebalance: rebalance,
    idle_timeout: 1.minute
  ) { |entity_id| GreetingEntity.new(entity_id) }
  peer_sharding.init(
    "Greeting",
    GreetingRequest,
    shard_count: 32,
    allocation: allocation,
    rebalance: rebalance,
    idle_timeout: 1.minute
  ) { |entity_id| GreetingEntity.new(entity_id) }

  greeting = seed_sharding.entity_ref_for(seed_type, "customer-42")
  reply = greeting.ask(GreetingRequest.new("Ada"), GreetingReply, 3.seconds).await(3.seconds)
  puts reply.message

  peer_cluster.leave
  peer_cluster.await_removed(10.seconds)
ensure
  peer_system.shutdown
  seed_system.shutdown
end
