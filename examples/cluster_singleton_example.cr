require "../src/movie"

struct SingletonRequest
  include JSON::Serializable

  getter value : String

  def initialize(@value : String)
  end
end

struct SingletonReply
  include JSON::Serializable

  getter value : String

  def initialize(@value : String)
  end
end

class SingletonService < Movie::AbstractBehavior(SingletonRequest)
  def initialize(@node : String, @started : Channel(String))
  end

  def on_signal(signal : Movie::SystemMessage)
    @started.send(@node) if signal.is_a?(Movie::PreStart)
  end

  def receive(message : SingletonRequest, context : Movie::ActorContext(SingletonRequest))
    Movie::Ask.reply_if_asked(
      context.sender,
      SingletonReply.new("#{@node}:#{message.value}")
    )
    Movie::Behaviors(SingletonRequest).same
  end
end

private def await_singleton_cluster(nodes : Array(Movie::Cluster::ClusterExtension)) : Nil
  deadline = Time.instant + 10.seconds
  until nodes.all? { |node| node.snapshot.members.count(&.status.up?) == nodes.size && node.converged? }
    raise "cluster did not converge" if Time.instant >= deadline
    sleep 20.milliseconds
  end
end

Movie::Remote::MessageRegistry.register(SingletonRequest)
Movie::Remote::MessageRegistry.register(SingletonReply)

seed_system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, name: "a-singleton-seed")
peer_system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, name: "b-singleton-peer")

begin
  seed_remote = seed_system.enable_remoting("127.0.0.1", 0, 1)
  peer_system.enable_remoting("127.0.0.1", 0, 1)
  seed_cluster = seed_system.enable_cluster(Movie::Cluster::ClusterSettings.new(
    cluster_name: "singleton-example"
  ))
  peer_cluster = peer_system.enable_cluster(Movie::Cluster::ClusterSettings.new(
    cluster_name: "singleton-example",
    seed_nodes: [seed_remote.address]
  ))
  await_singleton_cluster([seed_cluster, peer_cluster])

  started = Channel(String).new(2)
  seed_singleton = Movie::ClusterSingleton.get(seed_system)
  peer_singleton = Movie::ClusterSingleton.get(peer_system)
  seed_ref = seed_singleton.init("service", SingletonRequest) do
    SingletonService.new("seed", started)
  end
  peer_ref = peer_singleton.init("service", SingletonRequest) do
    SingletonService.new("peer", started)
  end

  puts "eager owner: #{started.receive}"
  first = peer_ref.ask(SingletonRequest.new("remote"), SingletonReply, 3.seconds).await(3.seconds)
  puts first.value

  seed_cluster.leave
  seed_cluster.await_removed(10.seconds)
  puts "handoff owner: #{started.receive}"
  second = peer_ref.ask(SingletonRequest.new("after-handoff"), SingletonReply, 3.seconds).await(3.seconds)
  puts second.value
  puts "stats: #{peer_singleton.stats}"
ensure
  peer_system.shutdown
  seed_system.shutdown
end
