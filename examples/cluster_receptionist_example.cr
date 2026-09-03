require "../src/movie"

struct Work
  include JSON::Serializable

  getter value : String

  def initialize(@value : String)
  end
end

struct WorkReply
  include JSON::Serializable

  getter value : String

  def initialize(@value : String)
  end
end

class Worker < Movie::AbstractBehavior(Work)
  def receive(message : Work, context : Movie::ActorContext(Work))
    Movie::Ask.reply_if_asked(context.sender, WorkReply.new("worker:#{message.value}"))
    Movie::Behaviors(Work).same
  end
end

class WorkerListings < Movie::AbstractBehavior(Movie::Cluster::ReceptionistListing(Work))
  def receive(
    message : Movie::Cluster::ReceptionistListing(Work),
    context : Movie::ActorContext(Movie::Cluster::ReceptionistListing(Work)),
  )
    puts "discovered workers: #{message.services.size}"
    Movie::Behaviors(Movie::Cluster::ReceptionistListing(Work)).same
  end
end

def wait_until(timeout_span : Time::Span = 5.seconds, &condition : -> Bool) : Nil
  deadline = Time.instant + timeout_span
  until condition.call
    raise "condition not met within #{timeout_span}" if Time.instant >= deadline
    sleep 10.milliseconds
  end
end

Movie::Remote::MessageRegistry.register(Work)
Movie::Remote::MessageRegistry.register(WorkReply)
seed = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, name: "receptionist-seed")
peer = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, name: "receptionist-peer")

begin
  seed_remote = seed.enable_remoting("127.0.0.1", 0, 1)
  peer.enable_remoting("127.0.0.1", 0, 1)
  settings = ->(seeds : Array(Movie::Address)) do
    Movie::Cluster::ClusterSettings.new(
      cluster_name: "receptionist-example",
      seed_nodes: seeds,
      join_retry_interval: 20.milliseconds,
      gossip_interval: 20.milliseconds,
      heartbeat_interval: 25.milliseconds,
      heartbeat_timeout: 500.milliseconds
    )
  end
  seed_cluster = seed.enable_cluster(settings.call([] of Movie::Address))
  peer_cluster = peer.enable_cluster(settings.call([seed_remote.address]))
  wait_until do
    seed_cluster.snapshot.members.count(&.status.up?) == 2 &&
      peer_cluster.snapshot.members.count(&.status.up?) == 2 &&
      seed_cluster.converged? && peer_cluster.converged?
  end

  key = Movie::Cluster::ServiceKey(Work).new("workers")
  seed_receptionist = Movie::ClusterReceptionist.get(seed)
  peer_receptionist = Movie::ClusterReceptionist.get(peer)
  observer = peer.spawn(WorkerListings.new, name: "worker-listings")
  peer_receptionist.subscribe(key, observer)
  worker = seed.spawn(Worker.new, name: "worker-1")
  seed_receptionist.register(key, worker)

  wait_until { peer_receptionist.find(key).services.size == 1 }
  service = peer_receptionist.find(key).services.first
  response = service.ask(Work.new("job-42"), WorkReply, 2.seconds).await(2.seconds)
  puts response.value

  worker.send_system(Movie::STOP)
  wait_until { peer_receptionist.find(key).empty? }

  peer_cluster.leave
  peer_cluster.await_removed
  seed_cluster.leave
  seed_cluster.await_removed
ensure
  peer.shutdown
  seed.shutdown
end
