require "../src/movie"

seed_system = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "cluster-seed")
seed_remote = seed_system.enable_remoting("127.0.0.1", 0, 1)
seed = seed_system.enable_cluster(Movie::Cluster::ClusterSettings.new(roles: ["seed"]))

worker_system = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "cluster-worker")
worker_system.enable_remoting("127.0.0.1", 0, 1)
worker = worker_system.enable_cluster(Movie::Cluster::ClusterSettings.new(
  seed_nodes: [seed_remote.address],
  roles: ["worker"],
  join_retry_interval: 50.milliseconds,
  gossip_interval: 50.milliseconds,
  heartbeat_interval: 100.milliseconds,
  heartbeat_timeout: 1.second
))

worker.await_up(5.seconds)
puts "members=#{worker.snapshot.members.size} leader=#{worker.snapshot.leader}"

worker.leave
worker.await_removed(5.seconds)
worker_system.shutdown
seed_system.shutdown
