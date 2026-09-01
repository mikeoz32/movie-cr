require "../../spec_helper"
require "../../../src/movie"

CLUSTER_BENCH_ENABLED = ENV["MOVIE_CLUSTER_BENCH"]? == "1"

private def wait_for_cluster_benchmark(timeout_span : Time::Span = 10.seconds, &block : -> Bool) : Nil
  deadline = Time.instant + timeout_span
  until yield
    raise "cluster benchmark did not converge within #{timeout_span}" if Time.instant >= deadline
    sleep 5.milliseconds
  end
end

if CLUSTER_BENCH_ENABLED
  describe "Movie cluster benchmarks" do
    it "measures membership convergence and steady heartbeat traffic" do
      node_count = ENV.fetch("MOVIE_CLUSTER_BENCH_NODES", "5").to_i
      raise "cluster benchmark requires at least two nodes" unless node_count >= 2
      systems = [] of Movie::ActorSystem(String)
      clusters = [] of Movie::Cluster::ClusterExtension
      settings = ->(seeds : Array(Movie::Address)) do
        Movie::Cluster::ClusterSettings.new(
          cluster_name: "benchmark-cluster",
          seed_nodes: seeds,
          join_retry_interval: 10.milliseconds,
          gossip_interval: 10.milliseconds,
          gossip_fanout: node_count,
          heartbeat_interval: 20.milliseconds,
          heartbeat_timeout: 250.milliseconds,
          max_members: node_count * 2
        )
      end

      begin
        seed_system = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "cluster-bench-0")
        systems << seed_system
        seed_remote = seed_system.enable_remoting("127.0.0.1", 0, 1)
        clusters << seed_system.enable_cluster(settings.call([] of Movie::Address))

        convergence = Time.measure do
          (1...node_count).each do |index|
            system = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "cluster-bench-#{index}")
            systems << system
            system.enable_remoting("127.0.0.1", 0, 1)
            clusters << system.enable_cluster(settings.call([seed_remote.address]))
          end
          wait_for_cluster_benchmark do
            clusters.all? do |cluster|
              cluster.snapshot.members.count(&.status.up?) == node_count && cluster.converged?
            end
          end
        end

        puts "\n  Cluster convergence: #{node_count} nodes in #{convergence.total_milliseconds.round(2)}ms"
        puts "  Gossip sent: #{clusters.sum(&.stats.gossip_sent)}, received: #{clusters.sum(&.stats.gossip_received)}"

        before = clusters.sum(&.stats.heartbeats_received)
        heartbeat_window = Time.measure { sleep 500.milliseconds }
        received = clusters.sum(&.stats.heartbeats_received) - before
        puts "  Heartbeat messages received: #{received} in #{heartbeat_window.total_milliseconds.round(2)}ms"
        puts "  Aggregate heartbeat rate: #{(received / heartbeat_window.total_seconds).round(0)} messages/sec"
        received.should be > 0
      ensure
        systems.reverse_each { |system| system.shutdown(1.second) rescue nil }
      end
    end
  end
end
