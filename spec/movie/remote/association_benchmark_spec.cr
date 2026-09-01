require "../../spec_helper"
require "../../../src/movie"

ASSOCIATION_BENCH_ENABLED = ENV["MOVIE_BENCH"]? == "1"

private class AssociationBenchmarkWatcher < Movie::AbstractBehavior(String)
  def receive(message : String, context : Movie::ActorContext(String))
    Movie::Behaviors(String).same
  end
end

private def wait_for_association_benchmark(timeout_span : Time::Span = 5.seconds, &block : -> Bool)
  deadline = Time.instant + timeout_span
  until yield
    raise "association benchmark timed out" if Time.instant >= deadline
    sleep 1.millisecond
  end
end

if ASSOCIATION_BENCH_ENABLED
  describe "Movie association benchmarks" do
    it "measures acknowledged control throughput" do
      iterations = ENV.fetch("MOVIE_ASSOCIATION_CONTROL_MESSAGES", "10000").to_i
      server = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "control-bench-server")
      server_remote = server.enable_remoting("127.0.0.1", 0, 1)
      server.spawn(Movie::Behaviors(String).same, name: "target")
      client = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "control-bench-client")
      client_remote = client.enable_remoting("127.0.0.1", 0, 1)
      watcher = client.spawn(AssociationBenchmarkWatcher.new, name: "watcher")
      target = Movie::ActorPath.new(
        Movie::Address.remote("control-bench-server", "127.0.0.1", server_remote.local_port),
        ["user", "target"]
      )
      remote_ref = client_remote.actor_ref(target, String)

      begin
        remote_ref.send_system(Movie::Watch.new(watcher).as(Movie::SystemMessage))
        remote_ref.send_system(Movie::Unwatch.new(watcher).as(Movie::SystemMessage))
        wait_for_association_benchmark { remote_ref.connection.pending_control_count == 0 }

        elapsed = Time.measure do
          iterations.times do |index|
            message = index.even? ? Movie::Watch.new(watcher) : Movie::Unwatch.new(watcher)
            remote_ref.send_system(message.as(Movie::SystemMessage))
          end
          wait_for_association_benchmark { remote_ref.connection.pending_control_count == 0 }
        end

        puts "\n  Acknowledged control: #{(iterations / elapsed.total_seconds).round(0)} messages/sec"
        puts "  Messages: #{iterations}, elapsed: #{elapsed.total_milliseconds.round(2)}ms"
        puts "  Final pending control: #{remote_ref.connection.pending_control_count}"
      ensure
        client.shutdown(1.second)
        server.shutdown(1.second)
      end
    end

    it "measures peer-restart reconnect latency" do
      restarts = ENV.fetch("MOVIE_ASSOCIATION_RESTARTS", "5").to_i
      settings = Movie::Remote::AssociationSettings.new(
        reconnect_min_backoff: 10.milliseconds,
        reconnect_max_backoff: 100.milliseconds,
        reconnect_jitter: 0.0,
        heartbeat_interval: 25.milliseconds,
        heartbeat_timeout: 250.milliseconds
      )
      server = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "reconnect-bench-server")
      server_remote = server.enable_remoting("127.0.0.1", 0, 1)
      port = server_remote.local_port
      client = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "reconnect-bench-client")
      client_remote = client.enable_remoting("127.0.0.1", 0, 1, settings)
      target = Movie::ActorPath.new(
        Movie::Address.remote("reconnect-bench-server", "127.0.0.1", port),
        ["user", "target"]
      )
      connection = client_remote.actor_ref(target, String).connection
      latencies = [] of Float64

      begin
        restarts.times do
          server.shutdown(1.second)
          wait_for_association_benchmark { !connection.active? }
          previous_generation = connection.generation
          started = Time.instant
          server = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: "reconnect-bench-server")
          server.enable_remoting("127.0.0.1", port, 1)
          wait_for_association_benchmark { connection.active? && connection.generation > previous_generation }
          latencies << (Time.instant - started).total_milliseconds
        end

        ordered = latencies.sort
        p50 = ordered[(ordered.size * 0.50).floor.to_i.clamp(0, ordered.size - 1)]
        p95 = ordered[(ordered.size * 0.95).floor.to_i.clamp(0, ordered.size - 1)]
        puts "\n  Peer restart reconnect latency: p50=#{p50.round(2)}ms p95=#{p95.round(2)}ms"
        puts "  Restarts: #{restarts}, attempts: #{connection.stats.connect_attempts}"
      ensure
        client.shutdown(1.second)
        server.shutdown(1.second)
      end
    end
  end
end
