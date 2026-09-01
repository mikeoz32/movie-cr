require "../../spec_helper"
require "../../../src/movie"
require "process/executable_path"

CLUSTER_STRESS_ENABLED = ENV["MOVIE_CLUSTER_STRESS"]? == "1"

private def run_cluster_peer : NoReturn
  name = ENV["MOVIE_CLUSTER_PEER_NAME"]
  port = ENV["MOVIE_CLUSTER_PEER_PORT"].to_i
  seeds = ENV.fetch("MOVIE_CLUSTER_PEER_SEEDS", "")
    .split(',')
    .reject(&.empty?)
    .map { |address| Movie::Address.parse(address) }
  secret = ENV.fetch("MOVIE_CLUSTER_PEER_SECRET", "cluster-stress-secret")
  system = Movie::ActorSystem(String).new(Movie::Behaviors(String).same, name: name)
  remote = system.enable_remoting(
    "127.0.0.1",
    port,
    1,
    Movie::Remote::AssociationSettings.new(
      reconnect_min_backoff: 10.milliseconds,
      reconnect_max_backoff: 100.milliseconds,
      reconnect_jitter: 0.0,
      heartbeat_interval: 25.milliseconds,
      heartbeat_timeout: 250.milliseconds,
      shared_secret: secret
    )
  )
  cluster = system.enable_cluster(Movie::Cluster::ClusterSettings.new(
    cluster_name: "stress-cluster",
    seed_nodes: seeds,
    join_retry_interval: 20.milliseconds,
    gossip_interval: 20.milliseconds,
    gossip_fanout: 3,
    heartbeat_interval: 20.milliseconds,
    heartbeat_timeout: 150.milliseconds
  ))
  puts "MOVIE_CLUSTER_READY unique=#{cluster.self_unique_address} address=#{remote.address}"
  STDOUT.flush

  while line = STDIN.gets
    command, argument = line.partition(' ')[0], line.partition(' ')[2]
    case command
    when "STATUS"
      snapshot = cluster.snapshot
      self_status = cluster.self_member.status
      state = snapshot.members.map do |member|
        "#{member.unique_address.node_uid}:#{member.status}:#{member.revision}:#{member.changed_by}"
      end.join(';')
      puts "MOVIE_CLUSTER_STATUS status=#{self_status} members=#{snapshot.members.size} up=#{snapshot.members.count(&.status.up?)} unreachable=#{snapshot.unreachable.size} converged=#{cluster.converged?} state=#{state}"
    when "JOIN"
      cluster.join(Movie::Address.parse(argument))
      puts "MOVIE_CLUSTER_JOIN accepted=true"
    when "LEAVE"
      puts "MOVIE_CLUSTER_LEAVE accepted=#{cluster.leave}"
    when "DOWN"
      puts "MOVIE_CLUSTER_DOWN accepted=#{cluster.down(Movie::Cluster::UniqueAddress.parse(argument))}"
    when "QUIT"
      puts "MOVIE_CLUSTER_QUIT accepted=true"
      STDOUT.flush
      break
    else
      puts "MOVIE_CLUSTER_ERROR command=#{command}"
    end
    STDOUT.flush
  end

  system.shutdown(1.second)
  exit
end

run_cluster_peer if ENV["MOVIE_CLUSTER_PEER_MODE"]? == "1"

private record ClusterPeerStatus,
  status : String,
  members : Int32,
  up : Int32,
  unreachable : Int32,
  converged : Bool,
  state : String

private class ClusterPeerProcess
  getter process : Process
  getter unique_address : Movie::Cluster::UniqueAddress
  getter address : Movie::Address

  @lines = Channel(String).new(128)
  @waited = false

  def initialize(@process : Process)
    spawn do
      while line = @process.output.gets
        @lines.send(line)
      end
    ensure
      @lines.close
    end
    ready = wait_line("MOVIE_CLUSTER_READY", 5.seconds)
    fields = parse_fields(ready)
    @unique_address = Movie::Cluster::UniqueAddress.parse(fields["unique"])
    @address = Movie::Address.parse(fields["address"])
  end

  def command(value : String) : Nil
    @process.input.puts(value)
    @process.input.flush
  end

  def status : ClusterPeerStatus
    command("STATUS")
    fields = parse_fields(wait_line("MOVIE_CLUSTER_STATUS", 2.seconds))
    ClusterPeerStatus.new(
      status: fields["status"],
      members: fields["members"].to_i,
      up: fields["up"].to_i,
      unreachable: fields["unreachable"].to_i,
      converged: fields["converged"] == "true",
      state: fields["state"]
    )
  end

  def wait_status(timeout_span : Time::Span = 10.seconds, &block : ClusterPeerStatus -> Bool) : ClusterPeerStatus
    deadline = Time.instant + timeout_span
    loop do
      current = status
      return current if yield current
      if Time.instant >= deadline
        raise "cluster peer status did not converge within #{timeout_span}: #{current}"
      end
      sleep 20.milliseconds
    end
  end

  def request(command : String, response_prefix : String) : String
    self.command(command)
    wait_line(response_prefix, 2.seconds)
  end

  def shutdown(timeout_span : Time::Span = 2.seconds) : Process::Status
    request("QUIT", "MOVIE_CLUSTER_QUIT") unless @process.terminated?
    wait_bounded(timeout_span)
  rescue ex : Exception
    @process.terminate(graceful: false) unless @process.terminated?
    wait_bounded(timeout_span)
  end

  def kill(timeout_span : Time::Span = 2.seconds) : Process::Status
    @process.terminate(graceful: false) unless @process.terminated?
    wait_bounded(timeout_span)
  end

  private def wait_line(prefix : String, timeout_span : Time::Span) : String
    deadline = Time.instant + timeout_span
    loop do
      remaining = deadline - Time.instant
      raise "cluster peer did not emit #{prefix}" if remaining <= Time::Span.zero
      select
      when line = @lines.receive
        return line if line.starts_with?(prefix)
      when timeout(remaining)
        raise "cluster peer did not emit #{prefix}"
      end
    end
  rescue Channel::ClosedError
    raise "cluster peer exited before #{prefix}"
  end

  private def parse_fields(line : String) : Hash(String, String)
    fields = {} of String => String
    line.split(' ').skip(1).each do |field|
      key, separator, value = field.partition('=')
      fields[key] = value unless separator.empty?
    end
    fields
  end

  private def wait_bounded(timeout_span : Time::Span) : Process::Status
    raise "cluster peer process was already waited" if @waited
    @waited = true
    completed = Channel(Process::Status).new(1)
    spawn { completed.send(@process.wait) }
    select
    when status = completed.receive
      status
    when timeout(timeout_span)
      @process.terminate(graceful: false) unless @process.terminated?
      select
      when status = completed.receive
        status
      when timeout(timeout_span)
        raise "cluster peer did not terminate after forced shutdown"
      end
    end
  end
end

private def reserve_cluster_ports(count : Int32) : Array(Int32)
  servers = Array(TCPServer).new(count) { TCPServer.new("127.0.0.1", 0) }
  servers.map(&.local_address.port)
ensure
  servers.try &.each(&.close)
end

private def spawn_cluster_peer(name : String, port : Int32, seeds : Array(Movie::Address)) : ClusterPeerProcess
  executable = Process.executable_path || raise "cannot resolve cluster stress executable"
  process = Process.new(
    executable,
    env: {
      "MOVIE_CLUSTER_PEER_MODE"  => "1",
      "MOVIE_CLUSTER_PEER_NAME"  => name,
      "MOVIE_CLUSTER_PEER_PORT"  => port.to_s,
      "MOVIE_CLUSTER_PEER_SEEDS" => seeds.join(','),
    },
    input: Process::Redirect::Pipe,
    output: Process::Redirect::Pipe,
    error: Process::Redirect::Inherit
  )
  ClusterPeerProcess.new(process)
end

private def wait_for_peer_convergence(peers : Array(ClusterPeerProcess), timeout_span : Time::Span = 10.seconds) : Nil
  deadline = Time.instant + timeout_span
  loop do
    statuses = peers.map(&.status)
    return if statuses.all? { |status| status.up == peers.size && status.converged }
    if Time.instant >= deadline
      raise "cluster peers did not converge within #{timeout_span}: #{statuses}"
    end
    sleep 20.milliseconds
  end
end

if CLUSTER_STRESS_ENABLED
  describe "Movie cluster multi-process stress" do
    it "covers seed ordering, convergence, reachability, leave, down, and restart incarnation" do
      seed_port, joiner_port, third_port = reserve_cluster_ports(3)
      seed_address = Movie::Address.remote("a-process-seed", "127.0.0.1", seed_port)
      joiner = spawn_cluster_peer("b-process-joiner", joiner_port, [seed_address])
      seed = nil.as(ClusterPeerProcess?)
      third = nil.as(ClusterPeerProcess?)
      restarted = nil.as(ClusterPeerProcess?)

      begin
        joiner.status.status.should eq("Joining")
        seed = spawn_cluster_peer("a-process-seed", seed_port, [] of Movie::Address)
        joiner.wait_status { |status| status.status == "Up" && status.members == 2 }
        seed.wait_status { |status| status.up == 2 }

        third = spawn_cluster_peer("c-process-third", third_port, [seed_address])
        wait_for_peer_convergence([seed, joiner, third])

        join_response = joiner.request("JOIN #{seed_address}", "MOVIE_CLUSTER_JOIN")
        join_response.should contain("accepted=true")
        seed.wait_status { |status| status.members == 3 }

        joiner.process.signal(Signal::STOP)
        seed.wait_status { |status| status.unreachable == 1 && status.members == 3 }
        joiner.process.signal(Signal::CONT)
        seed.wait_status { |status| status.unreachable == 0 && status.members == 3 }

        third.request("LEAVE", "MOVIE_CLUSTER_LEAVE").should contain("accepted=true")
        third.wait_status { |status| status.status == "Removed" }
        seed.wait_status { |status| status.members == 2 && status.up == 2 }

        old_incarnation = joiner.unique_address
        joiner.kill.success?.should be_false
        seed.wait_status { |status| status.unreachable == 1 && status.members == 2 }
        seed.request("DOWN #{old_incarnation}", "MOVIE_CLUSTER_DOWN").should contain("accepted=true")
        seed.wait_status { |status| status.members == 1 && status.unreachable == 0 }

        restarted = spawn_cluster_peer("b-process-joiner", joiner_port, [seed_address])
        restarted.unique_address.should_not eq(old_incarnation)
        restarted.wait_status { |status| status.status == "Up" && status.members == 2 }
        seed.wait_status { |status| status.up == 2 && status.unreachable == 0 }
      ensure
        restarted.try &.shutdown
        third.try &.shutdown
        joiner.kill rescue nil unless joiner.process.terminated?
        seed.try &.shutdown
      end
    end
  end
end
