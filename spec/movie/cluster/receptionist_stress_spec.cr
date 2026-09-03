require "../../spec_helper"
require "../../../src/movie"
require "process/executable_path"

module Movie::ReceptionistStress
  struct Ping
    include JSON::Serializable

    getter value : String

    def initialize(@value : String)
    end
  end

  struct Pong
    include JSON::Serializable

    getter node : String
    getter value : String

    def initialize(@node : String, @value : String)
    end
  end

  class Service < AbstractBehavior(Ping)
    def initialize(@node : String)
    end

    def receive(message : Ping, context : ActorContext(Ping))
      Ask.reply_if_asked(context.sender, Pong.new(@node, message.value))
      Behaviors(Ping).same
    end
  end
end

private def run_receptionist_stress_peer : NoReturn
  name = ENV["MOVIE_RECEPTIONIST_PEER_NAME"]
  port = ENV["MOVIE_RECEPTIONIST_PEER_PORT"].to_i
  service_name = ENV.fetch("MOVIE_RECEPTIONIST_PEER_SERVICE", "")
  seeds = ENV.fetch("MOVIE_RECEPTIONIST_PEER_SEEDS", "")
    .split(',')
    .reject(&.empty?)
    .map { |address| Movie::Address.parse(address) }
  association = Movie::Remote::AssociationSettings.new(
    reconnect_min_backoff: 10.milliseconds,
    reconnect_max_backoff: 100.milliseconds,
    reconnect_jitter: 0.0,
    heartbeat_interval: 25.milliseconds,
    heartbeat_timeout: 250.milliseconds,
    shared_secret: "receptionist-stress-secret"
  )
  system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, name: name)
  remote = system.enable_remoting("127.0.0.1", port, 1, association)
  cluster = system.enable_cluster(Movie::Cluster::ClusterSettings.new(
    cluster_name: "receptionist-stress",
    seed_nodes: seeds,
    join_retry_interval: 20.milliseconds,
    gossip_interval: 20.milliseconds,
    heartbeat_interval: 20.milliseconds,
    heartbeat_timeout: 150.milliseconds
  ))
  Movie::Remote::MessageRegistry.register(Movie::ReceptionistStress::Ping)
  Movie::Remote::MessageRegistry.register(Movie::ReceptionistStress::Pong)
  receptionist = Movie::ClusterReceptionist.get(system)
  key = Movie::Cluster::ServiceKey(Movie::ReceptionistStress::Ping).new("stress-services")
  cluster.await_up(10.seconds)
  unless service_name.empty?
    service = system.spawn(
      Movie::ReceptionistStress::Service.new(service_name),
      name: "service"
    )
    receptionist.register(key, service)
  end
  puts "MOVIE_RECEPTIONIST_READY unique=#{cluster.self_unique_address} address=#{remote.address}"
  STDOUT.flush

  while line = STDIN.gets
    command, _, argument = line.partition(' ')
    case command
    when "STATUS"
      snapshot = cluster.snapshot
      stats = receptionist.stats
      puts "MOVIE_RECEPTIONIST_STATUS members=#{snapshot.members.size} up=#{snapshot.members.count(&.status.up?)} unreachable=#{snapshot.unreachable.size} converged=#{cluster.converged?} services=#{receptionist.find(key).services.size} known_nodes=#{stats.known_nodes} purged=#{stats.purged_nodes}"
    when "ASK"
      service = receptionist.find(key).services.first
      response = service.ask(
        Movie::ReceptionistStress::Ping.new(argument),
        Movie::ReceptionistStress::Pong,
        3.seconds
      ).await(3.seconds)
      puts "MOVIE_RECEPTIONIST_REPLY node=#{response.node} value=#{response.value}"
    when "LEAVE"
      accepted = cluster.leave
      cluster.await_removed(5.seconds) if accepted
      puts "MOVIE_RECEPTIONIST_LEAVE accepted=#{accepted}"
    when "DOWN"
      accepted = cluster.down(Movie::Cluster::UniqueAddress.parse(argument))
      puts "MOVIE_RECEPTIONIST_DOWN accepted=#{accepted}"
    when "QUIT"
      puts "MOVIE_RECEPTIONIST_QUIT accepted=true"
      STDOUT.flush
      break
    else
      puts "MOVIE_RECEPTIONIST_ERROR command=#{command}"
    end
    STDOUT.flush
  end

  system.shutdown(1.second)
  exit
end

run_receptionist_stress_peer if ENV["MOVIE_RECEPTIONIST_PEER_MODE"]? == "1"

private record ReceptionistPeerStatus,
  members : Int32,
  up : Int32,
  unreachable : Int32,
  converged : Bool,
  services : Int32,
  known_nodes : Int32,
  purged : Int64

private class ReceptionistPeerProcess
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
    fields = parse_fields(wait_line("MOVIE_RECEPTIONIST_READY", 12.seconds))
    @unique_address = Movie::Cluster::UniqueAddress.parse(fields["unique"])
    @address = Movie::Address.parse(fields["address"])
  end

  def status : ReceptionistPeerStatus
    fields = request("STATUS", "MOVIE_RECEPTIONIST_STATUS")
    ReceptionistPeerStatus.new(
      members: fields["members"].to_i,
      up: fields["up"].to_i,
      unreachable: fields["unreachable"].to_i,
      converged: fields["converged"] == "true",
      services: fields["services"].to_i,
      known_nodes: fields["known_nodes"].to_i,
      purged: fields["purged"].to_i64
    )
  end

  def wait_status(
    timeout_span : Time::Span = 10.seconds,
    &condition : ReceptionistPeerStatus -> Bool
  ) : ReceptionistPeerStatus
    deadline = Time.instant + timeout_span
    loop do
      current = status
      return current if condition.call(current)
      raise "receptionist peer status did not converge: #{current}" if Time.instant >= deadline
      sleep 20.milliseconds
    end
  end

  def ask(value : String) : Hash(String, String)
    request("ASK #{value}", "MOVIE_RECEPTIONIST_REPLY", 5.seconds)
  end

  def leave : Bool
    request("LEAVE", "MOVIE_RECEPTIONIST_LEAVE", 8.seconds)["accepted"] == "true"
  end

  def down(target : Movie::Cluster::UniqueAddress) : Bool
    request("DOWN #{target}", "MOVIE_RECEPTIONIST_DOWN")["accepted"] == "true"
  end

  def shutdown : Nil
    request("QUIT", "MOVIE_RECEPTIONIST_QUIT") unless @process.terminated?
    wait_bounded
  rescue
    kill
  end

  def kill : Nil
    @process.terminate(graceful: false) unless @process.terminated?
    wait_bounded
  end

  private def request(
    command : String,
    prefix : String,
    timeout_span : Time::Span = 3.seconds,
  ) : Hash(String, String)
    @process.input.puts(command)
    @process.input.flush
    parse_fields(wait_line(prefix, timeout_span))
  end

  private def wait_line(prefix : String, timeout_span : Time::Span) : String
    deadline = Time.instant + timeout_span
    loop do
      remaining = deadline - Time.instant
      raise "receptionist peer did not emit #{prefix}" if remaining <= Time::Span.zero
      select
      when line = @lines.receive
        return line if line.starts_with?(prefix)
      when timeout(remaining)
        raise "receptionist peer did not emit #{prefix}"
      end
    end
  rescue Channel::ClosedError
    raise "receptionist peer exited before #{prefix}"
  end

  private def parse_fields(line : String) : Hash(String, String)
    line.split(' ').skip(1).to_h do |field|
      key, _, value = field.partition('=')
      {key, value}
    end
  end

  private def wait_bounded : Nil
    return if @waited
    @waited = true
    completed = Channel(Process::Status).new(1)
    spawn { completed.send(@process.wait) }
    select
    when completed.receive
    when timeout(3.seconds)
      @process.terminate(graceful: false) unless @process.terminated?
      completed.receive
    end
  end
end

private def reserve_receptionist_ports(count : Int32) : Array(Int32)
  servers = Array(TCPServer).new(count) { TCPServer.new("127.0.0.1", 0) }
  servers.map(&.local_address.port)
ensure
  servers.try &.each(&.close)
end

private def spawn_receptionist_peer(
  name : String,
  port : Int32,
  seeds : Array(Movie::Address),
  service : String = "",
) : ReceptionistPeerProcess
  executable = Process.executable_path || raise "cannot resolve receptionist stress executable"
  ReceptionistPeerProcess.new(Process.new(
    executable,
    env: {
      "MOVIE_RECEPTIONIST_PEER_MODE"    => "1",
      "MOVIE_RECEPTIONIST_PEER_NAME"    => name,
      "MOVIE_RECEPTIONIST_PEER_PORT"    => port.to_s,
      "MOVIE_RECEPTIONIST_PEER_SEEDS"   => seeds.join(','),
      "MOVIE_RECEPTIONIST_PEER_SERVICE" => service,
    },
    input: Process::Redirect::Pipe,
    output: Process::Redirect::Pipe,
    error: Process::Redirect::Inherit
  ))
end

if ENV["MOVIE_RECEPTIONIST_STRESS"]? == "1"
  describe "Movie cluster receptionist multi-process stress" do
    it "covers discovery, leave, unreachable filtering, downing, and a new UID" do
      seed_port, peer_port = reserve_receptionist_ports(2)
      seed = spawn_receptionist_peer(
        "a-receptionist-process",
        seed_port,
        [] of Movie::Address,
        "seed-v1"
      )
      peer = spawn_receptionist_peer(
        "b-receptionist-process",
        peer_port,
        [seed.address]
      )
      restarted = nil.as(ReceptionistPeerProcess?)

      begin
        peer.wait_status do |status|
          status.up == 2 && status.converged && status.services == 1
        end
        peer.ask("remote").should eq({"node" => "seed-v1", "value" => "remote"})

        seed.process.signal(Signal::STOP)
        begin
          peer.wait_status { |status| status.unreachable == 1 && status.services == 0 }
        ensure
          seed.process.signal(Signal::CONT) unless seed.process.terminated?
        end
        peer.wait_status { |status| status.unreachable == 0 && status.services == 1 }
        peer.ask("restored").should eq({"node" => "seed-v1", "value" => "restored"})

        seed.leave.should be_true
        peer.wait_status do |status|
          status.members == 1 && status.converged && status.services == 0
        end
        old_uid = seed.unique_address.node_uid
        seed.shutdown

        restarted = spawn_receptionist_peer(
          "a-receptionist-process",
          seed_port,
          [peer.address],
          "seed-v2"
        )
        restarted.unique_address.node_uid.should_not eq(old_uid)
        peer.wait_status do |status|
          status.up == 2 && status.converged && status.services == 1
        end
        peer.ask("restarted").should eq({"node" => "seed-v2", "value" => "restarted"})

        failed = restarted.unique_address
        restarted.kill
        peer.wait_status { |status| status.unreachable == 1 && status.services == 0 }
        peer.down(failed).should be_true
        final = peer.wait_status do |status|
          status.members == 1 && status.converged && status.known_nodes == 1
        end
        final.purged.should be >= 2_i64
      ensure
        restarted.try &.kill
        peer.try &.shutdown
        seed.try &.shutdown
      end
    end
  end
end
