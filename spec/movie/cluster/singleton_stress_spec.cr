require "../../spec_helper"
require "../../../src/movie"
require "process/executable_path"

module Movie::SingletonStress
  class StartCounter
    @value = Atomic(Int32).new(0)

    def increment : Nil
      @value.add(1)
    end

    def get : Int32
      @value.get
    end
  end

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
    def initialize(@node : String, @starts : StartCounter)
    end

    def on_signal(signal : SystemMessage)
      @starts.increment if signal.is_a?(PreStart)
    end

    def receive(message : Ping, context : ActorContext(Ping))
      Ask.reply_if_asked(context.sender, Pong.new(@node, message.value))
      Behaviors(Ping).same
    end
  end
end

private def run_singleton_stress_peer : NoReturn
  name = ENV["MOVIE_SINGLETON_PEER_NAME"]
  port = ENV["MOVIE_SINGLETON_PEER_PORT"].to_i
  seeds = ENV.fetch("MOVIE_SINGLETON_PEER_SEEDS", "")
    .split(',')
    .reject(&.empty?)
    .map { |address| Movie::Address.parse(address) }
  association = Movie::Remote::AssociationSettings.new(
    reconnect_min_backoff: 10.milliseconds,
    reconnect_max_backoff: 100.milliseconds,
    reconnect_jitter: 0.0,
    heartbeat_interval: 25.milliseconds,
    heartbeat_timeout: 250.milliseconds,
    shared_secret: "singleton-stress-secret"
  )
  system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, name: name)
  remote = system.enable_remoting("127.0.0.1", port, 1, association)
  cluster = system.enable_cluster(Movie::Cluster::ClusterSettings.new(
    cluster_name: "singleton-stress",
    seed_nodes: seeds,
    roles: ["singleton"],
    join_retry_interval: 20.milliseconds,
    gossip_interval: 20.milliseconds,
    heartbeat_interval: 20.milliseconds,
    heartbeat_timeout: 150.milliseconds
  ))
  Movie::Remote::MessageRegistry.register(Movie::SingletonStress::Ping)
  Movie::Remote::MessageRegistry.register(Movie::SingletonStress::Pong)
  starts = Movie::SingletonStress::StartCounter.new
  singleton = Movie::ClusterSingleton.get(system)
  ref = singleton.init(
    "stress-service",
    Movie::SingletonStress::Ping,
    roles: ["singleton"],
    activation_interval: 20.milliseconds,
    activation_timeout: 1.second
  ) { Movie::SingletonStress::Service.new(name, starts) }
  puts "MOVIE_SINGLETON_READY unique=#{cluster.self_unique_address} address=#{remote.address}"
  STDOUT.flush

  while line = STDIN.gets
    command, _, argument = line.partition(' ')
    case command
    when "STATUS"
      snapshot = cluster.snapshot
      stats = singleton.stats
      puts "MOVIE_SINGLETON_STATUS members=#{snapshot.members.size} up=#{snapshot.members.count(&.status.up?)} unreachable=#{snapshot.unreachable.size} converged=#{cluster.converged?} owner=#{ref.owner || ""} local=#{ref.locally_owned?} starts=#{starts.get} activations=#{stats.activations} reactivations=#{stats.reactivations}"
    when "ASK"
      response = ref.ask(
        Movie::SingletonStress::Ping.new(argument),
        Movie::SingletonStress::Pong,
        3.seconds
      ).await(3.seconds)
      puts "MOVIE_SINGLETON_REPLY node=#{response.node} value=#{response.value}"
    when "LEAVE"
      accepted = cluster.leave
      cluster.await_removed(5.seconds) if accepted
      puts "MOVIE_SINGLETON_LEAVE accepted=#{accepted}"
    when "DOWN"
      accepted = cluster.down(Movie::Cluster::UniqueAddress.parse(argument))
      puts "MOVIE_SINGLETON_DOWN accepted=#{accepted}"
    when "QUIT"
      puts "MOVIE_SINGLETON_QUIT accepted=true"
      STDOUT.flush
      break
    else
      puts "MOVIE_SINGLETON_ERROR command=#{command}"
    end
    STDOUT.flush
  end

  system.shutdown(1.second)
  exit
end

run_singleton_stress_peer if ENV["MOVIE_SINGLETON_PEER_MODE"]? == "1"

private record SingletonPeerStatus,
  members : Int32,
  up : Int32,
  unreachable : Int32,
  converged : Bool,
  owner : String,
  local : Bool,
  starts : Int32,
  activations : Int64,
  reactivations : Int64

private class SingletonPeerProcess
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
    fields = parse_fields(wait_line("MOVIE_SINGLETON_READY", 5.seconds))
    @unique_address = Movie::Cluster::UniqueAddress.parse(fields["unique"])
    @address = Movie::Address.parse(fields["address"])
  end

  def status : SingletonPeerStatus
    fields = request("STATUS", "MOVIE_SINGLETON_STATUS")
    SingletonPeerStatus.new(
      members: fields["members"].to_i,
      up: fields["up"].to_i,
      unreachable: fields["unreachable"].to_i,
      converged: fields["converged"] == "true",
      owner: fields["owner"],
      local: fields["local"] == "true",
      starts: fields["starts"].to_i,
      activations: fields["activations"].to_i64,
      reactivations: fields["reactivations"].to_i64
    )
  end

  def wait_status(
    timeout_span : Time::Span = 10.seconds,
    &condition : SingletonPeerStatus -> Bool
  ) : SingletonPeerStatus
    deadline = Time.instant + timeout_span
    loop do
      current = status
      return current if condition.call(current)
      raise "singleton peer status did not converge: #{current}" if Time.instant >= deadline
      sleep 20.milliseconds
    end
  end

  def ask(value : String) : Hash(String, String)
    request("ASK #{value}", "MOVIE_SINGLETON_REPLY", 5.seconds)
  end

  def leave : Bool
    request("LEAVE", "MOVIE_SINGLETON_LEAVE", 8.seconds)["accepted"] == "true"
  end

  def down(target : Movie::Cluster::UniqueAddress) : Bool
    request("DOWN #{target}", "MOVIE_SINGLETON_DOWN")["accepted"] == "true"
  end

  def shutdown : Nil
    request("QUIT", "MOVIE_SINGLETON_QUIT") unless @process.terminated?
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
      raise "singleton peer did not emit #{prefix}" if remaining <= Time::Span.zero
      select
      when line = @lines.receive
        return line if line.starts_with?(prefix)
      when timeout(remaining)
        raise "singleton peer did not emit #{prefix}"
      end
    end
  rescue Channel::ClosedError
    raise "singleton peer exited before #{prefix}"
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

private def reserve_singleton_ports(count : Int32) : Array(Int32)
  servers = Array(TCPServer).new(count) { TCPServer.new("127.0.0.1", 0) }
  servers.map(&.local_address.port)
ensure
  servers.try &.each(&.close)
end

private def spawn_singleton_peer(
  name : String,
  port : Int32,
  seeds : Array(Movie::Address),
) : SingletonPeerProcess
  executable = Process.executable_path || raise "cannot resolve singleton stress executable"
  SingletonPeerProcess.new(Process.new(
    executable,
    env: {
      "MOVIE_SINGLETON_PEER_MODE"  => "1",
      "MOVIE_SINGLETON_PEER_NAME"  => name,
      "MOVIE_SINGLETON_PEER_PORT"  => port.to_s,
      "MOVIE_SINGLETON_PEER_SEEDS" => seeds.join(','),
    },
    input: Process::Redirect::Pipe,
    output: Process::Redirect::Pipe,
    error: Process::Redirect::Inherit
  ))
end

if ENV["MOVIE_SINGLETON_STRESS"]? == "1"
  describe "Movie cluster singleton multi-process stress" do
    it "covers eager remote routing, graceful handoff, restart, abrupt loss, and explicit down" do
      seed_port, peer_port = reserve_singleton_ports(2)
      seed = spawn_singleton_peer(
        "a-singleton-process",
        seed_port,
        [] of Movie::Address
      )
      peer = spawn_singleton_peer(
        "b-singleton-process",
        peer_port,
        [seed.address]
      )
      restarted = nil.as(SingletonPeerProcess?)

      begin
        seed.wait_status do |status|
          status.up == 2 && status.converged && status.local && status.starts == 1
        end
        peer.wait_status do |status|
          status.up == 2 && status.converged && status.owner == seed.unique_address.to_s
        end
        peer.ask("remote").should eq({"node" => "a-singleton-process", "value" => "remote"})

        seed.leave.should be_true
        peer.wait_status do |status|
          status.members == 1 && status.converged && status.local && status.starts == 1
        end
        peer.ask("after-leave").should eq({
          "node"  => "b-singleton-process",
          "value" => "after-leave",
        })
        seed.shutdown

        restarted = spawn_singleton_peer(
          "a-singleton-process",
          seed_port,
          [peer.address]
        )
        peer.wait_status { |status| status.up == 2 && status.converged && status.local }
        restarted.wait_status do |status|
          status.up == 2 && status.converged && status.owner == peer.unique_address.to_s
        end
        restarted.ask("after-restart").should eq({
          "node"  => "b-singleton-process",
          "value" => "after-restart",
        })

        failed_owner = peer.unique_address
        peer.kill
        restarted.wait_status { |status| status.unreachable == 1 }
        restarted.down(failed_owner).should be_true
        restarted.wait_status do |status|
          status.members == 1 && status.converged && status.local && status.starts == 1
        end
        restarted.ask("after-down").should eq({
          "node"  => "a-singleton-process",
          "value" => "after-down",
        })
      ensure
        restarted.try &.shutdown
        peer.try &.kill
        seed.try &.shutdown
      end
    end
  end
end
