require "../../spec_helper"
require "../../../src/movie"
require "../../../src/movie/persistence/postgres"
require "process/executable_path"

module Movie::ShardingStress
  struct Added
    include JSON::Serializable
    getter amount : Int32

    def initialize(@amount : Int32)
    end
  end

  struct State
    include JSON::Serializable
    getter value : Int32

    def initialize(@value : Int32 = 0)
    end
  end

  struct Add
    include JSON::Serializable
    getter amount : Int32
    getter operation_id : Persistence::OperationId

    def initialize(@amount : Int32, @operation_id : Persistence::OperationId)
    end
  end

  struct Get
    include JSON::Serializable

    def initialize
    end
  end

  struct Reply
    include JSON::Serializable
    getter value : Int32

    def initialize(@value : Int32)
    end
  end

  alias Command = Add | Get

  class Counter < EventSourcedBehavior(Command, Added, State)
    protected def empty_state : State
      State.new
    end

    protected def apply_event(state : State, event : Added) : State
      State.new(state.value + event.amount)
    end

    protected def handle_command(
      state : State,
      command : Command,
      ctx : ActorContext(Command),
    ) : EventEffect(Added, State)
      sender = ctx.sender
      case command
      when Add
        persist(Added.new(command.amount), command.operation_id).then_run do |current|
          Ask.reply_if_asked(sender, Reply.new(current.value))
        end
      when Get
        none.then_run { |current| Ask.reply_if_asked(sender, Reply.new(current.value)) }
      else
        none
      end
    end
  end
end

private def with_sharding_route_retry(timeout_span : Time::Span, &block : -> T) : T forall T
  deadline = Time.instant + timeout_span
  loop do
    begin
      return yield
    rescue error : Movie::Cluster::NoShardOwnerError
      raise error if Time.instant >= deadline
      sleep 20.milliseconds
    end
  end
end

private def run_sharding_stress_peer : NoReturn
  postgres_url = ENV["MOVIE_POSTGRES_TEST_URL"]
  name = ENV["MOVIE_SHARDING_PEER_NAME"]
  run_id = ENV["MOVIE_SHARDING_RUN_ID"]
  port = ENV["MOVIE_SHARDING_PEER_PORT"].to_i
  seeds = ENV.fetch("MOVIE_SHARDING_PEER_SEEDS", "")
    .split(',')
    .reject(&.empty?)
    .map { |address| Movie::Address.parse(address) }
  config = Movie::Config.builder
    .set("name", name)
    .set("persistence.backend", "postgres")
    .set("persistence.connection-uri", postgres_url)
    .build
  system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, config)
  remote = system.enable_remoting("127.0.0.1", port, 1)
  cluster = system.enable_cluster(Movie::Cluster::ClusterSettings.new(
    cluster_name: "sharding-stress",
    seed_nodes: seeds,
    roles: ["backend"],
    join_retry_interval: 20.milliseconds,
    gossip_interval: 20.milliseconds,
    heartbeat_interval: 100.milliseconds,
    heartbeat_timeout: 1.second
  ))
  Movie::Remote::MessageRegistry.register(Movie::ShardingStress::Add)
  Movie::Remote::MessageRegistry.register(Movie::ShardingStress::Get)
  Movie::Remote::MessageRegistry.register(Movie::ShardingStress::Reply)
  persistence = Movie::EventSourcing.get(system)
  persistent_type = persistence.register_entity(
    Movie::ShardingStress::Counter,
    Movie::ShardingStress::Command
  ) do |id, store|
    Movie::ShardingStress::Counter.new(id.persistence_id, store)
  end
  sharding = Movie::ClusterSharding.get(system)
  entity_type = sharding.init_event_sourced(
    persistent_type,
    shard_count: 32,
    allocation: Movie::Cluster::RendezvousAllocation.new,
    rebalance: Movie::Cluster::RateLimitedRebalance.new(max_concurrent: 4),
    lease_duration: 2.seconds,
    lease_renew_interval: 500.milliseconds
  )
  puts "MOVIE_SHARDING_READY unique=#{cluster.self_unique_address} address=#{remote.address}"
  STDOUT.flush

  while line = STDIN.gets
    command, argument = line.partition(' ')[0], line.partition(' ')[2]
    case command
    when "STATUS"
      snapshot = cluster.snapshot
      stats = sharding.stats
      puts "MOVIE_SHARDING_STATUS members=#{snapshot.members.size} up=#{snapshot.members.count(&.status.up?)} unreachable=#{snapshot.unreachable.size} converged=#{cluster.converged?} retries=#{stats.lease_retries}"
    when "FIND_OWNER"
      target = Movie::Cluster::UniqueAddress.parse(argument)
      entity_id = (0...1_000).map { |suffix| "#{run_id}-stress-entity-#{suffix}" }.find do |candidate|
        shard_id = Movie::Cluster::StableHashPartitioner.new.shard_for(
          entity_type.name,
          candidate,
          32
        )
        sharding.allocations(entity_type)[shard_id]? == target
      end
      puts "MOVIE_SHARDING_OWNER entity=#{entity_id || ""}"
    when "OWNER"
      shard_id = Movie::Cluster::StableHashPartitioner.new.shard_for(
        entity_type.name,
        argument,
        32
      )
      owner = sharding.allocations(entity_type)[shard_id]?
      puts "MOVIE_SHARDING_ENTITY_OWNER owner=#{owner || ""}"
    when "ADD"
      entity_id, amount = argument.split(' ', 2)
      operation_id = Movie::Persistence::OperationId.random
      reply = with_sharding_route_retry(5.seconds) do
        sharding.entity_ref_for(entity_type, entity_id).ask(
          Movie::ShardingStress::Add.new(amount.to_i, operation_id),
          Movie::ShardingStress::Reply,
          5.seconds
        ).await(5.seconds)
      end
      puts "MOVIE_SHARDING_REPLY value=#{reply.value}"
    when "GET"
      reply = sharding.entity_ref_for(entity_type, argument).ask(
        Movie::ShardingStress::Get.new,
        Movie::ShardingStress::Reply,
        5.seconds
      ).await(5.seconds)
      puts "MOVIE_SHARDING_REPLY value=#{reply.value}"
    when "DOWN"
      accepted = cluster.down(Movie::Cluster::UniqueAddress.parse(argument))
      puts "MOVIE_SHARDING_DOWN accepted=#{accepted}"
    when "LEAVE"
      accepted = cluster.leave
      cluster.await_removed(5.seconds) if accepted
      puts "MOVIE_SHARDING_LEAVE accepted=#{accepted}"
    when "QUIT"
      puts "MOVIE_SHARDING_QUIT accepted=true"
      STDOUT.flush
      break
    else
      puts "MOVIE_SHARDING_ERROR command=#{command}"
    end
    STDOUT.flush
  end

  system.shutdown(1.second)
  exit
end

run_sharding_stress_peer if ENV["MOVIE_SHARDING_PEER_MODE"]? == "1"

private record ShardingPeerStatus,
  members : Int32,
  up : Int32,
  unreachable : Int32,
  converged : Bool,
  retries : Int64

private class ShardingPeerProcess
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
    fields = parse_fields(wait_line("MOVIE_SHARDING_READY", 10.seconds))
    @unique_address = Movie::Cluster::UniqueAddress.parse(fields["unique"])
    @address = Movie::Address.parse(fields["address"])
  end

  def status : ShardingPeerStatus
    fields = request("STATUS", "MOVIE_SHARDING_STATUS")
    ShardingPeerStatus.new(
      fields["members"].to_i,
      fields["up"].to_i,
      fields["unreachable"].to_i,
      fields["converged"] == "true",
      fields["retries"].to_i64
    )
  end

  def find_entity_owned_by(owner : Movie::Cluster::UniqueAddress) : String
    request("FIND_OWNER #{owner}", "MOVIE_SHARDING_OWNER")["entity"]
  end

  def wait_entity_owned_by(
    owner : Movie::Cluster::UniqueAddress,
    timeout_span : Time::Span = 10.seconds,
  ) : String
    deadline = Time.instant + timeout_span
    loop do
      entity_id = find_entity_owned_by(owner)
      return entity_id unless entity_id.empty?
      raise "sharding allocation did not assign an entity to #{owner}" if Time.instant >= deadline
      sleep 20.milliseconds
    end
  end

  def add(entity_id : String, amount : Int32) : Int32
    request("ADD #{entity_id} #{amount}", "MOVIE_SHARDING_REPLY", 8.seconds)["value"].to_i
  end

  def get(entity_id : String) : Int32
    request("GET #{entity_id}", "MOVIE_SHARDING_REPLY", 8.seconds)["value"].to_i
  end

  def down(owner : Movie::Cluster::UniqueAddress) : Bool
    request("DOWN #{owner}", "MOVIE_SHARDING_DOWN")["accepted"] == "true"
  end

  def wait_owner(
    entity_id : String,
    owner : Movie::Cluster::UniqueAddress,
    timeout_span : Time::Span = 10.seconds,
  ) : Nil
    deadline = Time.instant + timeout_span
    loop do
      current = request("OWNER #{entity_id}", "MOVIE_SHARDING_ENTITY_OWNER")["owner"]
      return if current == owner.to_s
      raise "entity #{entity_id} was not reassigned to #{owner}" if Time.instant >= deadline
      sleep 20.milliseconds
    end
  end

  def leave : Bool
    request("LEAVE", "MOVIE_SHARDING_LEAVE", 8.seconds)["accepted"] == "true"
  end

  def wait_status(timeout_span : Time::Span = 10.seconds, &condition : ShardingPeerStatus -> Bool) : ShardingPeerStatus
    deadline = Time.instant + timeout_span
    loop do
      current = status
      return current if condition.call(current)
      raise "sharding peer status did not converge: #{current}" if Time.instant >= deadline
      sleep 20.milliseconds
    end
  end

  def shutdown : Nil
    request("QUIT", "MOVIE_SHARDING_QUIT") unless @process.terminated?
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
      raise "sharding peer did not emit #{prefix}" if remaining <= Time::Span.zero
      select
      when line = @lines.receive
        return line if line.starts_with?(prefix)
      when timeout(remaining)
        raise "sharding peer did not emit #{prefix}"
      end
    end
  rescue Channel::ClosedError
    raise "sharding peer exited before #{prefix}"
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

private def reserve_sharding_ports(count : Int32) : Array(Int32)
  servers = Array(TCPServer).new(count) { TCPServer.new("127.0.0.1", 0) }
  servers.map(&.local_address.port)
ensure
  servers.try &.each(&.close)
end

private def spawn_sharding_peer(
  name : String,
  port : Int32,
  seeds : Array(Movie::Address),
  postgres_url : String,
  run_id : String,
) : ShardingPeerProcess
  executable = Process.executable_path || raise "cannot resolve sharding stress executable"
  ShardingPeerProcess.new(Process.new(
    executable,
    env: {
      "MOVIE_SHARDING_PEER_MODE"  => "1",
      "MOVIE_SHARDING_PEER_NAME"  => name,
      "MOVIE_SHARDING_PEER_PORT"  => port.to_s,
      "MOVIE_SHARDING_PEER_SEEDS" => seeds.join(','),
      "MOVIE_POSTGRES_TEST_URL"   => postgres_url,
      "MOVIE_SHARDING_RUN_ID"     => run_id,
    },
    input: Process::Redirect::Pipe,
    output: Process::Redirect::Pipe,
    error: Process::Redirect::Inherit
  ))
end

if ENV["MOVIE_SHARDING_STRESS"]? == "1" && (postgres_url = ENV["MOVIE_POSTGRES_TEST_URL"]?)
  describe "Movie persistent sharding multi-process stress" do
    it "routes to a rebalanced owner and recovers after graceful leave" do
      seed_port, peer_port = reserve_sharding_ports(2)
      run_id = UUID.random.to_s
      seed = spawn_sharding_peer("a-sharding-leave-seed", seed_port, [] of Movie::Address, postgres_url, run_id)
      peer = spawn_sharding_peer("b-sharding-leave-peer", peer_port, [seed.address], postgres_url, run_id)

      begin
        seed.wait_status { |status| status.up == 2 && status.converged }
        peer.wait_status { |status| status.up == 2 && status.converged }
        entity_id = seed.wait_entity_owned_by(peer.unique_address)
        seed.add(entity_id, 5).should eq(5)

        peer.leave.should be_true
        seed.wait_status { |status| status.members == 1 && status.converged }
        seed.wait_owner(entity_id, seed.unique_address)
        seed.get(entity_id).should eq(5)
      ensure
        peer.try &.shutdown
        seed.try &.shutdown
      end
    end

    it "recovers through a higher fencing epoch after abrupt loss and explicit down" do
      seed_port, peer_port = reserve_sharding_ports(2)
      run_id = UUID.random.to_s
      seed = spawn_sharding_peer("a-sharding-seed", seed_port, [] of Movie::Address, postgres_url, run_id)
      peer = spawn_sharding_peer("b-sharding-peer", peer_port, [seed.address], postgres_url, run_id)

      begin
        seed.wait_status { |status| status.up == 2 && status.converged }
        peer.wait_status { |status| status.up == 2 && status.converged }
        entity_id = seed.wait_entity_owned_by(peer.unique_address)
        seed.add(entity_id, 7).should eq(7)

        failed_owner = peer.unique_address
        peer.kill
        seed.wait_status { |status| status.unreachable == 1 }
        seed.down(failed_owner).should be_true
        seed.wait_status { |status| status.members == 1 && status.converged }
        seed.get(entity_id).should eq(7)
        seed.wait_owner(entity_id, seed.unique_address)
        seed.status.retries.should be > 0_i64
      ensure
        peer.try &.kill
        seed.try &.shutdown
      end
    end
  end
end
