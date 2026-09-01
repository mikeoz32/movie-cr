require "../../spec_helper"
require "../../../src/movie"

private struct ShardingPing
  include JSON::Serializable

  getter value : String

  def initialize(@value : String)
  end
end

private struct ShardingPong
  include JSON::Serializable

  getter value : String

  def initialize(@value : String)
  end
end

private class ShardingPingEntity < Movie::AbstractBehavior(ShardingPing)
  def initialize(
    @entity_id : String,
    @deliveries : Channel(String),
    @blocked_started : Channel(Nil)? = nil,
    @blocked_release : Channel(Nil)? = nil,
    @sender_paths : Channel(String?)? = nil,
  )
  end

  def receive(message : ShardingPing, context : Movie::ActorContext(ShardingPing))
    @deliveries.send("#{@entity_id}:#{message.value}")
    @sender_paths.try &.send(context.sender.try(&.path).try(&.to_s))
    if message.value == "block"
      @blocked_started.try &.send(nil)
      @blocked_release.try &.receive
    end
    Movie::Ask.reply_if_asked(context.sender, ShardingPong.new("#{@entity_id}:#{message.value}"))
    Movie::Behaviors(ShardingPing).same
  end
end

private def wait_for_sharding(timeout_span : Time::Span = 5.seconds, &condition : -> Bool)
  deadline = Time.instant + timeout_span
  until condition.call
    raise "sharding condition was not met within #{timeout_span}" if Time.instant >= deadline
    sleep 5.milliseconds
  end
end

describe Movie::ClusterShardingExtension do
  it "preserves the original sender path across caller, coordinator, and owner nodes" do
    Movie::Remote::MessageRegistry.register(ShardingPing)
    systems = Array(Movie::ActorSystem(Nil)).new(3) do |index|
      Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, name: "sender-hop-#{index}")
    end
    begin
      remotes = systems.map { |system| system.enable_remoting("127.0.0.1", 0) }
      settings = ->(seeds : Array(Movie::Address)) do
        Movie::Cluster::ClusterSettings.new(
          cluster_name: "sharding-sender-hop",
          seed_nodes: seeds,
          join_retry_interval: 20.milliseconds,
          gossip_interval: 20.milliseconds,
          heartbeat_interval: 25.milliseconds,
          heartbeat_timeout: 500.milliseconds
        )
      end
      clusters = systems.map_with_index do |system, index|
        seeds = index == 0 ? [] of Movie::Address : [remotes[0].address]
        system.enable_cluster(settings.call(seeds))
      end
      wait_for_sharding do
        clusters.all? do |cluster|
          cluster.snapshot.members.count(&.status.up?) == 3 && cluster.converged?
        end
      end

      deliveries = Channel(String).new(1)
      sender_paths = Channel(String?).new(1)
      shardings = systems.map { |system| Movie::ClusterSharding.get(system) }
      types = shardings.map_with_index do |sharding, index|
        sharding.init("SenderHop", ShardingPing, shard_count: 12) do |entity_id|
          ShardingPingEntity.new("node-#{index}/#{entity_id}", deliveries, sender_paths: sender_paths)
        end
      end
      wait_for_sharding do
        plans = shardings.map_with_index { |sharding, index| sharding.allocations(types[index]) }
        plans.all? { |plan| plan.size == 12 && plan == plans[0] }
      end

      coordinator = clusters.map(&.self_unique_address).min
      non_coordinators = clusters.each_with_index.reject do |cluster, _index|
        cluster.self_unique_address == coordinator
      end.to_a
      caller_cluster, caller_index = non_coordinators[0]
      owner_cluster, owner_index = non_coordinators[1]
      entity_id = (0...1_000).map { |suffix| "sender-hop-#{suffix}" }.find do |candidate|
        shard_id = Movie::Cluster::StableHashPartitioner.new.shard_for("SenderHop", candidate, 12)
        shardings[caller_index].allocations(types[caller_index])[shard_id]? == owner_cluster.self_unique_address
      end.not_nil!
      original_sender = systems[caller_index].spawn(Movie::Behaviors(String).same, name: "original-sender")

      shardings[caller_index].entity_ref_for(types[caller_index], entity_id)
        .tell_from(original_sender, ShardingPing.new("three-hop"))

      deliveries.receive.should contain("#{entity_id}:three-hop")
      sender_paths.receive.should eq(original_sender.path.not_nil!.to_s)
      shardings[caller_index].entity_ref_for(types[caller_index], entity_id) <<
        ShardingPing.new("senderless")
      deliveries.receive.should contain("#{entity_id}:senderless")
      sender_paths.receive.should eq(systems[owner_index].dead_letters.path.not_nil!.to_s)

      retry_message = ShardingPing.new("senderless-retry")
      retry_tag, retry_payload = Movie::Remote::MessageRegistry.prepare(retry_message)
      retry_shard = Movie::Cluster::StableHashPartitioner.new.shard_for("SenderHop", entity_id, 12)
      retry_envelope = Movie::Cluster::ShardingEnvelope.new(
        "SenderHop",
        entity_id,
        retry_shard,
        Movie::Cluster::ShardingMessage.new(retry_tag, retry_payload),
        Movie::Cluster::ShardingSettings.new(shard_count: 12).configuration_key,
        delivery_attempt: 1,
        senderless: true
      )
      coordinator_ref = remotes[caller_index].actor_ref(
        Movie::ActorPath.new(
          coordinator.address,
          ["system", Movie::ClusterShardingExtension::DAEMON_NAME]
        ),
        Movie::Cluster::ShardingEnvelope
      )
      coordinator_ref.tell_from(original_sender, retry_envelope)
      deliveries.receive.should contain("#{entity_id}:senderless-retry")
      sender_paths.receive.should eq(systems[owner_index].dead_letters.path.not_nil!.to_s)
      caller_cluster.self_unique_address.should_not eq(owner_cluster.self_unique_address)
    ensure
      systems.reverse_each(&.shutdown)
    end
  end

  it "activates an ordinary entity on demand through a logical reference" do
    Movie::Remote::MessageRegistry.register(ShardingPing)
    system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, name: "sharding-local")
    begin
      system.enable_remoting("127.0.0.1", 0)
      system.enable_cluster
      sharding = Movie::ClusterSharding.get(system)
      deliveries = Channel(String).new(1)
      entity_type = sharding.init("Ping", ShardingPing, shard_count: 32) do |entity_id|
        ShardingPingEntity.new(entity_id, deliveries)
      end

      ref = sharding.entity_ref_for(entity_type, "entity-1")
      ref << ShardingPing.new("hello")

      deliveries.receive.should eq("entity-1:hello")
      ref.entity_id.should eq("entity-1")
      ref.entity_type.should eq("Ping")
      sharding.local_entity_count.should eq(1)
      sharding.stats.routes.should eq(1_i64)
      sharding.stats.local_deliveries.should eq(1_i64)
      sharding.stats.activations.should eq(1_i64)
    ensure
      system.shutdown
    end
  end

  it "passivates an idle entity and reactivates it through the same reference" do
    Movie::Remote::MessageRegistry.register(ShardingPing)
    system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, name: "sharding-idle")
    begin
      system.enable_remoting("127.0.0.1", 0)
      system.enable_cluster
      sharding = Movie::ClusterSharding.get(system)
      deliveries = Channel(String).new(2)
      activations = Atomic(Int32).new(0)
      entity_type = sharding.init(
        "IdlePing",
        ShardingPing,
        shard_count: 8,
        idle_timeout: 50.milliseconds
      ) do |entity_id|
        activations.add(1)
        ShardingPingEntity.new(entity_id, deliveries)
      end
      ref = sharding.entity_ref_for(entity_type, "idle-1")

      ref << ShardingPing.new("first")
      deliveries.receive.should eq("idle-1:first")
      wait_for_sharding { sharding.local_entity_count == 0 }
      ref << ShardingPing.new("second")
      deliveries.receive.should eq("idle-1:second")
      activations.get.should eq(2)
      sharding.stats.activations.should eq(2_i64)
      sharding.stats.idle_passivations.should eq(1_i64)
    ensure
      system.shutdown
    end
  end

  it "routes an entity message to its allocated owner on another node" do
    Movie::Remote::MessageRegistry.register(ShardingPing)
    Movie::Remote::MessageRegistry.register(ShardingPong, "sharding-pong.v1")
    seed_system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, name: "sharding-seed")
    peer_system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, name: "sharding-peer")
    begin
      seed_remote = seed_system.enable_remoting("127.0.0.1", 0)
      peer_remote = peer_system.enable_remoting("127.0.0.1", 0)
      cluster_settings = ->(seeds : Array(Movie::Address)) do
        Movie::Cluster::ClusterSettings.new(
          cluster_name: "sharding-spec",
          seed_nodes: seeds,
          join_retry_interval: 20.milliseconds,
          gossip_interval: 20.milliseconds,
          heartbeat_interval: 25.milliseconds,
          heartbeat_timeout: 500.milliseconds
        )
      end
      seed_cluster = seed_system.enable_cluster(cluster_settings.call([] of Movie::Address))
      peer_cluster = peer_system.enable_cluster(cluster_settings.call([seed_remote.address]))
      wait_for_sharding do
        seed_cluster.snapshot.members.count(&.status.up?) == 2 &&
          peer_cluster.snapshot.members.count(&.status.up?) == 2 &&
          seed_cluster.converged? && peer_cluster.converged?
      end

      seed_deliveries = Channel(String).new(1)
      peer_deliveries = Channel(String).new(1)
      seed_sharding = Movie::ClusterSharding.get(seed_system)
      peer_sharding = Movie::ClusterSharding.get(peer_system)
      seed_type = seed_sharding.init("RemotePing", ShardingPing, shard_count: 16) do |entity_id|
        ShardingPingEntity.new("seed/#{entity_id}", seed_deliveries)
      end
      peer_type = peer_sharding.init("RemotePing", ShardingPing, shard_count: 16) do |entity_id|
        ShardingPingEntity.new("peer/#{entity_id}", peer_deliveries)
      end

      wait_for_sharding do
        seed_sharding.allocations(seed_type).size == 16 &&
          seed_sharding.allocations(seed_type) == peer_sharding.allocations(peer_type)
      end

      entity_id = (0...1_000).map(&.to_s).find do |candidate|
        shard_id = Movie::Cluster::StableHashPartitioner.new.shard_for("RemotePing", candidate, 16)
        seed_sharding.allocations(seed_type)[shard_id]? == peer_cluster.self_unique_address
      end.not_nil!
      remote_ref = seed_sharding.entity_ref_for(seed_type, entity_id)
      remote_ref << ShardingPing.new("remote")

      peer_deliveries.receive.should eq("peer/#{entity_id}:remote")
      seed_sharding.local_entity_count.should eq(0)
      peer_sharding.local_entity_count.should eq(1)
      peer_type.name.should eq(seed_type.name)
      seed_sharding.stats.remote_routes.should be >= 1_i64
      peer_sharding.stats.local_deliveries.should be >= 1_i64

      remote_ref.ask(
        ShardingPing.new("ask"),
        ShardingPong,
        2.seconds
      ).await(2.seconds).value.should eq("peer/#{entity_id}:ask")
      peer_deliveries.receive.should eq("peer/#{entity_id}:ask")

      coordinator = {seed_cluster.self_unique_address, peer_cluster.self_unique_address}.min
      non_coordinator = coordinator == seed_cluster.self_unique_address ? peer_cluster.self_unique_address : seed_cluster.self_unique_address
      coordinator_remote = coordinator == seed_cluster.self_unique_address ? seed_remote : peer_remote
      non_coordinator_remote = coordinator == seed_cluster.self_unique_address ? peer_remote : seed_remote
      coordinator_sharding = coordinator == seed_cluster.self_unique_address ? seed_sharding : peer_sharding
      non_coordinator_sharding = coordinator == seed_cluster.self_unique_address ? peer_sharding : seed_sharding
      coordinator_type = coordinator == seed_cluster.self_unique_address ? seed_type : peer_type
      coordinator_deliveries = coordinator == seed_cluster.self_unique_address ? seed_deliveries : peer_deliveries
      coordinator_owned_id = (0...1_000).map { |suffix| "forged-#{suffix}" }.find do |candidate|
        shard_id = Movie::Cluster::StableHashPartitioner.new.shard_for("RemotePing", candidate, 16)
        seed_sharding.allocations(seed_type)[shard_id]? == coordinator
      end.not_nil!
      forged_shard = Movie::Cluster::StableHashPartitioner.new.shard_for(
        "RemotePing",
        coordinator_owned_id,
        16
      )
      forged_message = ShardingPing.new("forged")
      forged_tag, forged_payload = Movie::Remote::MessageRegistry.prepare(forged_message)
      forged = Movie::Cluster::ShardingEnvelope.new(
        "RemotePing",
        coordinator_owned_id,
        forged_shard,
        Movie::Cluster::ShardingMessage.new(forged_tag, forged_payload),
        Movie::Cluster::ShardingSettings.new(shard_count: 16).configuration_key
      ).forwarded(coordinator)

      unauthenticated_before = coordinator_sharding.stats.rejected_envelopes
      unauthenticated_ref = non_coordinator_remote.actor_ref(
        Movie::ActorPath.new(coordinator.address, ["system", Movie::ClusterShardingExtension::DAEMON_NAME]),
        Movie::Cluster::ShardingEnvelope
      )
      expect_raises(Movie::Remote::RemoteAskError) do
        unauthenticated_ref.ask_serializable(forged.as_ask(2.seconds), 2.seconds).await(2.seconds)
      end
      coordinator_sharding.stats.rejected_envelopes.should be > unauthenticated_before

      wrong_owner_before = non_coordinator_sharding.stats.rejected_envelopes
      wrong_owner_ref = coordinator_remote.actor_ref(
        Movie::ActorPath.new(non_coordinator.address, ["system", Movie::ClusterShardingExtension::DAEMON_NAME]),
        Movie::Cluster::ShardingEnvelope
      )
      expect_raises(Movie::Remote::RemoteAskError) do
        wrong_owner_ref.ask_serializable(forged.as_ask(2.seconds), 2.seconds).await(2.seconds)
      end
      non_coordinator_sharding.stats.rejected_envelopes.should be > wrong_owner_before
      authorized_ref = coordinator_sharding.entity_ref_for(coordinator_type, coordinator_owned_id)
      authorized = authorized_ref.ask(ShardingPing.new("authorized"), ShardingPong, 2.seconds)
        .await(2.seconds)
      authorized.value.should contain("#{coordinator_owned_id}:authorized")
      coordinator_deliveries.receive.should eq(authorized.value)
      authorized_ref.send_system(Movie::STOP)
      wait_for_sharding do
        seed_sharding.local_entity_count == 0 && peer_sharding.local_entity_count == 1
      end

      seed_passivations = seed_sharding.stats.explicit_passivations
      peer_passivations = peer_sharding.stats.explicit_passivations
      remote_ref.send_system(Movie::STOP)
      wait_for_sharding { peer_sharding.local_entity_count == 0 }
      seed_sharding.stats.explicit_passivations.should eq(seed_passivations)
      peer_sharding.stats.explicit_passivations.should eq(peer_passivations + 1_i64)

      remote_ref << ShardingPing.new("before-leave")
      peer_deliveries.receive.should eq("peer/#{entity_id}:before-leave")
      peer_cluster.leave.should be_true
      peer_cluster.await_removed(5.seconds)
      wait_for_sharding do
        seed_cluster.snapshot.member(peer_cluster.self_unique_address).nil?
      end
      wait_for_sharding { peer_sharding.local_entity_count == 0 }

      remote_ref << ShardingPing.new("after-leave")
      seed_deliveries.receive.should eq("seed/#{entity_id}:after-leave")
    ensure
      peer_system.shutdown
      seed_system.shutdown
    end
  end

  it "rate-limits a live-node join and converges both allocation plans" do
    Movie::Remote::MessageRegistry.register(ShardingPing)
    seed_system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, name: "a-rebalance-seed")
    peer_system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, name: "z-rebalance-peer")
    begin
      seed_remote = seed_system.enable_remoting("127.0.0.1", 0)
      seed_cluster = seed_system.enable_cluster
      seed_sharding = Movie::ClusterSharding.get(seed_system)
      deliveries = Channel(String).new(64)
      blocked_started = Channel(Nil).new(1)
      blocked_release = Channel(Nil).new(1)
      seed_type = seed_sharding.init(
        "RebalancedPing",
        ShardingPing,
        shard_count: 8,
        rebalance: Movie::Cluster::RateLimitedRebalance.new(
          threshold: 1,
          max_concurrent: 1
        )
      ) do |entity_id|
        ShardingPingEntity.new(
          "seed/#{entity_id}",
          deliveries,
          blocked_started,
          blocked_release
        )
      end
      seed_secondary_type = seed_sharding.init(
        "IndependentRebalancedPing",
        ShardingPing,
        shard_count: 8,
        rebalance: Movie::Cluster::RateLimitedRebalance.new(
          threshold: 1,
          max_concurrent: 1
        )
      ) { |entity_id| ShardingPingEntity.new("seed-secondary/#{entity_id}", deliveries) }
      seed_sharding.allocations(seed_type).values
        .should eq([seed_cluster.self_unique_address] * 8)
      moving_id = (0...1_000).map(&.to_s).find do |candidate|
        Movie::Cluster::StableHashPartitioner.new.shard_for(
          "RebalancedPing",
          candidate,
          8
        ) == 1
      end.not_nil!
      moving_ref = seed_sharding.entity_ref_for(seed_type, moving_id)
      moving_ref << ShardingPing.new("block")
      deliveries.receive.should eq("seed/#{moving_id}:block")
      blocked_started.receive

      peer_remote = peer_system.enable_remoting("127.0.0.1", 0)
      peer_cluster = peer_system.enable_cluster(Movie::Cluster::ClusterSettings.new(
        cluster_name: seed_cluster.settings.cluster_name,
        seed_nodes: [seed_remote.address],
        join_retry_interval: 20.milliseconds,
        gossip_interval: 20.milliseconds,
        heartbeat_interval: 25.milliseconds,
        heartbeat_timeout: 500.milliseconds
      ))
      wait_for_sharding do
        seed_cluster.snapshot.members.count(&.status.up?) == 2 &&
          peer_cluster.snapshot.members.count(&.status.up?) == 2 &&
          seed_cluster.converged? && peer_cluster.converged?
      end
      peer_sharding = Movie::ClusterSharding.get(peer_system)
      peer_type = peer_sharding.init(
        "RebalancedPing",
        ShardingPing,
        shard_count: 8,
        rebalance: Movie::Cluster::RateLimitedRebalance.new(
          threshold: 1,
          max_concurrent: 1
        )
      ) { |entity_id| ShardingPingEntity.new("peer/#{entity_id}", deliveries) }
      peer_secondary_type = peer_sharding.init(
        "IndependentRebalancedPing",
        ShardingPing,
        shard_count: 8,
        rebalance: Movie::Cluster::RateLimitedRebalance.new(
          threshold: 1,
          max_concurrent: 1
        )
      ) { |entity_id| ShardingPingEntity.new("peer-secondary/#{entity_id}", deliveries) }

      wait_for_sharding { seed_sharding.handoff_in_progress?(seed_type, moving_id) }
      wait_for_sharding do
        allocations = seed_sharding.allocations(seed_secondary_type)
        allocations == peer_sharding.allocations(peer_secondary_type) &&
          allocations.values.count(seed_cluster.self_unique_address) == 4 &&
          allocations.values.count(peer_cluster.self_unique_address) == 4
      end
      20.times { |sequence| moving_ref << ShardingPing.new(sequence.to_s) }
      blocked_release.send(nil)
      moved_deliveries = [] of String
      deadline = Time.instant + 5.seconds
      while moved_deliveries.size < 20
        remaining = deadline - Time.instant
        if remaining <= Time::Span.zero
          fail("timed out waiting for handoff deliveries: #{moved_deliveries}; handoffs=#{seed_sharding.handoffs_in_progress}; seed_plan=#{seed_sharding.allocations(seed_type).values.tally}; peer_plan=#{peer_sharding.allocations(peer_type).values.tally}; seed_entities=#{seed_sharding.local_entity_count}; peer_entities=#{peer_sharding.local_entity_count}; seed_stats=#{seed_sharding.stats}; peer_stats=#{peer_sharding.stats}")
        end
        select
        when delivery = deliveries.receive
          moved_deliveries << delivery
        when timeout(remaining)
          fail("timed out waiting for handoff deliveries: #{moved_deliveries}; handoffs=#{seed_sharding.handoffs_in_progress}; seed_plan=#{seed_sharding.allocations(seed_type).values.tally}; peer_plan=#{peer_sharding.allocations(peer_type).values.tally}; seed_entities=#{seed_sharding.local_entity_count}; peer_entities=#{peer_sharding.local_entity_count}; seed_stats=#{seed_sharding.stats}; peer_stats=#{peer_sharding.stats}")
        end
      end
      moved_deliveries.should eq(
        Array(String).new(20) { |sequence| "peer/#{moving_id}:#{sequence}" }
      )

      wait_for_sharding do
        allocations = seed_sharding.allocations(seed_type)
        allocations == peer_sharding.allocations(peer_type) &&
          allocations.values.count(seed_cluster.self_unique_address) == 4 &&
          allocations.values.count(peer_cluster.self_unique_address) == 4
      end
      allocations = seed_sharding.allocations(seed_type)
      allocations.values.count(seed_cluster.self_unique_address).should eq(4)
      allocations.values.count(peer_cluster.self_unique_address).should eq(4)
      seed_sharding.stats.rebalance_moves.should eq(8_i64)

      coordinator = {seed_cluster.self_unique_address, peer_cluster.self_unique_address}.min
      target = coordinator == seed_cluster.self_unique_address ? peer_cluster.self_unique_address : seed_cluster.self_unique_address
      source_remote = coordinator == seed_cluster.self_unique_address ? seed_remote : peer_remote
      target_sharding = coordinator == seed_cluster.self_unique_address ? peer_sharding : seed_sharding
      target_type = coordinator == seed_cluster.self_unique_address ? peer_type : seed_type
      target_plan = target_sharding.allocations(target_type)
      stale_plan = target_plan.keys.to_h { |shard_id| {shard_id, coordinator} }
      stale_update = Movie::Cluster::ShardingEnvelope.plan_update(
        "RebalancedPing",
        Movie::Cluster::ShardingSettings.new(
          shard_count: 8,
          rebalance: Movie::Cluster::RateLimitedRebalance.new(
            threshold: 1,
            max_concurrent: 1
          )
        ).configuration_key,
        coordinator,
        1_i64,
        stale_plan
      )
      rejected_before = target_sharding.stats.rejected_envelopes
      source_remote.actor_ref(
        Movie::ActorPath.new(target.address, ["system", Movie::ClusterShardingExtension::DAEMON_NAME]),
        Movie::Cluster::ShardingEnvelope
      ) << stale_update
      wait_for_sharding { target_sharding.stats.rejected_envelopes > rejected_before }
      target_sharding.allocations(target_type).should eq(target_plan)
    ensure
      blocked_release.try &.send(nil)
      peer_system.shutdown
      seed_system.shutdown
    end
  end
end
