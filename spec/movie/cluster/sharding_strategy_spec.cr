require "../../spec_helper"
require "../../../src/movie"

private def sharding_member(
  name : String,
  port : Int32,
  roles : Array(String) = [] of String,
) : Movie::Cluster::Member
  unique = Movie::Cluster::UniqueAddress.new(
    Movie::Address.remote(name, "127.0.0.1", port),
    "uid-#{name}"
  )
  Movie::Cluster::Member.new(
    unique,
    Movie::Cluster::MemberStatus::Up,
    roles,
    1_i64,
    unique.node_uid
  )
end

describe Movie::Cluster::StableHashPartitioner do
  it "maps an entity key with a stable cross-process hash" do
    partitioner = Movie::Cluster::StableHashPartitioner.new

    partitioner.shard_for("Order", "order-42", 256).should eq(68)
  end

  it "rejects a non-positive shard count" do
    partitioner = Movie::Cluster::StableHashPartitioner.new

    expect_raises(ArgumentError, "shard count must be positive") do
      partitioner.shard_for("Order", "order-42", 0)
    end
  end
end

describe Movie::Cluster::LeastLoadedAllocation do
  it "balances shards and breaks equal-load ties by unique address" do
    first = sharding_member("a", 2551)
    second = sharding_member("b", 2552)
    strategy = Movie::Cluster::LeastLoadedAllocation.new
    allocations = {} of Int32 => Movie::Cluster::UniqueAddress

    6.times do |shard_id|
      owner = strategy.allocate(shard_id, [second, first], allocations).not_nil!
      allocations[shard_id] = owner
    end

    allocations.values.count(first.unique_address).should eq(3)
    allocations.values.count(second.unique_address).should eq(3)
    allocations[0].should eq(first.unique_address)
  end
end

describe Movie::Cluster::RendezvousAllocation do
  it "only moves existing shards to a member that was added" do
    first = sharding_member("a", 2551)
    second = sharding_member("b", 2552)
    added = sharding_member("c", 2553)
    strategy = Movie::Cluster::RendezvousAllocation.new

    before = (0...128).to_h do |shard_id|
      {shard_id, strategy.allocate(shard_id, [first, second], {} of Int32 => Movie::Cluster::UniqueAddress).not_nil!}
    end
    after = (0...128).to_h do |shard_id|
      {shard_id, strategy.allocate(shard_id, [first, second, added], {} of Int32 => Movie::Cluster::UniqueAddress).not_nil!}
    end

    before.each do |shard_id, owner|
      new_owner = after[shard_id]
      new_owner.should eq(owner) unless new_owner == added.unique_address
    end
    after.values.includes?(added.unique_address).should be_true
  end
end

describe Movie::Cluster::WeightedLeastLoadedAllocation do
  it "allocates proportionally to positive member capacity" do
    small = sharding_member("small", 2551, ["capacity-1"])
    large = sharding_member("large", 2552, ["capacity-3"])
    strategy = Movie::Cluster::WeightedLeastLoadedAllocation.new do |member|
      member.roles.includes?("capacity-3") ? 3 : 1
    end
    allocations = {} of Int32 => Movie::Cluster::UniqueAddress

    40.times do |shard_id|
      allocations[shard_id] = strategy.allocate(shard_id, [small, large], allocations).not_nil!
    end

    allocations.values.count(small.unique_address).should eq(10)
    allocations.values.count(large.unique_address).should eq(30)
  end

  it "rejects a non-positive capacity" do
    member = sharding_member("invalid", 2551)
    strategy = Movie::Cluster::WeightedLeastLoadedAllocation.new { |_candidate| 0 }

    expect_raises(ArgumentError, "member allocation weight must be positive") do
      strategy.allocate(0, [member], {} of Int32 => Movie::Cluster::UniqueAddress)
    end
  end
end

describe Movie::Cluster::RoleAwareAllocation do
  it "constrains a delegated strategy to up members with every required role" do
    frontend = sharding_member("frontend", 2551, ["frontend"])
    backend = sharding_member("backend", 2552, ["backend", "zone-a"])
    strategy = Movie::Cluster::RoleAwareAllocation.new(
      Movie::Cluster::LeastLoadedAllocation.new,
      ["backend", "zone-a"]
    )

    strategy.allocate(
      0,
      [frontend, backend],
      {} of Int32 => Movie::Cluster::UniqueAddress
    ).should eq(backend.unique_address)
  end
end

describe Movie::Cluster::RateLimitedRebalance do
  it "selects no more than the configured number of shards from overloaded owners" do
    first = sharding_member("a", 2551)
    second = sharding_member("b", 2552)
    allocations = (0...6).to_h { |shard_id| {shard_id, first.unique_address} }
    snapshot = Movie::Cluster::AllocationSnapshot.new([first, second], allocations)
    policy = Movie::Cluster::RateLimitedRebalance.new(threshold: 1, max_concurrent: 2)

    policy.select_shards(snapshot).should eq([0, 1])
    Movie::Cluster::NoRebalance.new.select_shards(snapshot).should be_empty
  end
end

describe Movie::Cluster::AllocationPlanner do
  it "applies bounded rebalance steps and converges to the selected strategy" do
    first = sharding_member("a", 2551)
    second = sharding_member("b", 2552)
    planner = Movie::Cluster::AllocationPlanner.new(
      8,
      Movie::Cluster::LeastLoadedAllocation.new,
      Movie::Cluster::RateLimitedRebalance.new(threshold: 1, max_concurrent: 2)
    )
    initial = planner.initial([first])

    step = planner.reconcile(initial, [first, second])
    step.allocations.count { |shard, owner| initial[shard] != owner }.should eq(2)
    step.pending.should be_true

    current = step.allocations
    while (next_step = planner.reconcile(current, [first, second])).pending
      current = next_step.allocations
    end
    current = next_step.allocations
    current.values.count(first.unique_address).should eq(4)
    current.values.count(second.unique_address).should eq(4)
  end

  it "converges to a weighted target instead of stopping at equal raw counts" do
    small = sharding_member("small-dynamic", 2551, ["capacity-1"])
    large = sharding_member("large-dynamic", 2552, ["capacity-3"])
    allocation = Movie::Cluster::WeightedLeastLoadedAllocation.new do |member|
      member.roles.includes?("capacity-3") ? 3 : 1
    end
    planner = Movie::Cluster::AllocationPlanner.new(
      40,
      allocation,
      Movie::Cluster::RateLimitedRebalance.new(threshold: 1, max_concurrent: 3)
    )
    current = planner.initial([small])

    loop do
      step = planner.reconcile(current, [small, large])
      current = step.allocations
      break unless step.pending
    end

    current.values.count(small.unique_address).should eq(10)
    current.values.count(large.unique_address).should eq(30)
  end

  it "forces shards off ineligible owners even when rebalancing is disabled" do
    first = sharding_member("a", 2551)
    second = sharding_member("b", 2552)
    planner = Movie::Cluster::AllocationPlanner.new(
      4,
      Movie::Cluster::LeastLoadedAllocation.new,
      Movie::Cluster::NoRebalance.new
    )
    initial = planner.initial([first, second])

    result = planner.reconcile(initial, [first])

    result.allocations.values.should eq([first.unique_address] * 4)
    result.pending.should be_false
  end
end

describe Movie::Cluster::ShardingSettings do
  it "distinguishes strategy configuration, not only strategy classes" do
    backend = Movie::Cluster::ShardingSettings.new(
      32,
      allocation: Movie::Cluster::RoleAwareAllocation.new(
        Movie::Cluster::LeastLoadedAllocation.new,
        ["backend"]
      ),
      rebalance: Movie::Cluster::RateLimitedRebalance.new(threshold: 1, max_concurrent: 2)
    )
    frontend = Movie::Cluster::ShardingSettings.new(
      32,
      allocation: Movie::Cluster::RoleAwareAllocation.new(
        Movie::Cluster::LeastLoadedAllocation.new,
        ["frontend"]
      ),
      rebalance: Movie::Cluster::RateLimitedRebalance.new(threshold: 1, max_concurrent: 4)
    )

    backend.compatible?(frontend).should be_false
  end
end
