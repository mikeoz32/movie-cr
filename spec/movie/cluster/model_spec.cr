require "../../spec_helper"
require "../../../src/movie"

private def cluster_member(
  system : String,
  port : Int32,
  node_uid : String,
  status : Movie::Cluster::MemberStatus,
  revision : Int64,
  changed_by : String = node_uid,
  roles : Array(String) = [] of String,
) : Movie::Cluster::Member
  unique = Movie::Cluster::UniqueAddress.new(
    Movie::Address.remote(system, "127.0.0.1", port),
    node_uid
  )
  Movie::Cluster::Member.new(unique, status, roles, revision, changed_by)
end

describe Movie::Cluster::MembershipState do
  it "orders process incarnations by address and node UID" do
    first = cluster_member("a", 2551, "node-b", Movie::Cluster::MemberStatus::Up, 1).unique_address
    second = cluster_member("b", 2551, "node-a", Movie::Cluster::MemberStatus::Up, 1).unique_address
    earlier_uid = cluster_member("a", 2551, "node-a", Movie::Cluster::MemberStatus::Up, 1).unique_address

    [second, first, earlier_uid].sort.should eq([earlier_uid, first, second])
    Movie::Cluster::UniqueAddress.parse(first.to_s).should eq(first)
  end

  it "converges monotonically and never resurrects a removed incarnation" do
    joining = cluster_member("node", 2551, "uid-1", Movie::Cluster::MemberStatus::Joining, 1)
    up = cluster_member("node", 2551, "uid-1", Movie::Cluster::MemberStatus::Up, 2)
    removed = cluster_member("node", 2551, "uid-1", Movie::Cluster::MemberStatus::Removed, 3)
    stale_up = cluster_member("node", 2551, "uid-1", Movie::Cluster::MemberStatus::Up, 99)
    state = Movie::Cluster::MembershipState.new

    state.merge([up, joining]).should eq(1)
    state.member(up.unique_address).should eq(up)
    state.merge([removed]).should eq(1)
    state.merge([stale_up]).should eq(0)
    state.member(up.unique_address).not_nil!.status.removed?.should be_true
  end

  it "retains a restarted process as a distinct incarnation at the same address" do
    old_member = cluster_member("node", 2551, "uid-old", Movie::Cluster::MemberStatus::Removed, 5)
    restarted = cluster_member("node", 2551, "uid-new", Movie::Cluster::MemberStatus::Joining, 1)
    state = Movie::Cluster::MembershipState.new

    state.merge([old_member, restarted]).should eq(2)
    state.all_members.size.should eq(2)
    state.active_members.should eq([restarted])
  end

  it "elects the lowest reachable up member and returns defensive snapshots" do
    local = cluster_member("node-c", 2553, "uid-c", Movie::Cluster::MemberStatus::Up, 1, roles: ["worker"])
    lowest = cluster_member("node-a", 2551, "uid-a", Movie::Cluster::MemberStatus::Up, 1, roles: ["seed"])
    unreachable = cluster_member("node-b", 2552, "uid-b", Movie::Cluster::MemberStatus::Up, 1)
    joining = cluster_member("node-0", 2550, "uid-0", Movie::Cluster::MemberStatus::Joining, 1)
    state = Movie::Cluster::MembershipState.new
    state.merge([local, lowest, unreachable, joining])
    unreachable_addresses = Set{unreachable.unique_address}

    snapshot = state.snapshot(local.unique_address, unreachable_addresses)
    snapshot.leader.should eq(lowest.unique_address)
    snapshot.unreachable.should eq([unreachable.unique_address])
    snapshot.members.map(&.unique_address).should eq([
      joining.unique_address,
      lowest.unique_address,
      unreachable.unique_address,
      local.unique_address,
    ])

    exposed_members = snapshot.members
    exposed_members.clear
    snapshot.members.size.should eq(4)
    exposed_roles = local.roles
    exposed_roles << "mutated"
    local.roles.should eq(["worker"])
  end

  it "breaks equal-status revision ties by change origin" do
    left = cluster_member("node", 2551, "uid", Movie::Cluster::MemberStatus::Up, 7, "node-a", ["left"])
    right = cluster_member("node", 2551, "uid", Movie::Cluster::MemberStatus::Up, 7, "node-z", ["right"])
    first = Movie::Cluster::MembershipState.new
    second = Movie::Cluster::MembershipState.new

    first.merge([left, right])
    second.merge([right, left])
    first.member(left.unique_address).should eq(right)
    second.member(left.unique_address).should eq(right)
  end

  it "produces an order-independent digest and rejects over-capacity gossip atomically" do
    first_member = cluster_member("a", 2551, "uid-a", Movie::Cluster::MemberStatus::Up, 1)
    second_member = cluster_member("b", 2552, "uid-b", Movie::Cluster::MemberStatus::Joining, 1)
    first = Movie::Cluster::MembershipState.new(capacity: 2)
    second = Movie::Cluster::MembershipState.new(capacity: 2)

    first.merge([first_member, second_member])
    second.merge([second_member, first_member])
    first.digest.should eq(second.digest)

    overflow = cluster_member("c", 2553, "uid-c", Movie::Cluster::MemberStatus::Up, 1)
    expect_raises(Movie::Cluster::MembershipCapacityError) do
      first.merge([overflow])
    end
    first.all_members.should eq([first_member, second_member])
  end
end
