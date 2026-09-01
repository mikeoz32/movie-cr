module Movie::Cluster
  alias ShardAllocations = Hash(Int32, UniqueAddress)

  # Maps a logical entity key to a stable shard number. Implementations must
  # return the same result in every process and across runtime restarts.
  abstract class EntityPartitioner
    abstract def shard_for(entity_type : String, entity_id : String, shard_count : Int32) : Int32

    def configuration_key : String
      self.class.name
    end

    protected def validate_shard_count(shard_count : Int32) : Nil
      raise ArgumentError.new("shard count must be positive") unless shard_count > 0
    end
  end

  # Allocation-independent FNV-1a partitioning. Crystal's runtime String#hash
  # is deliberately avoided because actor-system processes must agree on the
  # shard for an entity key.
  class StableHashPartitioner < EntityPartitioner
    OFFSET_BASIS = 14_695_981_039_346_656_037_u64
    PRIME        =          1_099_511_628_211_u64

    def shard_for(entity_type : String, entity_id : String, shard_count : Int32) : Int32
      validate_shard_count(shard_count)
      hash = OFFSET_BASIS
      entity_type.to_slice.each { |byte| hash = mix(hash, byte) }
      hash = mix(hash, 0_u8)
      entity_id.to_slice.each { |byte| hash = mix(hash, byte) }
      (hash % shard_count.to_u64).to_i32
    end

    private def mix(hash : UInt64, byte : UInt8) : UInt64
      (hash ^ byte.to_u64) &* PRIME
    end
  end

  # Chooses an owner for a shard from the currently eligible membership.
  # Implementations are invoked in shard-id order when a complete allocation
  # plan is built, so strategies may use the allocations already selected.
  abstract class ShardAllocationStrategy
    abstract def allocate(
      shard_id : Int32,
      members : Array(Member),
      current_allocations : ShardAllocations,
    ) : UniqueAddress?

    def configuration_key : String
      self.class.name
    end

    protected def eligible_members(members : Array(Member)) : Array(Member)
      members.select(&.status.up?).sort_by(&.unique_address)
    end
  end

  class LeastLoadedAllocation < ShardAllocationStrategy
    def allocate(
      shard_id : Int32,
      members : Array(Member),
      current_allocations : ShardAllocations,
    ) : UniqueAddress?
      candidates = eligible_members(members)
      candidates.min_by? do |member|
        {current_allocations.values.count(member.unique_address), member.unique_address}
      end.try(&.unique_address)
    end
  end

  # Highest-random-weight hashing keeps every shard on its previous owner when
  # another member is added, except for shards won by the new member.
  class RendezvousAllocation < ShardAllocationStrategy
    def allocate(
      shard_id : Int32,
      members : Array(Member),
      current_allocations : ShardAllocations,
    ) : UniqueAddress?
      eligible_members(members).max_by? do |member|
        {score(shard_id, member.unique_address), member.unique_address}
      end.try(&.unique_address)
    end

    private def score(shard_id : Int32, address : UniqueAddress) : UInt64
      hash = StableHashPartitioner::OFFSET_BASIS
      value = shard_id.to_u32
      4.times do |shift|
        byte = ((value >> (shift * 8)) & 0xff).to_u8
        hash = (hash ^ byte.to_u64) &* StableHashPartitioner::PRIME
      end
      hash = mix(hash, address.address.protocol)
      hash = mix(hash, address.address.system)
      hash = mix(hash, address.address.host || "")
      port = address.address.port || 0
      4.times do |shift|
        byte = ((port.to_u32 >> (shift * 8)) & 0xff).to_u8
        hash = (hash ^ byte.to_u64) &* StableHashPartitioner::PRIME
      end
      mix(hash, address.node_uid)
    end

    private def mix(hash : UInt64, value : String) : UInt64
      current = hash
      value.to_slice.each do |byte|
        current = (current ^ byte.to_u64) &* StableHashPartitioner::PRIME
      end
      (current ^ 0_u64) &* StableHashPartitioner::PRIME
    end
  end

  # Balances load relative to caller-supplied positive capacity weights.
  class WeightedLeastLoadedAllocation < ShardAllocationStrategy
    def initialize(@strategy_id : String = "default", &@weight_for : Member -> Int32)
      raise ArgumentError.new("weighted allocation strategy id must not be empty") if @strategy_id.empty?
    end

    def configuration_key : String
      "#{self.class.name}:#{@strategy_id}"
    end

    def allocate(
      shard_id : Int32,
      members : Array(Member),
      current_allocations : ShardAllocations,
    ) : UniqueAddress?
      candidates = eligible_members(members)
      weighted = candidates.map do |member|
        weight = @weight_for.call(member)
        raise ArgumentError.new("member allocation weight must be positive") unless weight > 0
        {member, weight}
      end
      weighted.min_by? do |member, weight|
        load = current_allocations.values.count(member.unique_address)
        {load.to_f64 / weight, member.unique_address}
      end.try(&.[0].unique_address)
    end
  end

  # Eligibility decorator that requires every configured role before invoking
  # the underlying placement algorithm.
  class RoleAwareAllocation < ShardAllocationStrategy
    def initialize(@delegate : ShardAllocationStrategy, roles : Array(String))
      raise ArgumentError.new("required allocation roles must not be empty") if roles.any?(&.empty?)
      @roles = roles.uniq.sort
    end

    def configuration_key : String
      String.build do |io|
        io << self.class.name << ':' << @delegate.configuration_key
        @roles.each { |role| io << ':' << role }
      end
    end

    def allocate(
      shard_id : Int32,
      members : Array(Member),
      current_allocations : ShardAllocations,
    ) : UniqueAddress?
      eligible = members.select do |member|
        roles = member.roles
        @roles.all? { |role| roles.includes?(role) }
      end
      @delegate.allocate(shard_id, eligible, current_allocations)
    end
  end

  class AllocationSnapshot
    getter members : Array(Member)
    getter allocations : ShardAllocations
    getter target_allocations : ShardAllocations?

    def initialize(
      members : Array(Member),
      allocations : ShardAllocations,
      target_allocations : ShardAllocations? = nil,
    )
      @members = members.dup
      @allocations = allocations.dup
      @target_allocations = target_allocations.try(&.dup)
    end
  end

  abstract class RebalancePolicy
    abstract def select_shards(snapshot : AllocationSnapshot) : Array(Int32)

    def configuration_key : String
      self.class.name
    end
  end

  class NoRebalance < RebalancePolicy
    def select_shards(snapshot : AllocationSnapshot) : Array(Int32)
      [] of Int32
    end
  end

  # Selects deterministic shards from overloaded owners without allowing a
  # membership change to move an unbounded number at once.
  class RateLimitedRebalance < RebalancePolicy
    def initialize(@threshold : Int32 = 1, @max_concurrent : Int32 = 4)
      raise ArgumentError.new("rebalance threshold must be non-negative") if @threshold < 0
      raise ArgumentError.new("maximum concurrent rebalances must be positive") unless @max_concurrent > 0
    end

    def configuration_key : String
      "#{self.class.name}:#{@threshold}:#{@max_concurrent}"
    end

    def select_shards(snapshot : AllocationSnapshot) : Array(Int32)
      owners = snapshot.members.select(&.status.up?).map(&.unique_address).sort
      return [] of Int32 if owners.size < 2

      if target = snapshot.target_allocations
        misplaced = snapshot.allocations.keys.sort.select do |shard_id|
          snapshot.allocations[shard_id]? != target[shard_id]?
        end
        return [] of Int32 if misplaced.empty?

        target_loads = owners.to_h { |owner| {owner, 0} }
        target.each_value { |owner| target_loads[owner] += 1 if target_loads.has_key?(owner) }
        target_range = target_loads.values.max - target_loads.values.min
        if target_range <= 1
          current_loads = owners.to_h { |owner| {owner, 0} }
          snapshot.allocations.each_value do |owner|
            current_loads[owner] += 1 if current_loads.has_key?(owner)
          end
          current_range = current_loads.values.max - current_loads.values.min
          return [] of Int32 if current_range <= @threshold
        end
        return misplaced.first(@max_concurrent)
      end

      loads = owners.to_h { |owner| {owner, 0} }
      snapshot.allocations.each_value do |owner|
        loads[owner] += 1 if loads.has_key?(owner)
      end

      selected = [] of Int32
      while selected.size < @max_concurrent
        most = owners.max_by { |owner| {loads[owner], owner} }
        least = owners.min_by { |owner| {loads[owner], owner} }
        break if loads[most] - loads[least] <= @threshold

        shard_id = snapshot.allocations.keys.sort.find do |candidate|
          snapshot.allocations[candidate] == most && !selected.includes?(candidate)
        end
        break unless shard_id
        selected << shard_id
        loads[most] -= 1
        loads[least] += 1
      end
      selected
    end
  end

  record AllocationPlanResult, allocations : ShardAllocations, pending : Bool

  # Builds the deterministic target allocation selected by a strategy, then
  # advances an existing allocation by the number of moves permitted by the
  # independent rebalance policy. Shards on ineligible owners always move.
  class AllocationPlanner
    def initialize(
      @shard_count : Int32,
      @allocation : ShardAllocationStrategy,
      @rebalance : RebalancePolicy,
    )
      raise ArgumentError.new("shard count must be positive") unless @shard_count > 0
    end

    def initial(members : Array(Member)) : ShardAllocations
      desired(members)
    end

    def reconcile(
      current : ShardAllocations,
      members : Array(Member),
    ) : AllocationPlanResult
      target = desired(members)
      eligible = members.select(&.status.up?).map(&.unique_address).to_set
      next_allocations = current.dup

      @shard_count.times do |shard_id|
        owner = next_allocations[shard_id]?
        next if owner && eligible.includes?(owner)
        if target_owner = target[shard_id]?
          next_allocations[shard_id] = target_owner
        else
          next_allocations.delete(shard_id)
        end
      end

      voluntary = target.keys.select do |shard_id|
        current_owner = next_allocations[shard_id]?
        current_owner && current_owner != target[shard_id]
      end.sort
      snapshot = AllocationSnapshot.new(members, next_allocations, target)
      suggested = @rebalance.select_shards(snapshot)
      allowance = suggested.size
      selected = suggested.select { |shard_id| voluntary.includes?(shard_id) }
      if selected.size < allowance
        voluntary.each do |shard_id|
          break if selected.size >= allowance
          selected << shard_id unless selected.includes?(shard_id)
        end
      end
      selected.each { |shard_id| next_allocations[shard_id] = target[shard_id] }

      remaining = target.any? do |shard_id, owner|
        next_allocations[shard_id]? != owner
      end
      AllocationPlanResult.new(next_allocations, remaining && allowance > 0)
    end

    private def desired(members : Array(Member)) : ShardAllocations
      allocations = {} of Int32 => UniqueAddress
      @shard_count.times do |shard_id|
        if owner = @allocation.allocate(shard_id, members, allocations)
          allocations[shard_id] = owner
        end
      end
      allocations
    end
  end
end
