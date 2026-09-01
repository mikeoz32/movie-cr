require "json"
require "set"
require "digest/sha256"
require "../path"

module Movie::Cluster
  class MembershipCapacityError < Exception
  end

  struct UniqueAddress
    include JSON::Serializable
    include Comparable(self)

    getter address : Movie::Address
    getter node_uid : String

    def initialize(@address : Movie::Address, @node_uid : String)
      raise ArgumentError.new("cluster address must be remote") unless @address.remote?
      raise ArgumentError.new("cluster node UID must not be empty") if @node_uid.empty?
    end

    def to_s(io : IO) : Nil
      io << @address << '#' << @node_uid
    end

    def to_s : String
      String.build { |io| to_s(io) }
    end

    def self.parse(value : String) : self
      separator = value.rindex('#') || raise ArgumentError.new("invalid unique address: #{value}")
      address = Movie::Address.parse(value.byte_slice(0, separator))
      node_uid = value.byte_slice(separator + 1, value.bytesize - separator - 1)
      new(address, node_uid)
    end

    def <=>(other : self) : Int32
      comparison = @address.to_s <=> other.address.to_s
      comparison == 0 ? @node_uid <=> other.node_uid : comparison
    end
  end

  enum MemberStatus
    Joining
    Up
    Leaving
    Exiting
    Down
    Removed

    def precedence : Int32
      value
    end

    def leader_candidate? : Bool
      up? || leaving?
    end
  end

  struct Member
    include JSON::Serializable

    getter unique_address : UniqueAddress
    getter status : MemberStatus
    getter revision : Int64
    getter changed_by : String

    @roles : Array(String)

    def initialize(
      @unique_address : UniqueAddress,
      @status : MemberStatus,
      roles : Array(String),
      @revision : Int64,
      @changed_by : String,
    )
      raise ArgumentError.new("member revision must be positive") unless @revision > 0
      raise ArgumentError.new("member change origin must not be empty") if @changed_by.empty?
      @roles = roles.uniq.sort
    end

    def roles : Array(String)
      @roles.dup
    end

    def ==(other : self) : Bool
      @unique_address == other.unique_address &&
        @status == other.status &&
        @roles == other.@roles &&
        @revision == other.revision &&
        @changed_by == other.changed_by
    end

    protected def deterministic_tie_break : String
      String.build do |io|
        io << @changed_by << '\0'
        @roles.each { |role| io << role << '\0' }
      end
    end
  end

  class ClusterSnapshot
    @members : Array(Member)
    @unreachable : Array(UniqueAddress)

    getter self_unique_address : UniqueAddress
    getter leader : UniqueAddress?

    def initialize(
      @self_unique_address : UniqueAddress,
      members : Array(Member),
      unreachable : Array(UniqueAddress),
      @leader : UniqueAddress?,
    )
      @members = members.dup
      @unreachable = unreachable.dup
    end

    def members : Array(Member)
      @members.dup
    end

    def unreachable : Array(UniqueAddress)
      @unreachable.dup
    end

    def member(unique_address : UniqueAddress) : Member?
      @members.find { |member| member.unique_address == unique_address }
    end
  end

  # Thread-safe convergent member-record store. Reachability remains local to
  # each observer and is supplied only when a public snapshot is materialized.
  class MembershipState
    @members = {} of UniqueAddress => Member
    @mutex = Mutex.new

    def initialize(@capacity : Int32 = 10_000)
      raise ArgumentError.new("membership capacity must be positive") unless @capacity > 0
    end

    def merge(incoming : Enumerable(Member)) : Int32
      records = incoming.to_a
      @mutex.synchronize do
        additions = records
          .map(&.unique_address)
          .uniq
          .count { |unique_address| !@members.has_key?(unique_address) }
        if @members.size + additions > @capacity
          raise MembershipCapacityError.new("membership capacity #{@capacity} is exhausted")
        end

        changes = 0
        records.each do |candidate|
          unique_address = candidate.unique_address
          current = @members[unique_address]?
          if current.nil? || supersedes?(candidate, current)
            @members[unique_address] = candidate
            changes += 1
          end
        end
        changes
      end
    end

    def member(unique_address : UniqueAddress) : Member?
      @mutex.synchronize { @members[unique_address]? }
    end

    def all_members : Array(Member)
      @mutex.synchronize { sorted(@members.values) }
    end

    def active_members : Array(Member)
      @mutex.synchronize { sorted(@members.values.reject(&.status.removed?)) }
    end

    def snapshot(self_unique_address : UniqueAddress, unreachable : Set(UniqueAddress)) : ClusterSnapshot
      @mutex.synchronize do
        active = sorted(@members.values.reject(&.status.removed?))
        unreachable_members = active
          .select { |member| unreachable.includes?(member.unique_address) }
          .map(&.unique_address)
          .sort
        candidates = active.select do |member|
          member.status.leader_candidate? && !unreachable.includes?(member.unique_address)
        end
        leader = candidates.min_by?(&.unique_address).try(&.unique_address)
        ClusterSnapshot.new(self_unique_address, active, unreachable_members, leader)
      end
    end

    def digest : String
      members = all_members
      canonical = String.build do |io|
        members.each do |member|
          io << member.unique_address << '\0'
          io << member.status.value << '\0'
          io << member.revision << '\0'
          io << member.changed_by << '\0'
          member.roles.each { |role| io << role << '\0' }
          io << '\n'
        end
      end
      Digest::SHA256.hexdigest(canonical)
    end

    private def supersedes?(candidate : Member, current : Member) : Bool
      status_comparison = candidate.status.precedence <=> current.status.precedence
      return status_comparison > 0 unless status_comparison == 0

      revision_comparison = candidate.revision <=> current.revision
      return revision_comparison > 0 unless revision_comparison == 0

      candidate.deterministic_tie_break > current.deterministic_tie_break
    end

    private def sorted(members : Array(Member)) : Array(Member)
      members.sort_by(&.unique_address)
    end
  end
end
