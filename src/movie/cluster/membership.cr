require "./event_bus"
require "./reachability"
require "./telemetry"

module Movie::Cluster
  # Owns membership, reachability, revisions, and transition event emission.
  # Network protocol scheduling remains in ClusterExtension.
  class MembershipCoordinator
    getter self_unique_address : UniqueAddress

    @state : MembershipState
    @revision = Atomic(Int64).new(0_i64)
    @reachability = ReachabilityState.new
    @events = ClusterEventBus.new

    def initialize(
      @self_unique_address : UniqueAddress,
      @roles : Array(String),
      capacity : Int32,
      @telemetry : ClusterTelemetry,
    )
      @state = MembershipState.new(capacity)
    end

    def initialize_self(status : MemberStatus) : Nil
      merge([member_with_status(@self_unique_address, status, @roles)])
    end

    def snapshot : ClusterSnapshot
      @state.snapshot(@self_unique_address, @reachability.unreachable_keys)
    end

    def self_member : Member
      member(@self_unique_address) || raise "cluster self member is missing"
    end

    def member(unique_address : UniqueAddress) : Member?
      @state.member(unique_address)
    end

    def all_members : Array(Member)
      @state.all_members
    end

    def active_members : Array(Member)
      @state.active_members
    end

    def digest : String
      @state.digest
    end

    def up? : Bool
      self_member.status.up?
    end

    def participating? : Bool
      status = self_member.status
      status.up? || status.leaving? || status.exiting?
    end

    def known_active?(sender : UniqueAddress) : Bool
      current = member(sender)
      !current.nil? && !current.status.removed? && !current.status.down?
    end

    def local_leader? : Bool
      snapshot.leader == @self_unique_address
    end

    def departure_coordinator? : Bool
      leader = snapshot.leader
      leader.nil? || leader == @self_unique_address
    end

    def converged?(seen_digests : Hash(String, String)) : Bool
      current_digest = digest
      unreachable = @reachability.unreachable_keys
      active_members.select(&.status.up?).all? do |current|
        current.unique_address == @self_unique_address ||
          unreachable.includes?(current.unique_address.key) ||
          seen_digests[current.unique_address.key]? == current_digest
      end
    end

    def merge(records : Enumerable(Member)) : Int32
      before = member_index(@state.all_members)
      previous_leader = snapshot.leader
      changes = @state.merge(records)
      return 0 if changes == 0

      @telemetry.membership_merged(changes)
      @state.all_members.each do |current|
        previous = before[current.unique_address.key]?
        next if previous && previous.status == current.status
        publish_member_status(current)
      end
      publish_leader_change(previous_leader)
      changes
    end

    def promote_joining_if_leader : Nil
      return unless local_leader?
      active_members.select(&.status.joining?).each do |joining|
        merge([member_with_status(joining.unique_address, MemberStatus::Up, joining.roles)])
      end
    end

    def apply_departure(target : UniqueAddress, status : MemberStatus) : Bool
      current = member(target)
      return false unless current
      return false if current.status.precedence >= status.precedence

      merge([member_with_status(current.unique_address, status, current.roles)]) > 0
    end

    def advance_departures : Array(UniqueAddress)
      removed = [] of UniqueAddress
      active_members.each do |current|
        next_status = case current.status
                      when .leaving?         then MemberStatus::Exiting
                      when .exiting?, .down? then MemberStatus::Removed
                      else                        next
                      end
        if apply_departure(current.unique_address, next_status) && next_status.removed?
          removed << current.unique_address
        end
      end
      removed
    end

    def check_reachability(peers : Array(Member), timeout_span : Time::Span) : Nil
      previous_leader = snapshot.leader
      newly_unreachable = @reachability.check(peers, timeout_span)
      @telemetry.heartbeat_timed_out(newly_unreachable.size)
      newly_unreachable.each do |current|
        @events.publish(ClusterEvent.member(ClusterEvent::Kind::UnreachableMember, current))
      end
      publish_leader_change(previous_leader) unless newly_unreachable.empty?
    end

    def mark_reachable(sender : UniqueAddress) : Nil
      previous_leader = snapshot.leader
      return unless @reachability.mark_reachable(sender)

      @telemetry.reachability_restored
      if current = member(sender)
        @events.publish(ClusterEvent.member(ClusterEvent::Kind::ReachableMember, current))
      end
      publish_leader_change(previous_leader)
    end

    def subscribe(subscriber : Movie::ActorRef(ClusterEvent), replay_state : Bool) : Nil
      @events.subscribe(subscriber)
      subscriber << ClusterEvent.current_state(snapshot) if replay_state
    end

    def unsubscribe(subscriber : Movie::ActorRef(ClusterEvent)) : Nil
      @events.unsubscribe(subscriber)
    end

    def subscriber_count : Int32
      @events.size
    end

    private def member_with_status(unique_address : UniqueAddress, status : MemberStatus, roles : Array(String)) : Member
      Member.new(
        unique_address,
        status,
        roles,
        next_revision,
        @self_unique_address.node_uid
      )
    end

    private def next_revision : Int64
      @revision.add(1) + 1
    end

    private def member_index(members : Array(Member)) : Hash(String, Member)
      members.each_with_object({} of String => Member) do |current, index|
        index[current.unique_address.key] = current
      end
    end

    private def publish_member_status(member : Member) : Nil
      kind = case member.status
             when .joining? then ClusterEvent::Kind::MemberJoined
             when .up?      then ClusterEvent::Kind::MemberUp
             when .leaving? then ClusterEvent::Kind::MemberLeaving
             when .exiting? then ClusterEvent::Kind::MemberExiting
             when .down?    then ClusterEvent::Kind::MemberDown
             when .removed? then ClusterEvent::Kind::MemberRemoved
             else                return
             end
      @events.publish(ClusterEvent.member(kind, member))
    end

    private def publish_leader_change(previous : UniqueAddress?) : Nil
      current = snapshot.leader
      @events.publish(ClusterEvent.leader_changed(previous, current)) unless current == previous
    end
  end
end
