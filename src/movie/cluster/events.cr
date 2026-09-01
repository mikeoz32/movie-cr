require "./model"

module Movie::Cluster
  struct ClusterEvent
    enum Kind
      CurrentState
      MemberJoined
      MemberUp
      MemberLeaving
      MemberExiting
      MemberDown
      MemberRemoved
      UnreachableMember
      ReachableMember
      LeaderChanged
    end

    getter kind : Kind
    getter member : Member?
    getter snapshot : ClusterSnapshot?
    getter previous_leader : UniqueAddress?
    getter leader : UniqueAddress?

    def initialize(
      @kind : Kind,
      @member : Member? = nil,
      @snapshot : ClusterSnapshot? = nil,
      @previous_leader : UniqueAddress? = nil,
      @leader : UniqueAddress? = nil,
    )
    end

    def self.current_state(snapshot : ClusterSnapshot) : self
      new(Kind::CurrentState, snapshot: snapshot)
    end

    def self.member(kind : Kind, member : Member) : self
      new(kind, member: member)
    end

    def self.leader_changed(previous : UniqueAddress?, current : UniqueAddress?) : self
      new(Kind::LeaderChanged, previous_leader: previous, leader: current)
    end
  end
end
