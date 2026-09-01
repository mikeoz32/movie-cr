require "json"
require "./model"

module Movie::Cluster
  CLUSTER_DAEMON_NAME = "cluster"

  struct ProtocolMessage
    include JSON::Serializable

    enum Kind
      Join
      Welcome
      JoinTick
      Gossip
      GossipAck
      GossipTick
      Heartbeat
      HeartbeatAck
      HeartbeatTick
      LeaveRequest
      DownRequest
    end

    getter kind : Kind
    getter cluster_name : String
    getter sender : UniqueAddress
    getter member : Member?
    getter gossip_round : Int64?
    getter heartbeat_sequence : Int64?
    getter digest : String?
    getter target : UniqueAddress?

    @members : Array(Member)

    def initialize(
      @kind : Kind,
      @cluster_name : String,
      @sender : UniqueAddress,
      @member : Member? = nil,
      members : Array(Member) = [] of Member,
      @gossip_round : Int64? = nil,
      @heartbeat_sequence : Int64? = nil,
      @digest : String? = nil,
      @target : UniqueAddress? = nil,
    )
      @members = members.dup
    end

    def members : Array(Member)
      @members.dup
    end

    def self.join(cluster_name : String, sender : UniqueAddress, member : Member) : self
      new(Kind::Join, cluster_name, sender, member)
    end

    def self.welcome(cluster_name : String, sender : UniqueAddress, members : Array(Member)) : self
      new(Kind::Welcome, cluster_name, sender, members: members)
    end

    def self.join_tick(cluster_name : String, sender : UniqueAddress) : self
      new(Kind::JoinTick, cluster_name, sender)
    end

    def self.gossip(
      cluster_name : String,
      sender : UniqueAddress,
      members : Array(Member),
      round : Int64,
      digest : String,
    ) : self
      new(Kind::Gossip, cluster_name, sender, members: members, gossip_round: round, digest: digest)
    end

    def self.gossip_ack(cluster_name : String, sender : UniqueAddress, round : Int64, digest : String) : self
      new(Kind::GossipAck, cluster_name, sender, gossip_round: round, digest: digest)
    end

    def self.gossip_tick(cluster_name : String, sender : UniqueAddress) : self
      new(Kind::GossipTick, cluster_name, sender)
    end

    def self.heartbeat(cluster_name : String, sender : UniqueAddress, sequence : Int64) : self
      new(Kind::Heartbeat, cluster_name, sender, heartbeat_sequence: sequence)
    end

    def self.heartbeat_ack(cluster_name : String, sender : UniqueAddress, sequence : Int64) : self
      new(Kind::HeartbeatAck, cluster_name, sender, heartbeat_sequence: sequence)
    end

    def self.heartbeat_tick(cluster_name : String, sender : UniqueAddress) : self
      new(Kind::HeartbeatTick, cluster_name, sender)
    end

    def self.leave_request(cluster_name : String, sender : UniqueAddress) : self
      new(Kind::LeaveRequest, cluster_name, sender, target: sender)
    end

    def self.down_request(cluster_name : String, sender : UniqueAddress, target : UniqueAddress) : self
      new(Kind::DownRequest, cluster_name, sender, target: target)
    end
  end
end
