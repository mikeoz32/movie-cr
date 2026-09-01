require "json"
require "./model"

module Movie::Cluster
  struct ProtocolMessage
    include JSON::Serializable

    enum Kind
      Join
      Welcome
      JoinTick
    end

    getter kind : Kind
    getter cluster_name : String
    getter sender : UniqueAddress
    getter member : Member?

    @members : Array(Member)

    def initialize(
      @kind : Kind,
      @cluster_name : String,
      @sender : UniqueAddress,
      @member : Member? = nil,
      members : Array(Member) = [] of Member,
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
  end
end
