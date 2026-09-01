module Movie::Cluster
  class ClusterConfigurationError < Exception
  end

  class ClusterSettings
    getter cluster_name : String
    getter join_retry_interval : Time::Span
    getter gossip_interval : Time::Span
    getter gossip_fanout : Int32
    getter max_members : Int32
    getter heartbeat_interval : Time::Span
    getter heartbeat_timeout : Time::Span

    @seed_nodes : Array(Movie::Address)
    @roles : Array(String)

    def initialize(
      @cluster_name : String = "movie-cluster",
      seed_nodes : Array(Movie::Address) = [] of Movie::Address,
      roles : Array(String) = [] of String,
      @join_retry_interval : Time::Span = 1.second,
      @gossip_interval : Time::Span = 1.second,
      @gossip_fanout : Int32 = 3,
      @max_members : Int32 = 10_000,
      @heartbeat_interval : Time::Span = 1.second,
      @heartbeat_timeout : Time::Span = 5.seconds,
    )
      raise ArgumentError.new("cluster name must not be empty") if @cluster_name.empty?
      raise ArgumentError.new("cluster join retry interval must be positive") unless @join_retry_interval > Time::Span.zero
      raise ArgumentError.new("cluster gossip interval must be positive") unless @gossip_interval > Time::Span.zero
      raise ArgumentError.new("cluster gossip fanout must be positive") unless @gossip_fanout > 0
      raise ArgumentError.new("cluster maximum members must be positive") unless @max_members > 0
      raise ArgumentError.new("cluster heartbeat interval must be positive") unless @heartbeat_interval > Time::Span.zero
      raise ArgumentError.new("cluster heartbeat timeout must exceed its interval") unless @heartbeat_timeout > @heartbeat_interval
      raise ArgumentError.new("cluster seed nodes must use remote addresses") unless seed_nodes.all?(&.remote?)
      raise ArgumentError.new("cluster roles must not be empty") if roles.any?(&.empty?)
      @seed_nodes = seed_nodes.uniq
      @roles = roles.uniq.sort
    end

    def seed_nodes : Array(Movie::Address)
      @seed_nodes.dup
    end

    def roles : Array(String)
      @roles.dup
    end
  end
end
