module Movie::Cluster
  record ClusterStats,
    join_attempts : Int64,
    gossip_rounds : Int64,
    gossip_sent : Int64,
    gossip_received : Int64,
    gossip_acks : Int64,
    membership_merges : Int64,
    heartbeats_sent : Int64,
    heartbeats_received : Int64,
    heartbeat_timeouts : Int64,
    reachability_restorations : Int64,
    protocol_rejections : Int64,
    membership_capacity_rejections : Int64,
    subscribers : Int32

  class ClusterTelemetry
    @join_attempts = Atomic(Int64).new(0_i64)
    @gossip_rounds = Atomic(Int64).new(0_i64)
    @gossip_sent = Atomic(Int64).new(0_i64)
    @gossip_received = Atomic(Int64).new(0_i64)
    @gossip_acks = Atomic(Int64).new(0_i64)
    @membership_merges = Atomic(Int64).new(0_i64)
    @heartbeats_sent = Atomic(Int64).new(0_i64)
    @heartbeats_received = Atomic(Int64).new(0_i64)
    @heartbeat_timeouts = Atomic(Int64).new(0_i64)
    @reachability_restorations = Atomic(Int64).new(0_i64)
    @protocol_rejections = Atomic(Int64).new(0_i64)
    @membership_capacity_rejections = Atomic(Int64).new(0_i64)

    def join_attempt : Nil
      @join_attempts.add(1)
    end

    def next_gossip_round : Int64
      @gossip_rounds.add(1) + 1
    end

    def gossip_sent : Nil
      @gossip_sent.add(1)
    end

    def gossip_received : Nil
      @gossip_received.add(1)
    end

    def gossip_ack : Nil
      @gossip_acks.add(1)
    end

    def membership_merged(changes : Int32) : Nil
      @membership_merges.add(changes.to_i64) if changes > 0
    end

    def heartbeat_sent : Nil
      @heartbeats_sent.add(1)
    end

    def heartbeat_received : Nil
      @heartbeats_received.add(1)
    end

    def heartbeat_timed_out(count : Int32) : Nil
      @heartbeat_timeouts.add(count.to_i64) if count > 0
    end

    def reachability_restored : Nil
      @reachability_restorations.add(1)
    end

    def protocol_rejected : Nil
      @protocol_rejections.add(1)
    end

    def membership_capacity_rejected : Nil
      @membership_capacity_rejections.add(1)
    end

    def snapshot(subscribers : Int32) : ClusterStats
      ClusterStats.new(
        join_attempts: @join_attempts.get,
        gossip_rounds: @gossip_rounds.get,
        gossip_sent: @gossip_sent.get,
        gossip_received: @gossip_received.get,
        gossip_acks: @gossip_acks.get,
        membership_merges: @membership_merges.get,
        heartbeats_sent: @heartbeats_sent.get,
        heartbeats_received: @heartbeats_received.get,
        heartbeat_timeouts: @heartbeat_timeouts.get,
        reachability_restorations: @reachability_restorations.get,
        protocol_rejections: @protocol_rejections.get,
        membership_capacity_rejections: @membership_capacity_rejections.get,
        subscribers: subscribers
      )
    end
  end
end
