module Movie::Cluster
  record ShardingStats,
    routes : Int64,
    local_deliveries : Int64,
    remote_routes : Int64,
    forwards : Int64,
    activations : Int64,
    passivation_requests : Int64,
    explicit_passivations : Int64,
    idle_passivations : Int64,
    handoff_shards : Int64,
    rebalance_moves : Int64,
    lease_acquisitions : Int64,
    lease_renewals : Int64,
    lease_losses : Int64,
    lease_retries : Int64,
    rejected_envelopes : Int64

  class ShardingTelemetry
    @routes = Atomic(Int64).new(0_i64)
    @local_deliveries = Atomic(Int64).new(0_i64)
    @remote_routes = Atomic(Int64).new(0_i64)
    @forwards = Atomic(Int64).new(0_i64)
    @activations = Atomic(Int64).new(0_i64)
    @passivation_requests = Atomic(Int64).new(0_i64)
    @explicit_passivations = Atomic(Int64).new(0_i64)
    @idle_passivations = Atomic(Int64).new(0_i64)
    @handoff_shards = Atomic(Int64).new(0_i64)
    @rebalance_moves = Atomic(Int64).new(0_i64)
    @lease_acquisitions = Atomic(Int64).new(0_i64)
    @lease_renewals = Atomic(Int64).new(0_i64)
    @lease_losses = Atomic(Int64).new(0_i64)
    @lease_retries = Atomic(Int64).new(0_i64)
    @rejected_envelopes = Atomic(Int64).new(0_i64)

    def routed : Nil
      @routes.add(1_i64)
    end

    def delivered_local : Nil
      @local_deliveries.add(1_i64)
    end

    def routed_remote : Nil
      @remote_routes.add(1_i64)
    end

    def forwarded : Nil
      @forwards.add(1_i64)
    end

    def activated : Nil
      @activations.add(1_i64)
    end

    def passivation_requested : Nil
      @passivation_requests.add(1_i64)
    end

    def passivated_explicit : Nil
      @explicit_passivations.add(1_i64)
    end

    def rebalanced(moves : Int32) : Nil
      @rebalance_moves.add(moves.to_i64) if moves > 0
    end

    def handed_off(count : Int32) : Nil
      @handoff_shards.add(count.to_i64) if count > 0
    end

    def passivated_idle(count : Int32) : Nil
      @idle_passivations.add(count.to_i64) if count > 0
    end

    def lease_retried : Nil
      @lease_retries.add(1_i64)
    end

    def lease_acquired : Nil
      @lease_acquisitions.add(1_i64)
    end

    def lease_renewed : Nil
      @lease_renewals.add(1_i64)
    end

    def leases_lost(count : Int32 = 1) : Nil
      @lease_losses.add(count.to_i64) if count > 0
    end

    def envelope_rejected : Nil
      @rejected_envelopes.add(1_i64)
    end

    def snapshot : ShardingStats
      ShardingStats.new(
        @routes.get,
        @local_deliveries.get,
        @remote_routes.get,
        @forwards.get,
        @activations.get,
        @passivation_requests.get,
        @explicit_passivations.get,
        @idle_passivations.get,
        @handoff_shards.get,
        @rebalance_moves.get,
        @lease_acquisitions.get,
        @lease_renewals.get,
        @lease_losses.get,
        @lease_retries.get,
        @rejected_envelopes.get
      )
    end
  end
end
