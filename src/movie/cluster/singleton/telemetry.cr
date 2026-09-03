module Movie::Cluster
  record SingletonStats,
    registrations : Int64,
    activation_attempts : Int64,
    activations : Int64,
    reactivations : Int64,
    activation_failures : Int64,
    tells : Int64,
    asks : Int64,
    stop_requests : Int64,
    routing_rejections : Int64,
    owner_changes : Int64,
    shared_sharding_handoffs_in_progress : Int32,
    shared_sharding_handoffs_completed : Int64,
    shared_sharding_rejected_envelopes : Int64,
    shared_sharding_lease_acquisitions : Int64,
    shared_sharding_lease_renewals : Int64,
    shared_sharding_lease_losses : Int64,
    shared_sharding_lease_retries : Int64

  class SingletonTelemetry
    @registrations = Atomic(Int64).new(0_i64)
    @activation_attempts = Atomic(Int64).new(0_i64)
    @activations = Atomic(Int64).new(0_i64)
    @reactivations = Atomic(Int64).new(0_i64)
    @activation_failures = Atomic(Int64).new(0_i64)
    @tells = Atomic(Int64).new(0_i64)
    @asks = Atomic(Int64).new(0_i64)
    @stop_requests = Atomic(Int64).new(0_i64)
    @routing_rejections = Atomic(Int64).new(0_i64)
    @owner_changes = Atomic(Int64).new(0_i64)

    def registered : Nil
      @registrations.add(1_i64)
    end

    def activation_attempted : Nil
      @activation_attempts.add(1_i64)
    end

    def activated(first : Bool) : Nil
      (first ? @activations : @reactivations).add(1_i64)
    end

    def activation_failed : Nil
      @activation_failures.add(1_i64)
    end

    def told : Nil
      @tells.add(1_i64)
    end

    def asked : Nil
      @asks.add(1_i64)
    end

    def stop_requested : Nil
      @stop_requests.add(1_i64)
    end

    def routing_rejected : Nil
      @routing_rejections.add(1_i64)
    end

    def owner_changed : Nil
      @owner_changes.add(1_i64)
    end

    def snapshot(
      handoffs_in_progress : Int32,
      sharding : ShardingStats,
    ) : SingletonStats
      SingletonStats.new(
        registrations: @registrations.get,
        activation_attempts: @activation_attempts.get,
        activations: @activations.get,
        reactivations: @reactivations.get,
        activation_failures: @activation_failures.get,
        tells: @tells.get,
        asks: @asks.get,
        stop_requests: @stop_requests.get,
        routing_rejections: @routing_rejections.get,
        owner_changes: @owner_changes.get,
        shared_sharding_handoffs_in_progress: handoffs_in_progress,
        shared_sharding_handoffs_completed: sharding.handoff_shards,
        shared_sharding_rejected_envelopes: sharding.rejected_envelopes,
        shared_sharding_lease_acquisitions: sharding.lease_acquisitions,
        shared_sharding_lease_renewals: sharding.lease_renewals,
        shared_sharding_lease_losses: sharding.lease_losses,
        shared_sharding_lease_retries: sharding.lease_retries
      )
    end
  end
end
