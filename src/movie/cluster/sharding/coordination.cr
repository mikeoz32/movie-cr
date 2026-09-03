module Movie
  class ClusterShardingExtension
    def handle_cluster_event(event : Cluster::ClusterEvent) : Nil
      providers = @providers_mutex.synchronize { @providers.values }
      coordinator = current_coordinator
      unless coordinator
        snapshot = @cluster.snapshot
        if !snapshot.unreachable.empty?
          providers.each { |provider| provider.suspend_ownership(false) }
        elsif member = event.member
          if member.unique_address == @cluster.self_unique_address &&
             (member.status.exiting? || member.status.down? || member.status.removed?)
            providers.each { |provider| provider.suspend_ownership(true) }
          end
        end
        schedule_rebalance
        return
      end

      prune_prepared_terms(providers, coordinator)
      if coordinator == @cluster.self_unique_address
        reconcile_allocations
      else
        providers.each { |provider| request_plan(provider, coordinator) }
      end
    end

    private def synchronize_provider(provider : Cluster::ShardedEntityProvider) : Nil
      if coordinator = current_coordinator
        if coordinator == @cluster.self_unique_address
          reconcile_provider(provider)
        else
          request_plan(provider, coordinator)
        end
      else
        schedule_plan_sync(provider.name)
      end
    end

    private def reconcile_allocations : Nil
      coordinator = current_coordinator
      unless coordinator
        schedule_rebalance
        return
      end
      unless coordinator == @cluster.self_unique_address
        providers = @providers_mutex.synchronize { @providers.values }
        providers.each { |provider| request_plan(provider, coordinator) }
        return
      end

      providers = @providers_mutex.synchronize { @providers.values }
      pending = false
      providers.each do |provider|
        pending = reconcile_provider(provider) || pending
      end
      schedule_rebalance if pending
    end

    private def reconcile_provider(
      provider : Cluster::ShardedEntityProvider,
      routing_locked : Bool = false,
    ) : Bool
      if routing_locked
        reconcile_provider_under_routing(provider)
      else
        @routing_mutex.synchronize { reconcile_provider_under_routing(provider) }
      end
    end

    private def reconcile_provider_under_routing(
      provider : Cluster::ShardedEntityProvider,
    ) : Bool
      return false unless coordinator?
      members = @cluster.snapshot.members
      current = plan_for(provider.name)
      coordinator = @cluster.self_unique_address
      generation = plan_generation_for(provider.name)
      if current.nil? || plan_coordinator_for(provider.name) != coordinator
        return true if bootstrap_plan_term?(provider, members, current, generation)
        candidate = take_bootstrap_candidate(provider.name)
        initial = candidate.try(&.allocations) || planner_for(provider).initial(members)
        next_generation = {generation, candidate.try(&.generation) || 0_i64}.max + 1_i64
        store_plan(provider, initial, coordinator, next_generation)
        broadcast_plan(provider, initial)
        return false unless candidate
        current = initial
      end

      current = current.not_nil!
      result = planner_for(provider).reconcile(current, members)
      shard_ids = (current.keys + result.allocations.keys).uniq
      moves = shard_ids.compact_map do |shard_id|
        previous_owner = current[shard_id]?
        next_owner = result.allocations[shard_id]?
        next if previous_owner == next_owner
        {shard_id, previous_owner, next_owner}
      end
      moves.each do |shard_id, previous_owner, next_owner|
        begin_handoff(provider, shard_id, previous_owner, next_owner, routing_locked: true)
      end
      result.pending || !moves.empty?
    end

    private def begin_handoff(
      provider : Cluster::ShardedEntityProvider,
      shard_id : Int32,
      previous_owner : Cluster::UniqueAddress?,
      next_owner : Cluster::UniqueAddress?,
      routing_locked : Bool = false,
    ) : Nil
      key = Cluster::ShardKey.new(provider.name, shard_id)
      started = if routing_locked
                  @handoffs_mutex.synchronize { @handoffs.add?(key) }
                else
                  @routing_mutex.synchronize do
                    @handoffs_mutex.synchronize { @handoffs.add?(key) }
                  end
                end
      return unless started
      coordinator = @cluster.self_unique_address
      run_handoff(provider, shard_id, previous_owner, next_owner, coordinator, key)
    end

    private def run_handoff(
      provider : Cluster::ShardedEntityProvider,
      shard_id : Int32,
      previous_owner : Cluster::UniqueAddress?,
      next_owner : Cluster::UniqueAddress?,
      coordinator : Cluster::UniqueAddress,
      key : Cluster::ShardKey,
    ) : Nil
      spawn do
        begin
          unless current_coordinator == coordinator &&
                 (next_owner.nil? || active_owner?(next_owner))
            abort_handoff(provider, key)
            next
          end
          passivate_previous_owner(provider, shard_id, previous_owner, next_owner, coordinator)
          prepare_next_owner(provider, shard_id, next_owner, coordinator) if next_owner
          @daemon.try &.tell_from(
            nil,
            Cluster::ShardingEnvelope.handoff_complete(
              provider.name,
              shard_id,
              provider.settings.configuration_key,
              coordinator,
              next_owner
            )
          )
        rescue error
          @telemetry.lease_retried if lease_wait?(error)
          Log.warn(exception: error) do
            "Shard handoff retry scheduled for #{provider.name}/#{shard_id}"
          end
          @system.scheduler.schedule_once(RECONCILE_INTERVAL) do
            unless @stopped.get
              run_handoff(provider, shard_id, previous_owner, next_owner, coordinator, key)
            end
          end
        end
      end
    end

    private def abort_handoff(
      provider : Cluster::ShardedEntityProvider,
      key : Cluster::ShardKey,
    ) : Nil
      pending = @routing_mutex.synchronize do
        @handoffs_mutex.synchronize do
          @handoffs.delete(key)
          @pending_deliveries.delete(key) || [] of Cluster::PendingShardingDelivery
        end
      end
      error = Cluster::NoShardOwnerError.new(provider.name, key.shard_id)
      pending.each { |delivery| reject_envelope(delivery.envelope, delivery.sender, error) }
      schedule_rebalance
    end

    private def lease_wait?(error : Exception) : Bool
      return true if error.is_a?(Cluster::ShardLeaseUnavailableError)
      if remote = error.as?(Remote::RemoteAskError)
        return remote.remote_class == Cluster::ShardLeaseUnavailableError.name
      end
      false
    end

    private def passivate_previous_owner(
      provider : Cluster::ShardedEntityProvider,
      shard_id : Int32,
      previous_owner : Cluster::UniqueAddress?,
      next_owner : Cluster::UniqueAddress?,
      coordinator : Cluster::UniqueAddress,
    ) : Nil
      return unless previous_owner
      member = @cluster.snapshot.member(previous_owner)
      return unless member && !member.status.down? && !member.status.removed?
      request = Cluster::ShardingEnvelope.passivate_shard(
        provider.name,
        shard_id,
        provider.settings.configuration_key,
        coordinator,
        next_owner
      )
      ask_control(previous_owner, request).await(HANDOFF_TIMEOUT)
    end

    private def prepare_next_owner(
      provider : Cluster::ShardedEntityProvider,
      shard_id : Int32,
      next_owner : Cluster::UniqueAddress,
      coordinator : Cluster::UniqueAddress,
    ) : Nil
      request = Cluster::ShardingEnvelope.prepare_shard(
        provider.name,
        shard_id,
        provider.settings.configuration_key,
        coordinator
      )
      ask_control(next_owner, request).await(HANDOFF_TIMEOUT)
    end

    private def ask_control(
      owner : Cluster::UniqueAddress,
      request : Cluster::ShardingEnvelope,
    ) : Future(Cluster::ShardingControlAck)
      if owner == @cluster.self_unique_address
        Movie::Ask.local(
          @system,
          @daemon.not_nil!,
          request,
          Cluster::ShardingControlAck,
          HANDOFF_TIMEOUT
        )
      else
        remote_ref(owner).ask(request, Cluster::ShardingControlAck, HANDOFF_TIMEOUT)
      end
    end

    private def handle_handoff_complete(
      provider : Cluster::ShardedEntityProvider,
      envelope : Cluster::ShardingEnvelope,
      remote_address : Address?,
      remote_node_uid : String?,
    ) : Nil
      validate_coordinator(envelope, remote_address, remote_node_uid)
      unless coordinator?
        raise Cluster::ClusterShardingConfigurationError.new(
          "Only the current sharding coordinator may complete a handoff"
        )
      end
      next_owner = envelope.next_owner
      @routing_mutex.synchronize do
        key = Cluster::ShardKey.new(provider.name, envelope.shard_id)
        current = plan_for(provider.name) || raise Cluster::NoShardOwnerError.new(
          provider.name,
          envelope.shard_id
        )
        updated = current.dup
        if next_owner
          updated[envelope.shard_id] = next_owner
        else
          updated.delete(envelope.shard_id)
        end
        store_plan(
          provider,
          updated,
          @cluster.self_unique_address,
          plan_generation_for(provider.name) + 1_i64
        )
        loop do
          pending = @handoffs_mutex.synchronize do
            @pending_deliveries.delete(key) || [] of Cluster::PendingShardingDelivery
          end
          pending.each do |delivery|
            if next_owner
              dispatch_to_owner(
                provider,
                delivery.envelope,
                delivery.sender,
                next_owner,
                @cluster.self_unique_address
              )
            else
              reject_envelope(
                delivery.envelope,
                delivery.sender,
                Cluster::NoShardOwnerError.new(provider.name, envelope.shard_id)
              )
            end
          end
          drained = @handoffs_mutex.synchronize do
            if @pending_deliveries[key]?.try(&.empty?) != false
              @pending_deliveries.delete(key)
              @handoffs.delete(key)
              true
            else
              false
            end
          end
          break if drained
        end
        broadcast_plan(provider, updated)
      end
      @telemetry.rebalanced(1)
      @telemetry.handed_off(1)
      schedule_rebalance
    end

    private def enqueue_during_handoff(
      provider : Cluster::ShardedEntityProvider,
      envelope : Cluster::ShardingEnvelope,
      sender : ActorRefBase?,
    ) : Bool
      key = Cluster::ShardKey.new(provider.name, envelope.shard_id)
      @handoffs_mutex.synchronize do
        return false unless @handoffs.includes?(key)
        deliveries = @pending_deliveries[key] ||= [] of Cluster::PendingShardingDelivery
        if deliveries.size >= HANDOFF_BUFFER_CAPACITY
          @telemetry.envelope_rejected
          raise Cluster::ClusterShardingConfigurationError.new(
            "Shard handoff buffer is full for #{provider.name}/#{envelope.shard_id}"
          )
        end
        deliveries << Cluster::PendingShardingDelivery.new(envelope, sender)
        true
      end
    end

    private def handle_plan_request(
      provider : Cluster::ShardedEntityProvider,
      envelope : Cluster::ShardingEnvelope,
      remote_address : Address?,
      remote_node_uid : String?,
    ) : Nil
      requester = envelope.requester || raise Cluster::ClusterShardingConfigurationError.new(
        "Sharding plan request is missing its requester"
      )
      unless authenticated_peer?(requester, remote_address, remote_node_uid)
        raise Cluster::ClusterShardingConfigurationError.new(
          "Sharding plan request has mismatched sender identity"
        )
      end
      return unless coordinator?
      unless plan_coordinator_for(provider.name) == @cluster.self_unique_address
        reconcile_provider(provider)
      end
      return unless plan_coordinator_for(provider.name) == @cluster.self_unique_address
      if plan = plan_for(provider.name)
        send_plan(provider, plan, requester)
      end
    end

    private def handle_plan_snapshot_request(
      provider : Cluster::ShardedEntityProvider,
      envelope : Cluster::ShardingEnvelope,
      remote_address : Address?,
      remote_node_uid : String?,
    ) : Nil
      requester = envelope.requester || raise Cluster::ClusterShardingConfigurationError.new(
        "Sharding plan snapshot request is missing its requester"
      )
      unless authenticated_peer?(requester, remote_address, remote_node_uid)
        raise Cluster::ClusterShardingConfigurationError.new(
          "Sharding plan snapshot request has mismatched sender identity"
        )
      end
      state = plan_state_for(provider.name) || return
      snapshot = Cluster::ShardingEnvelope.plan_snapshot(
        provider.name,
        provider.settings.configuration_key,
        state.coordinator,
        state.generation,
        state.allocations
      )
      remote_ref(requester).tell_from(@daemon, snapshot)
    end

    private def handle_plan_snapshot(
      provider : Cluster::ShardedEntityProvider,
      envelope : Cluster::ShardingEnvelope,
      remote_address : Address?,
      remote_node_uid : String?,
    ) : Nil
      unless coordinator? && authenticated_cluster_peer?(remote_address, remote_node_uid)
        raise Cluster::ClusterShardingConfigurationError.new(
          "Sharding plan snapshot has mismatched sender identity"
        )
      end
      raise Cluster::ClusterShardingConfigurationError.new(
        "Sharding plan snapshot generation must be positive"
      ) unless envelope.plan_generation > 0_i64
      allocations = allocations_from(envelope, provider)
      @plan_bootstrap_mutex.synchronize do
        return unless @plan_bootstrap_deadlines.has_key?(provider.name)
        current = @plan_bootstrap_candidates[provider.name]?
        if current.nil? || envelope.plan_generation > current.generation
          @plan_bootstrap_candidates[provider.name] = Cluster::PlanBootstrapCandidate.new(
            allocations,
            envelope.plan_generation
          )
        elsif envelope.plan_generation == current.generation && current.allocations != allocations
          raise Cluster::ClusterShardingConfigurationError.new(
            "Conflicting sharding plan snapshots at generation #{envelope.plan_generation}"
          )
        end
      end
    end

    private def handle_plan_update(
      provider : Cluster::ShardedEntityProvider,
      envelope : Cluster::ShardingEnvelope,
      remote_address : Address?,
      remote_node_uid : String?,
    ) : Nil
      validate_coordinator(envelope, remote_address, remote_node_uid)
      raise Cluster::ClusterShardingConfigurationError.new(
        "Sharding plan generation must be positive"
      ) unless envelope.plan_generation > 0_i64
      allocations = allocations_from(envelope, provider)
      current = plan_state_for(provider.name)
      if current
        if envelope.plan_generation < current.generation
          @telemetry.envelope_rejected
          return
        elsif envelope.plan_generation == current.generation
          unless current.coordinator == envelope.coordinator && current.allocations == allocations
            raise Cluster::ClusterShardingConfigurationError.new(
              "Conflicting sharding plan at generation #{envelope.plan_generation}"
            )
          end
          return
        end
      end
      store_plan(
        provider,
        allocations,
        envelope.coordinator.not_nil!,
        envelope.plan_generation
      )
      @plan_bootstrap_mutex.synchronize do
        @plan_bootstrap_deadlines.delete(provider.name)
        @plan_bootstrap_candidates.delete(provider.name)
      end
      @plan_syncs_mutex.synchronize { @plan_syncs.delete(provider.name) }
    end

    private def allocations_from(
      envelope : Cluster::ShardingEnvelope,
      provider : Cluster::ShardedEntityProvider,
    ) : Cluster::ShardAllocations
      allocations = Cluster::ShardAllocations.new
      envelope.assignments.each do |assignment|
        unless assignment.shard_id >= 0 && assignment.shard_id < provider.settings.shard_count
          raise Cluster::ClusterShardingConfigurationError.new(
            "Sharding plan contains invalid shard #{assignment.shard_id}"
          )
        end
        if allocations.has_key?(assignment.shard_id)
          raise Cluster::ClusterShardingConfigurationError.new(
            "Sharding plan contains duplicate shard #{assignment.shard_id}"
          )
        end
        allocations[assignment.shard_id] = assignment.owner
      end
      allocations
    end

    private def request_plan(
      provider : Cluster::ShardedEntityProvider,
      coordinator : Cluster::UniqueAddress,
    ) : Nil
      request = Cluster::ShardingEnvelope.plan_request(
        provider.name,
        provider.settings.configuration_key,
        @cluster.self_unique_address
      )
      if coordinator == @cluster.self_unique_address
        @daemon.not_nil!.tell_from(nil, request)
      else
        remote_ref(coordinator).tell_from(@daemon, request)
      end
      schedule_plan_sync(provider.name)
    end

    private def schedule_plan_sync(entity_type : String) : Nil
      added = @plan_syncs_mutex.synchronize { @plan_syncs.add?(entity_type) }
      return unless added
      @system.scheduler.schedule_once(PLAN_SYNC_INTERVAL) do
        @plan_syncs_mutex.synchronize { @plan_syncs.delete(entity_type) }
        unless @stopped.get
          begin
            synchronize_provider(provider_for(entity_type))
          rescue error
            Log.warn(exception: error) { "Sharding plan sync failed for #{entity_type}" }
            schedule_plan_sync(entity_type)
          end
        end
      end
    end

    private def broadcast_plan(
      provider : Cluster::ShardedEntityProvider,
      plan : Cluster::ShardAllocations,
    ) : Nil
      coordinator = @cluster.self_unique_address
      @cluster.snapshot.members.each do |member|
        next unless member.status.up?
        next if member.unique_address == coordinator
        send_plan(provider, plan, member.unique_address)
      end
    end

    private def send_plan(
      provider : Cluster::ShardedEntityProvider,
      plan : Cluster::ShardAllocations,
      target : Cluster::UniqueAddress,
    ) : Nil
      state = plan_state_for(provider.name) || raise Cluster::NoShardOwnerError.new(
        provider.name,
        -1
      )
      unless state.allocations == plan && state.coordinator == @cluster.self_unique_address
        raise Cluster::ClusterShardingConfigurationError.new(
          "Only the current coordinator may publish its authoritative sharding plan"
        )
      end
      update = Cluster::ShardingEnvelope.plan_update(
        provider.name,
        provider.settings.configuration_key,
        state.coordinator,
        state.generation,
        plan
      )
      if target == @cluster.self_unique_address
        @daemon.not_nil!.tell_from(nil, update)
      else
        remote_ref(target).tell_from(@daemon, update)
      end
    end

    private def store_plan(
      provider : Cluster::ShardedEntityProvider,
      plan : Cluster::ShardAllocations,
      coordinator : Cluster::UniqueAddress,
      generation : Int64,
    ) : Nil
      @plans_mutex.synchronize do
        @plans[provider.name] = plan.dup
        @plan_coordinators[provider.name] = coordinator
        @plan_generations[provider.name] = generation
      end
      prepared = prepared_shards_for(provider, coordinator)
      provider.retain_shards(local_shards(plan) | prepared)
      @prepared_shards_mutex.synchronize do
        @prepared_shards.reject! do |key, _coordinator|
          key.entity_type == provider.name && plan[key.shard_id]? == @cluster.self_unique_address
        end
      end
    end

    private def prune_prepared_terms(
      providers : Array(Cluster::ShardedEntityProvider),
      coordinator : Cluster::UniqueAddress,
    ) : Nil
      providers.each do |provider|
        prepared = prepared_shards_for(provider, coordinator)
        local = plan_for(provider.name).try { |plan| local_shards(plan) } || Set(Int32).new
        provider.retain_shards(local | prepared)
      end
    end

    private def prepared_shards_for(
      provider : Cluster::ShardedEntityProvider,
      coordinator : Cluster::UniqueAddress,
    ) : Set(Int32)
      @prepared_shards_mutex.synchronize do
        @prepared_shards.reject! do |key, prepared_by|
          key.entity_type == provider.name && prepared_by != coordinator
        end
        @prepared_shards.each_with_object(Set(Int32).new) do |(key, prepared_by), shards|
          if key.entity_type == provider.name && prepared_by == coordinator
            shards << key.shard_id
          end
        end
      end
    end

    private def bootstrap_plan_term?(
      provider : Cluster::ShardedEntityProvider,
      members : Array(Cluster::Member),
      local_plan : Cluster::ShardAllocations?,
      local_generation : Int64,
    ) : Bool
      now = Time.instant
      deadline = @plan_bootstrap_mutex.synchronize do
        if local_plan
          candidate = @plan_bootstrap_candidates[provider.name]?
          if candidate.nil? || local_generation > candidate.generation
            @plan_bootstrap_candidates[provider.name] = Cluster::PlanBootstrapCandidate.new(
              local_plan.dup,
              local_generation
            )
          end
        end
        @plan_bootstrap_deadlines[provider.name] ||= now + 500.milliseconds
      end
      peers = members.select do |member|
        member.status.up? && member.unique_address != @cluster.self_unique_address
      end
      return false if peers.empty?
      return false if now >= deadline

      peers.each do |member|
        request = Cluster::ShardingEnvelope.plan_snapshot_request(
          provider.name,
          provider.settings.configuration_key,
          @cluster.self_unique_address
        )
        remote_ref(member.unique_address).tell_from(@daemon, request)
      end
      schedule_rebalance
      true
    end

    private def take_bootstrap_candidate(entity_type : String) : Cluster::PlanBootstrapCandidate?
      @plan_bootstrap_mutex.synchronize do
        @plan_bootstrap_deadlines.delete(entity_type)
        @plan_bootstrap_candidates.delete(entity_type)
      end
    end

    private def plan_for(entity_type : String) : Cluster::ShardAllocations?
      @plans_mutex.synchronize { @plans[entity_type]?.try(&.dup) }
    end

    private def plan_state_for(entity_type : String) : Cluster::ShardingPlanState?
      @plans_mutex.synchronize do
        plan = @plans[entity_type]?
        coordinator = @plan_coordinators[entity_type]?
        generation = @plan_generations[entity_type]?
        if plan && coordinator && generation
          Cluster::ShardingPlanState.new(plan.dup, coordinator, generation)
        end
      end
    end

    private def plan_coordinator_for(entity_type : String) : Cluster::UniqueAddress?
      @plans_mutex.synchronize { @plan_coordinators[entity_type]? }
    end

    private def plan_generation_for(entity_type : String) : Int64
      @plans_mutex.synchronize { @plan_generations[entity_type]? || 0_i64 }
    end

    private def owner_for(
      provider : Cluster::ShardedEntityProvider,
      shard_id : Int32,
    ) : Cluster::UniqueAddress
      plan_for(provider.name).try(&.[shard_id]?) || raise Cluster::NoShardOwnerError.new(
        provider.name,
        shard_id
      )
    end

    private def local_shards(plan : Cluster::ShardAllocations) : Set(Int32)
      plan.each_with_object(Set(Int32).new) do |(shard_id, owner), shards|
        shards << shard_id if owner == @cluster.self_unique_address
      end
    end

    private def planner_for(provider : Cluster::ShardedEntityProvider) : Cluster::AllocationPlanner
      settings = provider.settings
      Cluster::AllocationPlanner.new(settings.shard_count, settings.allocation, settings.rebalance)
    end

    private def schedule_rebalance : Nil
      _, scheduled = @rebalance_scheduled.compare_and_set(false, true)
      return unless scheduled
      @system.scheduler.schedule_once(RECONCILE_INTERVAL) do
        @rebalance_scheduled.set(false)
        reconcile_allocations unless @stopped.get
      end
    end

    private def schedule_idle_sweep(provider : Cluster::ShardedEntityProvider) : Nil
      timeout = provider.settings.idle_timeout || return
      added = @idle_sweeps_mutex.synchronize { @idle_sweeps.add?(provider.name) }
      return unless added
      interval = timeout < 20.milliseconds ? timeout : timeout / 2
      @system.scheduler.schedule_once(interval) do
        unless @stopped.get
          begin
            current = provider_for(provider.name)
            @telemetry.passivated_idle(current.passivate_idle(Time.instant))
            @idle_sweeps_mutex.synchronize { @idle_sweeps.delete(provider.name) }
            schedule_idle_sweep(current)
          rescue error
            @idle_sweeps_mutex.synchronize { @idle_sweeps.delete(provider.name) }
            Log.warn(exception: error) { "Idle passivation sweep failed for #{provider.name}" }
          end
        end
      end
    end
  end
end
