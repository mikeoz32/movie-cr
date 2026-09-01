module Movie
  class ClusterShardingExtension
    def route(
      entity_type : String,
      entity_id : String,
      message : T,
      sender : ActorRefBase? = nil,
    ) : Nil forall T
      @telemetry.routed
      provider = provider_for(entity_type)
      send_to_coordinator(user_envelope(provider, entity_id, message), sender)
    rescue error : Cluster::NoShardOwnerError
      @telemetry.envelope_rejected
      raise error
    end

    def route_ask(
      entity_type : String,
      entity_id : String,
      message : T,
      response_type : R.class,
      timeout : Time::Span,
    ) : Future(R) forall T, R
      @telemetry.routed
      provider = provider_for(entity_type)
      envelope = user_envelope(provider, entity_id, message).as_ask(timeout)
      coordinator = routing_coordinator
      if coordinator == @cluster.self_unique_address
        Movie::Ask.local(@system, @daemon.not_nil!, envelope, response_type, timeout)
      else
        @telemetry.routed_remote
        remote_ref(coordinator).ask(envelope, response_type, timeout)
      end
    rescue error : Cluster::NoShardOwnerError
      @telemetry.envelope_rejected
      raise error
    end

    def passivate(entity_type : String, entity_id : String) : Bool
      @telemetry.passivation_requested
      provider = provider_for(entity_type)
      shard_id = provider.settings.partitioner.shard_for(
        entity_type,
        entity_id,
        provider.settings.shard_count
      )
      send_to_coordinator(
        Cluster::ShardingEnvelope.passivate(
          entity_type,
          entity_id,
          shard_id,
          provider.settings.configuration_key
        ),
        nil
      )
      true
    rescue error : Cluster::NoShardOwnerError
      @telemetry.envelope_rejected
      raise error
    end

    def handle_envelope(
      envelope : Cluster::ShardingEnvelope,
      sender : ActorRefBase?,
      remote_address : Address?,
      remote_node_uid : String?,
    ) : Nil
      provider = provider_for(envelope.entity_type)
      validate_settings(provider, envelope)
      case envelope.kind
      when .user?, .passivate?
        handle_user_envelope(provider, envelope, sender, remote_address, remote_node_uid)
      when .passivate_shard?
        handle_passivate_shard(provider, envelope, sender, remote_address, remote_node_uid)
      when .prepare_shard?
        handle_prepare_shard(provider, envelope, sender, remote_address, remote_node_uid)
      when .plan_request?
        handle_plan_request(provider, envelope, remote_address, remote_node_uid)
      when .plan_snapshot_request?
        handle_plan_snapshot_request(provider, envelope, remote_address, remote_node_uid)
      when .plan_snapshot?
        handle_plan_snapshot(provider, envelope, remote_address, remote_node_uid)
      when .plan_update?
        handle_plan_update(provider, envelope, remote_address, remote_node_uid)
      when .handoff_complete?
        handle_handoff_complete(provider, envelope, remote_address, remote_node_uid)
      end
    rescue error : Cluster::ShardLeaseUnavailableError | Cluster::ShardHandoffInProgressError
      if retry_lease_delivery?(provider.not_nil!, envelope) && !@stopped.get
        queue_delivery_retry(provider.not_nil!, envelope, logical_sender(envelope, sender), error)
      else
        reject_envelope(envelope, sender, error)
        Log.warn(exception: error) do
          "Shard delivery lease wait exhausted for #{envelope.entity_type}:#{envelope.entity_id}"
        end
      end
    rescue error : Cluster::ClusterShardingConfigurationError | Cluster::NoShardOwnerError
      reject_envelope(envelope, sender, error)
      Log.warn(exception: error) do
        "Rejected sharding envelope for #{envelope.entity_type}:#{envelope.entity_id}"
      end
    rescue error
      reject_envelope(envelope, sender, error)
      Log.error(exception: error) do
        "Unexpected sharding delivery failure for #{envelope.entity_type}:#{envelope.entity_id}"
      end
    end

    private def user_envelope(
      provider : Cluster::ShardedEntityProvider,
      entity_id : String,
      message : T,
    ) : Cluster::ShardingEnvelope forall T
      shard_id = provider.settings.partitioner.shard_for(
        provider.name,
        entity_id,
        provider.settings.shard_count
      )
      serializable = message.as(JSON::Serializable)
      tag, payload = Remote::MessageRegistry.prepare(serializable)
      Cluster::ShardingEnvelope.new(
        provider.name,
        entity_id,
        shard_id,
        Cluster::ShardingMessage.new(
          tag,
          payload,
          Remote::TypedMessageWrapper(JSON::Serializable).new(serializable)
        ),
        provider.settings.configuration_key
      )
    end

    private def send_to_coordinator(
      envelope : Cluster::ShardingEnvelope,
      sender : ActorRefBase?,
    ) : Nil
      coordinator = routing_coordinator
      if coordinator == @cluster.self_unique_address
        @daemon.not_nil!.tell_from(sender, envelope)
      else
        @telemetry.routed_remote
        remote_ref(coordinator).tell_from(sender, envelope)
      end
    end

    private def handle_user_envelope(
      provider : Cluster::ShardedEntityProvider,
      envelope : Cluster::ShardingEnvelope,
      sender : ActorRefBase?,
      remote_address : Address?,
      remote_node_uid : String?,
    ) : Nil
      expected_shard = provider.settings.partitioner.shard_for(
        envelope.entity_type,
        envelope.entity_id,
        provider.settings.shard_count
      )
      unless envelope.shard_id == expected_shard
        raise Cluster::ClusterShardingConfigurationError.new(
          "Envelope shard #{envelope.shard_id} does not match entity shard #{expected_shard}"
        )
      end

      if envelope.hop_count == 0
        origin_sender = logical_sender(envelope, sender)
        @routing_mutex.synchronize do
          coordinator = current_coordinator
          unless coordinator == @cluster.self_unique_address
            raise Cluster::NoShardOwnerError.new(provider.name, envelope.shard_id)
          end
          unless plan_coordinator_for(provider.name) == coordinator
            reconcile_provider(provider, routing_locked: true)
            unless plan_coordinator_for(provider.name) == coordinator
              raise Cluster::NoShardOwnerError.new(provider.name, envelope.shard_id)
            end
          end
          return if enqueue_during_delivery_retry(provider, envelope, origin_sender)
          return if enqueue_during_handoff(provider, envelope, origin_sender)

          owner = owner_for(provider, envelope.shard_id)
          unless active_owner?(owner)
            reconcile_provider(provider, routing_locked: true)
            return if enqueue_during_handoff(provider, envelope, origin_sender)
            owner = owner_for(provider, envelope.shard_id)
            unless active_owner?(owner)
              raise Cluster::NoShardOwnerError.new(provider.name, envelope.shard_id)
            end
          end
          dispatch_to_owner(provider, envelope, origin_sender, owner, coordinator.not_nil!)
        end
      else
        coordinator = current_coordinator
        unless envelope.hop_count == 1 &&
               envelope.coordinator == coordinator &&
               authenticated_peer?(coordinator, remote_address, remote_node_uid) &&
               authorized_local_owner?(provider, envelope.shard_id, coordinator)
          raise Cluster::ClusterShardingConfigurationError.new(
            "Sharding delivery was not authorized by the current coordinator"
          )
        end
        origin_sender = logical_sender(envelope, sender)
        return if enqueue_during_delivery_retry(provider, envelope, origin_sender)
        deliver_to_provider(provider, envelope, origin_sender)
      end
    end

    private def dispatch_to_owner(
      provider : Cluster::ShardedEntityProvider,
      envelope : Cluster::ShardingEnvelope,
      sender : ActorRefBase?,
      owner : Cluster::UniqueAddress,
      coordinator : Cluster::UniqueAddress,
    ) : Nil
      if owner == @cluster.self_unique_address
        deliver_to_provider(provider, envelope, sender)
      else
        @telemetry.forwarded
        forwarded = envelope.forwarded(coordinator, envelope.senderless || sender.nil?)
        if timeout_ms = envelope.ask_timeout_ms
          relay_ask(provider, remote_ref(owner), forwarded, sender, timeout_ms)
        else
          remote_ref(owner).tell_from(sender || @daemon, forwarded)
        end
      end
    end

    private def active_owner?(owner : Cluster::UniqueAddress) : Bool
      @cluster.snapshot.member(owner).try(&.status.up?) || false
    end

    private def relay_ask(
      provider : Cluster::ShardedEntityProvider,
      target : Remote::RemoteActorRef(Cluster::ShardingEnvelope),
      envelope : Cluster::ShardingEnvelope,
      sender : ActorRefBase?,
      timeout_ms : Int64,
    ) : Nil
      response = target.ask_serializable(envelope, timeout_ms.milliseconds)
      response.on_success do |value|
        Ask.reply_serializable_if_asked(sender, value)
      end
      response.on_failure do |error|
        reject_envelope(envelope, sender, error)
      end
      response.on_cancel do
        Ask.cancel_dynamic_if_asked(sender)
      end
    end

    private def reject_envelope(
      envelope : Cluster::ShardingEnvelope,
      sender : ActorRefBase?,
      error : Exception,
    ) : Nil
      @telemetry.envelope_rejected
      Ask.fail_dynamic_if_asked(sender, error) if envelope.ask_timeout_ms
    end

    private def deliver_to_provider(
      provider : Cluster::ShardedEntityProvider,
      envelope : Cluster::ShardingEnvelope,
      sender : ActorRefBase?,
    ) : Nil
      provider.authorize_shard(envelope.shard_id)
      if envelope.kind.passivate?
        @telemetry.passivated_explicit if provider.passivate(envelope.entity_id)
      else
        provider.deliver(envelope, sender)
        @telemetry.delivered_local
      end
    end

    private def handle_passivate_shard(
      provider : Cluster::ShardedEntityProvider,
      envelope : Cluster::ShardingEnvelope,
      sender : ActorRefBase?,
      remote_address : Address?,
      remote_node_uid : String?,
    ) : Nil
      validate_coordinator(envelope, remote_address, remote_node_uid)
      spawn do
        begin
          wait_for_delivery_retries(provider, envelope.shard_id)
          provider.passivate_shard(envelope.shard_id)
          Ask.reply_if_asked(sender, Cluster::ShardingControlAck.new)
        rescue error
          Ask.fail_if_asked(sender, error, Cluster::ShardingControlAck)
        end
      end
    end

    private def handle_prepare_shard(
      provider : Cluster::ShardedEntityProvider,
      envelope : Cluster::ShardingEnvelope,
      sender : ActorRefBase?,
      remote_address : Address?,
      remote_node_uid : String?,
    ) : Nil
      validate_coordinator(envelope, remote_address, remote_node_uid)
      provider.prepare_shard(envelope.shard_id)
      key = Cluster::ShardKey.new(provider.name, envelope.shard_id)
      @prepared_shards_mutex.synchronize do
        @prepared_shards[key] = envelope.coordinator.not_nil!
      end
      Ask.reply_if_asked(sender, Cluster::ShardingControlAck.new)
    rescue error
      Ask.fail_if_asked(sender, error, Cluster::ShardingControlAck)
    end

    private def validate_coordinator(
      envelope : Cluster::ShardingEnvelope,
      remote_address : Address?,
      remote_node_uid : String?,
    ) : Nil
      coordinator = current_coordinator
      return if envelope.coordinator == coordinator &&
                authenticated_peer?(coordinator, remote_address, remote_node_uid)
      raise Cluster::ClusterShardingConfigurationError.new(
        "Sharding control was not authorized by the current coordinator"
      )
    end

    private def authenticated_peer?(
      expected : Cluster::UniqueAddress?,
      remote_address : Address?,
      remote_node_uid : String?,
    ) : Bool
      return false unless expected
      if expected == @cluster.self_unique_address
        remote_address.nil? && remote_node_uid.nil?
      else
        remote_address == expected.address && remote_node_uid == expected.node_uid
      end
    end

    private def authenticated_cluster_peer?(
      remote_address : Address?,
      remote_node_uid : String?,
    ) : Bool
      return false unless remote_address && remote_node_uid
      @cluster.snapshot.members.any? do |member|
        member.status.up? &&
          member.unique_address.address == remote_address &&
          member.unique_address.node_uid == remote_node_uid
      end
    end

    private def authorized_local_owner?(
      provider : Cluster::ShardedEntityProvider,
      shard_id : Int32,
      coordinator : Cluster::UniqueAddress?,
    ) : Bool
      return false unless coordinator
      (plan_coordinator_for(provider.name) == coordinator &&
        plan_for(provider.name).try(&.[shard_id]?) == @cluster.self_unique_address) ||
        @prepared_shards_mutex.synchronize do
          @prepared_shards[Cluster::ShardKey.new(provider.name, shard_id)]? == coordinator
        end
    end

    private def retry_lease_delivery?(
      provider : Cluster::ShardedEntityProvider,
      envelope : Cluster::ShardingEnvelope,
    ) : Bool
      elapsed_ms = envelope.delivery_attempt.to_f64 * LEASE_RETRY_INTERVAL.total_milliseconds
      retry_window_ms = (
        provider.settings.lease_duration + provider.settings.lease_renew_interval
      ).total_milliseconds
      elapsed_ms < retry_window_ms
    end

    private def logical_sender(
      envelope : Cluster::ShardingEnvelope,
      sender : ActorRefBase?,
    ) : ActorRefBase?
      return nil if envelope.senderless || sender == @system.dead_letters
      sender
    end

    private def enqueue_during_delivery_retry(
      provider : Cluster::ShardedEntityProvider,
      envelope : Cluster::ShardingEnvelope,
      sender : ActorRefBase?,
    ) : Bool
      key = Cluster::ShardKey.new(provider.name, envelope.shard_id)
      @delivery_retries_mutex.synchronize do
        return false unless @delivery_retries.includes?(key)
        append_delivery_retry(key, envelope, sender)
        true
      end
    end

    private def queue_delivery_retry(
      provider : Cluster::ShardedEntityProvider,
      envelope : Cluster::ShardingEnvelope,
      sender : ActorRefBase?,
      error : Exception,
    ) : Nil
      retried = envelope.rerouted
      unless retry_lease_delivery?(provider, retried)
        reject_envelope(envelope, sender, error)
        return
      end
      @telemetry.lease_retried if lease_wait?(error)
      key = Cluster::ShardKey.new(provider.name, envelope.shard_id)
      started = @delivery_retries_mutex.synchronize do
        append_delivery_retry(key, retried, sender)
        newly_started = @delivery_retries.add?(key)
        if newly_started
          @delivery_retry_deadlines[key] = Time.instant + delivery_retry_window(provider)
        end
        newly_started
      end
      schedule_delivery_retry(provider, key) if started
    end

    private def append_delivery_retry(
      key : Cluster::ShardKey,
      envelope : Cluster::ShardingEnvelope,
      sender : ActorRefBase?,
    ) : Nil
      deliveries = @pending_delivery_retries[key] ||= [] of Cluster::PendingShardingDelivery
      if deliveries.size >= HANDOFF_BUFFER_CAPACITY
        @telemetry.envelope_rejected
        raise Cluster::ClusterShardingConfigurationError.new(
          "Shard delivery retry buffer is full for #{key.entity_type}/#{key.shard_id}"
        )
      end
      deliveries << Cluster::PendingShardingDelivery.new(envelope, sender)
    end

    private def schedule_delivery_retry(
      provider : Cluster::ShardedEntityProvider,
      key : Cluster::ShardKey,
    ) : Nil
      @system.scheduler.schedule_once(LEASE_RETRY_INTERVAL) do
        run_delivery_retry(provider, key) unless @stopped.get
      end
    end

    private def run_delivery_retry(
      provider : Cluster::ShardedEntityProvider,
      key : Cluster::ShardKey,
    ) : Nil
      processed = 0
      loop do
        delivery = @delivery_retries_mutex.synchronize do
          @pending_delivery_retries[key]?.try(&.first?)
        end
        return finish_delivery_retry(key) unless delivery

        deadline = @delivery_retries_mutex.synchronize { @delivery_retry_deadlines[key]? }
        unless deadline && Time.instant < deadline
          reject_pending_delivery_retries(
            key,
            Cluster::ShardLeaseUnavailableError.new(provider.name, key.shard_id)
          )
          return
        end

        coordinator = current_coordinator
        unless authorized_local_owner?(provider, key.shard_id, coordinator)
          reject_pending_delivery_retries(
            key,
            Cluster::NoShardOwnerError.new(provider.name, key.shard_id)
          )
          return
        end

        begin
          deliver_to_provider(provider, delivery.envelope, delivery.sender)
          return unless shift_delivery_retry(key)
        rescue error : Cluster::ShardLeaseUnavailableError | Cluster::ShardHandoffInProgressError
          retried = delivery.envelope.rerouted
          if retry_lease_delivery?(provider, retried)
            @telemetry.lease_retried if lease_wait?(error)
            @delivery_retries_mutex.synchronize do
              if pending = @pending_delivery_retries[key]?
                pending[0] = Cluster::PendingShardingDelivery.new(retried, delivery.sender) unless pending.empty?
              end
            end
            schedule_delivery_retry(provider, key)
            return
          end
          return unless reject_delivery_retry_head(key, delivery, error)
        rescue error
          return unless reject_delivery_retry_head(key, delivery, error)
        end

        processed += 1
        if processed >= DELIVERY_RETRY_DRAIN_BATCH
          spawn { run_delivery_retry(provider, key) unless @stopped.get }
          return
        end
      end
    end

    private def reject_delivery_retry_head(
      key : Cluster::ShardKey,
      delivery : Cluster::PendingShardingDelivery,
      error : Exception,
    ) : Bool
      reject_envelope(delivery.envelope, delivery.sender, error)
      shift_delivery_retry(key)
    end

    private def shift_delivery_retry(key : Cluster::ShardKey) : Bool
      @delivery_retries_mutex.synchronize do
        pending = @pending_delivery_retries[key]?
        pending.try &.shift?
        if pending.nil? || pending.empty?
          @pending_delivery_retries.delete(key)
          @delivery_retries.delete(key)
          @delivery_retry_deadlines.delete(key)
          false
        else
          true
        end
      end
    end

    private def reject_pending_delivery_retries(
      key : Cluster::ShardKey,
      error : Exception,
    ) : Nil
      pending = @delivery_retries_mutex.synchronize do
        @delivery_retries.delete(key)
        @delivery_retry_deadlines.delete(key)
        @pending_delivery_retries.delete(key) || [] of Cluster::PendingShardingDelivery
      end
      pending.each { |delivery| reject_envelope(delivery.envelope, delivery.sender, error) }
    end

    private def finish_delivery_retry(key : Cluster::ShardKey) : Nil
      @delivery_retries_mutex.synchronize do
        @pending_delivery_retries.delete(key)
        @delivery_retries.delete(key)
        @delivery_retry_deadlines.delete(key)
      end
    end

    private def wait_for_delivery_retries(
      provider : Cluster::ShardedEntityProvider,
      shard_id : Int32,
    ) : Nil
      key = Cluster::ShardKey.new(provider.name, shard_id)
      while @delivery_retries_mutex.synchronize { @delivery_retries.includes?(key) }
        raise Cluster::ShardHandoffInProgressError.new(provider.name, shard_id) if @stopped.get
        sleep 1.millisecond
      end
    end

    private def delivery_retry_window(provider : Cluster::ShardedEntityProvider) : Time::Span
      provider.settings.lease_duration + provider.settings.lease_renew_interval
    end
  end
end
