module Movie
  class ClusterReceptionistExtension
    def handle_envelope(
      envelope : Cluster::ReceptionistEnvelope,
      sender_path : ActorPath?,
      remote_address : Address?,
      remote_node_uid : String?,
    ) : Nil
      return if @stopped.get
      validate_envelope_identity(envelope, sender_path, remote_address, remote_node_uid)
      validate_registrations(envelope)
      changed = merge_remote_state(envelope)
      @telemetry.sync_received
      publish_listings if changed
    rescue error : Cluster::ReceptionistConfigurationError
      @telemetry.protocol_rejected
      Log.warn(exception: error) { "Rejected receptionist state from #{envelope.sender}" }
    end

    def handle_cluster_event(event : Cluster::ClusterEvent) : Nil
      return if @stopped.get
      member = event.member
      case event.kind
      when .member_down?, .member_removed?
        purge_node(member.not_nil!.unique_address) if member
        publish_listings
      when .member_up?
        send_local_state(member.not_nil!.unique_address) if member
        publish_listings
      when .current_state?
        purge_terminal_states
        publish_listings
        broadcast_local_state
      when .member_leaving?, .member_exiting?, .unreachable_member?, .reachable_member?
        publish_listings
      when .member_joined?, .leader_changed?
      end
    end

    private def schedule_sync(delay : Time::Span = SYNC_INTERVAL) : Nil
      @system.scheduler.schedule_once(delay) do
        unless @stopped.get
          @telemetry.sync_round
          broadcast_local_state
          schedule_sync
        end
      end
    end

    private def broadcast_local_state : Nil
      snapshot = @cluster.snapshot
      snapshot.members.each do |member|
        next unless member.status.up?
        next if member.unique_address == @cluster.self_unique_address
        send_local_state(member.unique_address)
      end
    end

    private def send_local_state(target : Cluster::UniqueAddress) : Nil
      return if target == @cluster.self_unique_address
      state = @mutex.synchronize { local_state_locked }
      envelope = Cluster::ReceptionistEnvelope.new(
        @cluster.settings.cluster_name,
        @cluster.self_unique_address,
        state.revision,
        state.registrations
      )
      @telemetry.sync_sent
      @transport.send_async(target, envelope)
    end

    private def validate_envelope_identity(
      envelope : Cluster::ReceptionistEnvelope,
      sender_path : ActorPath?,
      remote_address : Address?,
      remote_node_uid : String?,
    ) : Nil
      unless envelope.cluster_name == @cluster.settings.cluster_name
        raise Cluster::ReceptionistConfigurationError.new("receptionist cluster name mismatch")
      end
      sender = envelope.sender
      unless sender_path &&
             sender_path.address == sender.address &&
             sender_path.elements == ["system", DAEMON_NAME] &&
             remote_address == sender.address &&
             remote_node_uid == sender.node_uid
        raise Cluster::ReceptionistConfigurationError.new(
          "receptionist sender identity does not match its remoting association"
        )
      end
      known = @cluster.snapshot.member(sender)
      unless known && (known.status.up? || known.status.leaving?)
        raise Cluster::ReceptionistConfigurationError.new(
          "receptionist sender is not an active cluster member"
        )
      end
    end

    private def validate_registrations(envelope : Cluster::ReceptionistEnvelope) : Nil
      if envelope.revision < 0_i64
        raise Cluster::ReceptionistConfigurationError.new("receptionist revision must not be negative")
      end
      registrations = envelope.registrations
      if registrations.size > Cluster::ReceptionistLimits::REGISTRATIONS_PER_NODE
        raise Cluster::ReceptionistConfigurationError.new(
          "remote receptionist registration capacity " \
          "#{Cluster::ReceptionistLimits::REGISTRATIONS_PER_NODE} exceeded"
        )
      end
      if registrations.uniq.size != registrations.size
        raise Cluster::ReceptionistConfigurationError.new("receptionist state contains duplicate registrations")
      end
      candidate_types = {} of String => String
      registrations.each do |registration|
        unless registration.owner == envelope.sender &&
               registration.actor_path.address == envelope.sender.address
          raise Cluster::ReceptionistConfigurationError.new(
            "receptionist registration is not owned by its sending member"
          )
        end
        if registration.key_name.empty? ||
           registration.key_name.bytesize > Cluster::ReceptionistLimits::SERVICE_KEY_BYTES
          raise Cluster::ReceptionistConfigurationError.new("invalid receptionist service key")
        end
        if registration.message_type_name.empty? ||
           registration.message_type_name.bytesize > Cluster::ReceptionistLimits::MESSAGE_TYPE_BYTES
          raise Cluster::ReceptionistConfigurationError.new("invalid receptionist message type")
        end
        if registration.actor_path.to_s.bytesize > Cluster::ReceptionistLimits::ACTOR_PATH_BYTES
          raise Cluster::ReceptionistConfigurationError.new("receptionist actor path is too long")
        end
        if existing = candidate_types[registration.key_name]?
          unless existing == registration.message_type_name
            raise Cluster::ReceptionistConfigurationError.new(
              "receptionist state binds one key to multiple message types"
            )
          end
        else
          candidate_types[registration.key_name] = registration.message_type_name
        end
      end
    end

    private def merge_remote_state(envelope : Cluster::ReceptionistEnvelope) : Bool
      registrations = envelope.registrations.sort_by do |registration|
        {registration.key_name, registration.actor_path.to_s}
      end
      replacement = Cluster::ReceptionistNodeState.new(envelope.revision, registrations)
      @mutex.synchronize do
        current = @states[envelope.sender]?
        if current
          return false if envelope.revision < current.revision
          if envelope.revision == current.revision
            unless registrations == current.registrations
              raise Cluster::ReceptionistConfigurationError.new(
                "conflicting receptionist state at revision #{envelope.revision}"
              )
            end
            return false
          end
        end
        known_without_sender = @states.sum do |owner, state|
          owner == envelope.sender ? 0 : state.registrations.size
        end
        if known_without_sender + registrations.size > Cluster::ReceptionistLimits::KNOWN_REGISTRATIONS
          raise Cluster::ReceptionistConfigurationError.new(
            "receptionist known-registration capacity " \
            "#{Cluster::ReceptionistLimits::KNOWN_REGISTRATIONS} exceeded"
          )
        end
        new_key_types = validate_remote_key_types_locked(registrations)
        new_key_types.each { |name, message_type| @key_types[name] = message_type }
        @states[envelope.sender] = replacement
        true
      end
    end

    private def validate_remote_key_types_locked(
      registrations : Array(Cluster::ServiceRegistration),
    ) : Hash(String, String)
      additions = {} of String => String
      registrations.each do |registration|
        key_name = registration.key_name
        message_type_name = registration.message_type_name
        existing = @key_types[key_name]? || additions[key_name]?
        if existing && existing != message_type_name
          raise Cluster::ReceptionistConfigurationError.new(
            "Service key #{key_name} is already bound to #{existing}, not #{message_type_name}"
          )
        end
        additions[key_name] = message_type_name unless existing
      end
      if @key_types.size + additions.size > Cluster::ReceptionistLimits::KNOWN_SERVICE_KEYS
        raise Cluster::ReceptionistConfigurationError.new(
          "receptionist service-key capacity " \
          "#{Cluster::ReceptionistLimits::KNOWN_SERVICE_KEYS} exceeded"
        )
      end
      additions
    end

    private def purge_terminal_states : Nil
      active = @cluster.snapshot.members.each_with_object(Set(Cluster::UniqueAddress).new) do |member, nodes|
        unless member.status.down? || member.status.removed?
          nodes << member.unique_address
        end
      end
      purged = @mutex.synchronize do
        before = @states.size
        @states.reject! { |owner, _state| !active.includes?(owner) }
        before - @states.size
      end
      purged.times { @telemetry.node_purged }
    end

    private def purge_node(owner : Cluster::UniqueAddress) : Nil
      removed = @mutex.synchronize { !@states.delete(owner).nil? }
      @telemetry.node_purged if removed
    end
  end
end
