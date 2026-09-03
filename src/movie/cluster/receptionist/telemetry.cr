module Movie::Cluster
  record ReceptionistStats,
    local_registrations : Int32,
    known_nodes : Int32,
    visible_services : Int32,
    subscribers : Int32,
    registrations : Int64,
    deregistrations : Int64,
    sync_rounds : Int64,
    sync_messages_sent : Int64,
    sync_messages_received : Int64,
    listing_updates : Int64,
    purged_nodes : Int64,
    protocol_rejections : Int64

  class ReceptionistTelemetry
    @registrations = Atomic(Int64).new(0_i64)
    @deregistrations = Atomic(Int64).new(0_i64)
    @sync_rounds = Atomic(Int64).new(0_i64)
    @sync_messages_sent = Atomic(Int64).new(0_i64)
    @sync_messages_received = Atomic(Int64).new(0_i64)
    @listing_updates = Atomic(Int64).new(0_i64)
    @purged_nodes = Atomic(Int64).new(0_i64)
    @protocol_rejections = Atomic(Int64).new(0_i64)

    def registered : Nil
      @registrations.add(1_i64)
    end

    def deregistered(count : Int32 = 1) : Nil
      @deregistrations.add(count.to_i64) if count > 0
    end

    def sync_round : Nil
      @sync_rounds.add(1_i64)
    end

    def sync_sent : Nil
      @sync_messages_sent.add(1_i64)
    end

    def sync_received : Nil
      @sync_messages_received.add(1_i64)
    end

    def listing_updated : Nil
      @listing_updates.add(1_i64)
    end

    def node_purged : Nil
      @purged_nodes.add(1_i64)
    end

    def protocol_rejected : Nil
      @protocol_rejections.add(1_i64)
    end

    def snapshot(
      local_registrations : Int32,
      known_nodes : Int32,
      visible_services : Int32,
      subscribers : Int32,
    ) : ReceptionistStats
      ReceptionistStats.new(
        local_registrations: local_registrations,
        known_nodes: known_nodes,
        visible_services: visible_services,
        subscribers: subscribers,
        registrations: @registrations.get,
        deregistrations: @deregistrations.get,
        sync_rounds: @sync_rounds.get,
        sync_messages_sent: @sync_messages_sent.get,
        sync_messages_received: @sync_messages_received.get,
        listing_updates: @listing_updates.get,
        purged_nodes: @purged_nodes.get,
        protocol_rejections: @protocol_rejections.get
      )
    end
  end
end
