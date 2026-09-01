require "./model"

module Movie::Cluster
  class ShardingDaemon < Movie::AbstractBehavior(ShardingEnvelope)
    def initialize(@extension : Movie::ClusterShardingExtension)
    end

    def receive(
      message : ShardingEnvelope,
      context : Movie::ActorContext(ShardingEnvelope),
    )
      sender = context.sender
      @extension.handle_envelope(
        message,
        sender,
        sender.try(&.remote_peer_address),
        sender.try(&.remote_peer_node_uid)
      )
      Movie::Behaviors(ShardingEnvelope).same
    end
  end

  class ShardingClusterEventListener < Movie::AbstractBehavior(ClusterEvent)
    def initialize(@extension : Movie::ClusterShardingExtension)
    end

    def receive(message : ClusterEvent, context : Movie::ActorContext(ClusterEvent))
      @extension.handle_cluster_event(message)
      Movie::Behaviors(ClusterEvent).same
    end
  end
end
