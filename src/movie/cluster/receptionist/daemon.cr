module Movie::Cluster
  class ReceptionistDaemon < Movie::AbstractBehavior(ReceptionistEnvelope)
    def initialize(@extension : Movie::ClusterReceptionistExtension)
    end

    def receive(
      message : ReceptionistEnvelope,
      context : Movie::ActorContext(ReceptionistEnvelope),
    )
      sender = context.sender
      @extension.handle_envelope(
        message,
        sender.try(&.path),
        sender.try(&.remote_peer_address),
        sender.try(&.remote_peer_node_uid)
      )
      Movie::Behaviors(ReceptionistEnvelope).same
    end
  end

  class ReceptionistClusterEventListener < Movie::AbstractBehavior(ClusterEvent)
    def initialize(@extension : Movie::ClusterReceptionistExtension)
    end

    def receive(message : ClusterEvent, context : Movie::ActorContext(ClusterEvent))
      @extension.handle_cluster_event(message)
      Movie::Behaviors(ClusterEvent).same
    end
  end

  class ReceptionistWatcher < Movie::AbstractBehavior(Nil)
    def initialize(@extension : Movie::ClusterReceptionistExtension)
    end

    def receive(message : Nil, context : Movie::ActorContext(Nil))
      Movie::Behaviors(Nil).same
    end

    def on_signal(signal : Movie::SystemMessage)
      if terminated = signal.as?(Movie::Terminated)
        @extension.local_actor_terminated(terminated.actor)
      end
    end
  end
end
