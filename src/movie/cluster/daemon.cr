require "./protocol"

module Movie::Cluster
  class ClusterDaemon < Movie::AbstractBehavior(ProtocolMessage)
    def initialize(@extension : ClusterExtension)
    end

    def receive(message : ProtocolMessage, context : Movie::ActorContext(ProtocolMessage))
      sender = context.sender
      @extension.handle_protocol(
        message,
        sender.try(&.path),
        sender.try(&.remote_peer_address),
        sender.try(&.remote_peer_node_uid)
      )
      Movie::Behaviors(ProtocolMessage).same
    end
  end
end
