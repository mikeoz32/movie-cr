require "./protocol"

module Movie::Cluster
  class ClusterDaemon < Movie::AbstractBehavior(ProtocolMessage)
    def initialize(@extension : ClusterExtension)
    end

    def receive(message : ProtocolMessage, context : Movie::ActorContext(ProtocolMessage))
      sender = context.sender
      remote_peer = sender.as?(Movie::Remote::RemotePeerIdentity)
      @extension.handle_protocol(
        message,
        sender.try(&.path),
        remote_peer.try(&.remote_address),
        remote_peer.try(&.remote_node_uid)
      )
      Movie::Behaviors(ProtocolMessage).same
    end
  end
end
