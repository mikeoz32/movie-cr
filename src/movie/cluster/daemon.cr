require "./protocol"

module Movie::Cluster
  class ClusterDaemon < Movie::AbstractBehavior(ProtocolMessage)
    def initialize(@extension : ClusterExtension)
    end

    def receive(message : ProtocolMessage, context : Movie::ActorContext(ProtocolMessage))
      @extension.handle_protocol(message, context.sender.try(&.path))
      Movie::Behaviors(ProtocolMessage).same
    end
  end
end
