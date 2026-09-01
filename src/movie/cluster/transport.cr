require "./protocol"

module Movie::Cluster
  class ClusterTransport
    @daemon : Movie::ActorRef(ProtocolMessage)?
    @remote_refs = {} of String => Movie::Remote::RemoteActorRef(ProtocolMessage)
    @mutex = Mutex.new

    def initialize(@remote : Movie::Remote::RemoteExtension)
    end

    def daemon=(daemon : Movie::ActorRef(ProtocolMessage)) : Nil
      @daemon = daemon
    end

    def send(address : Movie::Address, message : ProtocolMessage) : Nil
      daemon = @daemon || return
      remote_ref(address).tell_from(daemon, message)
    end

    def send_async(address : Movie::Address, message : ProtocolMessage) : Nil
      spawn { send(address, message) }
    end

    def close : Nil
      @mutex.synchronize { @remote_refs.clear }
      @daemon = nil
    end

    private def remote_ref(address : Movie::Address) : Movie::Remote::RemoteActorRef(ProtocolMessage)
      key = address.to_s
      @mutex.synchronize do
        @remote_refs[key] ||= @remote.actor_ref(
          Movie::ActorPath.new(address, ["system", CLUSTER_DAEMON_NAME]),
          ProtocolMessage
        )
      end
    end
  end
end
