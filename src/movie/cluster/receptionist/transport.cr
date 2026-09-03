module Movie::Cluster
  class ReceptionistTransport
    Log = ::Log.for(self)

    @daemon : Movie::ActorRef(ReceptionistEnvelope)?
    @remote_refs = {} of String => Movie::Remote::RemoteActorRef(ReceptionistEnvelope)
    @mutex = Mutex.new

    def initialize(@remote : Movie::Remote::RemoteExtension)
    end

    def daemon=(daemon : Movie::ActorRef(ReceptionistEnvelope)) : Nil
      @daemon = daemon
    end

    def send(target : UniqueAddress, envelope : ReceptionistEnvelope) : Nil
      daemon = @daemon || return
      remote_ref(target).tell_from(daemon, envelope)
    end

    def send_async(target : UniqueAddress, envelope : ReceptionistEnvelope) : Nil
      spawn do
        send(target, envelope)
      rescue error
        Log.debug { "Receptionist sync to #{target} failed: #{error.message}" }
      end
    end

    def close : Nil
      @mutex.synchronize { @remote_refs.clear }
      @daemon = nil
    end

    private def remote_ref(target : UniqueAddress) : Movie::Remote::RemoteActorRef(ReceptionistEnvelope)
      key = target.to_s
      @mutex.synchronize do
        @remote_refs[key] ||= @remote.actor_ref(
          Movie::ActorPath.new(target.address, ["system", Movie::ClusterReceptionistExtension::DAEMON_NAME]),
          ReceptionistEnvelope
        )
      end
    end
  end
end
