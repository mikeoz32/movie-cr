module Movie
  module Remote
    # Akka-style extension id for remoting.
    # Uses system config for host/port/stripe-count and requires remoting.enabled = true.
    class Remoting < Movie::ExtensionId(RemoteExtension)
      def create(system : Movie::AbstractActorSystem) : RemoteExtension
        cfg = system.config
        enabled = cfg.get_bool(ActorSystemConfig::REMOTING_ENABLED, false)
        raise "Remoting not enabled. Set remoting.enabled = true in config." unless enabled

        host = cfg.get_string(ActorSystemConfig::REMOTING_HOST, "127.0.0.1")
        port = cfg.get_int(ActorSystemConfig::REMOTING_PORT, 2552)
        stripe_count = cfg.get_int(ActorSystemConfig::REMOTING_STRIPE_COUNT, StripedConnectionPool::DEFAULT_STRIPE_COUNT)
        RemoteExtension.new(system, host, port, stripe_count)
      end
    end
  end
end
