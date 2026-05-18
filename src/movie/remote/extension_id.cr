module Movie
  module Remote
    # Akka-style extension id for remoting.
    # Uses system config for host/port/stripe-count and requires remoting.enabled = true.
    class Remoting < Movie::ExtensionId(RemoteExtension)
      def create(system : Movie::AbstractActorSystem) : RemoteExtension
        cfg = system.config
        enabled = cfg.get_bool("remoting.enabled", false)
        raise "Remoting not enabled. Set remoting.enabled = true in config." unless enabled

        host = cfg.get_string("remoting.host", "127.0.0.1")
        port = cfg.get_int("remoting.port", 2552)
        stripe_count = cfg.get_int("remoting.stripe-count", StripedConnectionPool::DEFAULT_STRIPE_COUNT)
        system.enable_remoting(host, port, stripe_count)
      end
    end

  end
end
