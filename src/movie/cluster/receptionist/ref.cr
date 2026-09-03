module Movie::Cluster
  # A typed actor reference resolved from a receptionist listing.
  class ServiceRef(T) < Movie::ActorRefBase
    getter service_path : Movie::ActorPath

    @local : Movie::ActorRef(T)?
    @remote : Movie::Remote::RemoteActorRef(T)?

    def initialize(system : Movie::AbstractActorSystem, @service_path : Movie::ActorPath)
      @local = nil
      @remote = nil
      if system.local_path?(@service_path)
        @local = system.actor_for(@service_path, T).as(Movie::ActorRef(T))
      else
        remote = system.remote || raise ReceptionistConfigurationError.new(
          "remoting is required to resolve #{@service_path}"
        )
        @remote = remote.actor_ref(@service_path, T)
      end
      super(0, @service_path)
    end

    def <<(message : T) : Nil
      with_target { |target| target << message }
    end

    def tell_from(sender : Movie::ActorRefBase?, message : T) : Nil
      with_target { |target| target.tell_from(sender, message) }
    end

    def ask(
      message : T,
      response_type : R.class,
      timeout : Time::Span = 30.seconds,
    ) : Movie::Future(R) forall R
      with_target { |target| target.ask(message, response_type, timeout) }
    end

    def send_system(message : Movie::SystemMessage) : Nil
      with_target { |target| target.send_system(message) }
    end

    def ==(other : Movie::ActorRefBase) : Bool
      other.path == @service_path
    end

    def hash(hasher)
      @service_path.hash(hasher)
    end

    private macro with_target(&block)
      if %target = @local
        {{ block.args.first.id }} = %target
        {{ block.body }}
      else
        {{ block.args.first.id }} = @remote.not_nil!
        {{ block.body }}
      end
    end
  end
end
