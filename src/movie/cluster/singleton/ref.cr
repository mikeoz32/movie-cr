module Movie::Cluster
  class ClusterSingletonRef(T) < Movie::ActorRefBase
    getter name : String

    def initialize(
      @extension : Movie::ClusterSingletonExtension,
      @name : String,
      @delegate : ShardedEntityRef(T),
    )
      super(0)
    end

    def <<(message : T) : Nil
      tell_from(nil, message)
    end

    def tell_from(sender : Movie::ActorRefBase?, message : T) : Nil
      @extension.record_tell
      @delegate.tell_from(sender, message)
    rescue error
      @extension.record_routing_rejection(error)
      raise error
    end

    def ask(
      message : T,
      response_type : R.class,
      timeout : Time::Span = 30.seconds,
    ) : Movie::Future(R) forall R
      @extension.record_ask
      future = @delegate.ask(message, response_type, timeout)
      future.on_failure { |error| @extension.record_routing_rejection(error) }
      future
    rescue error
      @extension.record_routing_rejection(error)
      raise error
    end

    def owner : UniqueAddress?
      @extension.owner(@name)
    end

    def locally_owned? : Bool
      @extension.locally_owned?(@name)
    end

    def send_system(message : Movie::SystemMessage) : Nil
      case message
      when Movie::Stop
        @extension.record_stop
        @delegate.send_system(message)
        @extension.ensure_active(@name)
      else
        raise Movie::Remote::RemoteUnsupportedSystemMessageError.new(
          "Cluster singleton references only support Stop"
        )
      end
    rescue error
      @extension.record_routing_rejection(error)
      raise error
    end
  end
end
