module Movie::Cluster
  class ShardedEntityRef(T) < Movie::ActorRefBase
    getter entity_type : String
    getter entity_id : String

    def initialize(
      @extension : Movie::ClusterShardingExtension,
      entity_type : ShardedEntityType(T),
      @entity_id : String,
    )
      raise ArgumentError.new("entity id must not be empty") if @entity_id.empty?
      @entity_type = entity_type.name
      super(0)
    end

    def <<(message : T) : Nil
      tell_from(nil, message)
    end

    def tell_from(sender : Movie::ActorRefBase?, message : T) : Nil
      @extension.route(@entity_type, @entity_id, message, sender)
    end

    def ask(
      message : T,
      response_type : R.class,
      timeout : Time::Span = 30.seconds,
    ) : Movie::Future(R) forall R
      @extension.route_ask(@entity_type, @entity_id, message, response_type, timeout)
    end

    def send_system(message : Movie::SystemMessage) : Nil
      case message
      when Movie::Stop
        @extension.passivate(@entity_type, @entity_id)
      else
        raise Movie::Remote::RemoteUnsupportedSystemMessageError.new(
          "Sharded entity references only support Stop"
        )
      end
    end
  end
end
