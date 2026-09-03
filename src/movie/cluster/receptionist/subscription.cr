module Movie::Cluster
  module ReceptionistListingBuilder
    Log = ::Log.for(self)

    def self.build(
      system : Movie::AbstractActorSystem,
      key : ServiceKey(T),
      registrations : Array(ServiceRegistration),
    ) : ReceptionistListing(T) forall T
      services = registrations.compact_map do |registration|
        begin
          ServiceRef(T).new(system, registration.actor_path)
        rescue error
          Log.debug do
            "Skipped unresolved receptionist service #{registration.actor_path}: #{error.message}"
          end
          nil
        end
      end
      ReceptionistListing(T).new(key, services)
    end
  end

  abstract class ReceptionistSubscriptionBase
    getter key_name : String
    getter message_type_name : String
    getter subscriber_id : Int32

    def initialize(
      @key_name : String,
      @message_type_name : String,
      @subscriber_id : Int32,
    )
    end

    abstract def publish(registrations : Array(ServiceRegistration)) : Bool
  end

  class ReceptionistSubscription(T) < ReceptionistSubscriptionBase
    def initialize(
      @system : Movie::AbstractActorSystem,
      @key : ServiceKey(T),
      @subscriber : Movie::ActorRef(ReceptionistListing(T)),
    )
      super(@key.name, T.name, @subscriber.id)
    end

    def publish(registrations : Array(ServiceRegistration)) : Bool
      @subscriber << ReceptionistListingBuilder.build(@system, @key, registrations)
      true
    rescue
      false
    end
  end
end
