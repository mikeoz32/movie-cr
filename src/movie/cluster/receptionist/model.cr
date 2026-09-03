require "json"

module Movie::Cluster
  module ReceptionistLimits
    SERVICE_KEY_BYTES      =     256
    MESSAGE_TYPE_BYTES     =     512
    ACTOR_PATH_BYTES       =   4_096
    REGISTRATIONS_PER_NODE =  10_000
    KNOWN_REGISTRATIONS    = 100_000
    KNOWN_SERVICE_KEYS     = 100_000
    LOCAL_SUBSCRIPTIONS    =  10_000
  end

  class ReceptionistConfigurationError < Exception
  end

  # A stable logical service name bound to one actor message type.
  struct ServiceKey(T)
    getter name : String

    def initialize(@name : String)
      raise ArgumentError.new("service key name must not be empty") if @name.empty?
      if @name.bytesize > ReceptionistLimits::SERVICE_KEY_BYTES
        raise ArgumentError.new(
          "service key name must not exceed #{ReceptionistLimits::SERVICE_KEY_BYTES} bytes"
        )
      end
      if T.name.bytesize > ReceptionistLimits::MESSAGE_TYPE_BYTES
        raise ArgumentError.new(
          "service message type must not exceed #{ReceptionistLimits::MESSAGE_TYPE_BYTES} bytes"
        )
      end
    end

    def message_type_name : String
      T.name
    end
  end

  struct ServiceRegistration
    include JSON::Serializable

    getter key_name : String
    getter message_type_name : String
    getter actor_path : Movie::ActorPath
    getter owner : UniqueAddress

    def initialize(
      @key_name : String,
      @message_type_name : String,
      @actor_path : Movie::ActorPath,
      @owner : UniqueAddress,
    )
    end
  end

  class ReceptionistNodeState
    getter revision : Int64

    @registrations : Array(ServiceRegistration)

    def initialize(@revision : Int64, registrations : Array(ServiceRegistration))
      @registrations = registrations.dup
    end

    def registrations : Array(ServiceRegistration)
      @registrations.dup
    end
  end

  class ReceptionistListing(T)
    getter key : ServiceKey(T)

    @services : Array(ServiceRef(T))

    def initialize(@key : ServiceKey(T), services : Array(ServiceRef(T)))
      @services = services.dup
    end

    def services : Array(ServiceRef(T))
      @services.dup
    end

    def empty? : Bool
      @services.empty?
    end
  end
end
