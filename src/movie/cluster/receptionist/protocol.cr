require "json"

module Movie::Cluster
  struct ReceptionistEnvelope
    include JSON::Serializable

    getter cluster_name : String
    getter sender : UniqueAddress
    getter revision : Int64
    @registrations : Array(ServiceRegistration)

    def initialize(
      @cluster_name : String,
      @sender : UniqueAddress,
      @revision : Int64,
      registrations : Array(ServiceRegistration),
    )
      @registrations = registrations.dup
    end

    def registrations : Array(ServiceRegistration)
      @registrations.dup
    end
  end
end
