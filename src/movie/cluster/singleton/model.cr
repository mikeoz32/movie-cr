module Movie::Cluster
  enum SingletonBackingKind
    Behavior
    EventSourced
    DurableState
  end

  # The complete compatibility identity of the singleton implementation below
  # its public logical name. Keeping this structured avoids ambiguous delimiter
  # strings while still providing a canonical key to cluster sharding.
  struct SingletonBackingIdentity
    getter kind : SingletonBackingKind
    getter entity_type_name : String?
    getter message_type_name : String
    getter entity_id : String
    getter settings_key : String
    getter lease_duration : Time::Span?
    getter lease_renew_interval : Time::Span?

    def initialize(
      @kind : SingletonBackingKind,
      @entity_type_name : String?,
      @message_type_name : String,
      @entity_id : String,
      @settings_key : String,
      @lease_duration : Time::Span? = nil,
      @lease_renew_interval : Time::Span? = nil,
    )
      raise ArgumentError.new("singleton entity id must not be empty") if @entity_id.empty?
      raise ArgumentError.new("singleton message type must not be empty") if @message_type_name.empty?
    end

    def configuration_key : String
      String.build do |io|
        io << @kind.value << ';'
        write_optional_string(io, @entity_type_name)
        write_string(io, @message_type_name)
        write_string(io, @entity_id)
        write_string(io, @settings_key)
        write_optional_span(io, @lease_duration)
        write_optional_span(io, @lease_renew_interval)
      end
    end

    private def write_optional_string(io : IO, value : String?) : Nil
      if value
        io << '1'
        write_string(io, value)
      else
        io << '0'
      end
    end

    private def write_string(io : IO, value : String) : Nil
      io << value.bytesize << ':' << value
    end

    private def write_optional_span(io : IO, value : Time::Span?) : Nil
      if value
        io << '1' << value.total_nanoseconds << ';'
      else
        io << '0'
      end
    end
  end

  # A singleton always occupies shard zero, while its identity is embedded in
  # the sharding compatibility key so nodes cannot silently disagree on the
  # backing entity id or persistence mode.
  class SingletonPartitioner < EntityPartitioner
    def initialize(@identity : SingletonBackingIdentity, @entity_id : String)
      raise ArgumentError.new("singleton entity id must not be empty") if @entity_id.empty?
    end

    def shard_for(entity_type : String, entity_id : String, shard_count : Int32) : Int32
      validate_shard_count(shard_count)
      unless entity_id == @entity_id
        raise ClusterShardingConfigurationError.new(
          "Singleton #{@identity.configuration_key} received unexpected entity id #{entity_id}"
        )
      end
      0
    end

    def configuration_key : String
      identity = @identity.configuration_key
      String.build do |io|
        io << self.class.name << ':' << identity.bytesize << ':' << identity
      end
    end
  end

  class ClusterSingletonConfigurationError < Exception
  end

  class SingletonSettings
    getter roles : Array(String)
    getter activation_interval : Time::Span
    getter activation_timeout : Time::Span

    def initialize(
      roles : Array(String) = [] of String,
      @activation_interval : Time::Span = 250.milliseconds,
      @activation_timeout : Time::Span = 2.seconds,
    )
      raise ArgumentError.new("singleton roles must not be empty") if roles.any?(&.empty?)
      unless @activation_interval > Time::Span.zero
        raise ArgumentError.new("singleton activation interval must be positive")
      end
      unless @activation_timeout > Time::Span.zero
        raise ArgumentError.new("singleton activation timeout must be positive")
      end
      @roles = roles.uniq.sort
    end

    def configuration_key : String
      String.build do |io|
        io << @roles.size << ';'
        @roles.each { |role| io << role.bytesize << ':' << role }
        io << @activation_interval.total_nanoseconds << ';'
        io << @activation_timeout.total_nanoseconds
      end
    end
  end

  abstract class SingletonRegistrationBase
    getter name : String
    getter message_type_name : String
    getter settings : SingletonSettings
    getter backing_identity : SingletonBackingIdentity

    def initialize(
      @name : String,
      @message_type_name : String,
      @settings : SingletonSettings,
      @backing_identity : SingletonBackingIdentity,
    )
    end

    abstract def activate : Movie::Future(ShardingControlAck)
    abstract def owner : UniqueAddress?
    abstract def handoff_in_progress? : Bool
  end

  class SingletonRegistration(T) < SingletonRegistrationBase
    getter ref : ClusterSingletonRef(T)

    def initialize(
      name : String,
      settings : SingletonSettings,
      backing_identity : SingletonBackingIdentity,
      @sharding : Movie::ClusterShardingExtension,
      @entity_type : ShardedEntityType(T),
      @entity_id : String,
      extension : Movie::ClusterSingletonExtension,
    )
      super(name, T.name, settings, backing_identity)
      @ref = ClusterSingletonRef(T).new(
        extension,
        name,
        @sharding.entity_ref_for(@entity_type, @entity_id)
      )
    end

    def activate : Movie::Future(ShardingControlAck)
      @sharding.activate(@entity_type, @entity_id, @settings.activation_timeout)
    end

    def owner : UniqueAddress?
      @sharding.owner_for(@entity_type, @entity_id)
    end

    def handoff_in_progress? : Bool
      @sharding.handoff_in_progress?(@entity_type, @entity_id)
    end
  end
end
