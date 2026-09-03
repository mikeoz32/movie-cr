require "json"
require "../../remote/json_payload"
require "../../remote/message_registry"
require "./strategies"

module Movie::Cluster
  class ClusterShardingConfigurationError < Exception
  end

  class NoShardOwnerError < Exception
    getter entity_type : String
    getter shard_id : Int32

    def initialize(@entity_type : String, @shard_id : Int32)
      super("No eligible owner for #{@entity_type} shard #{@shard_id}")
    end
  end

  class ShardLeaseUnavailableError < Exception
    getter entity_type : String
    getter shard_id : Int32

    def initialize(@entity_type : String, @shard_id : Int32)
      super("Persistent shard #{@entity_type}/#{@shard_id} is fenced by another owner")
    end
  end

  class ShardHandoffInProgressError < Exception
    getter entity_type : String
    getter shard_id : Int32

    def initialize(@entity_type : String, @shard_id : Int32)
      super("Shard #{@entity_type}/#{@shard_id} is draining on its previous owner")
    end
  end

  class ShardingSettings
    getter shard_count : Int32
    getter partitioner : EntityPartitioner
    getter allocation : ShardAllocationStrategy
    getter rebalance : RebalancePolicy
    getter lease_duration : Time::Span
    getter lease_renew_interval : Time::Span
    getter idle_timeout : Time::Span?

    def initialize(
      @shard_count : Int32 = 256,
      @partitioner : EntityPartitioner = StableHashPartitioner.new,
      @allocation : ShardAllocationStrategy = LeastLoadedAllocation.new,
      @rebalance : RebalancePolicy = RateLimitedRebalance.new,
      @lease_duration : Time::Span = 10.seconds,
      @lease_renew_interval : Time::Span = 3.seconds,
      @idle_timeout : Time::Span? = nil,
    )
      raise ArgumentError.new("shard count must be positive") unless @shard_count > 0
      raise ArgumentError.new("shard lease duration must be positive") unless @lease_duration > Time::Span.zero
      unless @lease_renew_interval > Time::Span.zero && @lease_renew_interval < @lease_duration
        raise ArgumentError.new("shard lease renewal interval must be positive and shorter than its duration")
      end
      if idle_timeout = @idle_timeout
        raise ArgumentError.new("entity idle timeout must be positive") unless idle_timeout > Time::Span.zero
      end
    end

    def compatible?(other : self) : Bool
      @shard_count == other.shard_count &&
        @partitioner.configuration_key == other.partitioner.configuration_key &&
        @allocation.configuration_key == other.allocation.configuration_key &&
        @rebalance.configuration_key == other.rebalance.configuration_key &&
        @lease_duration == other.lease_duration &&
        @lease_renew_interval == other.lease_renew_interval &&
        @idle_timeout == other.idle_timeout
    end

    def configuration_key : String
      String.build do |io|
        io << @shard_count << '|'
        io << @partitioner.configuration_key << '|'
        io << @allocation.configuration_key << '|'
        io << @rebalance.configuration_key << '|'
        io << @lease_duration.total_nanoseconds << '|'
        io << @lease_renew_interval.total_nanoseconds << '|'
        if timeout = @idle_timeout
          io << timeout.total_nanoseconds
        else
          io << "none"
        end
      end
    end
  end

  record ShardedEntityType(T), name : String

  struct ShardAssignment
    include JSON::Serializable

    getter shard_id : Int32
    getter owner : UniqueAddress

    def initialize(@shard_id : Int32, @owner : UniqueAddress)
    end
  end

  struct ShardingControlAck
    include JSON::Serializable

    getter accepted : Bool

    def initialize(@accepted : Bool = true)
    end
  end

  # Nested message payload with a direct JSON writer. The type tag precedes the
  # value on the wire, allowing the receiving pull parser to decode a registered
  # message without materializing JSON::Any or an intermediate payload String.
  class ShardingMessage
    getter message_type : String
    getter payload : Movie::Remote::JsonPayload
    getter wrapper : Movie::Remote::MessageWrapper?

    def initialize(
      @message_type : String,
      @payload : Movie::Remote::JsonPayload,
      @wrapper : Movie::Remote::MessageWrapper? = nil,
    )
    end
  end

  module ShardingMessageConverter
    extend self

    def to_json(value : ShardingMessage, json : JSON::Builder) : Nil
      json.object do
        json.field("type", value.message_type)
        json.field("payload") { value.payload.to_json(json) }
      end
    end

    def from_json(pull : JSON::PullParser) : ShardingMessage
      message_type = nil.as(String?)
      payload = nil.as(Movie::Remote::JsonPayload?)
      pull.read_object do |key|
        case key
        when "type"
          message_type = pull.read_string
        when "payload"
          type = message_type
          payload = if type
                      Movie::Remote::MessageRegistry.decode_payload(type, pull)
                    else
                      Movie::Remote::RawJsonPayload.new(pull.read_raw)
                    end
        else
          pull.skip
        end
      end
      type = message_type || pull.raise("Missing JSON attribute: type")
      ShardingMessage.new(
        type,
        payload || pull.raise("Missing JSON attribute: payload")
      )
    end
  end

  struct ShardingEnvelope
    include JSON::Serializable

    enum Kind
      User
      Activate
      Passivate
      PassivateShard
      PrepareShard
      PlanRequest
      PlanSnapshotRequest
      PlanSnapshot
      PlanUpdate
      HandoffComplete
    end

    getter kind : Kind
    getter entity_type : String
    getter entity_id : String
    getter shard_id : Int32
    getter hop_count : Int32
    getter delivery_attempt : Int32
    getter settings_key : String
    getter coordinator : UniqueAddress?
    getter requester : UniqueAddress?
    getter next_owner : UniqueAddress?
    getter assignments : Array(ShardAssignment)
    getter ask_timeout_ms : Int64?
    getter plan_generation : Int64
    getter senderless : Bool

    @[JSON::Field(converter: Movie::Cluster::ShardingMessageConverter)]
    getter message : ShardingMessage

    def initialize(
      @entity_type : String,
      @entity_id : String,
      @shard_id : Int32,
      @message : ShardingMessage,
      @settings_key : String,
      @hop_count : Int32 = 0,
      @kind : Kind = Kind::User,
      @delivery_attempt : Int32 = 0,
      @coordinator : UniqueAddress? = nil,
      @requester : UniqueAddress? = nil,
      @next_owner : UniqueAddress? = nil,
      @assignments : Array(ShardAssignment) = [] of ShardAssignment,
      @ask_timeout_ms : Int64? = nil,
      @plan_generation : Int64 = 0_i64,
      @senderless : Bool = false,
    )
    end

    def as_ask(timeout : Time::Span) : self
      self.class.new(
        entity_type: @entity_type,
        entity_id: @entity_id,
        shard_id: @shard_id,
        message: @message,
        settings_key: @settings_key,
        hop_count: @hop_count,
        kind: @kind,
        delivery_attempt: @delivery_attempt,
        coordinator: @coordinator,
        requester: @requester,
        next_owner: @next_owner,
        assignments: @assignments,
        ask_timeout_ms: timeout.total_milliseconds.to_i64,
        plan_generation: @plan_generation,
        senderless: @senderless
      )
    end

    def forwarded(coordinator : UniqueAddress, senderless : Bool = false) : self
      self.class.new(
        entity_type: @entity_type,
        entity_id: @entity_id,
        shard_id: @shard_id,
        message: @message,
        settings_key: @settings_key,
        hop_count: @hop_count + 1,
        kind: @kind,
        delivery_attempt: @delivery_attempt,
        coordinator: coordinator,
        requester: @requester,
        next_owner: @next_owner,
        assignments: @assignments,
        ask_timeout_ms: @ask_timeout_ms,
        plan_generation: @plan_generation,
        senderless: senderless
      )
    end

    def rerouted : self
      self.class.new(
        entity_type: @entity_type,
        entity_id: @entity_id,
        shard_id: @shard_id,
        message: @message,
        settings_key: @settings_key,
        kind: @kind,
        delivery_attempt: @delivery_attempt + 1,
        coordinator: @coordinator,
        requester: @requester,
        next_owner: @next_owner,
        assignments: @assignments,
        ask_timeout_ms: @ask_timeout_ms,
        plan_generation: @plan_generation,
        senderless: @senderless
      )
    end

    def self.passivate(
      entity_type : String,
      entity_id : String,
      shard_id : Int32,
      settings_key : String,
    ) : self
      new(
        entity_type,
        entity_id,
        shard_id,
        empty_message,
        settings_key,
        kind: Kind::Passivate
      )
    end

    def self.activate(
      entity_type : String,
      entity_id : String,
      shard_id : Int32,
      settings_key : String,
    ) : self
      new(
        entity_type,
        entity_id,
        shard_id,
        empty_message,
        settings_key,
        kind: Kind::Activate
      )
    end

    def self.passivate_shard(
      entity_type : String,
      shard_id : Int32,
      settings_key : String,
      coordinator : UniqueAddress,
      next_owner : UniqueAddress?,
    ) : self
      new(
        entity_type,
        "",
        shard_id,
        empty_message,
        settings_key,
        kind: Kind::PassivateShard,
        coordinator: coordinator,
        next_owner: next_owner
      )
    end

    def self.plan_request(
      entity_type : String,
      settings_key : String,
      requester : UniqueAddress,
    ) : self
      new(
        entity_type,
        "",
        0,
        empty_message,
        settings_key,
        kind: Kind::PlanRequest,
        requester: requester
      )
    end

    def self.prepare_shard(
      entity_type : String,
      shard_id : Int32,
      settings_key : String,
      coordinator : UniqueAddress,
    ) : self
      new(
        entity_type,
        "",
        shard_id,
        empty_message,
        settings_key,
        kind: Kind::PrepareShard,
        coordinator: coordinator
      )
    end

    def self.plan_snapshot_request(
      entity_type : String,
      settings_key : String,
      requester : UniqueAddress,
    ) : self
      new(
        entity_type,
        "",
        0,
        empty_message,
        settings_key,
        kind: Kind::PlanSnapshotRequest,
        requester: requester
      )
    end

    def self.plan_snapshot(
      entity_type : String,
      settings_key : String,
      coordinator : UniqueAddress,
      generation : Int64,
      allocations : ShardAllocations,
    ) : self
      new(
        entity_type,
        "",
        0,
        empty_message,
        settings_key,
        kind: Kind::PlanSnapshot,
        coordinator: coordinator,
        assignments: assignments_for(allocations),
        plan_generation: generation
      )
    end

    def self.plan_update(
      entity_type : String,
      settings_key : String,
      coordinator : UniqueAddress,
      generation : Int64,
      allocations : ShardAllocations,
    ) : self
      new(
        entity_type,
        "",
        0,
        empty_message,
        settings_key,
        kind: Kind::PlanUpdate,
        coordinator: coordinator,
        assignments: assignments_for(allocations),
        plan_generation: generation
      )
    end

    def self.handoff_complete(
      entity_type : String,
      shard_id : Int32,
      settings_key : String,
      coordinator : UniqueAddress,
      next_owner : UniqueAddress?,
    ) : self
      new(
        entity_type,
        "",
        shard_id,
        empty_message,
        settings_key,
        kind: Kind::HandoffComplete,
        coordinator: coordinator,
        next_owner: next_owner
      )
    end

    private def self.empty_message : ShardingMessage
      ShardingMessage.new(
        "movie.cluster.sharding.control.empty.v1",
        Movie::Remote::RawJsonPayload.new("{}")
      )
    end

    private def self.assignments_for(allocations : ShardAllocations) : Array(ShardAssignment)
      allocations.map do |shard_id, owner|
        ShardAssignment.new(shard_id, owner)
      end.sort_by(&.shard_id)
    end
  end
end
