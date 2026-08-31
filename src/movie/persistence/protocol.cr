module Movie
  module Persistence
    # Persistence id that combines entity type and entity id.
    record Id, entity_type : String, entity_id : String do
      def persistence_id : String
        "#{@entity_type}:#{@entity_id}"
      end
    end

    record EntityId(C), value : Id

    record EntityType(C), name : String do
      def id(entity_id : String) : EntityId(C)
        EntityId(C).new(Id.new(@name, entity_id))
      end
    end

    def self.id(type : T.class, entity_id : String) : Id forall T
      Id.new(type.name, entity_id)
    end

    def self.entity_name(id : Id) : String
      type = id.entity_type.downcase.gsub(/[^a-z0-9]+/, "-")
      ent = id.entity_id.downcase.gsub(/[^a-z0-9]+/, "-")
      "entity-#{type}-#{ent}"
    end

    # A storage request owns its response type and execution dispatch. Adding a
    # new operation does not require another switch in ConnectionActor.
    module ConnectionRequest
      abstract def dispatch(worker : ConnectionWorker, sender : Movie::ActorRefBase?) : Nil
    end

    module TypedConnectionRequest(T)
      include ConnectionRequest

      abstract def execute(connection : BackendConnection) : T

      def retryable_after_connection_loss? : Bool
        false
      end

      def dispatch(worker : ConnectionWorker, sender : Movie::ActorRefBase?) : Nil
        result = worker.execute(retryable: retryable_after_connection_loss?) do |connection|
          execute(connection)
        end
        Movie::Ask.reply_if_asked(sender, result)
      rescue error
        Movie::Ask.fail_if_asked(sender, error, T)
      end
    end

    module RetryableConnectionRequest(T)
      include TypedConnectionRequest(T)

      def retryable_after_connection_loss? : Bool
        true
      end
    end

    # Database connection messages
    alias DbArgs = Array(DB::Any)

    struct DbExec
      include TypedConnectionRequest(Bool)
      getter sql : String
      getter args : DbArgs

      def initialize(@sql : String, @args : DbArgs = [] of DB::Any)
      end

      def execute(connection : BackendConnection) : Bool
        connection.exec(self)
        true
      end
    end

    struct DbExecLastId
      include TypedConnectionRequest(Int64)
      getter sql : String
      getter args : DbArgs

      def initialize(@sql : String, @args : DbArgs = [] of DB::Any)
      end

      def execute(connection : BackendConnection) : Int64
        connection.exec_last_id(self)
      end
    end

    struct DbQueryString
      include TypedConnectionRequest(String?)
      getter sql : String
      getter args : DbArgs

      def initialize(@sql : String, @args : DbArgs = [] of DB::Any)
      end

      def execute(connection : BackendConnection) : String?
        connection.query_string(self)
      end
    end

    struct DbQueryStrings
      include TypedConnectionRequest(Array(String))
      getter sql : String
      getter args : DbArgs

      def initialize(@sql : String, @args : DbArgs = [] of DB::Any)
      end

      def execute(connection : BackendConnection) : Array(String)
        connection.query_strings(self)
      end
    end

    record SerializedEvent, manifest : String, payload : String
    record StoredEvent, sequence_nr : Int64, manifest : String, payload : String
    record SnapshotRecord, sequence_nr : Int64, manifest : String, payload : String
    record StateRecord, revision : Int64, manifest : String, payload : String?, deleted : Bool
    record WriteResult, revision : Int64, duplicate : Bool
    record RetentionResult, deleted_events : Int64, through_sequence_nr : Int64
    record MaintenanceResult, backend : String, completed : Bool
    record EventEnvelope,
      offset : Int64,
      persistence_id : String,
      sequence_nr : Int64,
      manifest : String,
      payload : String
    record EventPage, events : Array(EventEnvelope), next_offset : Int64, has_more : Bool

    struct OutboxEntry
      getter message_id : String
      getter destination : String
      getter manifest : String
      getter payload : String

      def initialize(
        @message_id : String,
        @destination : String,
        @manifest : String,
        @payload : String,
      )
        raise ArgumentError.new("Outbox message id cannot be empty") if @message_id.empty?
        raise ArgumentError.new("Outbox destination cannot be empty") if @destination.empty?
        raise ArgumentError.new("Outbox manifest cannot be empty") if @manifest.empty?
      end
    end

    record StoredOutboxEntry,
      offset : Int64,
      message_id : String,
      persistence_id : String,
      operation_id : OperationId,
      destination : String,
      manifest : String,
      payload : String,
      attempts : Int64,
      last_error : String?

    record OperationId, value : String do
      def initialize(@value : String)
        raise ArgumentError.new("Persistence operation id cannot be empty") if @value.empty?
      end

      def self.random : self
        new(UUID.random.to_s)
      end
    end

    class ConcurrentWriteError < Exception
      getter persistence_id : String
      getter expected_revision : Int64
      getter actual_revision : Int64

      def initialize(@persistence_id : String, @expected_revision : Int64, @actual_revision : Int64)
        super("Concurrent persistence write for #{@persistence_id}: expected revision #{@expected_revision}, actual #{@actual_revision}")
      end
    end

    class OperationConflictError < Exception
      getter persistence_id : String
      getter operation_id : OperationId

      def initialize(@persistence_id : String, @operation_id : OperationId)
        super("Persistence operation #{@operation_id.value} was reused with different content for #{@persistence_id}")
      end
    end

    class UnsafeRetentionError < Exception
      getter persistence_id : String
      getter requested_sequence_nr : Int64
      getter snapshot_sequence_nr : Int64?

      def initialize(
        @persistence_id : String,
        @requested_sequence_nr : Int64,
        @snapshot_sequence_nr : Int64?,
      )
        covered = @snapshot_sequence_nr.try(&.to_s) || "none"
        super("Cannot delete events for #{@persistence_id} through #{@requested_sequence_nr}; snapshot covers #{covered}")
      end
    end

    class ProjectionOffsetRegressionError < Exception
      getter projection : String
      getter requested_offset : Int64
      getter stored_offset : Int64

      def initialize(@projection : String, @requested_offset : Int64, @stored_offset : Int64)
        super("Projection #{@projection} cannot move from #{@stored_offset} back to #{@requested_offset}")
      end
    end

    class ProjectionBehindRetentionError < Exception
      getter projection_offset : Int64
      getter required_offset : Int64

      def initialize(@projection_offset : Int64, @required_offset : Int64)
        super("Cannot retain events through query offset #{@required_offset}; a projection is at #{@projection_offset}")
      end
    end

    struct EnsureEventStore
      include RetryableConnectionRequest(Bool)

      def execute(connection : BackendConnection) : Bool
        connection.ensure_event_store
        true
      end
    end

    struct EnsureStateStore
      include RetryableConnectionRequest(Bool)

      def execute(connection : BackendConnection) : Bool
        connection.ensure_state_store
        true
      end
    end

    struct GetSchemaVersion
      include RetryableConnectionRequest(Int64)

      def execute(connection : BackendConnection) : Int64
        connection.schema_version
      end
    end

    struct AppendEvents
      include RetryableConnectionRequest(WriteResult)
      getter persistence_id : String
      getter expected_revision : Int64
      getter operation_id : OperationId
      getter events : Array(SerializedEvent)
      getter outbox : Array(OutboxEntry)

      def initialize(
        @persistence_id : String,
        @expected_revision : Int64,
        @operation_id : OperationId,
        @events : Array(SerializedEvent),
        @outbox : Array(OutboxEntry) = [] of OutboxEntry,
      )
      end

      def execute(connection : BackendConnection) : WriteResult
        connection.append_events(self)
      end
    end

    struct LoadEvents
      include RetryableConnectionRequest(Array(StoredEvent))
      getter persistence_id : String
      getter after_sequence_nr : Int64

      def initialize(@persistence_id : String, @after_sequence_nr : Int64 = 0_i64)
      end

      def execute(connection : BackendConnection) : Array(StoredEvent)
        connection.load_events(self)
      end
    end

    struct SaveSnapshot
      include RetryableConnectionRequest(Bool)
      getter persistence_id : String
      getter snapshot : SnapshotRecord

      def initialize(@persistence_id : String, @snapshot : SnapshotRecord)
      end

      def execute(connection : BackendConnection) : Bool
        connection.save_snapshot(self)
        true
      end
    end

    struct LoadSnapshot
      include RetryableConnectionRequest(SnapshotRecord?)
      getter persistence_id : String

      def initialize(@persistence_id : String)
      end

      def execute(connection : BackendConnection) : SnapshotRecord?
        connection.load_snapshot(self)
      end
    end

    struct DeleteSnapshot
      include RetryableConnectionRequest(Bool)
      getter persistence_id : String

      def initialize(@persistence_id : String)
      end

      def execute(connection : BackendConnection) : Bool
        connection.delete_snapshot(self)
        true
      end
    end

    struct DeleteEventsTo
      include RetryableConnectionRequest(RetentionResult)
      getter persistence_id : String
      getter sequence_nr : Int64

      def initialize(@persistence_id : String, @sequence_nr : Int64)
        raise ArgumentError.new("Retention sequence number cannot be negative") if @sequence_nr < 0_i64
      end

      def execute(connection : BackendConnection) : RetentionResult
        connection.delete_events_to(self)
      end
    end

    struct RunMaintenance
      include TypedConnectionRequest(MaintenanceResult)

      def execute(connection : BackendConnection) : MaintenanceResult
        connection.run_maintenance(self)
      end
    end

    struct QueryEvents
      include RetryableConnectionRequest(EventPage)
      getter after_offset : Int64
      getter limit : Int32
      getter persistence_id : String?

      def initialize(@after_offset : Int64, @limit : Int32 = 100, @persistence_id : String? = nil)
        raise ArgumentError.new("Query offset cannot be negative") if @after_offset < 0_i64
        unless 1 <= @limit <= 1_000
          raise ArgumentError.new("Query limit must be between 1 and 1000")
        end
      end

      def execute(connection : BackendConnection) : EventPage
        connection.query_events(self)
      end
    end

    struct LoadProjectionOffset
      include RetryableConnectionRequest(Int64)
      getter projection : String

      def initialize(@projection : String)
        raise ArgumentError.new("Projection name cannot be empty") if @projection.empty?
      end

      def execute(connection : BackendConnection) : Int64
        connection.load_projection_offset(self)
      end
    end

    struct SaveProjectionOffset
      include RetryableConnectionRequest(Int64)
      getter projection : String
      getter offset : Int64

      def initialize(@projection : String, @offset : Int64)
        raise ArgumentError.new("Projection name cannot be empty") if @projection.empty?
        raise ArgumentError.new("Projection offset cannot be negative") if @offset < 0_i64
      end

      def execute(connection : BackendConnection) : Int64
        connection.save_projection_offset(self)
      end
    end

    struct DeleteProjectionOffset
      include RetryableConnectionRequest(Bool)
      getter projection : String

      def initialize(@projection : String)
        raise ArgumentError.new("Projection name cannot be empty") if @projection.empty?
      end

      def execute(connection : BackendConnection) : Bool
        connection.delete_projection_offset(self)
      end
    end

    struct SaveState
      include RetryableConnectionRequest(WriteResult)
      getter persistence_id : String
      getter expected_revision : Int64
      getter operation_id : OperationId
      getter manifest : String
      getter payload : String
      getter outbox : Array(OutboxEntry)

      def initialize(
        @persistence_id : String,
        @expected_revision : Int64,
        @operation_id : OperationId,
        @manifest : String,
        @payload : String,
        @outbox : Array(OutboxEntry) = [] of OutboxEntry,
      )
      end

      def execute(connection : BackendConnection) : WriteResult
        connection.save_state(self)
      end
    end

    struct LoadState
      include RetryableConnectionRequest(StateRecord?)
      getter persistence_id : String

      def initialize(@persistence_id : String)
      end

      def execute(connection : BackendConnection) : StateRecord?
        connection.load_state(self)
      end
    end

    struct DeleteState
      include RetryableConnectionRequest(WriteResult)
      getter persistence_id : String
      getter expected_revision : Int64
      getter operation_id : OperationId
      getter outbox : Array(OutboxEntry)

      def initialize(
        @persistence_id : String,
        @expected_revision : Int64,
        @operation_id : OperationId,
        @outbox : Array(OutboxEntry) = [] of OutboxEntry,
      )
      end

      def execute(connection : BackendConnection) : WriteResult
        connection.delete_state(self)
      end
    end

    struct ClaimOutbox
      include RetryableConnectionRequest(Array(StoredOutboxEntry))
      getter owner : String
      getter limit : Int32
      getter now_epoch_ms : Int64
      getter lease_until_epoch_ms : Int64

      def initialize(
        @owner : String,
        @limit : Int32,
        @now_epoch_ms : Int64,
        @lease_until_epoch_ms : Int64,
      )
        raise ArgumentError.new("Outbox owner cannot be empty") if @owner.empty?
        unless 1 <= @limit <= 1_000
          raise ArgumentError.new("Outbox claim limit must be between 1 and 1000")
        end
        if @lease_until_epoch_ms <= @now_epoch_ms
          raise ArgumentError.new("Outbox lease must end after its start")
        end
      end

      def self.for(
        owner : String,
        limit : Int32 = 100,
        lease : Time::Span = 30.seconds,
        now : Time = Time.utc,
      ) : self
        now_epoch_ms = now.to_unix_ms
        new(owner, limit, now_epoch_ms, now_epoch_ms + lease.total_milliseconds.to_i64)
      end

      def execute(connection : BackendConnection) : Array(StoredOutboxEntry)
        connection.claim_outbox(self)
      end
    end

    struct AcknowledgeOutbox
      include TypedConnectionRequest(Int64)
      getter owner : String
      getter message_ids : Array(String)

      def initialize(@owner : String, @message_ids : Array(String))
        raise ArgumentError.new("Outbox owner cannot be empty") if @owner.empty?
      end

      def execute(connection : BackendConnection) : Int64
        connection.acknowledge_outbox(self)
      end
    end

    struct ReleaseOutbox
      include TypedConnectionRequest(Bool)
      getter owner : String
      getter message_id : String
      getter error : String

      def initialize(@owner : String, @message_id : String, @error : String)
        raise ArgumentError.new("Outbox owner cannot be empty") if @owner.empty?
        raise ArgumentError.new("Outbox message id cannot be empty") if @message_id.empty?
      end

      def execute(connection : BackendConnection) : Bool
        connection.release_outbox(self)
      end
    end

    alias ConnectionMessage = DbExec | DbExecLastId | DbQueryString | DbQueryStrings |
                              EnsureEventStore | EnsureStateStore | GetSchemaVersion |
                              AppendEvents | LoadEvents |
                              SaveSnapshot | LoadSnapshot | DeleteSnapshot |
                              DeleteEventsTo | RunMaintenance |
                              QueryEvents | LoadProjectionOffset | SaveProjectionOffset | DeleteProjectionOffset |
                              ClaimOutbox | AcknowledgeOutbox | ReleaseOutbox |
                              SaveState | LoadState | DeleteState

    # Backend-neutral journal contract. Implementations must preserve operation-id
    # deduplication, optimistic revisions, and atomic event batches.
    module JournalBackend
      abstract def ensure_event_store : Nil
      abstract def append_events(message : AppendEvents) : WriteResult
      abstract def load_events(message : LoadEvents) : Array(StoredEvent)
    end

    # Backend-neutral snapshot contract used by event-sourced recovery.
    module SnapshotBackend
      abstract def save_snapshot(message : SaveSnapshot) : Nil
      abstract def load_snapshot(message : LoadSnapshot) : SnapshotRecord?
      abstract def delete_snapshot(message : DeleteSnapshot) : Nil
    end

    # Backend-neutral durable-state contract.
    module DurableStateBackend
      abstract def ensure_state_store : Nil
      abstract def save_state(message : SaveState) : WriteResult
      abstract def load_state(message : LoadState) : StateRecord?
      abstract def delete_state(message : DeleteState) : WriteResult
    end

    module RetentionBackend
      abstract def delete_events_to(message : DeleteEventsTo) : RetentionResult
      abstract def run_maintenance(message : RunMaintenance) : MaintenanceResult
    end

    module QueryBackend
      abstract def query_events(message : QueryEvents) : EventPage
      abstract def load_projection_offset(message : LoadProjectionOffset) : Int64
      abstract def save_projection_offset(message : SaveProjectionOffset) : Int64
      abstract def delete_projection_offset(message : DeleteProjectionOffset) : Bool
    end

    module OutboxBackend
      abstract def claim_outbox(message : ClaimOutbox) : Array(StoredOutboxEntry)
      abstract def acknowledge_outbox(message : AcknowledgeOutbox) : Int64
      abstract def release_outbox(message : ReleaseOutbox) : Bool
    end

    # One physical backend connection. A connection is owned by exactly one
    # ConnectionWorker and is never used from another execution context.
    abstract class BackendConnection
      include SchemaBackend
      include JournalBackend
      include SnapshotBackend
      include DurableStateBackend
      include RetentionBackend
      include QueryBackend
      include OutboxBackend

      abstract def connection_lost?(error : Exception) : Bool
      abstract def close : Nil

      # Optional production capabilities retain compatibility with lightweight
      # custom backends. SQL backends override all of them.
      def ensure_schema : Nil
        ensure_event_store
        ensure_state_store
      end

      def schema_version : Int64
        0_i64
      end

      def delete_events_to(message : DeleteEventsTo) : RetentionResult
        raise NotImplementedError.new("Retention is not supported by this persistence backend")
      end

      def run_maintenance(message : RunMaintenance) : MaintenanceResult
        raise NotImplementedError.new("Maintenance is not supported by this persistence backend")
      end

      def query_events(message : QueryEvents) : EventPage
        raise NotImplementedError.new("Persistence query is not supported by this backend")
      end

      def load_projection_offset(message : LoadProjectionOffset) : Int64
        raise NotImplementedError.new("Projection checkpoints are not supported by this backend")
      end

      def save_projection_offset(message : SaveProjectionOffset) : Int64
        raise NotImplementedError.new("Projection checkpoints are not supported by this backend")
      end

      def delete_projection_offset(message : DeleteProjectionOffset) : Bool
        raise NotImplementedError.new("Projection checkpoints are not supported by this backend")
      end

      def claim_outbox(message : ClaimOutbox) : Array(StoredOutboxEntry)
        raise NotImplementedError.new("Transactional outbox is not supported by this backend")
      end

      def acknowledge_outbox(message : AcknowledgeOutbox) : Int64
        raise NotImplementedError.new("Transactional outbox is not supported by this backend")
      end

      def release_outbox(message : ReleaseOutbox) : Bool
        raise NotImplementedError.new("Transactional outbox is not supported by this backend")
      end

      # Raw SQL requests are retained for compatibility with the low-level
      # connection API. Persistence itself never depends on them.
      def exec(message : DbExec) : Nil
        raise NotImplementedError.new("Raw SQL is not supported by this persistence backend")
      end

      def exec_last_id(message : DbExecLastId) : Int64
        raise NotImplementedError.new("Last-insert ids are not supported by this persistence backend")
      end

      def query_string(message : DbQueryString) : String?
        raise NotImplementedError.new("Raw SQL is not supported by this persistence backend")
      end

      def query_strings(message : DbQueryStrings) : Array(String)
        raise NotImplementedError.new("Raw SQL is not supported by this persistence backend")
      end
    end

    # Immutable backend factory shared by the connection workers in one pool.
    abstract class Backend
      abstract def name : String
      abstract def connect : BackendConnection
    end
  end
end
