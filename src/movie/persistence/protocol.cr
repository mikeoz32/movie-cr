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

      def dispatch(worker : ConnectionWorker, sender : Movie::ActorRefBase?) : Nil
        Movie::Ask.reply_if_asked(sender, worker.execute { |connection| execute(connection) })
      rescue error
        Movie::Ask.fail_if_asked(sender, error, T)
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

    struct EnsureEventStore
      include TypedConnectionRequest(Bool)

      def execute(connection : BackendConnection) : Bool
        connection.ensure_event_store
        true
      end
    end

    struct EnsureStateStore
      include TypedConnectionRequest(Bool)

      def execute(connection : BackendConnection) : Bool
        connection.ensure_state_store
        true
      end
    end

    struct AppendEvents
      include TypedConnectionRequest(WriteResult)
      getter persistence_id : String
      getter expected_revision : Int64
      getter operation_id : OperationId
      getter events : Array(SerializedEvent)

      def initialize(
        @persistence_id : String,
        @expected_revision : Int64,
        @operation_id : OperationId,
        @events : Array(SerializedEvent),
      )
      end

      def execute(connection : BackendConnection) : WriteResult
        connection.append_events(self)
      end
    end

    struct LoadEvents
      include TypedConnectionRequest(Array(StoredEvent))
      getter persistence_id : String
      getter after_sequence_nr : Int64

      def initialize(@persistence_id : String, @after_sequence_nr : Int64 = 0_i64)
      end

      def execute(connection : BackendConnection) : Array(StoredEvent)
        connection.load_events(self)
      end
    end

    struct SaveSnapshot
      include TypedConnectionRequest(Bool)
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
      include TypedConnectionRequest(SnapshotRecord?)
      getter persistence_id : String

      def initialize(@persistence_id : String)
      end

      def execute(connection : BackendConnection) : SnapshotRecord?
        connection.load_snapshot(self)
      end
    end

    struct DeleteSnapshot
      include TypedConnectionRequest(Bool)
      getter persistence_id : String

      def initialize(@persistence_id : String)
      end

      def execute(connection : BackendConnection) : Bool
        connection.delete_snapshot(self)
        true
      end
    end

    struct SaveState
      include TypedConnectionRequest(WriteResult)
      getter persistence_id : String
      getter expected_revision : Int64
      getter operation_id : OperationId
      getter manifest : String
      getter payload : String

      def initialize(
        @persistence_id : String,
        @expected_revision : Int64,
        @operation_id : OperationId,
        @manifest : String,
        @payload : String,
      )
      end

      def execute(connection : BackendConnection) : WriteResult
        connection.save_state(self)
      end
    end

    struct LoadState
      include TypedConnectionRequest(StateRecord?)
      getter persistence_id : String

      def initialize(@persistence_id : String)
      end

      def execute(connection : BackendConnection) : StateRecord?
        connection.load_state(self)
      end
    end

    struct DeleteState
      include TypedConnectionRequest(WriteResult)
      getter persistence_id : String
      getter expected_revision : Int64
      getter operation_id : OperationId

      def initialize(@persistence_id : String, @expected_revision : Int64, @operation_id : OperationId)
      end

      def execute(connection : BackendConnection) : WriteResult
        connection.delete_state(self)
      end
    end

    alias ConnectionMessage = DbExec | DbExecLastId | DbQueryString | DbQueryStrings |
                              EnsureEventStore | EnsureStateStore | AppendEvents | LoadEvents |
                              SaveSnapshot | LoadSnapshot | DeleteSnapshot |
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

    # One physical backend connection. A connection is owned by exactly one
    # ConnectionWorker and is never used from another execution context.
    abstract class BackendConnection
      include JournalBackend
      include SnapshotBackend
      include DurableStateBackend

      abstract def connection_lost?(error : Exception) : Bool
      abstract def close : Nil

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
