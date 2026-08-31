require "json"
require "digest/sha256"
require "sqlite3"
require "./extension_id"
require "./system"
require "./behavior"
require "./context"
require "./ask"

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

    # Owns one database connection on a dedicated OS thread. Jobs are bounded and
    # always execute on the same isolated execution context as the connection.
    class ConnectionWorker
      class Stopped < Exception
      end

      private abstract class Work
        abstract def execute(connection : BackendConnection)
        abstract def fail(error : Exception)
      end

      private class TypedWork(T) < Work
        def initialize(@operation : Proc(BackendConnection, T), @promise : Movie::Promise(T))
        end

        def execute(connection : BackendConnection)
          @promise.try_success(@operation.call(connection))
        end

        def fail(error : Exception)
          @promise.try_failure(error)
        end
      end

      @jobs : Channel(Work)
      @ready : Movie::Promise(Nil)
      @stopped : Atomic(Bool)
      @execution_context : Fiber::ExecutionContext::Isolated

      def initialize(@backend : Backend, name : String, queue_capacity : Int32 = 256)
        @jobs = Channel(Work).new(queue_capacity < 1 ? 1 : queue_capacity)
        @ready = Movie::Promise(Nil).new
        @stopped = Atomic(Bool).new(false)
        @execution_context = Fiber::ExecutionContext::Isolated.new(name) { run }
      end

      def initialize(db_uri : String, name : String, queue_capacity : Int32 = 256)
        initialize(SQLiteBackend.new(db_uri), name, queue_capacity)
      end

      def execute(&operation : BackendConnection -> T) : T forall T
        @ready.future.await
        raise Stopped.new("Database connection worker is stopped") if @stopped.get

        promise = Movie::Promise(T).new
        work = TypedWork(T).new(operation, promise)
        begin
          @jobs.send(work)
        rescue Channel::ClosedError
          raise Stopped.new("Database connection worker is stopped")
        end
        promise.future.await
      end

      def close
        _, changed = @stopped.compare_and_set(false, true)
        if changed
          begin
            @jobs.close
          rescue Channel::ClosedError
          end
        end
        @execution_context.wait
      end

      private def run
        connection = nil.as(BackendConnection?)
        begin
          connection = @backend.connect
          @ready.try_success(nil)

          loop do
            work = @jobs.receive
            begin
              current = connection || @backend.connect
              connection = current
              work.execute(current)
            rescue error
              work.fail(error)
              if current = connection
                if current.connection_lost?(error)
                  begin
                    current.close
                  rescue
                  end
                  connection = nil
                end
              end
            end
          end
        rescue Channel::ClosedError
        rescue error
          @ready.try_failure(error)
          @stopped.set(true)
          begin
            @jobs.close
          rescue Channel::ClosedError
          end
          drain_with_failure(error)
        ensure
          connection.try &.close
        end
      end

      private def drain_with_failure(error : Exception)
        loop do
          work = @jobs.receive?
          break unless work
          work.fail(error)
        end
      rescue Channel::ClosedError
      end
    end

    # Shared SQL implementation. Dialects only supply connection setup and
    # schema introspection; all persistence invariants live here.
    abstract class SqlBackendConnection < BackendConnection
      def initialize(@connection : DB::Connection)
      end

      def exec(message : DbExec) : Nil
        with_connection { |connection| connection.exec(message.sql, args: message.args) }
      end

      def exec_last_id(message : DbExecLastId) : Int64
        with_connection do |connection|
          result = connection.exec(message.sql, args: message.args)
          result.last_insert_id
        end
      end

      def query_string(message : DbQueryString) : String?
        with_connection { |connection| connection.query_one?(message.sql, args: message.args, as: String) }
      end

      def query_strings(message : DbQueryStrings) : Array(String)
        with_connection do |connection|
          values = [] of String
          connection.query_each(message.sql, args: message.args) do |rs|
            values << rs.read(String)
          end
          values
        end
      end

      def ensure_event_store : Nil
        with_connection { |connection| ensure_event_store(connection) }
      end

      private def ensure_event_store(connection : DB::Connection)
        configure_schema(connection)
        if table_exists?(connection, "event_journal") && !table_columns(connection, "event_journal").includes?("sequence_nr")
          migrate_legacy_event_store(connection)
        end
        create_event_store(connection)
        synchronize_event_stream_revisions(connection)
        connection.exec(<<-SQL)
          CREATE TABLE IF NOT EXISTS snapshot_store (
            persistence_id TEXT PRIMARY KEY,
            sequence_nr BIGINT NOT NULL,
            manifest TEXT NOT NULL,
            payload TEXT NOT NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
          )
        SQL
      end

      private def create_event_store(conn : DB::Connection)
        conn.exec(<<-SQL)
          CREATE TABLE IF NOT EXISTS event_journal (
            persistence_id TEXT NOT NULL,
            sequence_nr BIGINT NOT NULL,
            manifest TEXT NOT NULL,
            payload TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (persistence_id, sequence_nr)
          )
        SQL
        conn.exec(<<-SQL)
          CREATE TABLE IF NOT EXISTS event_stream (
            persistence_id TEXT PRIMARY KEY,
            revision BIGINT NOT NULL
          )
        SQL
        conn.exec(<<-SQL)
          CREATE TABLE IF NOT EXISTS journal_operation (
            persistence_id TEXT NOT NULL,
            operation_id TEXT NOT NULL,
            fingerprint TEXT NOT NULL,
            revision BIGINT NOT NULL,
            PRIMARY KEY (persistence_id, operation_id)
          )
        SQL
      end

      private def synchronize_event_stream_revisions(connection : DB::Connection)
        revisions = connection.query_all(
          "SELECT persistence_id, MAX(sequence_nr) FROM event_journal GROUP BY persistence_id",
          as: {String, Int64}
        )
        revisions.each do |persistence_id, revision|
          connection.exec(
            "INSERT INTO event_stream (persistence_id, revision) VALUES (?, ?) " +
            "ON CONFLICT(persistence_id) DO UPDATE SET revision = excluded.revision " +
            "WHERE event_stream.revision < excluded.revision",
            args: [persistence_id, revision] of DB::Any
          )
        end
      end

      def ensure_state_store : Nil
        with_connection { |connection| ensure_state_store(connection) }
      end

      private def ensure_state_store(connection : DB::Connection)
        configure_schema(connection)
        if table_exists?(connection, "durable_state") && !table_columns(connection, "durable_state").includes?("revision")
          migrate_legacy_state_store(connection)
        end
        create_state_store(connection)
      end

      private def create_state_store(conn : DB::Connection)
        conn.exec(<<-SQL)
          CREATE TABLE IF NOT EXISTS durable_state (
            persistence_id TEXT PRIMARY KEY,
            revision BIGINT NOT NULL,
            manifest TEXT NOT NULL,
            payload TEXT,
            deleted INTEGER NOT NULL DEFAULT 0,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
          )
        SQL
        conn.exec(<<-SQL)
          CREATE TABLE IF NOT EXISTS state_operation (
            persistence_id TEXT NOT NULL,
            operation_id TEXT NOT NULL,
            fingerprint TEXT NOT NULL,
            revision BIGINT NOT NULL,
            PRIMARY KEY (persistence_id, operation_id)
          )
        SQL
      end

      private def migrate_legacy_event_store(connection : DB::Connection)
        connection.transaction do |transaction|
          conn = transaction.connection
          conn.exec("ALTER TABLE event_journal RENAME TO event_journal_legacy")
          create_event_store(conn)
          conn.exec(<<-SQL)
            INSERT INTO event_journal (persistence_id, sequence_nr, manifest, payload, created_at)
            SELECT current.persistence_id,
                   (SELECT COUNT(*) FROM event_journal_legacy previous
                    WHERE previous.persistence_id = current.persistence_id AND previous.id <= current.id),
                   'legacy', current.payload, COALESCE(current.created_at, CURRENT_TIMESTAMP)
            FROM event_journal_legacy current
            ORDER BY current.id
          SQL
          conn.exec("DROP TABLE event_journal_legacy")
        end
      end

      private def migrate_legacy_state_store(connection : DB::Connection)
        connection.transaction do |transaction|
          conn = transaction.connection
          conn.exec("ALTER TABLE durable_state RENAME TO durable_state_legacy")
          create_state_store(conn)
          conn.exec(<<-SQL)
            INSERT INTO durable_state (persistence_id, revision, manifest, payload, deleted, updated_at)
            SELECT persistence_id, 1, 'legacy', payload, 0, COALESCE(updated_at, CURRENT_TIMESTAMP)
            FROM durable_state_legacy
          SQL
          conn.exec("DROP TABLE durable_state_legacy")
        end
      end

      def append_events(message : AppendEvents) : WriteResult
        with_connection { |connection| append_events(connection, message) }
      end

      private def append_events(connection : DB::Connection, message : AppendEvents) : WriteResult
        fingerprint = event_fingerprint(message.events)
        result = connection.transaction do |transaction|
          conn = transaction.connection
          operation = conn.exec(
            "INSERT INTO journal_operation (persistence_id, operation_id, fingerprint, revision) " +
            "VALUES (?, ?, ?, ?) ON CONFLICT(persistence_id, operation_id) DO NOTHING",
            args: [
              message.persistence_id,
              message.operation_id.value,
              fingerprint,
              message.expected_revision + message.events.size,
            ] of DB::Any
          )
          unless operation.rows_affected == 1
            stored = conn.query_one(
              "SELECT fingerprint, revision FROM journal_operation WHERE persistence_id = ? AND operation_id = ?",
              args: [message.persistence_id, message.operation_id.value] of DB::Any,
              as: {String, Int64}
            )
            unless stored[0] == fingerprint
              raise OperationConflictError.new(message.persistence_id, message.operation_id)
            end
            next WriteResult.new(stored[1], true)
          end

          conn.exec(
            "INSERT INTO event_stream (persistence_id, revision) VALUES (?, 0) ON CONFLICT(persistence_id) DO NOTHING",
            args: [message.persistence_id] of DB::Any
          )

          if message.events.empty?
            actual = event_revision(conn, message.persistence_id)
            unless actual == message.expected_revision
              raise ConcurrentWriteError.new(message.persistence_id, message.expected_revision, actual)
            end
            next WriteResult.new(actual, false)
          end

          next_revision = message.expected_revision + message.events.size
          updated = conn.exec(
            "UPDATE event_stream SET revision = ? WHERE persistence_id = ? AND revision = ?",
            args: [next_revision, message.persistence_id, message.expected_revision] of DB::Any
          )
          unless updated.rows_affected == 1
            actual = event_revision(conn, message.persistence_id)
            raise ConcurrentWriteError.new(message.persistence_id, message.expected_revision, actual)
          end

          sequence_nr = message.expected_revision
          message.events.each do |event|
            sequence_nr += 1
            conn.exec(
              "INSERT INTO event_journal (persistence_id, sequence_nr, manifest, payload) VALUES (?, ?, ?, ?)",
              args: [message.persistence_id, sequence_nr, event.manifest, event.payload] of DB::Any
            )
          end
          WriteResult.new(sequence_nr, false)
        end
        result.not_nil!
      rescue error
        raise error unless database_error?(error)
        actual = event_revision(connection, message.persistence_id)
        if actual != message.expected_revision
          raise ConcurrentWriteError.new(message.persistence_id, message.expected_revision, actual)
        end
        raise error
      end

      private def event_fingerprint(events : Array(SerializedEvent)) : String
        Digest::SHA256.hexdigest do |digest|
          events.each do |event|
            digest << event.manifest.bytesize.to_s << ":" << event.manifest
            digest << event.payload.bytesize.to_s << ":" << event.payload
          end
        end
      end

      private def event_revision(conn : DB::Connection, persistence_id : String) : Int64
        conn.query_one?(
          "SELECT revision FROM event_stream WHERE persistence_id = ?",
          args: [persistence_id] of DB::Any,
          as: Int64
        ) || 0_i64
      end

      def load_events(message : LoadEvents) : Array(StoredEvent)
        with_connection do |connection|
          rows = connection.query_all(
            "SELECT sequence_nr, manifest, payload FROM event_journal " +
            "WHERE persistence_id = ? AND sequence_nr > ? ORDER BY sequence_nr ASC",
            args: [message.persistence_id, message.after_sequence_nr] of DB::Any,
            as: {Int64, String, String}
          )
          rows.map { |row| StoredEvent.new(row[0], row[1], row[2]) }
        end
      end

      def save_snapshot(message : SaveSnapshot) : Nil
        with_connection do |connection|
          snapshot = message.snapshot
          connection.exec(
            "INSERT INTO snapshot_store (persistence_id, sequence_nr, manifest, payload, updated_at) " +
            "VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP) " +
            "ON CONFLICT(persistence_id) DO UPDATE SET sequence_nr = excluded.sequence_nr, " +
            "manifest = excluded.manifest, payload = excluded.payload, updated_at = CURRENT_TIMESTAMP " +
            "WHERE excluded.sequence_nr >= snapshot_store.sequence_nr",
            args: [message.persistence_id, snapshot.sequence_nr, snapshot.manifest, snapshot.payload] of DB::Any
          )
        end
      end

      def load_snapshot(message : LoadSnapshot) : SnapshotRecord?
        with_connection do |connection|
          row = connection.query_one?(
            "SELECT sequence_nr, manifest, payload FROM snapshot_store WHERE persistence_id = ?",
            args: [message.persistence_id] of DB::Any,
            as: {Int64, String, String}
          )
          row.try { |value| SnapshotRecord.new(value[0], value[1], value[2]) }
        end
      end

      def delete_snapshot(message : DeleteSnapshot) : Nil
        with_connection do |connection|
          connection.exec(
            "DELETE FROM snapshot_store WHERE persistence_id = ?",
            args: [message.persistence_id] of DB::Any
          )
        end
      end

      def save_state(message : SaveState) : WriteResult
        write_state(
          message.persistence_id,
          message.expected_revision,
          message.operation_id,
          message.manifest,
          message.payload,
          false
        )
      end

      def delete_state(message : DeleteState) : WriteResult
        write_state(message.persistence_id, message.expected_revision, message.operation_id, "deleted", nil, true)
      end

      private def write_state(
        persistence_id : String,
        expected_revision : Int64,
        operation_id : OperationId,
        manifest : String,
        payload : String?,
        deleted : Bool,
      ) : WriteResult
        with_connection do |connection|
          write_state(connection, persistence_id, expected_revision, operation_id, manifest, payload, deleted)
        end
      end

      private def write_state(
        connection : DB::Connection,
        persistence_id : String,
        expected_revision : Int64,
        operation_id : OperationId,
        manifest : String,
        payload : String?,
        deleted : Bool,
      ) : WriteResult
        revision = expected_revision + 1
        fingerprint = state_fingerprint(manifest, payload, deleted)
        result = connection.transaction do |transaction|
          conn = transaction.connection
          operation = conn.exec(
            "INSERT INTO state_operation (persistence_id, operation_id, fingerprint, revision) " +
            "VALUES (?, ?, ?, ?) ON CONFLICT(persistence_id, operation_id) DO NOTHING",
            args: [persistence_id, operation_id.value, fingerprint, revision] of DB::Any
          )
          unless operation.rows_affected == 1
            stored = conn.query_one(
              "SELECT fingerprint, revision FROM state_operation WHERE persistence_id = ? AND operation_id = ?",
              args: [persistence_id, operation_id.value] of DB::Any,
              as: {String, Int64}
            )
            unless stored[0] == fingerprint
              raise OperationConflictError.new(persistence_id, operation_id)
            end
            next WriteResult.new(stored[1], true)
          end

          write = conn.exec(
            "INSERT INTO durable_state (persistence_id, revision, manifest, payload, deleted, updated_at) " +
            "SELECT ?, ?, ?, ?, ?, CURRENT_TIMESTAMP " +
            "WHERE ? = 0 OR EXISTS (SELECT 1 FROM durable_state WHERE persistence_id = ?) " +
            "ON CONFLICT(persistence_id) DO UPDATE SET revision = excluded.revision, " +
            "manifest = excluded.manifest, payload = excluded.payload, deleted = excluded.deleted, " +
            "updated_at = CURRENT_TIMESTAMP WHERE durable_state.revision = ?",
            args: [
              persistence_id,
              revision,
              manifest,
              payload,
              deleted ? 1_i64 : 0_i64,
              expected_revision,
              persistence_id,
              expected_revision,
            ] of DB::Any
          )
          unless write.rows_affected == 1
            actual = state_revision(conn, persistence_id)
            raise ConcurrentWriteError.new(persistence_id, expected_revision, actual)
          end
          WriteResult.new(revision, false)
        end
        result.not_nil!
      rescue error
        raise error unless database_error?(error)
        actual = state_revision(connection, persistence_id)
        if actual != expected_revision
          raise ConcurrentWriteError.new(persistence_id, expected_revision, actual)
        end
        raise error
      end

      private def state_fingerprint(manifest : String, payload : String?, deleted : Bool) : String
        Digest::SHA256.hexdigest do |digest|
          digest << manifest.bytesize.to_s << ":" << manifest
          if value = payload
            digest << value.bytesize.to_s << ":" << value
          else
            digest << "nil"
          end
          digest << (deleted ? "1" : "0")
        end
      end

      private def state_revision(conn : DB::Connection, persistence_id : String) : Int64
        conn.query_one?(
          "SELECT revision FROM durable_state WHERE persistence_id = ?",
          args: [persistence_id] of DB::Any,
          as: Int64
        ) || 0_i64
      end

      def load_state(message : LoadState) : StateRecord?
        with_connection do |connection|
          row = connection.query_one?(
            "SELECT revision, manifest, payload, deleted FROM durable_state WHERE persistence_id = ?",
            args: [message.persistence_id] of DB::Any,
            as: {Int64, String, String?, Int64}
          )
          row.try { |value| StateRecord.new(value[0], value[1], value[2], value[3] != 0) }
        end
      end

      private def with_connection(&operation : DB::Connection -> T) : T forall T
        yield @connection
      end

      protected def configure_schema(connection : DB::Connection) : Nil
      end

      protected abstract def table_exists?(connection : DB::Connection, name : String) : Bool
      protected abstract def table_columns(connection : DB::Connection, name : String) : Array(String)
      protected abstract def database_error?(error : Exception) : Bool

      def connection_lost?(error : Exception) : Bool
        error.is_a?(DB::ConnectionLost) || @connection.closed?
      end

      def close : Nil
        @connection.close
      end
    end

    class SQLiteBackendConnection < SqlBackendConnection
      protected def configure_schema(connection : DB::Connection) : Nil
        connection.exec("PRAGMA journal_mode = WAL")
      end

      protected def table_exists?(connection : DB::Connection, name : String) : Bool
        !connection.query_one?(
          "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
          args: [name] of DB::Any,
          as: String
        ).nil?
      end

      protected def table_columns(connection : DB::Connection, name : String) : Array(String)
        columns = [] of String
        connection.query_each("PRAGMA table_info(#{name})") do |row|
          row.read(Int64)
          columns << row.read(String)
        end
        columns
      end

      protected def database_error?(error : Exception) : Bool
        error.is_a?(SQLite3::Exception)
      end
    end

    class SQLiteBackend < Backend
      getter uri : String

      def initialize(@uri : String)
      end

      def name : String
        "sqlite"
      end

      def connect : BackendConnection
        connection = DB.connect(@uri)
        connection.exec("PRAGMA busy_timeout = 5000")
        SQLiteBackendConnection.new(connection)
      end
    end

    # Actor that owns one backend connection worker and executes requests
    # sequentially without exposing SQL to persistence behaviors.
    class ConnectionActor < Movie::AbstractBehavior(ConnectionMessage)
      @worker : ConnectionWorker? = nil

      def initialize(@backend : Backend, @queue_capacity : Int32 = 256)
      end

      def initialize(db_uri : String, @queue_capacity : Int32 = 256)
        @backend = SQLiteBackend.new(db_uri)
      end

      def receive(message, context)
        message.dispatch(ensure_worker, context.sender)
        Movie::Behaviors(ConnectionMessage).same
      end

      def on_signal(signal : SystemMessage)
        case signal
        when PreStart
          ensure_worker
        when PreStop
          begin
            @worker.try &.close
          rescue
          end
        end
      end

      private def ensure_worker : ConnectionWorker
        @worker ||= ConnectionWorker.new(@backend, "movie-db-#{object_id}", @queue_capacity)
      end
    end

    # Pool that routes DB messages to connection actors in round-robin order.
    class ConnectionPool < Movie::AbstractBehavior(ConnectionMessage)
      @next_index : Int32 = 0

      def initialize(@connections : Array(Movie::ActorRef(ConnectionMessage)))
      end

      def receive(message, context)
        raise "Connection pool is empty" if @connections.empty?
        connection = next_connection
        connection.tell_from(context.sender, message)
        Movie::Behaviors(ConnectionMessage).same
      end

      def self.behavior(backend : Backend, pool_size : Int32, queue_capacity : Int32 = 256)
        Movie::Behaviors(ConnectionMessage).setup do |ctx|
          size = pool_size < 1 ? 1 : pool_size
          connections = Array(Movie::ActorRef(ConnectionMessage)).new(size) do |i|
            ctx.spawn(ConnectionActor.new(backend, queue_capacity), name: "db-#{i}")
          end
          ConnectionPool.new(connections)
        end
      end

      def self.behavior(db_uri : String, pool_size : Int32, queue_capacity : Int32 = 256)
        behavior(SQLiteBackend.new(db_uri), pool_size, queue_capacity)
      end

      private def next_connection : Movie::ActorRef(ConnectionMessage)
        index = @next_index % @connections.size
        @next_index = (@next_index + 1) % @connections.size
        @connections[index]
      end
    end

    struct GetEntity
      getter persistence_id : Id
      getter spawn : Proc(Movie::ActorContext(GetEntity), Id, Movie::ActorRefBase)

      def initialize(
        @persistence_id : Id,
        @spawn : Proc(Movie::ActorContext(GetEntity), Id, Movie::ActorRefBase),
      )
      end
    end

    alias RegistryMessage = GetEntity

    # Registry actor that owns persistent entities for a specific extension.
    class EntityRegistry < Movie::AbstractBehavior(RegistryMessage)
      def initialize
        @entities = {} of Id => Movie::ActorRefBase
      end

      def receive(message, context)
        case message
        when GetEntity
          if ref = @entities[message.persistence_id]?
            Movie::Ask.reply_if_asked(context.sender, ref)
          else
            ref = message.spawn.call(context, message.persistence_id)
            if ref.path.nil?
              ref.path = context.path.try { |p| p / Persistence.entity_name(message.persistence_id) }
            end
            @entities[message.persistence_id] = ref
            Movie::Ask.reply_if_asked(context.sender, ref)
          end
        end
        Movie::Behaviors(RegistryMessage).same
      end

      def on_signal(signal : SystemMessage)
        if signal.is_a?(Terminated)
          @entities.reject! { |_id, ref| ref == signal.actor }
        end
      end
    end

    # Shared factory and lookup contract used by persistent extensions.
    class EntityProvider
      alias Factory = Proc(Movie::ActorContext(RegistryMessage), Id, Movie::ActorRefBase)

      def initialize(
        @system : Movie::AbstractActorSystem,
        @registry : Movie::ActorRef(RegistryMessage),
        @timeout : Time::Span,
      )
        @factories = {} of String => Factory
        @mutex = Mutex.new
      end

      def register(key : String, factory : Factory)
        @mutex.synchronize do
          raise "Entity factory already registered for #{key}" if @factories.has_key?(key)
          @factories[key] = factory
        end
      end

      def resolve(persistence_id : Id) : Movie::ActorRefBase
        spawn_proc = @mutex.synchronize do
          @factories[persistence_id.entity_type]? ||
            raise "Entity type not registered: #{persistence_id.entity_type}"
        end
        @system.ask(
          @registry,
          GetEntity.new(persistence_id, spawn_proc),
          Movie::ActorRefBase,
          @timeout
        ).await(@timeout)
      end
    end

    alias EventStoreMessage = AppendEvents | LoadEvents | SaveSnapshot | LoadSnapshot | DeleteSnapshot
    alias StateStoreMessage = SaveState | LoadState | DeleteState

    private module StoreForwarder
      def self.forward(
        ctx : Movie::ActorContext(U),
        pool : Movie::ActorRef(ConnectionMessage),
        timeout : Time::Span,
        message : M,
        response_type : T.class,
        &before : -> B
      ) forall U, M, T, B
        sender = ctx.sender
        begin
          before.call
          future = ctx.ask(pool, message, response_type, timeout)
        rescue error
          Movie::Ask.fail_if_asked(sender, error, response_type)
          return
        end

        future.on_complete do |result|
          case result.status
          when Movie::FutureStatus::Success
            Movie::Ask.reply_if_asked(sender, result.value.as(T))
          when Movie::FutureStatus::Failure
            Movie::Ask.fail_if_asked(sender, result.error.not_nil!, response_type)
          when Movie::FutureStatus::Cancelled
            Movie::Ask.fail_if_asked(sender, Movie::FutureCancelled.new, response_type)
          when Movie::FutureStatus::Pending
          end
        end
      end
    end

    # Actor that serializes access to the event journal.
    class EventStoreActor < Movie::AbstractBehavior(EventStoreMessage)
      @schema_ready : Bool = false
      @pool : Movie::ActorRef(ConnectionMessage)
      @timeout : Time::Span

      def initialize(@pool : Movie::ActorRef(ConnectionMessage), @timeout : Time::Span = 5.seconds)
      end

      def receive(message, context)
        case message
        when AppendEvents
          execute(context, message, WriteResult)
        when LoadEvents
          execute(context, message, Array(StoredEvent))
        when SaveSnapshot
          execute(context, message, Bool)
        when LoadSnapshot
          execute(context, message, SnapshotRecord?)
        when DeleteSnapshot
          execute(context, message, Bool)
        end
        Movie::Behaviors(EventStoreMessage).same
      end

      private def execute(ctx : Movie::ActorContext(U), message : M, response_type : T.class) forall U, M, T
        StoreForwarder.forward(ctx, @pool, @timeout, message, response_type) { ensure_schema(ctx) }
      end

      private def ensure_schema(ctx : Movie::ActorContext(U)) forall U
        return if @schema_ready
        ctx.ask(@pool, EnsureEventStore.new, Bool, @timeout).await(@timeout)
        @schema_ready = true
      end
    end

    # Actor that serializes access to durable state storage.
    class StateStoreActor < Movie::AbstractBehavior(StateStoreMessage)
      @schema_ready : Bool = false
      @pool : Movie::ActorRef(ConnectionMessage)
      @timeout : Time::Span

      def initialize(@pool : Movie::ActorRef(ConnectionMessage), @timeout : Time::Span = 5.seconds)
      end

      def receive(message, context)
        case message
        when SaveState
          execute(context, message, WriteResult)
        when LoadState
          execute(context, message, StateRecord?)
        when DeleteState
          execute(context, message, WriteResult)
        end
        Movie::Behaviors(StateStoreMessage).same
      end

      private def execute(ctx : Movie::ActorContext(U), message : M, response_type : T.class) forall U, M, T
        StoreForwarder.forward(ctx, @pool, @timeout, message, response_type) { ensure_schema(ctx) }
      end

      private def ensure_schema(ctx : Movie::ActorContext(U)) forall U
        return if @schema_ready
        ctx.ask(@pool, EnsureStateStore.new, Bool, @timeout).await(@timeout)
        @schema_ready = true
      end
    end

    class EventStoreClient
      def initialize(@ref : Movie::ActorRef(EventStoreMessage), @timeout : Time::Span = 5.seconds)
      end

      def append(
        ctx : Movie::ActorContext(U),
        persistence_id : String,
        expected_revision : Int64,
        operation_id : OperationId,
        events : Array(SerializedEvent),
      ) : WriteResult forall U
        message = AppendEvents.new(persistence_id, expected_revision, operation_id, events)
        ctx.ask(@ref, message, WriteResult, @timeout).await(@timeout)
      end

      def read(ctx : Movie::ActorContext(U), persistence_id : String, after_sequence_nr : Int64 = 0_i64) : Array(StoredEvent) forall U
        ctx.ask(@ref, LoadEvents.new(persistence_id, after_sequence_nr), Array(StoredEvent), @timeout).await(@timeout)
      end

      def save_snapshot(ctx : Movie::ActorContext(U), persistence_id : String, snapshot : SnapshotRecord) : Bool forall U
        ctx.ask(@ref, SaveSnapshot.new(persistence_id, snapshot), Bool, @timeout).await(@timeout)
      end

      def load_snapshot(ctx : Movie::ActorContext(U), persistence_id : String) : SnapshotRecord? forall U
        ctx.ask(@ref, LoadSnapshot.new(persistence_id), SnapshotRecord?, @timeout).await(@timeout)
      end

      def delete_snapshot(ctx : Movie::ActorContext(U), persistence_id : String) : Bool forall U
        ctx.ask(@ref, DeleteSnapshot.new(persistence_id), Bool, @timeout).await(@timeout)
      end
    end

    class StateStoreClient
      def initialize(@ref : Movie::ActorRef(StateStoreMessage), @timeout : Time::Span = 5.seconds)
      end

      def save(
        ctx : Movie::ActorContext(U),
        persistence_id : String,
        expected_revision : Int64,
        operation_id : OperationId,
        manifest : String,
        payload : String,
      ) : WriteResult forall U
        message = SaveState.new(persistence_id, expected_revision, operation_id, manifest, payload)
        ctx.ask(@ref, message, WriteResult, @timeout).await(@timeout)
      end

      def load(ctx : Movie::ActorContext(U), persistence_id : String) : StateRecord? forall U
        ctx.ask(@ref, LoadState.new(persistence_id), StateRecord?, @timeout).await(@timeout)
      end

      def delete(
        ctx : Movie::ActorContext(U),
        persistence_id : String,
        expected_revision : Int64,
        operation_id : OperationId,
      ) : WriteResult forall U
        message = DeleteState.new(persistence_id, expected_revision, operation_id)
        ctx.ask(@ref, message, WriteResult, @timeout).await(@timeout)
      end
    end
  end

  # Database extension that manages connection pool actors.
  class DatabaseExtension < Extension
    getter pool : ActorRef(Persistence::ConnectionMessage)
    getter operation_timeout : Time::Span
    getter backend_name : String

    def initialize(
      @system : AbstractActorSystem,
      backend : Persistence::Backend,
      @pool_size : Int32,
      queue_capacity : Int32,
      @operation_timeout : Time::Span,
    )
      @backend_name = backend.name
      @pool = @system.spawn(Persistence::ConnectionPool.behavior(backend, @pool_size, queue_capacity))
    end

    def stop
      @pool.send_system(Movie::STOP)
    end
  end

  class Database < ExtensionId(DatabaseExtension)
    def create(system : AbstractActorSystem) : DatabaseExtension
      cfg = system.config
      path = cfg.get_string(ActorSystemConfig::PERSISTENCE_DB_PATH, "data/movie_persistence.sqlite3")
      pool_size = cfg.get_int(ActorSystemConfig::PERSISTENCE_POOL_SIZE, 1)
      queue_capacity = cfg.get_int(ActorSystemConfig::PERSISTENCE_IO_QUEUE_CAPACITY, 256)
      operation_timeout = cfg.get_duration(ActorSystemConfig::PERSISTENCE_OPERATION_TIMEOUT, 5.seconds)
      parent = File.dirname(path)
      Dir.mkdir_p(parent) unless parent == "." || Dir.exists?(parent)
      backend = Persistence::SQLiteBackend.new("sqlite3:#{path}")
      DatabaseExtension.new(system, backend, pool_size, queue_capacity, operation_timeout)
    end
  end

  class EventEffect(E, S)
    getter events : Array(E)
    getter operation_id : Persistence::OperationId?
    getter callbacks : Array(Proc(S, Nil))
    getter? stop

    def initialize(
      @events : Array(E) = [] of E,
      @operation_id : Persistence::OperationId? = nil,
      @stop : Bool = false,
    )
      @callbacks = [] of Proc(S, Nil)
    end

    def then_run(&callback : S ->) : self
      @callbacks << callback
      self
    end
  end

  enum DurableAction
    None
    Persist
    Delete
  end

  class DurableEffect(S)
    getter action : DurableAction
    getter state : S?
    getter operation_id : Persistence::OperationId?
    getter callbacks : Array(Proc(S, Nil))
    getter? stop

    def initialize(
      @action : DurableAction,
      @state : S? = nil,
      @operation_id : Persistence::OperationId? = nil,
      @stop : Bool = false,
    )
      @callbacks = [] of Proc(S, Nil)
    end

    def then_run(&callback : S ->) : self
      @callbacks << callback
      self
    end
  end

  abstract class EventSourcedBehavior(C, E, S) < AbstractBehavior(C)
    @state : S
    @recovered : Bool = false
    @sequence_nr : Int64 = 0_i64

    def initialize(@persistence_id : String, @store : Movie::Persistence::EventStoreClient)
      @state = empty_state
    end

    def receive(message : C, ctx : ActorContext(C))
      recover(ctx) unless @recovered
      command_state = copy_state(@state)
      effect = handle_command(command_state, message, ctx)
      previous_sequence_nr = @sequence_nr
      next_state = command_state
      effect.events.each do |event|
        next_state = apply_event(next_state, event)
      end

      unless effect.events.empty?
        serialized = effect.events.map do |event|
          Movie::Persistence::SerializedEvent.new(event_manifest(event), serialize_event(event))
        end
        begin
          result = @store.append(
            ctx,
            @persistence_id,
            @sequence_nr,
            effect.operation_id.not_nil!,
            serialized
          )
          @sequence_nr = result.revision
          if result.duplicate
            reset_recovery_state
            recover(ctx)
          else
            @state = next_state
            save_snapshot_if_due(ctx, previous_sequence_nr)
          end
        rescue error
          on_persist_failure(error)
          raise error
        end
      end

      effect.callbacks.each &.call(@state)
      effect.stop? ? Behaviors(C).stopped : Behaviors(C).same
    end

    def on_signal(signal : SystemMessage)
      if signal.is_a?(PreRestart)
        reset_recovery_state
      end
      on_persistence_signal(signal)
    end

    protected abstract def empty_state : S
    protected abstract def apply_event(state : S, event : E) : S
    protected abstract def handle_command(state : S, command : C, ctx : ActorContext(C)) : EventEffect(E, S)

    protected def persist(event : E, operation_id : Persistence::OperationId) : EventEffect(E, S)
      EventEffect(E, S).new([event], operation_id)
    end

    protected def persist_all(
      events : Enumerable(E),
      operation_id : Persistence::OperationId,
    ) : EventEffect(E, S)
      EventEffect(E, S).new(events.to_a, operation_id)
    end

    protected def none : EventEffect(E, S)
      EventEffect(E, S).new
    end

    protected def stop : EventEffect(E, S)
      EventEffect(E, S).new(stop: true)
    end

    protected def event_manifest(event : E) : String
      event.class.name
    end

    protected def deserialize_event(manifest : String, payload : String) : E
      E.from_json(payload)
    end

    protected def snapshot_manifest(state : S) : String
      state.class.name
    end

    protected def deserialize_snapshot(manifest : String, payload : String) : S
      S.from_json(payload)
    end

    protected def snapshot_every : Int32?
      nil
    end

    protected def on_recovery_completed(state : S, sequence_nr : Int64)
    end

    protected def on_recovery_failure(error : Exception)
    end

    protected def on_persist_failure(error : Exception)
    end

    protected def on_persistence_signal(signal : SystemMessage)
    end

    private def serialize_event(event : E) : String
      String.build { |json| event.to_json(json) }
    end

    private def copy_state(state : S) : S
      payload = String.build { |json| state.to_json(json) }
      deserialize_snapshot(snapshot_manifest(state), payload)
    end

    private def recover(ctx : ActorContext(C))
      return if @recovered
      state = copy_state(empty_state)
      sequence_nr = 0_i64

      if snapshot = @store.load_snapshot(ctx, @persistence_id)
        state = deserialize_snapshot(snapshot.manifest, snapshot.payload)
        sequence_nr = snapshot.sequence_nr
      end

      @store.read(ctx, @persistence_id, sequence_nr).each do |stored|
        expected = sequence_nr + 1
        unless stored.sequence_nr == expected
          raise "Journal gap for #{@persistence_id}: expected sequence #{expected}, got #{stored.sequence_nr}"
        end
        event = deserialize_event(stored.manifest, stored.payload)
        state = apply_event(state, event)
        sequence_nr = stored.sequence_nr
      end

      @state = state
      @sequence_nr = sequence_nr
      @recovered = true
      on_recovery_completed(@state, @sequence_nr)
    rescue error
      reset_recovery_state
      on_recovery_failure(error)
      raise error
    end

    private def save_snapshot_if_due(ctx : ActorContext(C), previous_sequence_nr : Int64)
      interval = snapshot_every
      return unless interval && interval > 0
      return unless previous_sequence_nr // interval < @sequence_nr // interval

      payload = String.build { |json| @state.to_json(json) }
      snapshot = Movie::Persistence::SnapshotRecord.new(@sequence_nr, snapshot_manifest(@state), payload)
      @store.save_snapshot(ctx, @persistence_id, snapshot)
    end

    private def reset_recovery_state
      @state = empty_state
      @sequence_nr = 0_i64
      @recovered = false
    end
  end

  abstract class DurableStateBehavior(C, S) < AbstractBehavior(C)
    @state : S
    @loaded : Bool = false
    @revision : Int64 = 0_i64

    def initialize(@persistence_id : String, @store : Movie::Persistence::StateStoreClient)
      @state = empty_state
    end

    def receive(message : C, ctx : ActorContext(C))
      recover(ctx) unless @loaded
      effect = handle_command(copy_state(@state), message, ctx)
      begin
        case effect.action
        when DurableAction::Persist
          next_state = effect.state.not_nil!
          payload = String.build { |json| next_state.to_json(json) }
          result = @store.save(
            ctx,
            @persistence_id,
            @revision,
            effect.operation_id.not_nil!,
            state_manifest(next_state),
            payload
          )
          @revision = result.revision
          if result.duplicate
            reset_recovery_state
            recover(ctx)
          else
            @state = next_state
          end
        when DurableAction::Delete
          result = @store.delete(ctx, @persistence_id, @revision, effect.operation_id.not_nil!)
          @revision = result.revision
          if result.duplicate
            reset_recovery_state
            recover(ctx)
          else
            @state = empty_state
          end
        when DurableAction::None
        end
      rescue error
        on_persist_failure(error)
        raise error
      end

      effect.callbacks.each &.call(@state)
      effect.stop? ? Behaviors(C).stopped : Behaviors(C).same
    end

    def on_signal(signal : SystemMessage)
      if signal.is_a?(PreRestart)
        reset_recovery_state
      end
      on_persistence_signal(signal)
    end

    protected abstract def empty_state : S
    protected abstract def handle_command(state : S, command : C, ctx : ActorContext(C)) : DurableEffect(S)

    protected def persist(state : S, operation_id : Persistence::OperationId) : DurableEffect(S)
      DurableEffect(S).new(DurableAction::Persist, state, operation_id)
    end

    protected def delete(operation_id : Persistence::OperationId) : DurableEffect(S)
      DurableEffect(S).new(DurableAction::Delete, operation_id: operation_id)
    end

    protected def none : DurableEffect(S)
      DurableEffect(S).new(DurableAction::None)
    end

    protected def stop : DurableEffect(S)
      DurableEffect(S).new(DurableAction::None, stop: true)
    end

    protected def state_manifest(state : S) : String
      state.class.name
    end

    protected def deserialize_state(manifest : String, payload : String) : S
      S.from_json(payload)
    end

    protected def on_recovery_completed(state : S, revision : Int64)
    end

    protected def on_recovery_failure(error : Exception)
    end

    protected def on_persist_failure(error : Exception)
    end

    protected def on_persistence_signal(signal : SystemMessage)
    end

    private def copy_state(state : S) : S
      payload = String.build { |json| state.to_json(json) }
      deserialize_state(state_manifest(state), payload)
    end

    private def recover(ctx : ActorContext(C))
      return if @loaded
      if stored = @store.load(ctx, @persistence_id)
        @revision = stored.revision
        if stored.deleted
          @state = empty_state
        else
          @state = deserialize_state(stored.manifest, stored.payload.not_nil!)
        end
      else
        @state = empty_state
        @revision = 0_i64
      end
      @loaded = true
      on_recovery_completed(@state, @revision)
    rescue error
      reset_recovery_state
      on_recovery_failure(error)
      raise error
    end

    private def reset_recovery_state
      @state = empty_state
      @revision = 0_i64
      @loaded = false
    end
  end

  abstract class PersistentEntityExtension(SM, S) < Extension
    def initialize(
      @system : AbstractActorSystem,
      @store_ref : Movie::ActorRef(SM),
      @store : S,
      @registry : Movie::ActorRef(Persistence::RegistryMessage),
      @timeout : Time::Span = 5.seconds,
    )
      @entities = Persistence::EntityProvider.new(@system, @registry, @timeout)
    end

    def stop
      @registry.send_system(Movie::STOP)
      @store_ref.send_system(Movie::STOP)
    end

    def register_entity(
      entity_type : E.class,
      message_type : C.class,
      &factory : Persistence::Id, S -> AbstractBehavior(C)
    ) : Persistence::EntityType(C) forall E, C
      key = entity_type.name
      spawn = ->(ctx : Movie::ActorContext(Persistence::RegistryMessage), id : Persistence::Id) do
        behavior = factory.call(id, @store)
        ctx.spawn(behavior).as(Movie::ActorRefBase)
      end
      @entities.register(key, spawn)
      Persistence::EntityType(C).new(key)
    end

    def get_entity_ref(persistence_id : Persistence::EntityId(T)) : ActorRef(T) forall T
      @entities.resolve(persistence_id.value).as(ActorRef(T))
    end
  end

  class EventSourcingExtension < PersistentEntityExtension(
    Persistence::EventStoreMessage,
    Persistence::EventStoreClient,
  )
    # Keep a concrete virtual entrypoint so Extension#stop dispatch compiles for
    # this specialization on every supported Crystal version.
    def stop
      super
    end
  end

  class DurableStateExtension < PersistentEntityExtension(
    Persistence::StateStoreMessage,
    Persistence::StateStoreClient,
  )
    # See EventSourcingExtension#stop.
    def stop
      super
    end
  end

  class EventSourcing < ExtensionId(EventSourcingExtension)
    def create(system : AbstractActorSystem) : EventSourcingExtension
      db_ext = Movie::Database.get(system)
      store_ref = system.spawn(Persistence::EventStoreActor.new(db_ext.pool, db_ext.operation_timeout))
      store = Persistence::EventStoreClient.new(store_ref, db_ext.operation_timeout)
      registry = system.spawn(Persistence::EntityRegistry.new)
      EventSourcingExtension.new(system, store_ref, store, registry)
    end
  end

  class DurableState < ExtensionId(DurableStateExtension)
    def create(system : AbstractActorSystem) : DurableStateExtension
      db_ext = Movie::Database.get(system)
      store_ref = system.spawn(Persistence::StateStoreActor.new(db_ext.pool, db_ext.operation_timeout))
      store = Persistence::StateStoreClient.new(store_ref, db_ext.operation_timeout)
      registry = system.spawn(Persistence::EntityRegistry.new)
      DurableStateExtension.new(system, store_ref, store, registry)
    end
  end
end
