require "socket"
require "./wire_envelope"
require "./frame_codec"
require "./inbound_frame_reader"
require "./outbound_writer"
require "./message_registry"
require "./control_delivery"
require "../path"

module Movie::Remote
  # Server accepts incoming TCP connections from remote actor systems.
  class Server
    Log = ::Log.for(self)

    getter host : String
    getter port : Int32
    getter? running : Bool = false

    @tcp_server : TCPServer?
    @accept_fiber : Fiber?
    @connections : Array(InboundConnection)
    @connections_mutex : Mutex
    @control_deduplicator : ControlDeduplicator

    def initialize(
      @system : Movie::AbstractActorSystem,
      @host : String,
      @port : Int32,
      @path_registry : Movie::PathRegistry,
      @local_address : Proc(Address),
      @node_uid : String,
      @on_message : Proc(WireEnvelope, InboundConnection, Nil),
      @on_disconnect : Proc(InboundConnection, Nil),
      @settings : AssociationSettings = AssociationSettings.new,
    )
      @connections = [] of InboundConnection
      @connections_mutex = Mutex.new
      @control_deduplicator = ControlDeduplicator.new(@settings.control_deduplication_capacity)
    end

    # Starts the server.
    def start : Bool
      return true if @running

      begin
        @tcp_server = TCPServer.new(@host, @port)
        @running = true
        start_accept_loop
        Log.info { "Server started on #{@host}:#{@port}" }
        true
      rescue ex : Socket::BindError
        Log.error { "Failed to bind to #{@host}:#{@port}: #{ex.message}" }
        false
      end
    end

    # Stops the server and closes all connections.
    def stop
      return unless @running
      @running = false

      if server = @tcp_server
        server.close rescue nil
      end

      # Copy connections to close outside the mutex to avoid recursive locking
      # when close() calls connection_closed()
      connections_to_close = @connections_mutex.synchronize do
        conns = @connections.dup
        @connections.clear
        conns
      end

      connections_to_close.each(&.close_without_callback)

      Log.info { "Server stopped" }
    end

    # Returns the actual bound port (useful when binding to port 0).
    def local_port : Int32
      @tcp_server.try(&.local_address.port) || @port
    end

    def control_deduplication_stats : ControlDeduplicationStats
      @control_deduplicator.stats
    end

    # Releases receiver-side cursors only for a node incarnation that the
    # operator has independently confirmed will never reconnect.
    def retire_control_node(node_uid : String) : Int32
      raise ArgumentError.new("control node UID must not be empty") if node_uid.empty?
      @control_deduplicator.retire_node(node_uid)
    end

    private def start_accept_loop
      @accept_fiber = spawn do
        accept_loop
      end
    end

    private def accept_loop
      server = @tcp_server
      return unless server

      loop do
        break unless @running

        client = begin
          server.accept
        rescue ex : IO::Error
          break unless @running
          Log.debug { "Accept error: #{ex.message}" }
          next
        end

        handle_new_connection(client)
      end
    end

    private def handle_new_connection(socket : TCPSocket)
      socket.tcp_nodelay = true
      transport = @settings.wrap(socket)

      connection = InboundConnection.new(
        socket: socket,
        transport: transport,
        server: self,
        path_registry: @path_registry,
        local_system: @system.name,
        local_address: @local_address,
        node_uid: @node_uid,
        shared_secret: @settings.shared_secret,
        heartbeat_interval: @settings.heartbeat_interval,
        heartbeat_timeout: @settings.heartbeat_timeout,
        control_deduplicator: @control_deduplicator,
        on_message: @on_message
      )

      @connections_mutex.synchronize do
        @connections << connection
      end

      connection.start

      Log.info { "Accepted connection from #{socket.remote_address}" }
    end

    # Called by InboundConnection when it closes.
    protected def connection_closed(connection : InboundConnection)
      @connections_mutex.synchronize do
        @connections.delete(connection)
      end
      @on_disconnect.call(connection)
    end

    protected def connection_stopped(connection : InboundConnection)
      @on_disconnect.call(connection)
    end
  end
end

require "./inbound_connection"
