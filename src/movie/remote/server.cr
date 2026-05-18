require "socket"
require "./wire_envelope"
require "./frame_codec"
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

    def initialize(
      @system : Movie::AbstractActorSystem,
      @host : String,
      @port : Int32,
      @path_registry : Movie::PathRegistry,
      @on_message : Proc(WireEnvelope, InboundConnection, Nil)
    )
      @connections = [] of InboundConnection
      @connections_mutex = Mutex.new
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

      connection = InboundConnection.new(
        socket: socket,
        server: self,
        path_registry: @path_registry,
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
    end
  end

  # InboundConnection handles an incoming connection from a remote system.
  class InboundConnection
    Log = ::Log.for(self)

    getter? connected : Bool = true

    @socket : TCPSocket
    @write_mutex : Mutex
    @reader_fiber : Fiber?
    @remote_address : Address?

    def initialize(
      @socket : TCPSocket,
      @server : Server,
      @path_registry : Movie::PathRegistry,
      @on_message : Proc(WireEnvelope, InboundConnection, Nil)
    )
      @write_mutex = Mutex.new
    end

    # Starts reading from the connection.
    def start
      @reader_fiber = spawn do
        reader_loop
      end
    end

    # Sends an envelope to the remote system.
    def send(envelope : WireEnvelope) : Bool
      return false unless @connected

      begin
        @write_mutex.synchronize do
          FrameCodec.encode(envelope, @socket)
        end
        true
      rescue ex : IO::Error
        Log.error { "Failed to send: #{ex.message}" }
        close
        false
      end
    end

    # Closes the connection.
    def close
      return unless @connected
      @connected = false
      @socket.close rescue nil
      @server.connection_closed(self)
      Log.debug { "Inbound connection closed" }
    end

    # Closes the connection without notifying the server (used during server shutdown).
    def close_without_callback
      return unless @connected
      @connected = false
      @socket.close rescue nil
      Log.debug { "Inbound connection closed (no callback)" }
    end

    # Returns the remote address if known (from handshake).
    def remote_address : Address?
      @remote_address
    end

    # Sets the remote address (from handshake).
    def remote_address=(@remote_address : Address?)
    end

    private def reader_loop
      loop do
        break unless @connected

        envelope = begin
          FrameCodec.decode(@socket)
        rescue ex : IO::Error
          Log.debug { "Read error: #{ex.message}" }
          break
        end

        break if envelope.nil?

        handle_incoming(envelope)
      end

      close
    end

    private def handle_incoming(envelope : WireEnvelope)
      case envelope.kind
      when .handshake?
        handle_handshake(envelope)
      else
        @on_message.call(envelope, self)
      end
    end

    private def handle_handshake(envelope : WireEnvelope)
      system_name = envelope.payload["system"]?.try(&.as_s)
      address_str = envelope.payload["address"]?.try(&.as_s)

      if address_str
        @remote_address = Address.parse(address_str)
        Log.info { "Handshake from #{system_name} at #{address_str}" }
      end
    end
  end
end
