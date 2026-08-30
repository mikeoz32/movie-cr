require "socket"
require "./wire_envelope"
require "./frame_codec"
require "./outbound_writer"
require "../path"

module Movie::Remote
  # Connection manages an outbound TCP connection to a remote actor system.
  class Connection
    Log = ::Log.for(self)

    getter address : Address
    getter? connected : Bool = false
    getter? closed : Bool = false

    @socket : TCPSocket?
    @outbound_writer : OutboundWriter?
    @pending_asks : Hash(String, Channel(WireEnvelope))
    @pending_asks_mutex : Mutex
    @reader_fiber : Fiber?
    @frame_decoder : FrameCodec::Decoder

    def initialize(
      @address : Address,
      @path_registry : Movie::PathRegistry,
      @system : Movie::AbstractActorSystem,
      @on_message : Proc(WireEnvelope, Nil)? = nil,
    )
      @pending_asks = {} of String => Channel(WireEnvelope)
      @pending_asks_mutex = Mutex.new
      @frame_decoder = FrameCodec::Decoder.new
    end

    # Connects to the remote system.
    def connect : Bool
      return true if @connected
      return false if @closed

      host = @address.host
      port = @address.port

      unless host && port
        Log.error { "Cannot connect to local address: #{@address}" }
        return false
      end

      begin
        socket = TCPSocket.new(host, port)
        socket.tcp_nodelay = true
        @socket = socket
        @outbound_writer = OutboundWriter.new(socket) { |error| handle_write_error(error) }
        @connected = true
        @outbound_writer.not_nil!.start
        start_reader
        Log.info { "Connected to #{@address}" }
        true
      rescue ex : Socket::ConnectError
        Log.error { "Failed to connect to #{@address}: #{ex.message}" }
        false
      end
    end

    # Sends an envelope to the remote system.
    def send(envelope : WireEnvelope) : Bool
      return false unless @connected
      writer = @outbound_writer
      return false unless writer

      writer.enqueue(envelope)
    end

    # Registers a pending ask request and returns the channel to wait on.
    def register_pending_ask(correlation_id : String) : Channel(WireEnvelope)
      channel = Channel(WireEnvelope).new(1)
      @pending_asks_mutex.synchronize do
        @pending_asks[correlation_id] = channel
      end
      channel
    end

    # Removes a pending ask (e.g., on timeout).
    def remove_pending_ask(correlation_id : String)
      @pending_asks_mutex.synchronize do
        @pending_asks.delete(correlation_id)
      end
    end

    # Closes the connection.
    def close
      return if @closed
      @closed = true
      @connected = false
      @outbound_writer.try &.close

      if socket = @socket
        socket.close rescue nil
      end

      # Close all pending ask channels
      @pending_asks_mutex.synchronize do
        @pending_asks.each_value(&.close)
        @pending_asks.clear
      end

      Log.info { "Connection to #{@address} closed" }
    end

    private def start_reader
      @reader_fiber = spawn do
        reader_loop
      end
    end

    private def reader_loop
      socket = @socket
      return unless socket

      loop do
        break if @closed

        envelope = begin
          @frame_decoder.decode(socket)
        rescue ex : Exception
          Log.debug { "Read error from #{@address}: #{ex.message}" }
          break
        end

        break if envelope.nil?

        begin
          handle_incoming(envelope)
        rescue ex : Exception
          Log.error(exception: ex) { "Protocol error from #{@address}" }
          break
        end
      end
    ensure
      handle_disconnect unless @closed
    end

    private def handle_incoming(envelope : WireEnvelope)
      case envelope.kind
      when .ask_response?
        # Route to pending ask
        if correlation_id = envelope.correlation_id
          channel = @pending_asks_mutex.synchronize { @pending_asks.delete(correlation_id) }
          if channel
            channel.send(envelope)
          else
            Log.warn { "Received ask response for unknown correlation: #{correlation_id}" }
          end
        end
      else
        # Delegate to callback handler
        if handler = @on_message
          handler.call(envelope)
        else
          Log.warn { "Received message but no handler configured" }
        end
      end
    end

    private def handle_disconnect
      return if @closed
      Log.info { "Disconnected from #{@address}" }
      close
    end

    private def handle_write_error(error : Exception) : Nil
      if error.is_a?(IO::Error)
        Log.error { "Failed to send to #{@address}: #{error.message}" }
        handle_disconnect
      else
        Log.error(exception: error) { "Failed to encode outbound message to #{@address}" }
      end
    end
  end
end
