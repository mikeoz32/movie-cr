require "socket"
require "sync/mutex"
require "./association"
require "./wire_envelope"
require "./frame_codec"
require "./outbound_writer"
require "../path"

module Movie::Remote
  # A durable logical association whose socket generation can be replaced
  # after failure. Actor refs retain this object rather than a transient socket.
  class Connection
    Log = ::Log.for(self)

    enum State
      Disconnected
      Connecting
      Handshaking
      Active
      Backoff
      Stopped
    end

    getter address : Address

    @state = Atomic(Int32).new(State::Disconnected.value)
    @generation = Atomic(Int64).new(0_i64)
    @last_received_ms = Atomic(Int64).new(0_i64)
    @socket : IO?
    @outbound_writer : OutboundWriter?
    @pending_asks : Hash(String, Channel(WireEnvelope))
    @pending_asks_mutex : Mutex
    @control_stream = UUID.random.to_s
    @control_sequence = 0_i64
    @pending_control : Hash(Int64, WireEnvelope)
    @control_mutex : Mutex
    @lifecycle_mutex : Mutex
    @reconnect_running = Atomic(Bool).new(false)
    @connect_attempts = Atomic(Int64).new(0_i64)
    @successful_connections = Atomic(Int64).new(0_i64)
    @disconnects = Atomic(Int64).new(0_i64)
    @heartbeat_timeouts = Atomic(Int64).new(0_i64)
    @protocol_failures = Atomic(Int64).new(0_i64)

    def initialize(
      @address : Address,
      @path_registry : Movie::PathRegistry,
      @system : Movie::AbstractActorSystem,
      @local_address : Proc(Address),
      @node_uid : String,
      @settings : AssociationSettings = AssociationSettings.new,
      @on_message : Proc(WireEnvelope, Nil)? = nil,
    )
      @pending_asks = {} of String => Channel(WireEnvelope)
      @pending_asks_mutex = Mutex.new
      @pending_control = {} of Int64 => WireEnvelope
      @control_mutex = Mutex.new
      @lifecycle_mutex = Mutex.new
    end

    def state : State
      State.from_value(@state.get)
    end

    def generation : Int64
      @generation.get
    end

    def connected? : Bool
      active?
    end

    def active? : Bool
      state.active?
    end

    def closed? : Bool
      state.stopped?
    end

    # Makes one synchronous attempt, then keeps retrying in the background.
    def connect : Bool
      return true if active?
      return false if closed?

      connected = attempt_connect
      schedule_reconnect unless connected
      connected
    end

    # User traffic is accepted only by an active generation and is never
    # replayed across reconnects.
    def send(envelope : WireEnvelope) : Bool
      return false unless active?
      writer = @lifecycle_mutex.synchronize { @outbound_writer }
      return false unless writer

      writer.enqueue(envelope)
    end

    def register_pending_ask(correlation_id : String) : Channel(WireEnvelope)
      channel = Channel(WireEnvelope).new(1)
      @pending_asks_mutex.synchronize { @pending_asks[correlation_id] = channel }
      channel
    end

    def remove_pending_ask(correlation_id : String)
      @pending_asks_mutex.synchronize { @pending_asks.delete(correlation_id) }
    end

    def pending_control_count : Int32
      @control_mutex.synchronize { @pending_control.size }
    end

    def stats : ConnectionStats
      ConnectionStats.new(
        state: state,
        generation: generation,
        connect_attempts: @connect_attempts.get,
        successful_connections: @successful_connections.get,
        disconnects: @disconnects.get,
        heartbeat_timeouts: @heartbeat_timeouts.get,
        protocol_failures: @protocol_failures.get,
        last_received_at_ms: @last_received_ms.get,
        pending_asks: @pending_asks_mutex.synchronize { @pending_asks.size },
        pending_control: pending_control_count
      )
    end

    # Reliable system/control messages remain pending until acknowledged and
    # are replayed in sequence after a new socket generation is active.
    def send_control(envelope : WireEnvelope) : Bool
      writer = @lifecycle_mutex.synchronize { active? ? @outbound_writer : nil }
      @control_mutex.synchronize do
        return false if closed? || @pending_control.size >= @settings.control_buffer_capacity
        @control_sequence += 1
        envelope.control_stream = @control_stream
        envelope.control_sequence = @control_sequence
        @pending_control[@control_sequence] = envelope
      end

      writer.try &.enqueue(envelope)
      true
    end

    def close
      writer = nil.as(OutboundWriter?)
      socket = nil.as(IO?)
      @lifecycle_mutex.synchronize do
        return if closed?
        transition(State::Stopped)
        writer = @outbound_writer
        socket = @socket
        @outbound_writer = nil
        @socket = nil
      end
      writer.try &.close
      socket.try &.close rescue nil
      fail_pending_asks
      @control_mutex.synchronize { @pending_control.clear }
      Log.info { "Association to #{@address} stopped" }
    end

    private def attempt_connect : Bool
      generation = 0_i64
      transport = nil.as(IO?)

      @lifecycle_mutex.synchronize do
        return true if active?
        return false if closed?

        transition(State::Connecting)
        @connect_attempts.add(1)
        host = @address.host
        port = @address.port
        unless host && port
          transition(State::Disconnected)
          return false
        end

        begin
          transport = @settings.connect(host, port)
          if tcp = transport.as?(TCPSocket)
            tcp.read_timeout = @settings.handshake_timeout
          end
          transition(State::Handshaking)

          association_id = UUID.random.to_s
          handshake = AssociationHandshake.create(
            system: @system.name,
            address: @local_address.call.to_s,
            node_uid: @node_uid,
            association_id: association_id,
            shared_secret: @settings.shared_secret
          )
          FrameCodec.encode(WireEnvelope.handshake(handshake), transport.not_nil!)
          response = FrameCodec::Decoder.new.decode(transport.not_nil!)
          validate_handshake_response(response, association_id)
          transport.as?(TCPSocket).try { |tcp| tcp.read_timeout = nil }

          generation = @generation.add(1) + 1
          writer = OutboundWriter.new(transport.not_nil!) do |error|
            handle_write_error(error, generation)
          end
          @socket = transport
          @outbound_writer = writer
          @last_received_ms.set(Time.utc.to_unix_ms)
          @successful_connections.add(1)
          transition(State::Active)
          writer.start
          resend_pending_control(writer)
        rescue ex : Exception
          transport.try &.close rescue nil
          @socket = nil
          @outbound_writer = nil
          transition(State::Disconnected) unless closed?
          Log.debug { "Association attempt to #{@address} failed: #{ex.message}" }
          return false
        end
      end

      start_reader(transport.not_nil!, generation)
      start_heartbeat(generation)
      Log.info { "Association generation #{generation} connected to #{@address}" }
      true
    end

    private def validate_handshake_response(response : WireEnvelope?, association_id : String) : Nil
      raise RemoteDeliveryError.new("peer closed during association handshake") unless response
      if response.kind.handshake_reject?
        rejection = HandshakeRejection.from_json(response.payload_data.json_source)
        protocol_failure("association rejected: #{rejection.reason}")
      end
      protocol_failure("expected handshake acknowledgement") unless response.kind.handshake_ack?

      handshake = AssociationHandshake.from_json(response.payload_data.json_source)
      protocol_failure("peer protocol version mismatch") unless handshake.protocol_version == PROTOCOL_VERSION
      protocol_failure("peer system mismatch") unless handshake.system == @address.system
      protocol_failure("association acknowledgement mismatch") unless handshake.association_id == association_id
      protocol_failure("peer authentication failed") unless handshake.authenticated?(@settings.shared_secret)
      missing_capabilities = DEFAULT_CAPABILITIES.reject { |capability| handshake.capabilities.includes?(capability) }
      unless missing_capabilities.empty?
        protocol_failure("peer capabilities are missing: #{missing_capabilities.join(", ")}")
      end
    end

    private def start_reader(socket : IO, generation : Int64) : Nil
      spawn do
        decoder = FrameCodec::Decoder.new(MessageRegistry.payload_decoder)
        loop do
          break unless current_generation?(generation)
          envelope = decoder.decode(socket)
          break unless envelope
          @last_received_ms.set(Time.utc.to_unix_ms)
          handle_incoming(envelope, generation)
        end
      rescue ex : Exception
        Log.debug { "Read error from #{@address}: #{ex.message}" }
      ensure
        handle_disconnect(generation)
      end
    end

    private def start_heartbeat(generation : Int64) : Nil
      spawn do
        loop do
          sleep @settings.heartbeat_interval
          break unless current_generation?(generation)

          silence = Time.utc.to_unix_ms - @last_received_ms.get
          if silence > @settings.heartbeat_timeout.total_milliseconds
            @heartbeat_timeouts.add(1)
            Log.warn { "Association #{@address} timed out after #{silence}ms" }
            handle_disconnect(generation)
            break
          end
          unless send(WireEnvelope.heartbeat)
            handle_disconnect(generation)
            break
          end
        end
      end
    end

    private def handle_incoming(envelope : WireEnvelope, generation : Int64) : Nil
      case envelope.kind
      when .ask_response?
        if correlation_id = envelope.correlation_id
          channel = @pending_asks_mutex.synchronize { @pending_asks.delete(correlation_id) }
          channel.try &.send(envelope)
        end
      when .heartbeat?
        send(WireEnvelope.heartbeat_ack)
      when .heartbeat_ack?
        # last-received activity was updated before dispatch
      when .control_ack?
        stream = envelope.control_stream
        sequence = envelope.control_sequence
        if stream == @control_stream && sequence
          @control_mutex.synchronize { @pending_control.delete(sequence) }
        end
      when .handshake?, .handshake_ack?, .handshake_reject?
        protocol_failure("unexpected handshake frame on active generation #{generation}")
      else
        @on_message.try &.call(envelope)
      end
    end

    private def handle_disconnect(generation : Int64) : Nil
      writer = nil.as(OutboundWriter?)
      socket = nil.as(IO?)
      disconnected = @lifecycle_mutex.synchronize do
        if closed? || @generation.get != generation || !active?
          false
        else
          transition(State::Disconnected)
          writer = @outbound_writer
          socket = @socket
          @outbound_writer = nil
          @socket = nil
          true
        end
      end
      return unless disconnected

      writer.try &.close
      socket.try &.close rescue nil
      @disconnects.add(1)
      fail_pending_asks
      Log.info { "Association generation #{generation} disconnected from #{@address}" }
      schedule_reconnect
    end

    private def handle_write_error(error : Exception, generation : Int64) : Nil
      if error.is_a?(IO::Error)
        Log.error { "Failed to send to #{@address}: #{error.message}" }
        handle_disconnect(generation)
      else
        Log.error(exception: error) { "Failed to encode outbound message to #{@address}" }
      end
    end

    private def schedule_reconnect : Nil
      _, started = @reconnect_running.compare_and_set(false, true)
      return unless started

      spawn do
        delay = @settings.reconnect_min_backoff
        begin
          until closed? || active?
            transition(State::Backoff)
            sleep jittered(delay)
            break if closed? || attempt_connect
            next_delay = delay.total_nanoseconds * @settings.reconnect_factor
            delay = {Time::Span.new(nanoseconds: next_delay.to_i64), @settings.reconnect_max_backoff}.min
          end
        ensure
          @reconnect_running.set(false)
          schedule_reconnect unless closed? || active?
        end
      end
    end

    private def jittered(delay : Time::Span) : Time::Span
      jitter = @settings.reconnect_jitter
      return delay if jitter == 0.0
      factor = 1.0 - jitter + Random.rand * jitter * 2.0
      Time::Span.new(nanoseconds: (delay.total_nanoseconds * factor).to_i64)
    end

    private def fail_pending_asks : Nil
      @pending_asks_mutex.synchronize do
        @pending_asks.each_value(&.close)
        @pending_asks.clear
      end
    end

    private def resend_pending_control(writer : OutboundWriter) : Nil
      @control_mutex.synchronize do
        @pending_control.keys.sort.each do |sequence|
          break unless writer.enqueue(@pending_control[sequence])
        end
      end
    end

    private def current_generation?(generation : Int64) : Bool
      active? && @generation.get == generation
    end

    private def protocol_failure(message : String) : NoReturn
      @protocol_failures.add(1)
      raise RemoteDeliveryError.new(message)
    end

    private def transition(state : State) : Nil
      @state.set(state.value)
    end
  end

  record ConnectionStats,
    state : Connection::State,
    generation : Int64,
    connect_attempts : Int64,
    successful_connections : Int64,
    disconnects : Int64,
    heartbeat_timeouts : Int64,
    protocol_failures : Int64,
    last_received_at_ms : Int64,
    pending_asks : Int32,
    pending_control : Int32
end
