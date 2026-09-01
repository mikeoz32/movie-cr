require "socket"
require "sync/mutex"
require "./association"
require "./association_negotiator"
require "./control_delivery"
require "./pending_asks"
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
    @last_received_monotonic_ns = Atomic(Int64).new(0_i64)
    @socket : IO?
    @outbound_writer : OutboundWriter?
    @pending_asks : PendingAskRegistry
    @pending_control : PendingControlBuffer
    @lifecycle_mutex : Mutex
    @reconnect_running = Atomic(Bool).new(false)
    @connect_attempts = Atomic(Int64).new(0_i64)
    @successful_connections = Atomic(Int64).new(0_i64)
    @disconnects = Atomic(Int64).new(0_i64)
    @heartbeat_timeouts = Atomic(Int64).new(0_i64)
    @protocol_failures = Atomic(Int64).new(0_i64)
    @remote_node_uid : String?

    def initialize(
      @address : Address,
      @path_registry : Movie::PathRegistry,
      @system : Movie::AbstractActorSystem,
      @local_address : Proc(Address),
      @node_uid : String,
      @settings : AssociationSettings = AssociationSettings.new,
      @on_message : Proc(WireEnvelope, Nil)? = nil,
    )
      @pending_asks = PendingAskRegistry.new
      @pending_control = PendingControlBuffer.new(@settings.control_buffer_capacity)
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
      @pending_asks.register(correlation_id)
    end

    def remove_pending_ask(correlation_id : String)
      @pending_asks.remove(correlation_id)
    end

    def pending_control_count : Int32
      @pending_control.size
    end

    def stats : ConnectionStats
      last_received_ns = @last_received_monotonic_ns.get
      ConnectionStats.new(
        state: state,
        generation: generation,
        connect_attempts: @connect_attempts.get,
        successful_connections: @successful_connections.get,
        disconnects: @disconnects.get,
        heartbeat_timeouts: @heartbeat_timeouts.get,
        protocol_failures: @protocol_failures.get,
        last_received_at_ms: last_received_ns == 0 ? 0_i64 : AssociationClock.wall_milliseconds(last_received_ns),
        pending_asks: @pending_asks.size,
        pending_control: pending_control_count
      )
    end

    # Reliable system/control messages remain pending until acknowledged and
    # are replayed in sequence after a new socket generation is active.
    def send_control(envelope : WireEnvelope) : Bool
      writer = nil.as(OutboundWriter?)
      ready = @lifecycle_mutex.synchronize do
        return false if closed?
        writer = @outbound_writer if active?
        @pending_control.offer(envelope)
      end
      return false unless ready

      writer.try &.enqueue(ready)
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
      @pending_control.clear
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
          peer_handshake = AssociationNegotiator.connect(
            transport.not_nil!,
            handshake,
            @address.system,
            @settings.shared_secret
          )
          if remote_node_uid = @remote_node_uid
            @pending_control.rebase if remote_node_uid != peer_handshake.node_uid
          end
          @remote_node_uid = peer_handshake.node_uid
          transport.as?(TCPSocket).try { |tcp| tcp.read_timeout = nil }

          generation = @generation.add(1) + 1
          writer = OutboundWriter.new(transport.not_nil!) do |error|
            handle_write_error(error, generation)
          end
          @socket = transport
          @outbound_writer = writer
          record_activity
          @successful_connections.add(1)
          transition(State::Active)
          writer.start
          resend_pending_control(writer)
        rescue ex : Exception
          @protocol_failures.add(1) if ex.is_a?(AssociationProtocolError)
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

    private def start_reader(socket : IO, generation : Int64) : Nil
      spawn do
        decoder = FrameCodec::Decoder.new(MessageRegistry.payload_decoder)
        loop do
          break unless current_generation?(generation)
          envelope = decoder.decode(socket)
          break unless envelope
          record_activity
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

          silence_ns = AssociationClock.now_nanoseconds - @last_received_monotonic_ns.get
          if silence_ns > @settings.heartbeat_timeout.total_nanoseconds
            @heartbeat_timeouts.add(1)
            Log.warn { "Association #{@address} timed out after #{silence_ns / 1_000_000}ms" }
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
          channel = @pending_asks.remove(correlation_id)
          channel.try &.send(envelope)
        end
      when .heartbeat?
        send(WireEnvelope.heartbeat_ack)
      when .heartbeat_ack?
        # last-received activity was updated before dispatch
      when .control_ack?
        @pending_control.acknowledge(envelope.control_stream, envelope.control_sequence)
      when .handshake?, .handshake_ack?, .handshake_confirm?, .handshake_ready?, .handshake_reject?
        @protocol_failures.add(1)
        raise AssociationProtocolError.new("unexpected handshake frame on active generation #{generation}")
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
            break unless enter_backoff?
            sleep @settings.reconnect_delay(delay)
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

    private def record_activity : Nil
      @last_received_monotonic_ns.set(AssociationClock.now_nanoseconds)
    end

    private def fail_pending_asks : Nil
      @pending_asks.fail_all
    end

    private def resend_pending_control(writer : OutboundWriter) : Nil
      @pending_control.pending.each do |envelope|
        break unless writer.enqueue(envelope)
      end
    end

    private def current_generation?(generation : Int64) : Bool
      active? && @generation.get == generation
    end

    private def enter_backoff? : Bool
      @lifecycle_mutex.synchronize do
        return false if closed? || active?
        transition(State::Backoff)
        true
      end
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
