require "socket"
require "./wire_envelope"
require "./frame_codec"
require "./inbound_frame_reader"
require "./outbound_writer"
require "./message_registry"
require "./control_delivery"
require "../path"

module Movie::Remote
  # InboundConnection handles an incoming connection from a remote system.
  class InboundConnection
    Log = ::Log.for(self)

    getter? connected : Bool = true

    @socket : TCPSocket
    @transport : IO
    @outbound_writer : OutboundWriter
    @reader_fiber : Fiber?
    @remote_address : Address?
    @remote_node_uid : String?
    @association_id : String?
    @pending_handshake : AssociationHandshake?
    @server_challenge : AssociationHandshake?
    @handshake_complete = false
    @batch_decoder : InboundFrameBatchDecoder
    @last_received_monotonic_ns = Atomic(Int64).new(AssociationClock.now_nanoseconds)
    @watches = {} of String => {Movie::ActorRefBase, Movie::ActorRefBase}
    @watches_mutex = Mutex.new

    def initialize(
      @socket : TCPSocket,
      @transport : IO,
      @server : Server,
      @path_registry : Movie::PathRegistry,
      @local_system : String,
      @local_address : Proc(Address),
      @node_uid : String,
      @control_deduplicator : ControlDeduplicator,
      @on_message : Proc(WireEnvelope, InboundConnection, Nil),
      @shared_secret : String? = nil,
      @heartbeat_interval : Time::Span = 1.second,
      @heartbeat_timeout : Time::Span = 5.seconds,
    )
      @socket.read_buffering = false
      @outbound_writer = OutboundWriter.new(@transport) { |error| handle_write_error(error) }
      @batch_decoder = InboundFrameBatchDecoder.new(@transport, MessageRegistry.payload_decoder)
    end

    # Starts reading from the connection.
    def start
      @outbound_writer.start
      @reader_fiber = spawn do
        reader_loop
      end
      start_heartbeat_watchdog
    end

    # Sends an envelope to the remote system.
    def send(envelope : WireEnvelope) : Bool
      return false unless @connected

      @outbound_writer.enqueue(envelope)
    end

    # Closes the connection.
    def close
      return unless @connected
      @connected = false
      @outbound_writer.close
      close_transport
      @server.connection_closed(self)
      Log.debug { "Inbound connection closed" }
    end

    # Closes the connection without notifying the server (used during server shutdown).
    def close_without_callback
      return unless @connected
      @connected = false
      @outbound_writer.close
      close_transport
      @server.connection_stopped(self)
      Log.debug { "Inbound connection closed (no callback)" }
    end

    # Returns the remote address if known (from handshake).
    def remote_address : Address?
      @remote_address
    end

    # Sets the remote address (from handshake).
    def remote_address=(@remote_address : Address?)
    end

    def track_watch(target : Movie::ActorRefBase, watcher : Movie::ActorRefBase) : Nil
      key = watch_key(target, watcher)
      @watches_mutex.synchronize { @watches[key] = {target, watcher} }
    end

    def untrack_watch(target : Movie::ActorRefBase, watcher : Movie::ActorRefBase) : Nil
      @watches_mutex.synchronize { @watches.delete(watch_key(target, watcher)) }
    end

    def drain_watches : Array({Movie::ActorRefBase, Movie::ActorRefBase})
      @watches_mutex.synchronize do
        values = @watches.values
        @watches.clear
        values
      end
    end

    private def reader_loop
      loop do
        break unless @connected

        batch = begin
          @batch_decoder.next_batch
        rescue ex : MalformedMessagePayloadError
          Log.error { ex.message }
          next
        rescue ex : Exception
          Log.debug { "Read/protocol error: #{ex.message}" }
          break
        end

        break unless batch

        begin
          batch.each { |ready_envelope| handle_incoming(ready_envelope) }
        rescue ex : Exception
          Log.error(exception: ex) { "Protocol error from #{@socket.remote_address}" }
          break
        end
      end
    ensure
      close
    end

    private def handle_incoming(envelope : WireEnvelope)
      @last_received_monotonic_ns.set(AssociationClock.now_nanoseconds)
      case envelope.kind
      when .handshake?
        handle_handshake(envelope)
      when .handshake_confirm?
        handle_handshake_confirmation(envelope)
      else
        unless @handshake_complete
          reject_handshake("association handshake required")
          return
        end
        if envelope.kind.heartbeat?
          send(WireEnvelope.heartbeat_ack)
        elsif envelope.kind.system_message?
          handle_control(envelope)
        elsif envelope.kind.handshake_ack? || envelope.kind.handshake_ready? || envelope.kind.handshake_reject?
          reject_handshake("unexpected handshake response")
        else
          @on_message.call(envelope, self)
        end
      end
    end

    private def handle_write_error(error : Exception) : Nil
      if error.is_a?(IO::Error)
        Log.error { "Failed to send: #{error.message}" }
        close
      else
        Log.error(exception: error) { "Failed to encode outbound message" }
      end
    end

    private def start_heartbeat_watchdog : Nil
      spawn do
        loop do
          sleep @heartbeat_interval
          break unless @connected
          silence_ns = AssociationClock.now_nanoseconds - @last_received_monotonic_ns.get
          if silence_ns > @heartbeat_timeout.total_nanoseconds
            Log.warn { "Inbound association timed out after #{silence_ns / 1_000_000}ms" }
            close
            break
          end
        end
      end
    end

    private def handle_handshake(envelope : WireEnvelope)
      if @handshake_complete || @pending_handshake
        reject_handshake("association handshake already completed")
        return
      end

      handshake = AssociationHandshake.from_json(envelope.payload_data.json_source)
      unless handshake.protocol_version == PROTOCOL_VERSION
        reject_handshake("unsupported protocol version #{handshake.protocol_version}; expected #{PROTOCOL_VERSION}")
        return
      end
      unless handshake.authenticated?(@shared_secret)
        reject_handshake("association authentication failed")
        return
      end
      missing_capabilities = DEFAULT_CAPABILITIES.reject { |capability| handshake.capabilities.includes?(capability) }
      unless missing_capabilities.empty?
        reject_handshake("required capabilities are missing: #{missing_capabilities.join(", ")}")
        return
      end

      remote_address = Address.parse(handshake.address)
      unless remote_address.system == handshake.system
        reject_handshake("handshake system does not match advertised address")
        return
      end

      response = AssociationHandshake.create(
        system: @local_system,
        address: @local_address.call.to_s,
        node_uid: @node_uid,
        association_id: handshake.association_id,
        shared_secret: @shared_secret
      )
      FrameCodec.encode(WireEnvelope.handshake_ack(response), @transport)
      @pending_handshake = handshake
      @server_challenge = response
    rescue ex : Exception
      reject_handshake("invalid association handshake: #{ex.message}")
    end

    private def handle_handshake_confirmation(envelope : WireEnvelope) : Nil
      handshake = @pending_handshake
      challenge = @server_challenge
      unless handshake && challenge
        reject_handshake("association confirmation has no pending challenge")
        return
      end

      confirmation = AssociationConfirmation.from_json(envelope.payload_data.json_source)
      valid_identity = confirmation.association_id == handshake.association_id &&
                       confirmation.node_uid == handshake.node_uid &&
                       confirmation.client_nonce == handshake.nonce &&
                       confirmation.server_nonce == challenge.nonce
      unless valid_identity && confirmation.authenticated?(@shared_secret)
        reject_handshake("association confirmation failed")
        return
      end

      @remote_address = Address.parse(handshake.address)
      @remote_node_uid = handshake.node_uid
      @association_id = handshake.association_id
      @pending_handshake = nil
      @server_challenge = nil
      @handshake_complete = true
      FrameCodec.encode(WireEnvelope.handshake_ready(handshake.association_id), @transport)
      Log.info { "Association #{handshake.association_id} from #{handshake.system} at #{handshake.address} established" }
    rescue ex : Exception
      reject_handshake("invalid association handshake: #{ex.message}")
    end

    private def reject_handshake(reason : String) : Nil
      FrameCodec.encode(WireEnvelope.handshake_reject(reason), @transport)
    ensure
      close
    end

    private def handle_control(envelope : WireEnvelope) : Nil
      node_uid = @remote_node_uid
      stream = envelope.control_stream
      sequence = envelope.control_sequence
      unless node_uid && stream && sequence
        raise RemoteDeliveryError.new("system message is missing control sequence metadata")
      end

      observation = @control_deduplicator.deliver(node_uid, stream, sequence) do
        @on_message.call(envelope, self)
      end
      case observation
      when .new?
        send(WireEnvelope.control_ack(stream, sequence))
      when .duplicate?
        send(WireEnvelope.control_ack(stream, sequence))
      when .gap?
        raise RemoteDeliveryError.new("control sequence gap for #{stream}: #{sequence}")
      end
    end

    private def watch_key(target : Movie::ActorRefBase, watcher : Movie::ActorRefBase) : String
      "#{target.path.try(&.to_s) || target.id}\0#{watcher.path.try(&.to_s) || watcher.id}"
    end

    private def close_transport : Nil
      @transport.close rescue nil
      @socket.close rescue nil unless @transport.same?(@socket)
    end
  end
end
