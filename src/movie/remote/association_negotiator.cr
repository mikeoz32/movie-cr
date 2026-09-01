require "./association"
require "./frame_codec"

module Movie::Remote
  # Performs the client side of the challenge-response association handshake.
  # This code runs once per socket generation and stays outside the message hot
  # path.
  module AssociationNegotiator
    extend self

    def connect(
      io : IO,
      handshake : AssociationHandshake,
      expected_system : String,
      shared_secret : String?,
    ) : AssociationHandshake
      decoder = FrameCodec::Decoder.new
      FrameCodec.encode(WireEnvelope.handshake(handshake), io)
      peer = validate_challenge(decoder.decode(io), handshake.association_id, expected_system, shared_secret)

      confirmation = AssociationConfirmation.create(handshake, peer, shared_secret)
      FrameCodec.encode(WireEnvelope.handshake_confirm(confirmation), io)
      validate_ready(decoder.decode(io), handshake.association_id)
      peer
    end

    private def validate_challenge(
      response : WireEnvelope?,
      association_id : String,
      expected_system : String,
      shared_secret : String?,
    ) : AssociationHandshake
      protocol_error("peer closed during association handshake") unless response
      reject_if_needed(response)
      protocol_error("expected handshake acknowledgement") unless response.kind.handshake_ack?

      handshake = AssociationHandshake.from_json(response.payload_data.json_source)
      protocol_error("peer protocol version mismatch") unless handshake.protocol_version == PROTOCOL_VERSION
      protocol_error("peer system mismatch") unless handshake.system == expected_system
      protocol_error("association acknowledgement mismatch") unless handshake.association_id == association_id
      protocol_error("peer authentication failed") unless handshake.authenticated?(shared_secret)
      missing = DEFAULT_CAPABILITIES.reject { |capability| handshake.capabilities.includes?(capability) }
      protocol_error("peer capabilities are missing: #{missing.join(", ")}") unless missing.empty?
      handshake
    rescue ex : AssociationProtocolError
      raise ex
    rescue ex
      protocol_error("invalid handshake acknowledgement: #{ex.message}")
    end

    private def validate_ready(response : WireEnvelope?, association_id : String) : Nil
      protocol_error("peer closed before association confirmation") unless response
      reject_if_needed(response)
      protocol_error("expected handshake ready") unless response.kind.handshake_ready?
      ready = HandshakeReady.from_json(response.payload_data.json_source)
      protocol_error("handshake ready association mismatch") unless ready.association_id == association_id
    rescue ex : AssociationProtocolError
      raise ex
    rescue ex
      protocol_error("invalid handshake ready frame: #{ex.message}")
    end

    private def reject_if_needed(response : WireEnvelope) : Nil
      return unless response.kind.handshake_reject?

      rejection = HandshakeRejection.from_json(response.payload_data.json_source)
      protocol_error("association rejected: #{rejection.reason}")
    end

    private def protocol_error(message : String) : NoReturn
      raise AssociationProtocolError.new(message)
    end
  end
end
