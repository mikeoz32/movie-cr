require "json"
require "openssl/hmac"
require "socket"
require "uuid"

module Movie::Remote
  class AssociationProtocolError < Exception
  end

  private module AuthenticationProof
    extend self

    def matches?(left : String, right : String) : Bool
      return false unless left.bytesize == right.bytesize

      difference = 0_u8
      right_bytes = right.to_slice
      left.to_slice.each_with_index { |byte, index| difference |= byte ^ right_bytes[index] }
      difference == 0
    end
  end

  # Elapsed-time source for liveness decisions. Wall-clock timestamps remain
  # available in telemetry, but clock corrections cannot affect timeouts.
  private module AssociationClock
    extend self

    EPOCH         = Time.instant
    EPOCH_WALL_MS = Time.utc.to_unix_ms

    def now_nanoseconds : Int64
      (Time.instant - EPOCH).total_nanoseconds.to_i64
    end

    def wall_milliseconds(monotonic_nanoseconds : Int64) : Int64
      EPOCH_WALL_MS + monotonic_nanoseconds // 1_000_000
    end
  end

  PROTOCOL_VERSION        = 1
  CAPABILITY_CONTROL_ACKS = "control-acks-v1"
  CAPABILITY_HEARTBEATS   = "heartbeats-v1"
  DEFAULT_CAPABILITIES    = [CAPABILITY_CONTROL_ACKS, CAPABILITY_HEARTBEATS]

  alias ClientTransportFactory = Proc(String, Int32, IO)
  alias ServerTransportWrapper = Proc(TCPSocket, IO)

  # Tunable lifecycle and security policy for outbound and inbound
  # associations. Transport factories can wrap TCP sockets with TLS without
  # coupling the remoting protocol to certificate provisioning.
  class AssociationSettings
    getter reconnect_min_backoff : Time::Span
    getter reconnect_max_backoff : Time::Span
    getter reconnect_factor : Float64
    getter reconnect_jitter : Float64
    getter handshake_timeout : Time::Span
    getter heartbeat_interval : Time::Span
    getter heartbeat_timeout : Time::Span
    getter control_buffer_capacity : Int32
    getter shared_secret : String?
    getter client_transport_factory : ClientTransportFactory?
    getter server_transport_wrapper : ServerTransportWrapper?

    def initialize(
      @reconnect_min_backoff : Time::Span = 50.milliseconds,
      @reconnect_max_backoff : Time::Span = 2.seconds,
      @reconnect_factor : Float64 = 2.0,
      @reconnect_jitter : Float64 = 0.2,
      @handshake_timeout : Time::Span = 2.seconds,
      @heartbeat_interval : Time::Span = 1.second,
      @heartbeat_timeout : Time::Span = 5.seconds,
      @control_buffer_capacity : Int32 = 1024,
      @shared_secret : String? = nil,
      @client_transport_factory : ClientTransportFactory? = nil,
      @server_transport_wrapper : ServerTransportWrapper? = nil,
    )
      raise ArgumentError.new("reconnect minimum backoff must be positive") unless @reconnect_min_backoff > Time::Span.zero
      raise ArgumentError.new("reconnect maximum backoff must not be smaller than minimum") unless @reconnect_max_backoff >= @reconnect_min_backoff
      raise ArgumentError.new("reconnect factor must be at least 1") unless @reconnect_factor >= 1.0
      raise ArgumentError.new("reconnect jitter must be between 0 and 1") unless @reconnect_jitter.in?(0.0..1.0)
      raise ArgumentError.new("handshake timeout must be positive") unless @handshake_timeout > Time::Span.zero
      raise ArgumentError.new("heartbeat interval must be positive") unless @heartbeat_interval > Time::Span.zero
      raise ArgumentError.new("heartbeat timeout must exceed heartbeat interval") unless @heartbeat_timeout > @heartbeat_interval
      raise ArgumentError.new("control buffer capacity must be positive") unless @control_buffer_capacity > 0
      raise ArgumentError.new("shared secret must not be empty") if @shared_secret.try(&.empty?)
    end

    def connect(host : String, port : Int32) : IO
      if factory = @client_transport_factory
        factory.call(host, port)
      else
        socket = TCPSocket.new(host, port, connect_timeout: @handshake_timeout.total_seconds)
        socket.tcp_nodelay = true
        socket
      end
    end

    def wrap(socket : TCPSocket) : IO
      @server_transport_wrapper.try(&.call(socket)) || socket
    end

    # Applies symmetric jitter while preserving reconnect_max_backoff as a
    # hard upper bound. The sample argument makes boundary behavior testable.
    def reconnect_delay(delay : Time::Span, random : Float64 = Random.rand) : Time::Span
      raise ArgumentError.new("reconnect random sample must be between 0 and 1") unless random.in?(0.0..1.0)
      factor = 1.0 - @reconnect_jitter + random * @reconnect_jitter * 2.0
      jittered = Time::Span.new(nanoseconds: (delay.total_nanoseconds * factor).to_i64)
      {jittered, @reconnect_max_backoff}.min
    end
  end

  # Identity negotiated before a socket may carry actor traffic. The optional
  # proof authenticates all identity fields without putting the shared secret
  # on the wire.
  record AssociationHandshake,
    protocol_version : Int32,
    system : String,
    address : String,
    node_uid : String,
    association_id : String,
    capabilities : Array(String),
    nonce : String,
    auth_proof : String do
    include JSON::Serializable

    def self.create(
      system : String,
      address : String,
      node_uid : String,
      association_id : String,
      shared_secret : String? = nil,
      protocol_version : Int32 = PROTOCOL_VERSION,
      capabilities : Array(String) = DEFAULT_CAPABILITIES,
      nonce : String = UUID.random.to_s,
    ) : self
      unsigned = new(
        protocol_version,
        system,
        address,
        node_uid,
        association_id,
        capabilities,
        nonce,
        ""
      )
      proof = shared_secret ? unsigned.signature(shared_secret) : ""
      new(
        protocol_version,
        system,
        address,
        node_uid,
        association_id,
        capabilities,
        nonce,
        proof
      )
    end

    def authenticated?(shared_secret : String?) : Bool
      return @auth_proof.empty? unless shared_secret
      AuthenticationProof.matches?(@auth_proof, signature(shared_secret))
    end

    protected def signature(shared_secret : String) : String
      OpenSSL::HMAC.hexdigest(:sha256, shared_secret, canonical_identity)
    end

    private def canonical_identity : String
      String.build do |io|
        io << @protocol_version << '\n'
        io << @system << '\n'
        io << @address << '\n'
        io << @node_uid << '\n'
        io << @association_id << '\n'
        @capabilities.each { |capability| io << capability << '\0' }
        io << '\n' << @nonce
      end
    end
  end

  record HandshakeRejection, reason : String do
    include JSON::Serializable
  end

  # Completes the server-challenge round trip. A captured confirmation cannot
  # authenticate a second socket because each server response has a fresh
  # nonce.
  record AssociationConfirmation,
    association_id : String,
    node_uid : String,
    client_nonce : String,
    server_nonce : String,
    auth_proof : String do
    include JSON::Serializable

    def self.create(client : AssociationHandshake, server : AssociationHandshake, shared_secret : String?) : self
      unsigned = new(
        association_id: client.association_id,
        node_uid: client.node_uid,
        client_nonce: client.nonce,
        server_nonce: server.nonce,
        auth_proof: ""
      )
      proof = shared_secret ? unsigned.signature(shared_secret) : ""
      new(
        association_id: unsigned.association_id,
        node_uid: unsigned.node_uid,
        client_nonce: unsigned.client_nonce,
        server_nonce: unsigned.server_nonce,
        auth_proof: proof
      )
    end

    def authenticated?(shared_secret : String?) : Bool
      return @auth_proof.empty? unless shared_secret
      AuthenticationProof.matches?(@auth_proof, signature(shared_secret))
    end

    protected def signature(shared_secret : String) : String
      OpenSSL::HMAC.hexdigest(:sha256, shared_secret, canonical_challenge)
    end

    private def canonical_challenge : String
      String.build do |io|
        io << @association_id << '\n'
        io << @node_uid << '\n'
        io << @client_nonce << '\n'
        io << @server_nonce
      end
    end
  end

  record HandshakeReady, association_id : String do
    include JSON::Serializable
  end
end
