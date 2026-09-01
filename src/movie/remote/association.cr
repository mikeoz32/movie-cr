require "json"
require "openssl/hmac"
require "socket"
require "uuid"

module Movie::Remote
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
      secure_equals?(@auth_proof, signature(shared_secret))
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

    private def secure_equals?(left : String, right : String) : Bool
      return false unless left.bytesize == right.bytesize

      difference = 0_u8
      left.to_slice.each_with_index do |byte, index|
        difference |= byte ^ right.to_slice[index]
      end
      difference == 0
    end
  end

  record HandshakeRejection, reason : String do
    include JSON::Serializable
  end

  enum ControlObservation
    New
    Duplicate
    Gap
  end

  # Tracks the last contiguous sequence for each stable node/connection stream.
  # The server owns one instance so reconnecting sockets share deduplication.
  class ControlDeduplicator
    @sequences = {} of String => Int64
    @mutex = Mutex.new

    def initialize(@max_streams : Int32 = 8192)
      raise ArgumentError.new("maximum control streams must be positive") unless @max_streams > 0
    end

    def observe(node_uid : String, stream : String, sequence : Int64) : ControlObservation
      @mutex.synchronize do
        key = "#{node_uid}\0#{stream}"
        observation = classify(key, sequence)
        remember(key, sequence) if observation.new?
        observation
      end
    end

    # Advances the deduplication cursor only after the delivery callback
    # succeeds, so a transient routing failure cannot turn a retry into a
    # falsely acknowledged duplicate.
    def deliver(node_uid : String, stream : String, sequence : Int64, &) : ControlObservation
      @mutex.synchronize do
        key = "#{node_uid}\0#{stream}"
        observation = classify(key, sequence)
        if observation.new?
          yield
          remember(key, sequence)
        end
        observation
      end
    end

    def forget(node_uid : String) : Nil
      prefix = "#{node_uid}\0"
      @mutex.synchronize { @sequences.reject! { |key, _| key.starts_with?(prefix) } }
    end

    private def classify(key : String, sequence : Int64) : ControlObservation
      previous = @sequences[key]? || 0_i64
      return ControlObservation::Duplicate if sequence <= previous
      return ControlObservation::Gap if sequence != previous + 1

      ControlObservation::New
    end

    private def remember(key : String, sequence : Int64) : Nil
      unless @sequences.has_key?(key)
        @sequences.shift if @sequences.size >= @max_streams
      end
      @sequences[key] = sequence
    end
  end
end
