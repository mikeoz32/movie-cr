require "./connection"
require "../path"

module Movie::Remote
  # StripedConnectionPool maintains multiple parallel connections to a remote system.
  # Actors are deterministically mapped to connections to preserve message ordering
  # while enabling parallel sending across different actors.
  class StripedConnectionPool
    Log = ::Log.for(self)

    # Default number of parallel connections (should match CPU cores)
    DEFAULT_STRIPE_COUNT = 8

    getter address : Address
    getter stripe_count : Int32
    getter? connected : Bool = false

    @stripes : Array(Connection)
    @round_robin : Atomic(Int32) = Atomic(Int32).new(0)

    def initialize(
      @address : Address,
      @path_registry : Movie::PathRegistry,
      @system : Movie::AbstractActorSystem,
      @stripe_count : Int32 = DEFAULT_STRIPE_COUNT,
      @on_message : Proc(WireEnvelope, Nil)? = nil
    )
      @stripes = Array(Connection).new(@stripe_count) do
        Connection.new(
          address: @address,
          path_registry: @path_registry,
          system: @system,
          on_message: @on_message
        )
      end
    end

    # Connects all stripes in parallel.
    # Returns true if all connections succeeded.
    def connect : Bool
      return true if @connected

      results = Array(Bool).new(@stripe_count, false)
      channels = @stripes.map_with_index do |conn, i|
        ch = Channel(Bool).new(1)
        spawn do
          ch.send(conn.connect)
        end
        {i, ch}
      end

      channels.each do |(i, ch)|
        results[i] = ch.receive
      end

      success_count = results.count(true)
      @connected = success_count == @stripe_count

      if @connected
        Log.info { "Connected #{@stripe_count} stripes to #{@address}" }
      else
        Log.warn { "Only #{success_count}/#{@stripe_count} stripes connected to #{@address}" }
      end

      @connected
    end

    # Returns the connection for a specific actor path.
    # Uses consistent hashing to ensure messages to the same actor
    # always go through the same connection (preserving order).
    def connection_for(actor_path : ActorPath) : Connection
      stripe_index = actor_path.to_s.hash.abs % @stripe_count
      @stripes[stripe_index]
    end

    # Returns the connection for a path string.
    def connection_for(path_str : String) : Connection
      stripe_index = path_str.hash.abs % @stripe_count
      @stripes[stripe_index]
    end

    # Returns the next connection in round-robin order.
    # Use for messages where ordering doesn't matter.
    def next_connection : Connection
      index = @round_robin.add(1).abs % @stripe_count
      @stripes[index]
    end

    # Returns a specific stripe by index.
    def stripe(index : Int32) : Connection
      @stripes[index % @stripe_count]
    end

    # Sends an envelope through the appropriate stripe based on target path.
    def send(envelope : WireEnvelope) : Bool
      conn = connection_for(envelope.target_path)
      conn.send(envelope)
    end

    # Sends an envelope through a specific stripe (for load balancing).
    def send_round_robin(envelope : WireEnvelope) : Bool
      conn = next_connection
      conn.send(envelope)
    end

    # Closes all connections.
    def close
      @connected = false
      @stripes.each(&.close)
      Log.info { "Closed #{@stripe_count} stripes to #{@address}" }
    end

    # Returns connection statistics.
    def stats : PoolStats
      connected_count = @stripes.count(&.connected?)
      PoolStats.new(
        stripe_count: @stripe_count,
        connected_count: connected_count,
        address: @address.to_s
      )
    end
  end

  # Statistics for a connection pool.
  struct PoolStats
    getter stripe_count : Int32
    getter connected_count : Int32
    getter address : String

    def initialize(@stripe_count, @connected_count, @address)
    end

    def fully_connected? : Bool
      @connected_count == @stripe_count
    end

    def to_s : String
      "Pool(#{@address}): #{@connected_count}/#{@stripe_count} connected"
    end
  end
end
