require "json"

module Movie
  # Address represents the location of an actor system.
  # For local actors, host and port are nil.
  # For remote actors, they specify the network location.
  struct Address
    include JSON::Serializable

    getter protocol : String
    getter system : String
    getter host : String?
    getter port : Int32?

    def initialize(@protocol : String, @system : String, @host : String? = nil, @port : Int32? = nil)
    end

    # Creates a local address for the given system name.
    def self.local(system_name : String) : Address
      new("movie", system_name, nil, nil)
    end

    # Creates a remote address.
    def self.remote(system_name : String, host : String, port : Int32) : Address
      new("movie.tcp", system_name, host, port)
    end

    # Returns true if this is a local address (no host/port).
    def local? : Bool
      @host.nil? && @port.nil?
    end

    # Returns true if this is a remote address.
    def remote? : Bool
      !local?
    end

    # Parses an address URI string.
    # Format: protocol://system@host:port or protocol://system (for local)
    # Examples:
    #   "movie://my-system" -> local address
    #   "movie.tcp://my-system@127.0.0.1:2552" -> remote address
    def self.parse(uri : String) : Address
      # Match: protocol://system or protocol://system@host:port
      if match = uri.match(/^([a-z.]+):\/\/([^@\/]+)(?:@([^:]+):(\d+))?$/)
        protocol = match[1]
        system = match[2]
        host = match[3]?
        port = match[4]?.try(&.to_i32)

        if host && port
          new(protocol, system, host, port)
        else
          new(protocol, system, nil, nil)
        end
      else
        raise ArgumentError.new("Invalid address URI: #{uri}")
      end
    end

    # Returns the canonical string representation.
    def to_s : String
      if remote?
        "#{@protocol}://#{@system}@#{@host}:#{@port}"
      else
        "#{@protocol}://#{@system}"
      end
    end

    def to_s(io : IO) : Nil
      io << to_s
    end

    def ==(other : Address) : Bool
      @protocol == other.protocol &&
        @system == other.system &&
        @host == other.host &&
        @port == other.port
    end

    def hash(hasher)
      hasher = @protocol.hash(hasher)
      hasher = @system.hash(hasher)
      hasher = @host.hash(hasher)
      hasher = @port.hash(hasher)
      hasher
    end
  end

  # ActorPath represents the full path to an actor, including its address
  # and the hierarchical path within the actor system.
  struct ActorPath
    include JSON::Serializable

    getter address : Address
    getter elements : Array(String)

    def initialize(@address : Address, @elements : Array(String))
    end

    # Creates a root path for the given address.
    def self.root(address : Address) : ActorPath
      new(address, [] of String)
    end

    # Returns the name of this actor (last path element).
    def name : String
      @elements.last? || ""
    end

    # Returns the parent path, or nil if this is the root.
    def parent : ActorPath?
      return nil if @elements.empty?
      ActorPath.new(@address, @elements[0...-1])
    end

    # Returns true if this is a root path.
    def root? : Bool
      @elements.empty?
    end

    # Creates a child path with the given name.
    def /(child : String) : ActorPath
      ActorPath.new(@address, @elements + [child])
    end

    # Parses an actor path string.
    # Format: address/path/to/actor
    # Examples:
    #   "movie://my-system/user/actor1"
    #   "movie.tcp://my-system@127.0.0.1:2552/user/actor1"
    def self.parse(path : String) : ActorPath
      # Find the first slash after :// that's part of the actor path
      protocol_end = path.index("://")
      raise ArgumentError.new("Invalid actor path: #{path}") unless protocol_end

      # Find the path separator after the address
      address_end = path.index('/', protocol_end + 3)

      if address_end
        address_str = path[0...address_end]
        path_str = path[(address_end + 1)..]
        elements = path_str.split('/').reject(&.empty?)
      else
        address_str = path
        elements = [] of String
      end

      address = Address.parse(address_str)
      new(address, elements)
    end

    # Returns the canonical string representation.
    def to_s : String
      if @elements.empty?
        @address.to_s
      else
        "#{@address}/#{@elements.join("/")}"
      end
    end

    def to_s(io : IO) : Nil
      io << to_s
    end

    def ==(other : ActorPath) : Bool
      @address == other.address && @elements == other.elements
    end

    def hash(hasher)
      hasher = @address.hash(hasher)
      hasher = @elements.hash(hasher)
      hasher
    end
  end
end
