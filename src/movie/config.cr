require "yaml"
require "json"

module Movie
  # ConfigValue represents any value that can be stored in configuration.
  # It's a recursive type that supports nested structures.
  alias ConfigValue = String | Int64 | Float64 | Bool | Array(ConfigValue) | Hash(String, ConfigValue) | Nil

  # ConfigError is raised when config access fails.
  class ConfigError < Exception
  end

  # MissingConfigError is raised when a required config path doesn't exist.
  class MissingConfigError < ConfigError
    getter path : String

    def initialize(@path : String)
      super("Configuration path not found: #{@path}")
    end
  end

  # WrongTypeConfigError is raised when config value has unexpected type.
  class WrongTypeConfigError < ConfigError
    getter path : String
    getter expected : String
    getter actual : String

    def initialize(@path : String, @expected : String, @actual : String)
      super("Wrong type at '#{@path}': expected #{@expected}, got #{@actual}")
    end
  end

  # Config provides path-based access to configuration values.
  #
  # Example:
  #   config = Movie::Config.builder
  #     .set("name", "my-system")
  #     .set("remoting.host", "127.0.0.1")
  #     .set("remoting.port", 9000)
  #     .build
  #
  #   config.get_string("name")           # => "my-system"
  #   config.get_int("remoting.port")     # => 9000
  #   config.get_config("remoting")       # => Config subsection
  #
  class Config
    @root : Hash(String, ConfigValue)

    def initialize(@root : Hash(String, ConfigValue) = {} of String => ConfigValue)
    end

    # Creates a new ConfigBuilder for programmatic config construction.
    def self.builder : ConfigBuilder
      ConfigBuilder.new
    end

    # Creates an empty config.
    def self.empty : Config
      new({} of String => ConfigValue)
    end

    # --- Loaders ---

    # Parses a YAML string into a Config.
    #
    # Example:
    #   config = Movie::Config.from_yaml(<<-YAML)
    #     name: my-system
    #     remoting:
    #       host: 127.0.0.1
    #       port: 9000
    #   YAML
    #
    def self.from_yaml(yaml_string : String) : Config
      yaml_any = YAML.parse(yaml_string)
      root = convert_yaml_value(yaml_any)
      case root
      when Hash(String, ConfigValue)
        new(root)
      else
        raise ConfigError.new("YAML root must be a mapping/object")
      end
    end

    # Loads a Config from a YAML file.
    def self.load_yaml(path : String) : Config
      from_yaml(File.read(path))
    end

    # Loads a Config from a YAML file with fallback for missing values.
    # If file exists, loads it and uses default as fallback.
    # If file doesn't exist, returns default.
    def self.load_yaml(path : String, default : Config) : Config
      if File.exists?(path)
        load_yaml(path).with_fallback(default)
      else
        default
      end
    end

    # Parses a JSON string into a Config.
    #
    # Example:
    #   config = Movie::Config.from_json(%({
    #     "name": "my-system",
    #     "remoting": {
    #       "host": "127.0.0.1",
    #       "port": 9000
    #     }
    #   }))
    #
    def self.from_json(json_string : String) : Config
      json_any = JSON.parse(json_string)
      root = convert_json_value(json_any)
      case root
      when Hash(String, ConfigValue)
        new(root)
      else
        raise ConfigError.new("JSON root must be an object")
      end
    end

    # Loads a Config from a JSON file.
    def self.load_json(path : String) : Config
      from_json(File.read(path))
    end

    # Loads a Config from a JSON file with fallback for missing values.
    # If file exists, loads it and uses default as fallback.
    # If file doesn't exist, returns default.
    def self.load_json(path : String, default : Config) : Config
      if File.exists?(path)
        load_json(path).with_fallback(default)
      else
        default
      end
    end

    # Loads config from file, detecting format by extension (.yml, .yaml, .json).
    def self.load(path : String) : Config
      case File.extname(path).downcase
      when ".yml", ".yaml"
        load_yaml(path)
      when ".json"
        load_json(path)
      else
        raise ConfigError.new("Unknown config file format: #{path}")
      end
    end

    # Loads config from file with fallback for missing values.
    # If file exists, loads it and uses default as fallback.
    # If file doesn't exist, returns default.
    def self.load(path : String, default : Config) : Config
      if File.exists?(path)
        load(path).with_fallback(default)
      else
        default
      end
    end

    # --- Path existence ---

    # Returns true if the given path exists in the config.
    def has_path?(path : String) : Bool
      !get_value(path).nil?
    end

    # --- String accessors ---

    # Returns the string value at the given path.
    # Raises MissingConfigError if path doesn't exist.
    # Raises WrongTypeConfigError if value is not a string.
    def get_string(path : String) : String
      value = get_value!(path)
      case value
      when String
        value
      else
        raise WrongTypeConfigError.new(path, "String", value.class.name)
      end
    end

    # Returns the string value at the given path, or default if not found.
    def get_string(path : String, default : String) : String
      value = get_value(path)
      case value
      when String
        value
      when Nil
        default
      else
        raise WrongTypeConfigError.new(path, "String", value.class.name)
      end
    end

    # --- Integer accessors ---

    # Returns the integer value at the given path.
    def get_int(path : String) : Int32
      get_long(path).to_i32
    end

    # Returns the integer value at the given path, or default if not found.
    def get_int(path : String, default : Int32) : Int32
      if has_path?(path)
        get_int(path)
      else
        default
      end
    end

    # Returns the Int64 value at the given path.
    def get_long(path : String) : Int64
      value = get_value!(path)
      case value
      when Int64
        value
      when Float64
        value.to_i64
      when String
        value.to_i64
      else
        raise WrongTypeConfigError.new(path, "Int64", value.class.name)
      end
    end

    # Returns the Int64 value at the given path, or default if not found.
    def get_long(path : String, default : Int64) : Int64
      if has_path?(path)
        get_long(path)
      else
        default
      end
    end

    # --- Float accessors ---

    # Returns the float value at the given path.
    def get_float(path : String) : Float64
      value = get_value!(path)
      case value
      when Float64
        value
      when Int64
        value.to_f64
      when String
        value.to_f64
      else
        raise WrongTypeConfigError.new(path, "Float64", value.class.name)
      end
    end

    # Returns the float value at the given path, or default if not found.
    def get_float(path : String, default : Float64) : Float64
      if has_path?(path)
        get_float(path)
      else
        default
      end
    end

    # --- Boolean accessors ---

    # Returns the boolean value at the given path.
    def get_bool(path : String) : Bool
      value = get_value!(path)
      case value
      when Bool
        value
      when String
        case value.downcase
        when "true", "yes", "on", "1"
          true
        when "false", "no", "off", "0"
          false
        else
          raise WrongTypeConfigError.new(path, "Bool", "String(#{value})")
        end
      else
        raise WrongTypeConfigError.new(path, "Bool", value.class.name)
      end
    end

    # Returns the boolean value at the given path, or default if not found.
    def get_bool(path : String, default : Bool) : Bool
      if has_path?(path)
        get_bool(path)
      else
        default
      end
    end

    # --- Duration accessors ---

    # Returns the duration value at the given path.
    # Supports formats: "100ms", "5s", "2m", "1h", "1d"
    # Also accepts numeric values as milliseconds.
    def get_duration(path : String) : Time::Span
      value = get_value!(path)
      parse_duration(path, value)
    end

    # Returns the duration value at the given path, or default if not found.
    def get_duration(path : String, default : Time::Span) : Time::Span
      if has_path?(path)
        get_duration(path)
      else
        default
      end
    end

    # --- Array accessors ---

    # Returns the array value at the given path.
    def get_array(path : String) : Array(ConfigValue)
      value = get_value!(path)
      case value
      when Array(ConfigValue)
        value
      else
        raise WrongTypeConfigError.new(path, "Array", value.class.name)
      end
    end

    # Returns an array of strings at the given path.
    def get_string_array(path : String) : Array(String)
      get_array(path).map do |v|
        case v
        when String
          v
        else
          raise WrongTypeConfigError.new(path, "Array(String)", "Array containing #{v.class.name}")
        end
      end
    end

    # Returns an array of strings at the given path, or default if not found.
    def get_string_array(path : String, default : Array(String)) : Array(String)
      if has_path?(path)
        get_string_array(path)
      else
        default
      end
    end

    # Returns an array of integers at the given path.
    def get_int_array(path : String) : Array(Int32)
      get_array(path).map do |v|
        case v
        when Int64
          v.to_i32
        else
          raise WrongTypeConfigError.new(path, "Array(Int32)", "Array containing #{v.class.name}")
        end
      end
    end

    # --- Subsection accessor ---

    # Returns a Config subsection at the given path.
    # Raises MissingConfigError if path doesn't exist.
    # Raises WrongTypeConfigError if value is not a hash.
    def get_config(path : String) : Config
      value = get_value!(path)
      case value
      when Hash(String, ConfigValue)
        Config.new(value)
      else
        raise WrongTypeConfigError.new(path, "Config (Hash)", value.class.name)
      end
    end

    # Returns a Config subsection at the given path, or empty Config if not found.
    def get_config(path : String, default : Config) : Config
      if has_path?(path)
        get_config(path)
      else
        default
      end
    end

    # --- Raw value accessor ---

    # Returns the raw value at the given path, or nil if not found.
    def get_value(path : String) : ConfigValue
      parts = path.split('.')
      current : ConfigValue = @root

      parts.each do |part|
        case current
        when Hash(String, ConfigValue)
          if current.has_key?(part)
            current = current[part]
          else
            return nil
          end
        else
          return nil
        end
      end

      current
    end

    # Returns the raw value at the given path.
    # Raises MissingConfigError if path doesn't exist.
    def get_value!(path : String) : ConfigValue
      value = get_value(path)
      raise MissingConfigError.new(path) if value.nil?
      value
    end

    # Subscript access - returns raw ConfigValue
    def [](path : String) : ConfigValue
      get_value!(path)
    end

    # Subscript access with nil for missing paths
    def []?(path : String) : ConfigValue
      get_value(path)
    end

    # --- Merging ---

    # Returns a new Config with values from other merged in.
    # Values from other override values in self.
    def with_fallback(other : Config) : Config
      Config.new(deep_merge(other.@root, @root))
    end

    # Returns a new Config with values from other overriding self.
    def with_override(other : Config) : Config
      Config.new(deep_merge(@root, other.@root))
    end

    # Returns a new Config with environment variable overrides applied.
    # Environment variables are mapped from MOVIE_* pattern:
    #   MOVIE_NAME          -> name
    #   MOVIE_REMOTING_PORT -> remoting.port
    #   MOVIE_CLUSTER_SEED_NODES -> cluster.seed_nodes
    #
    # Values are auto-converted:
    #   - "true"/"false" -> Bool
    #   - Numeric strings -> Int64 or Float64
    #   - Comma-separated -> Array(String)
    #   - Other -> String
    #
    # Example:
    #   # With MOVIE_REMOTING_PORT=9000 MOVIE_DEBUG=true
    #   config = base_config.with_env_overrides
    #   config.get_int("remoting.port")  # => 9000
    #   config.get_bool("debug")         # => true
    #
    def with_env_overrides(prefix : String = "MOVIE") : Config
      overrides = Config.builder

      ENV.each do |key, value|
        next unless key.starts_with?("#{prefix}_")

        # Convert MOVIE_REMOTING_PORT to remoting.port
        path = key[(prefix.size + 1)..]
          .downcase
          .gsub("_", ".")

        # Auto-convert value
        converted = convert_env_value(value)
        set_builder_value(overrides, path, converted)
      end

      with_override(overrides.build)
    end

    # --- Utilities ---

    # Returns all top-level keys.
    def keys : Array(String)
      @root.keys
    end

    # Returns true if config is empty.
    def empty? : Bool
      @root.empty?
    end

    # Returns the root hash (for serialization).
    def to_h : Hash(String, ConfigValue)
      @root
    end

    private def parse_duration(path : String, value : ConfigValue) : Time::Span
      case value
      when Int64
        # Treat as milliseconds
        value.milliseconds
      when Float64
        value.milliseconds
      when String
        parse_duration_string(path, value)
      else
        raise WrongTypeConfigError.new(path, "Duration", value.class.name)
      end
    end

    private def parse_duration_string(path : String, str : String) : Time::Span
      # Match patterns like "100ms", "5s", "2m", "1h", "1d"
      if match = str.match(/^(\d+(?:\.\d+)?)\s*(ns|us|ms|s|m|h|d)$/i)
        amount = match[1].to_f64
        unit = match[2].downcase

        case unit
        when "ns"
          amount.nanoseconds
        when "us"
          amount.microseconds
        when "ms"
          amount.milliseconds
        when "s"
          amount.seconds
        when "m"
          amount.minutes
        when "h"
          amount.hours
        when "d"
          amount.days
        else
          raise WrongTypeConfigError.new(path, "Duration", "String(#{str})")
        end
      elsif match = str.match(/^(\d+)$/)
        # Plain number - treat as milliseconds
        match[1].to_i64.milliseconds
      else
        raise WrongTypeConfigError.new(path, "Duration", "String(#{str})")
      end
    end

    private def deep_merge(base : Hash(String, ConfigValue), override : Hash(String, ConfigValue)) : Hash(String, ConfigValue)
      result = base.dup
      override.each do |key, value|
        if result.has_key?(key)
          existing = result[key]
          case {existing, value}
          when {Hash(String, ConfigValue), Hash(String, ConfigValue)}
            result[key] = deep_merge(existing, value)
          else
            result[key] = value
          end
        else
          result[key] = value
        end
      end
      result
    end

    # Converts environment variable string to appropriate ConfigValue type.
    private def convert_env_value(value : String) : ConfigValue
      # Boolean
      case value.downcase
      when "true", "yes", "on", "1"
        return true
      when "false", "no", "off", "0"
        return false
      end

      # Integer
      if value.matches?(/^-?\d+$/)
        return value.to_i64
      end

      # Float
      if value.matches?(/^-?\d+\.\d+$/)
        return value.to_f64
      end

      # Array (comma-separated)
      if value.includes?(",")
        return value.split(",").map(&.strip.as(ConfigValue))
      end

      # String (default)
      value
    end

    # Sets a value on a ConfigBuilder, handling different types.
    private def set_builder_value(builder : ConfigBuilder, path : String, value : ConfigValue)
      case value
      when String
        builder.set(path, value)
      when Int64
        builder.set(path, value)
      when Float64
        builder.set(path, value)
      when Bool
        builder.set(path, value)
      when Array(ConfigValue)
        # Convert to string array for builder
        str_array = value.compact_map { |v| v.as?(String) }
        builder.set(path, str_array) unless str_array.empty?
      end
    end

    # --- YAML/JSON conversion helpers ---

    protected def self.convert_yaml_value(yaml : YAML::Any) : ConfigValue
      case yaml.raw
      when Nil
        nil
      when Bool
        yaml.as_bool
      when Int64
        yaml.as_i64
      when Float64
        yaml.as_f
      when String
        yaml.as_s
      when Array(YAML::Any)
        yaml.as_a.map { |v| convert_yaml_value(v) }.as(ConfigValue)
      when Hash(YAML::Any, YAML::Any)
        result = {} of String => ConfigValue
        yaml.as_h.each do |k, v|
          key = k.as_s
          result[key] = convert_yaml_value(v)
        end
        result.as(ConfigValue)
      else
        yaml.raw.to_s
      end
    end

    protected def self.convert_json_value(json : JSON::Any) : ConfigValue
      case json.raw
      when Nil
        nil
      when Bool
        json.as_bool
      when Int64
        json.as_i64
      when Float64
        json.as_f
      when String
        json.as_s
      when Array(JSON::Any)
        json.as_a.map { |v| convert_json_value(v) }.as(ConfigValue)
      when Hash(String, JSON::Any)
        result = {} of String => ConfigValue
        json.as_h.each do |k, v|
          result[k] = convert_json_value(v)
        end
        result.as(ConfigValue)
      else
        json.raw.to_s
      end
    end
  end

  # ConfigBuilder provides a fluent API for building Config instances.
  #
  # Example:
  #   config = Movie::Config.builder
  #     .set("name", "my-system")
  #     .set("remoting.host", "127.0.0.1")
  #     .set("remoting.port", 9000)
  #     .set("remoting.enabled", true)
  #     .build
  #
  class ConfigBuilder
    @root : Hash(String, ConfigValue)

    def initialize
      @root = {} of String => ConfigValue
    end

    # Sets a value at the given path.
    # Paths can be nested using dots: "remoting.port"
    def set(path : String, value : String) : self
      set_value(path, value.as(ConfigValue))
      self
    end

    def set(path : String, value : Int32) : self
      set_value(path, value.to_i64.as(ConfigValue))
      self
    end

    def set(path : String, value : Int64) : self
      set_value(path, value.as(ConfigValue))
      self
    end

    def set(path : String, value : Float64) : self
      set_value(path, value.as(ConfigValue))
      self
    end

    def set(path : String, value : Bool) : self
      set_value(path, value.as(ConfigValue))
      self
    end

    def set(path : String, value : Array(String)) : self
      set_value(path, value.map(&.as(ConfigValue)).as(ConfigValue))
      self
    end

    def set(path : String, value : Array(Int32)) : self
      set_value(path, value.map(&.to_i64.as(ConfigValue)).as(ConfigValue))
      self
    end

    # Sets a duration value (stored as string for serialization).
    def set_duration(path : String, value : Time::Span) : self
      str = format_duration(value)
      set_value(path, str.as(ConfigValue))
      self
    end

    # Merges another config's values into this builder.
    def merge(config : Config) : self
      deep_merge_into(@root, config.to_h)
      self
    end

    # Builds the final Config instance.
    def build : Config
      Config.new(@root.dup)
    end

    private def set_value(path : String, value : ConfigValue)
      parts = path.split('.')
      current = @root

      # Navigate/create nested hashes for all but the last part
      parts[0...-1].each do |part|
        if current.has_key?(part)
          existing = current[part]
          case existing
          when Hash(String, ConfigValue)
            current = existing
          else
            # Overwrite non-hash value with new hash
            new_hash = {} of String => ConfigValue
            current[part] = new_hash
            current = new_hash
          end
        else
          new_hash = {} of String => ConfigValue
          current[part] = new_hash
          current = new_hash
        end
      end

      # Set the value at the leaf
      current[parts.last] = value
    end

    private def format_duration(span : Time::Span) : String
      total_ns = span.total_nanoseconds

      if total_ns < 1000
        "#{total_ns.to_i64}ns"
      elsif total_ns < 1_000_000
        "#{(total_ns / 1000).to_i64}us"
      elsif total_ns < 1_000_000_000
        "#{(total_ns / 1_000_000).to_i64}ms"
      elsif total_ns < 60_000_000_000
        "#{(total_ns / 1_000_000_000).to_i64}s"
      elsif total_ns < 3_600_000_000_000
        "#{(total_ns / 60_000_000_000).to_i64}m"
      elsif total_ns < 86_400_000_000_000
        "#{(total_ns / 3_600_000_000_000).to_i64}h"
      else
        "#{(total_ns / 86_400_000_000_000).to_i64}d"
      end
    end

    private def deep_merge_into(target : Hash(String, ConfigValue), source : Hash(String, ConfigValue))
      source.each do |key, value|
        if target.has_key?(key)
          existing = target[key]
          case {existing, value}
          when {Hash(String, ConfigValue), Hash(String, ConfigValue)}
            deep_merge_into(existing, value)
          else
            target[key] = value
          end
        else
          target[key] = value
        end
      end
    end
  end

  # Default configuration for ActorSystem.
  # These values can be overridden via YAML/JSON config files or environment variables.
  #
  # Configuration paths:
  #   name                           - Actor system name (default: auto-generated)
  #   supervision.strategy           - Default supervision strategy: restart|stop|resume|escalate
  #   supervision.scope              - Supervision scope: one-for-one|all-for-one
  #   supervision.max-restarts       - Max restarts before giving up
  #   supervision.within             - Time window for max-restarts
  #   supervision.backoff.min        - Minimum backoff delay
  #   supervision.backoff.max        - Maximum backoff delay
  #   supervision.backoff.factor     - Backoff multiplier
  #   supervision.backoff.jitter     - Random jitter factor (0.0-1.0)
  #   remoting.enabled               - Enable remoting on startup
  #   remoting.host                  - Bind host for remoting
  #   remoting.port                  - Bind port for remoting
  #   remoting.stripe-count          - Connection pool stripe count
  #
  module ActorSystemConfig
    # Returns the default configuration for an ActorSystem.
    def self.default : Config
      Config.builder
        # System
        .set("name", "")  # Empty means auto-generate

        # Supervision defaults
        .set("supervision.strategy", "restart")
        .set("supervision.scope", "one-for-one")
        .set("supervision.max-restarts", 3)
        .set_duration("supervision.within", 1.second)
        .set_duration("supervision.backoff.min", 10.milliseconds)
        .set_duration("supervision.backoff.max", 1.second)
        .set("supervision.backoff.factor", 2.0)
        .set("supervision.backoff.jitter", 0.0)

        # Remoting defaults
        .set("remoting.enabled", false)
        .set("remoting.host", "127.0.0.1")
        .set("remoting.port", 2552)
        .set("remoting.stripe-count", 8)

        .build
    end

    # Parses a SupervisionStrategy from string.
    def self.parse_strategy(str : String) : SupervisionStrategy
      case str.downcase
      when "restart"  then SupervisionStrategy::RESTART
      when "stop"     then SupervisionStrategy::STOP
      when "resume"   then SupervisionStrategy::RESUME
      when "escalate" then SupervisionStrategy::ESCALATE
      else
        raise ConfigError.new("Unknown supervision strategy: #{str}")
      end
    end

    # Parses a SupervisionScope from string.
    def self.parse_scope(str : String) : SupervisionScope
      case str.downcase.gsub("-", "_")
      when "one_for_one" then SupervisionScope::ONE_FOR_ONE
      when "all_for_one" then SupervisionScope::ALL_FOR_ONE
      else
        raise ConfigError.new("Unknown supervision scope: #{str}")
      end
    end

    # Parses a RestartStrategy from string.
    def self.parse_restart_strategy(str : String) : RestartStrategy
      case str.downcase
      when "restart" then RestartStrategy::RESTART
      when "stop"    then RestartStrategy::STOP
      else
        raise ConfigError.new("Unknown restart strategy: #{str}")
      end
    end

    # Creates a SupervisionConfig from a Config.
    def self.supervision_config(config : Config) : SupervisionConfig
      SupervisionConfig.new(
        strategy: parse_strategy(config.get_string("supervision.strategy", "restart")),
        scope: parse_scope(config.get_string("supervision.scope", "one-for-one")),
        max_restarts: config.get_int("supervision.max-restarts", 3),
        within: config.get_duration("supervision.within", 1.second),
        backoff_min: config.get_duration("supervision.backoff.min", 10.milliseconds),
        backoff_max: config.get_duration("supervision.backoff.max", 1.second),
        backoff_factor: config.get_float("supervision.backoff.factor", 2.0),
        jitter: config.get_float("supervision.backoff.jitter", 0.0)
      )
    end

    # Creates a RestartStrategy from a Config.
    def self.restart_strategy(config : Config) : RestartStrategy
      parse_restart_strategy(config.get_string("supervision.strategy", "restart"))
    end
  end
end
