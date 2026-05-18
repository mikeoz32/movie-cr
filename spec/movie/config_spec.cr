require "../spec_helper"
require "../../src/movie"

describe Movie::Config do
  describe "ConfigBuilder" do
    it "builds config with simple values" do
      config = Movie::Config.builder
        .set("name", "test-system")
        .set("port", 9000)
        .set("enabled", true)
        .set("ratio", 0.5)
        .build

      config.get_string("name").should eq("test-system")
      config.get_int("port").should eq(9000)
      config.get_bool("enabled").should eq(true)
      config.get_float("ratio").should eq(0.5)
    end

    it "builds config with nested paths" do
      config = Movie::Config.builder
        .set("remoting.host", "127.0.0.1")
        .set("remoting.port", 9000)
        .set("remoting.pool.size", 8)
        .build

      config.get_string("remoting.host").should eq("127.0.0.1")
      config.get_int("remoting.port").should eq(9000)
      config.get_int("remoting.pool.size").should eq(8)
    end

    it "builds config with arrays" do
      config = Movie::Config.builder
        .set("hosts", ["host1", "host2", "host3"])
        .set("ports", [8080, 8081, 8082])
        .build

      config.get_string_array("hosts").should eq(["host1", "host2", "host3"])
      config.get_int_array("ports").should eq([8080, 8081, 8082])
    end

    it "builds config with duration values" do
      config = Movie::Config.builder
        .set_duration("timeout", 5.seconds)
        .set_duration("interval", 100.milliseconds)
        .build

      config.get_duration("timeout").should eq(5.seconds)
      config.get_duration("interval").should eq(100.milliseconds)
    end
  end

  describe "#has_path?" do
    it "returns true for existing paths" do
      config = Movie::Config.builder
        .set("name", "test")
        .set("nested.value", 42)
        .build

      config.has_path?("name").should be_true
      config.has_path?("nested.value").should be_true
      config.has_path?("nested").should be_true
    end

    it "returns false for missing paths" do
      config = Movie::Config.builder
        .set("name", "test")
        .build

      config.has_path?("missing").should be_false
      config.has_path?("name.nested").should be_false
    end
  end

  describe "#get_string" do
    it "returns string value" do
      config = Movie::Config.builder.set("key", "value").build
      config.get_string("key").should eq("value")
    end

    it "returns default for missing path" do
      config = Movie::Config.empty
      config.get_string("missing", "default").should eq("default")
    end

    it "raises MissingConfigError for missing path without default" do
      config = Movie::Config.empty
      expect_raises(Movie::MissingConfigError) do
        config.get_string("missing")
      end
    end

    it "raises WrongTypeConfigError for wrong type" do
      config = Movie::Config.builder.set("key", 42).build
      expect_raises(Movie::WrongTypeConfigError) do
        config.get_string("key")
      end
    end
  end

  describe "#get_int" do
    it "returns int value" do
      config = Movie::Config.builder.set("port", 9000).build
      config.get_int("port").should eq(9000)
    end

    it "returns default for missing path" do
      config = Movie::Config.empty
      config.get_int("missing", 8080).should eq(8080)
    end

    it "converts float to int" do
      config = Movie::Config.builder.set("value", 42.7).build
      config.get_int("value").should eq(42)
    end
  end

  describe "#get_bool" do
    it "returns bool value" do
      config = Movie::Config.builder
        .set("enabled", true)
        .set("disabled", false)
        .build

      config.get_bool("enabled").should be_true
      config.get_bool("disabled").should be_false
    end

    it "parses string bool values" do
      config = Movie::Config.builder
        .set("yes", "yes")
        .set("true", "true")
        .set("on", "on")
        .set("no", "no")
        .set("false", "false")
        .set("off", "off")
        .build

      config.get_bool("yes").should be_true
      config.get_bool("true").should be_true
      config.get_bool("on").should be_true
      config.get_bool("no").should be_false
      config.get_bool("false").should be_false
      config.get_bool("off").should be_false
    end

    it "returns default for missing path" do
      config = Movie::Config.empty
      config.get_bool("missing", true).should be_true
    end
  end

  describe "#get_duration" do
    it "parses duration strings" do
      config = Movie::Config.builder
        .set("ns", "500ns")
        .set("us", "100us")
        .set("ms", "250ms")
        .set("s", "5s")
        .set("m", "2m")
        .set("h", "1h")
        .set("d", "1d")
        .build

      config.get_duration("ns").should eq(500.nanoseconds)
      config.get_duration("us").should eq(100.microseconds)
      config.get_duration("ms").should eq(250.milliseconds)
      config.get_duration("s").should eq(5.seconds)
      config.get_duration("m").should eq(2.minutes)
      config.get_duration("h").should eq(1.hour)
      config.get_duration("d").should eq(1.day)
    end

    it "treats numeric values as milliseconds" do
      config = Movie::Config.builder.set("timeout", 500).build
      config.get_duration("timeout").should eq(500.milliseconds)
    end

    it "returns default for missing path" do
      config = Movie::Config.empty
      config.get_duration("missing", 1.second).should eq(1.second)
    end
  end

  describe "#get_config" do
    it "returns subsection as Config" do
      config = Movie::Config.builder
        .set("remoting.host", "127.0.0.1")
        .set("remoting.port", 9000)
        .set("remoting.pool.size", 8)
        .build

      remoting = config.get_config("remoting")
      remoting.get_string("host").should eq("127.0.0.1")
      remoting.get_int("port").should eq(9000)

      pool = remoting.get_config("pool")
      pool.get_int("size").should eq(8)
    end

    it "returns default for missing path" do
      config = Movie::Config.empty
      default = Movie::Config.builder.set("default", true).build
      config.get_config("missing", default).get_bool("default").should be_true
    end
  end

  describe "#with_fallback" do
    it "uses fallback values for missing keys" do
      base = Movie::Config.builder
        .set("name", "base")
        .build

      fallback = Movie::Config.builder
        .set("name", "fallback")
        .set("port", 8080)
        .build

      merged = base.with_fallback(fallback)
      merged.get_string("name").should eq("base")  # base wins
      merged.get_int("port").should eq(8080)       # from fallback
    end

    it "deep merges nested configs" do
      base = Movie::Config.builder
        .set("remoting.host", "localhost")
        .build

      fallback = Movie::Config.builder
        .set("remoting.host", "0.0.0.0")
        .set("remoting.port", 9000)
        .build

      merged = base.with_fallback(fallback)
      merged.get_string("remoting.host").should eq("localhost")  # base wins
      merged.get_int("remoting.port").should eq(9000)            # from fallback
    end
  end

  describe "#with_override" do
    it "overrides values with other config" do
      base = Movie::Config.builder
        .set("name", "base")
        .set("port", 8080)
        .build

      override = Movie::Config.builder
        .set("port", 9000)
        .build

      merged = base.with_override(override)
      merged.get_string("name").should eq("base")   # unchanged
      merged.get_int("port").should eq(9000)        # overridden
    end
  end

  describe "subscript access" do
    it "returns raw value with []" do
      config = Movie::Config.builder.set("key", "value").build
      config["key"].should eq("value")
    end

    it "raises for missing path with []" do
      config = Movie::Config.empty
      expect_raises(Movie::MissingConfigError) do
        config["missing"]
      end
    end

    it "returns nil for missing path with []?" do
      config = Movie::Config.empty
      config["missing"]?.should be_nil
    end
  end

  describe ".from_yaml" do
    it "parses simple YAML" do
      yaml = <<-YAML
        name: test-system
        port: 9000
        enabled: true
        ratio: 0.5
      YAML

      config = Movie::Config.from_yaml(yaml)
      config.get_string("name").should eq("test-system")
      config.get_int("port").should eq(9000)
      config.get_bool("enabled").should be_true
      config.get_float("ratio").should eq(0.5)
    end

    it "parses nested YAML" do
      yaml = <<-YAML
        remoting:
          host: 127.0.0.1
          port: 9000
          pool:
            size: 8
      YAML

      config = Movie::Config.from_yaml(yaml)
      config.get_string("remoting.host").should eq("127.0.0.1")
      config.get_int("remoting.port").should eq(9000)
      config.get_int("remoting.pool.size").should eq(8)
    end

    it "parses arrays in YAML" do
      yaml = <<-YAML
        hosts:
          - host1
          - host2
          - host3
      YAML

      config = Movie::Config.from_yaml(yaml)
      config.get_string_array("hosts").should eq(["host1", "host2", "host3"])
    end

    it "parses duration strings from YAML" do
      yaml = <<-YAML
        timeout: 5s
        interval: 100ms
      YAML

      config = Movie::Config.from_yaml(yaml)
      config.get_duration("timeout").should eq(5.seconds)
      config.get_duration("interval").should eq(100.milliseconds)
    end

    it "uses fallback for missing values in partial config" do
      # Partial YAML - only overrides name
      yaml = <<-YAML
        name: custom-name
      YAML

      default = Movie::Config.builder
        .set("name", "default-name")
        .set("port", 8080)
        .set("debug", false)
        .build

      # Simulate load with fallback (from_yaml + with_fallback)
      config = Movie::Config.from_yaml(yaml).with_fallback(default)

      config.get_string("name").should eq("custom-name")  # from YAML
      config.get_int("port").should eq(8080)              # from default
      config.get_bool("debug").should eq(false)           # from default
    end
  end

  describe ".from_json" do
    it "parses simple JSON" do
      json = %({
        "name": "test-system",
        "port": 9000,
        "enabled": true,
        "ratio": 0.5
      })

      config = Movie::Config.from_json(json)
      config.get_string("name").should eq("test-system")
      config.get_int("port").should eq(9000)
      config.get_bool("enabled").should be_true
      config.get_float("ratio").should eq(0.5)
    end

    it "parses nested JSON" do
      json = %({
        "remoting": {
          "host": "127.0.0.1",
          "port": 9000
        }
      })

      config = Movie::Config.from_json(json)
      config.get_string("remoting.host").should eq("127.0.0.1")
      config.get_int("remoting.port").should eq(9000)
    end

    it "parses arrays in JSON" do
      json = %({"hosts": ["host1", "host2", "host3"]})

      config = Movie::Config.from_json(json)
      config.get_string_array("hosts").should eq(["host1", "host2", "host3"])
    end
  end

  describe "#with_env_overrides" do
    it "applies environment variable overrides" do
      # Set test env vars
      ENV["MOVIE_TEST_NAME"] = "env-system"
      ENV["MOVIE_TEST_PORT"] = "9001"
      ENV["MOVIE_TEST_ENABLED"] = "true"
      ENV["MOVIE_TEST_RATIO"] = "0.75"

      begin
        base = Movie::Config.builder
          .set("name", "base-system")
          .set("port", 8080)
          .build

        config = base.with_env_overrides("MOVIE_TEST")

        config.get_string("name").should eq("env-system")
        config.get_int("port").should eq(9001)
        config.get_bool("enabled").should be_true
        config.get_float("ratio").should eq(0.75)
      ensure
        ENV.delete("MOVIE_TEST_NAME")
        ENV.delete("MOVIE_TEST_PORT")
        ENV.delete("MOVIE_TEST_ENABLED")
        ENV.delete("MOVIE_TEST_RATIO")
      end
    end

    it "converts nested paths from underscores" do
      ENV["MOVIE_TEST_REMOTING_HOST"] = "env-host"
      ENV["MOVIE_TEST_REMOTING_PORT"] = "9999"

      begin
        base = Movie::Config.empty
        config = base.with_env_overrides("MOVIE_TEST")

        config.get_string("remoting.host").should eq("env-host")
        config.get_int("remoting.port").should eq(9999)
      ensure
        ENV.delete("MOVIE_TEST_REMOTING_HOST")
        ENV.delete("MOVIE_TEST_REMOTING_PORT")
      end
    end

    it "parses comma-separated values as arrays" do
      ENV["MOVIE_TEST_HOSTS"] = "host1,host2,host3"

      begin
        base = Movie::Config.empty
        config = base.with_env_overrides("MOVIE_TEST")

        config.get_string_array("hosts").should eq(["host1", "host2", "host3"])
      ensure
        ENV.delete("MOVIE_TEST_HOSTS")
      end
    end
  end
end
