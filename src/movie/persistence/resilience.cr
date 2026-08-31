module Movie
  module Persistence
    class CircuitOpenError < Exception
      def initialize
        super("Persistence connection circuit is open")
      end
    end

    class ResiliencePolicy
      getter max_retries : Int32
      getter min_backoff : Time::Span
      getter max_backoff : Time::Span
      getter circuit_failure_threshold : Int32
      getter circuit_reset_timeout : Time::Span

      def initialize(
        @max_retries : Int32 = 0,
        @min_backoff : Time::Span = Time::Span.zero,
        @max_backoff : Time::Span = Time::Span.zero,
        @circuit_failure_threshold : Int32 = 5,
        @circuit_reset_timeout : Time::Span = 5.seconds,
      )
        raise ArgumentError.new("Persistence max retries cannot be negative") if @max_retries < 0
        raise ArgumentError.new("Persistence backoff cannot be negative") if @min_backoff < Time::Span.zero
        if @max_backoff < @min_backoff
          raise ArgumentError.new("Persistence max backoff cannot be below min backoff")
        end
        if @circuit_failure_threshold < 1
          raise ArgumentError.new("Persistence circuit failure threshold must be positive")
        end
        unless @circuit_reset_timeout > Time::Span.zero
          raise ArgumentError.new("Persistence circuit reset timeout must be positive")
        end
      end

      def self.disabled : self
        new
      end
    end
  end
end
