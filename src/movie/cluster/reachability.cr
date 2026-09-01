require "set"
require "./model"

module Movie::Cluster
  private module ClusterClock
    extend self

    EPOCH = Time.instant

    def now_nanoseconds : Int64
      (Time.instant - EPOCH).total_nanoseconds.to_i64
    end
  end

  class ReachabilityState
    @last_heartbeat_ns = {} of String => Int64
    @unreachable = Set(String).new
    @mutex = Mutex.new

    def unreachable_keys : Set(String)
      @mutex.synchronize { @unreachable.dup }
    end

    def check(peers : Array(Member), timeout_span : Time::Span) : Array(Member)
      now = ClusterClock.now_nanoseconds
      active_keys = peers.map(&.unique_address.key).to_set
      newly_unreachable = [] of Member
      @mutex.synchronize do
        @last_heartbeat_ns.reject! { |key, _| !active_keys.includes?(key) }
        @unreachable.reject! { |key| !active_keys.includes?(key) }
        peers.each do |peer|
          key = peer.unique_address.key
          last_seen = @last_heartbeat_ns[key]? || begin
            @last_heartbeat_ns[key] = now
            now
          end
          if now - last_seen > timeout_span.total_nanoseconds && @unreachable.add?(key)
            newly_unreachable << peer
          end
        end
      end
      newly_unreachable
    end

    def mark_reachable(sender : UniqueAddress) : Bool
      @mutex.synchronize do
        @last_heartbeat_ns[sender.key] = ClusterClock.now_nanoseconds
        @unreachable.delete(sender.key)
      end
    end
  end
end
