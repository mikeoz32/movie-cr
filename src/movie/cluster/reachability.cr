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
    @last_heartbeat_ns = {} of UniqueAddress => Int64
    @unreachable = Set(UniqueAddress).new
    @mutex = Mutex.new

    def unreachable : Set(UniqueAddress)
      @mutex.synchronize { @unreachable.dup }
    end

    def check(peers : Array(Member), timeout_span : Time::Span) : Array(Member)
      now = ClusterClock.now_nanoseconds
      active_addresses = peers.map(&.unique_address).to_set
      newly_unreachable = [] of Member
      @mutex.synchronize do
        @last_heartbeat_ns.reject! { |address, _| !active_addresses.includes?(address) }
        @unreachable.reject! { |address| !active_addresses.includes?(address) }
        peers.each do |peer|
          address = peer.unique_address
          last_seen = @last_heartbeat_ns[address]? || begin
            @last_heartbeat_ns[address] = now
            now
          end
          if now - last_seen > timeout_span.total_nanoseconds && @unreachable.add?(address)
            newly_unreachable << peer
          end
        end
      end
      newly_unreachable
    end

    def mark_reachable(sender : UniqueAddress) : Bool
      @mutex.synchronize do
        @last_heartbeat_ns[sender] = ClusterClock.now_nanoseconds
        @unreachable.delete(sender)
      end
    end
  end
end
