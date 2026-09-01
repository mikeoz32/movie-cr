require "./events"

module Movie::Cluster
  class ClusterEventBus
    Log = ::Log.for(self)

    @subscribers = [] of Movie::ActorRef(ClusterEvent)
    @mutex = Mutex.new

    def subscribe(subscriber : Movie::ActorRef(ClusterEvent)) : Nil
      @mutex.synchronize { @subscribers << subscriber unless @subscribers.includes?(subscriber) }
    end

    def unsubscribe(subscriber : Movie::ActorRef(ClusterEvent)) : Nil
      @mutex.synchronize { @subscribers.delete(subscriber) }
    end

    def size : Int32
      @mutex.synchronize { @subscribers.size }
    end

    def publish(event : ClusterEvent) : Nil
      subscribers = @mutex.synchronize { @subscribers.dup }
      failed = [] of Movie::ActorRef(ClusterEvent)
      subscribers.each do |subscriber|
        begin
          subscriber << event
        rescue ex : Exception
          failed << subscriber
          Log.debug { "Removed stopped cluster subscriber: #{ex.message}" }
        end
      end
      return if failed.empty?
      @mutex.synchronize { failed.each { |subscriber| @subscribers.delete(subscriber) } }
    end
  end
end
