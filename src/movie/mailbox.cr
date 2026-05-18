module Movie
  class Envelope(T)
    getter message : T
    getter sender : ActorRefBase?

    def initialize(@message : T, @sender : ActorRefBase?)
    end
  end

  class Mailbox(T)
    @scheduled = false
    @processing = false

    def initialize(@dispatcher : Dispatcher, @context : ActorContext(T))
      @inbox = Queue(Envelope(T)).new
      @system = Queue(Envelope(SystemMessage)).new
      @mutex = Mutex.new
    end

    def dispatch
      @mutex.synchronize { @processing = true }
      @system.dequeue do |message|
        @context.on_system_message(message) unless message.nil?
      end
      @inbox.dequeue do |message|
        @context.on_message(message) unless message.nil?
      end

      need_schedule = false
      @mutex.synchronize do
        @processing = false
        @scheduled = false
        need_schedule = @inbox.size > 0 || @system.size > 0
        @scheduled = true if need_schedule
      end
      @dispatcher.dispatch(self) if need_schedule
    end

    def send(message : Envelope(T))
      @inbox.enqueue(message)
      schedule_dispatch
    end

    def send_system(message : Envelope(SystemMessage))
      @system.enqueue(message)
      schedule_dispatch
    end

    def <<(message : Envelope(T))
      send(message)
    end

    def purge_inbox
      @inbox = Queue(Envelope(T)).new
    end

    private def schedule_dispatch
      should_schedule = false
      @mutex.synchronize do
        return if @scheduled || @processing
        @scheduled = true
        should_schedule = true
      end
      @dispatcher.dispatch(self) if should_schedule
    end
  end

  class MailboxManager
    def create(dispatcher, context)
      Mailbox.new(dispatcher, context)
    end
  end
end
