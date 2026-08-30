require "deque"

module Movie
  class Envelope(T)
    getter message : T
    getter sender : ActorRefBase?

    def initialize(@message : T, @sender : ActorRefBase?)
    end
  end

  # :nodoc:
  struct MailboxEnvelope(T)
    getter message : T
    getter sender : ActorRefBase?

    def initialize(@message : T, @sender : ActorRefBase?)
    end
  end

  # :nodoc:
  class MailboxQueue(T)
    def initialize
      @items = Deque(T).new
      @mutex = Mutex.new
    end

    def size : Int32
      @mutex.synchronize { @items.size }
    end

    def enqueue(value : T) : Nil
      @mutex.synchronize { @items << value }
    end

    def dequeue : T?
      @mutex.synchronize { @items.shift? }
    end
  end

  class Mailbox(T)
    MAX_MESSAGES_PER_DISPATCH = 100

    @scheduled = false
    @processing = false

    def initialize(@dispatcher : Dispatcher, @context : ActorContext(T))
      @inbox = MailboxQueue(MailboxEnvelope(T)).new
      @system = MailboxQueue(MailboxEnvelope(SystemMessage)).new
      @mutex = Mutex.new
    end

    def dispatch
      @context.system.actor_dispatch_enter
      @mutex.synchronize { @processing = true }

      processed = 0
      begin
        loop do
          # Control messages always get a chance before the next user message.
          if message = @system.dequeue
            @context.on_system_message(message)
          else
            unless @context.accepts_user_messages?
              purge_inbox if @context.discard_user_messages?
              break
            end

            message = @inbox.dequeue
            break unless message
            @context.on_message(message)
          end

          processed += 1
          break if processed >= MAX_MESSAGES_PER_DISPATCH
        end
      ensure
        need_schedule = false
        @mutex.synchronize do
          @processing = false
          @scheduled = false
          need_schedule = @system.size > 0 || (@context.accepts_user_messages? && @inbox.size > 0)
          @scheduled = true if need_schedule
        end
        @context.system.actor_dispatch_leave
        @dispatcher.dispatch(self) if need_schedule
      end
    end

    def send(message : Envelope(T))
      enqueue(message.message, message.sender)
    end

    def enqueue(message : T, sender : ActorRefBase?)
      @inbox.enqueue(MailboxEnvelope(T).new(message, sender))
      schedule_dispatch
    end

    def send_system(message : Envelope(SystemMessage))
      enqueue_system(message.message, message.sender)
    end

    def enqueue_system(message : SystemMessage, sender : ActorRefBase?)
      @system.enqueue(MailboxEnvelope(SystemMessage).new(message, sender))
      schedule_dispatch
    end

    def <<(message : Envelope(T))
      send(message)
    end

    def purge_inbox
      @inbox = MailboxQueue(MailboxEnvelope(T)).new
    end

    def wake
      schedule_dispatch
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
