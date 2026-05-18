
module Movie
  class QueueNode(T)
    getter next : QueueNode(T)?
    getter value : T

    def initialize(@value : T)
      @next = nil
    end

    def link(node : QueueNode(T))
      @next = node
    end
  end

  class Queue(T)
    getter first : QueueNode(T)?
    getter last : QueueNode(T)?
    @size : Int32

    def initialize
      @first = nil
      @last = nil
      @mutex = Mutex.new
      @size = 0
    end

    def size : Int32
      @mutex.synchronize { @size }
    end

    def enqueue(value : T)
      node = QueueNode(T).new(value.as(T))
      @mutex.synchronize do
        if @first.nil?
          @first = node
          @last = node
        else
          @last.as(QueueNode(T)).link(node) if @last
        end
        @last = node
        @size += 1
      end

    end

    def dequeue
      @mutex.synchronize do
        dequeue_no_lock
      end
    end

    def dequeue_no_lock
      return nil if @first.nil?
      value = @first.as(QueueNode(T)).value
      @first = @first.as(QueueNode(T)).next
      @last = nil if @first.nil?
      @size -= 1
      value
    end

    def dequeue(&)
      loop do
        value = dequeue
        break if value.nil?
        yield value
      end
    end
  end
end
