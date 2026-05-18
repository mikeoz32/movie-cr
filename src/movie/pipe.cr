module Movie
  module Pipe
    abstract class Message(T)
    end

    class Success(T) < Message(T)
      getter value : T

      def initialize(@value : T)
      end
    end

    class Failure(T) < Message(T)
      getter error : Exception

      def initialize(@error : Exception)
      end
    end
  end
end
