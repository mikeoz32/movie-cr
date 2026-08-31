module Movie
  module Persistence
    record ProjectionRunResult, processed : Int32, offset : Int64, has_more : Bool
    record OutboxDispatchResult, claimed : Int32, delivered : Int32, failed : Int32

    # Bounded, restartable projection step. The checkpoint advances only after
    # the handler returns, so a crash can redeliver but cannot skip an event.
    class ProjectionRunner
      def initialize(
        @database : Movie::DatabaseExtension,
        @name : String,
        @page_size : Int32 = 100,
        @persistence_id : String? = nil,
      )
      end

      def run_once(&handler : EventEnvelope ->) : ProjectionRunResult
        offset = @database.projection_offset(@name)
        page = @database.query_events(offset, @page_size, @persistence_id)
        processed = 0
        page.events.each do |event|
          handler.call(event)
          offset = @database.save_projection_offset(@name, event.offset)
          processed += 1
        end
        ProjectionRunResult.new(processed, offset, page.has_more)
      end
    end

    # Claims a bounded lease, invokes the external publisher, then acknowledges
    # success or releases the record for an at-least-once retry.
    class OutboxDispatcher
      def initialize(
        @database : Movie::DatabaseExtension,
        @owner : String,
        @batch_size : Int32 = 100,
        @lease : Time::Span = 30.seconds,
      )
      end

      def run_once(&publisher : StoredOutboxEntry ->) : OutboxDispatchResult
        claimed = @database.claim_outbox(@owner, @batch_size, @lease)
        delivered = 0
        failed = 0
        claimed.each do |message|
          begin
            publisher.call(message)
            delivered += @database.acknowledge_outbox(@owner, [message.message_id]).to_i
          rescue error
            failed += 1
            @database.release_outbox(
              @owner,
              message.message_id,
              error.message || error.class.name
            )
          end
        end
        OutboxDispatchResult.new(claimed.size, delivered, failed)
      end
    end
  end
end
