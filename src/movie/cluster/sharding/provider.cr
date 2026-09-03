require "./model"

module Movie::Cluster
  abstract class ShardedEntityProvider
    getter name : String
    getter message_type_name : String
    getter settings : ShardingSettings

    def initialize(
      @name : String,
      @message_type_name : String,
      @settings : ShardingSettings,
      @telemetry : ShardingTelemetry,
    )
    end

    abstract def deliver(
      envelope : ShardingEnvelope,
      sender : Movie::ActorRefBase?,
    ) : Nil
    # Returns true only when this request created a new local entity instance.
    abstract def activate(entity_id : String, shard_id : Int32) : Bool
    abstract def passivate(entity_id : String) : Bool
    abstract def passivate_shard(shard_id : Int32) : Nil
    abstract def retain_shards(shard_ids : Set(Int32)) : Nil
    abstract def authorize_shard(shard_id : Int32) : Nil
    abstract def prepare_shard(shard_id : Int32) : Nil
    abstract def suspend_ownership(release : Bool) : Nil
    abstract def passivate_idle(now : Time::Instant) : Int32

    def handle_cluster_event(event : ClusterEvent) : Nil
    end

    abstract def stop_all : Nil
    abstract def local_entity_count : Int32
  end

  class BehaviorEntityProvider(T) < ShardedEntityProvider
    @entities = {} of String => Movie::ActorRef(T)
    @entity_shards = {} of String => Int32
    @last_activity = {} of String => Time::Instant
    @draining_shards = {} of Int32 => Channel(Nil)
    @mutex = Mutex.new

    def initialize(
      name : String,
      settings : ShardingSettings,
      @system : Movie::AbstractActorSystem,
      telemetry : ShardingTelemetry,
      &@factory : String -> Movie::AbstractBehavior(T)
    )
      super(name, T.name, settings, telemetry)
    end

    def deliver(
      envelope : ShardingEnvelope,
      sender : Movie::ActorRefBase?,
    ) : Nil
      wrapper = envelope.message.wrapper || Movie::Remote::MessageRegistry.deserialize(
        envelope.message.message_type,
        envelope.message.payload
      )
      ref, _activated = entity_ref(envelope.entity_id, envelope.shard_id)
      ref.tell_from(sender, wrapper.unwrap(T))
    end

    def activate(entity_id : String, shard_id : Int32) : Bool
      _ref, activated = entity_ref(entity_id, shard_id)
      activated
    end

    def passivate_shard(shard_id : Int32) : Nil
      drain_shard(shard_id)
    end

    def passivate(entity_id : String) : Bool
      ref = @mutex.synchronize do
        @entity_shards.delete(entity_id)
        @last_activity.delete(entity_id)
        @entities.delete(entity_id)
      end
      return false unless ref
      ref.send_system(Movie::STOP)
      true
    end

    def retain_shards(shard_ids : Set(Int32)) : Nil
      refs = remove_entities { |_entity_id, shard_id| !shard_ids.includes?(shard_id) }
      refs.each(&.send_system(Movie::STOP))
    end

    def authorize_shard(shard_id : Int32) : Nil
    end

    def prepare_shard(shard_id : Int32) : Nil
    end

    def suspend_ownership(release : Bool) : Nil
      refs = remove_entities { |_entity_id, _shard_id| true }
      refs.each(&.send_system(Movie::STOP))
    end

    def passivate_idle(now : Time::Instant) : Int32
      timeout = settings.idle_timeout || return 0
      refs = remove_entities do |entity_id, _shard_id|
        last = @last_activity[entity_id]?
        !last.nil? && now - last >= timeout
      end
      refs.each(&.send_system(Movie::STOP))
      refs.size
    end

    def stop_all : Nil
      refs = @mutex.synchronize do
        current = @entities.values
        @entities.clear
        @entity_shards.clear
        @last_activity.clear
        @draining_shards.each_value(&.close)
        @draining_shards.clear
        current
      end
      refs.each(&.send_system(Movie::STOP))
    end

    def local_entity_count : Int32
      @mutex.synchronize do
        @entities.reject! do |entity_id, ref|
          stopped = @system.context(ref.id).nil?
          if stopped
            @entity_shards.delete(entity_id)
            @last_activity.delete(entity_id)
          end
          stopped
        end
        @entities.size
      end
    end

    private def entity_ref(
      entity_id : String,
      shard_id : Int32,
    ) : Tuple(Movie::ActorRef(T), Bool)
      @mutex.synchronize do
        if @draining_shards.has_key?(shard_id)
          raise ShardHandoffInProgressError.new(name, shard_id)
        end
        if current = @entities[entity_id]?
          if @system.context(current.id)
            @last_activity[entity_id] = Time.instant
            return {current, false}
          end
          @entities.delete(entity_id)
        end
        ref = @system.spawn(@factory.call(entity_id))
        @telemetry.activated
        @entity_shards[entity_id] = shard_id
        @last_activity[entity_id] = Time.instant
        @entities[entity_id] = ref
        {ref, true}
      end
    end

    private def remove_entities(&remove : String, Int32 -> Bool) : Array(Movie::ActorRef(T))
      @mutex.synchronize do
        entity_ids = @entity_shards.select { |entity_id, shard_id| remove.call(entity_id, shard_id) }.keys
        refs = [] of Movie::ActorRef(T)
        entity_ids.each do |entity_id|
          if ref = @entities.delete(entity_id)
            refs << ref
          end
          @entity_shards.delete(entity_id)
          @last_activity.delete(entity_id)
        end
        refs
      end
    end

    private def drain_shard(shard_id : Int32) : Nil
      channel = nil.as(Channel(Nil)?)
      refs = [] of Movie::ActorRef(T)
      first = false
      @mutex.synchronize do
        if current = @draining_shards[shard_id]?
          channel = current
        else
          channel = Channel(Nil).new
          @draining_shards[shard_id] = channel.not_nil!
          first = true
          entity_ids = @entity_shards.select do |_entity_id, candidate|
            candidate == shard_id
          end.keys
          entity_ids.each do |entity_id|
            refs << @entities.delete(entity_id).not_nil! if @entities.has_key?(entity_id)
            @entity_shards.delete(entity_id)
            @last_activity.delete(entity_id)
          end
        end
      end

      unless first
        channel.not_nil!.receive?
        return
      end

      begin
        refs.each(&.send_system(Movie::DRAIN_AND_STOP))
        refs.each do |ref|
          while @system.context(ref.id)
            sleep 1.millisecond
          end
        end
      ensure
        @mutex.synchronize { @draining_shards.delete(shard_id) }
        channel.not_nil!.close
      end
    end
  end
end
