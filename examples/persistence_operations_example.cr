require "../src/movie"
require "../src/movie/persistence"

path = "/tmp/movie_persistence_operations_#{UUID.random}.sqlite3"
config = Movie::Config.builder
  .set("name", "persistence-operations-example")
  .set("persistence.db-path", path)
  .build
system = Movie::ActorSystem(Nil).new(Movie::Behaviors(Nil).same, config)

begin
  database = Movie::Database.get(system)
  readiness = database.readiness
  raise readiness.error || "persistence is not ready" unless readiness.ready

  persistence_id = "orders:example"
  operation_id = Movie::Persistence::OperationId.random
  message_id = UUID.random.to_s
  payload = String.build do |io|
    JSON.build(io) do |json|
      json.object { json.field "order_id", "example" }
    end
  end
  outbox = Movie::Persistence::OutboxEntry.new(
    message_id,
    "billing",
    "OrderPlaced",
    payload
  )

  write = system.ask(
    database.pool,
    Movie::Persistence::AppendEvents.new(
      persistence_id,
      0_i64,
      operation_id,
      [Movie::Persistence::SerializedEvent.new("OrderPlaced", payload)],
      [outbox]
    ),
    Movie::Persistence::WriteResult,
    database.operation_timeout
  ).await(database.operation_timeout)
  puts "journal revision: #{write.revision}"

  projection_name = "orders-example"
  projection = Movie::Persistence::ProjectionRunner.new(
    database,
    projection_name,
    persistence_id: persistence_id
  )
  projection.run_once do |event|
    puts "projected event offset #{event.offset}: #{event.manifest}"
  end

  dispatcher = Movie::Persistence::OutboxDispatcher.new(database, "example-dispatcher")
  dispatcher.run_once do |message|
    puts "publish #{message.message_id} to #{message.destination}"
  end
  database.delete_projection_offset(projection_name)

  metrics = database.metrics
  puts "completed persistence operations: #{metrics.completed}"
ensure
  system.shutdown
  File.delete(path) if File.exists?(path)
end
