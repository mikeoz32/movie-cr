require "../spec_helper"
require "../../src/movie"
require "../../src/movie/persistence"

describe Movie::Persistence::ConnectionActor do
  it "runs SQLite work on a dedicated isolated execution context" do
    path = "/tmp/movie_connection_worker_#{UUID.random}.sqlite3"
    worker = Movie::Persistence::ConnectionWorker.new("sqlite3:#{path}", "connection-worker-spec")
    caller_thread = Thread.current.object_id

    worker_thread, value = worker.execute do |connection|
      connection.exec("CREATE TABLE worker_probe (value INTEGER)")
      connection.exec("INSERT INTO worker_probe (value) VALUES (42)")
      {Thread.current.object_id, connection.query_one("SELECT value FROM worker_probe", as: Int64)}
    end

    worker_thread.should_not eq(caller_thread)
    value.should eq(42_i64)

    worker.close
    expect_raises(Movie::Persistence::ConnectionWorker::Stopped) do
      worker.execute { |_connection| true }
    end
  ensure
    worker.try &.close
    File.delete(path) if path && File.exists?(path)
  end

  it "does not open the database connection in the constructor" do
    path = "/tmp/movie_connection_actor_#{UUID.random}.sqlite3"
    db_uri = "sqlite3:#{path}"

    File.delete?(path)

    Movie::Persistence::ConnectionActor.new(db_uri)

    File.exists?(path).should be_false
  end
end
