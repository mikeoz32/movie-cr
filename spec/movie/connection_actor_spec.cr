require "../spec_helper"
require "../../src/movie"
require "../../src/movie/persistence"

describe Movie::Persistence::ConnectionActor do
  it "does not open the database connection in the constructor" do
    path = "/tmp/movie_connection_actor_#{UUID.random}.sqlite3"
    db_uri = "sqlite3:#{path}"

    File.delete?(path)

    Movie::Persistence::ConnectionActor.new(db_uri)

    File.exists?(path).should be_false
  end
end
