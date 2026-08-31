require "../spec_helper"
require "../../src/movie"
require "../../src/movie/persistence"
describe "Movie persistence production contracts" do
  describe "snapshot-safe retention" do
    it "compacts only snapshotted events while preserving revision and deduplication" do
      path = "/tmp/movie_retention_#{UUID.random}.sqlite3"
      backend = Movie::Persistence::SQLiteBackend.new("sqlite3:#{path}")
      connection = backend.connect
      connection.ensure_event_store
      operation_id = Movie::Persistence::OperationId.random
      events = (1..5).map do |number|
        Movie::Persistence::SerializedEvent.new("Added", number.to_s)
      end
      append = Movie::Persistence::AppendEvents.new("retained", 0_i64, operation_id, events)
      connection.append_events(append).revision.should eq(5_i64)
      connection.save_snapshot(
        Movie::Persistence::SaveSnapshot.new(
          "retained",
          Movie::Persistence::SnapshotRecord.new(3_i64, "Counter", "3")
        )
      )

      expect_raises(Movie::Persistence::UnsafeRetentionError) do
        connection.delete_events_to(Movie::Persistence::DeleteEventsTo.new("retained", 4_i64))
      end

      result = connection.delete_events_to(
        Movie::Persistence::DeleteEventsTo.new("retained", 3_i64)
      )
      result.should eq(Movie::Persistence::RetentionResult.new(3_i64, 3_i64))
      connection.load_events(Movie::Persistence::LoadEvents.new("retained"))
        .map(&.sequence_nr)
        .should eq([4_i64, 5_i64])

      connection.append_events(append)
        .should eq(Movie::Persistence::WriteResult.new(5_i64, true))
      connection.append_events(
        Movie::Persistence::AppendEvents.new(
          "retained",
          5_i64,
          Movie::Persistence::OperationId.random,
          [Movie::Persistence::SerializedEvent.new("Added", "6")]
        )
      ).revision.should eq(6_i64)

      maintenance = connection.run_maintenance(Movie::Persistence::RunMaintenance.new)
      maintenance.completed.should be_true
      maintenance.backend.should eq("sqlite")
    ensure
      connection.try &.close
      File.delete(path) if path && File.exists?(path)
    end
  end

  describe "persistence query and projections" do
    it "pages globally ordered events and stores monotonic projection checkpoints" do
      path = "/tmp/movie_query_#{UUID.random}.sqlite3"
      connection = Movie::Persistence::SQLiteBackend.new("sqlite3:#{path}").connect
      connection.ensure_event_store
      [
        {"stream-a", "a1"},
        {"stream-b", "b1"},
        {"stream-a", "a2"},
      ].each do |persistence_id, payload|
        revision = persistence_id == "stream-a" && payload == "a2" ? 1_i64 : 0_i64
        connection.append_events(
          Movie::Persistence::AppendEvents.new(
            persistence_id,
            revision,
            Movie::Persistence::OperationId.random,
            [Movie::Persistence::SerializedEvent.new("Added", payload)]
          )
        )
      end

      first = connection.query_events(Movie::Persistence::QueryEvents.new(0_i64, 2))
      first.events.map(&.payload).should eq(["a1", "b1"])
      first.events.map(&.offset).should eq(first.events.map(&.offset).sort)
      first.has_more.should be_true

      second = connection.query_events(
        Movie::Persistence::QueryEvents.new(first.next_offset, 2)
      )
      second.events.map(&.payload).should eq(["a2"])
      second.has_more.should be_false

      connection.load_projection_offset(
        Movie::Persistence::LoadProjectionOffset.new("totals")
      ).should eq(0_i64)
      connection.save_projection_offset(
        Movie::Persistence::SaveProjectionOffset.new("totals", second.next_offset)
      ).should eq(second.next_offset)
      connection.save_projection_offset(
        Movie::Persistence::SaveProjectionOffset.new("totals", second.next_offset)
      ).should eq(second.next_offset)
      expect_raises(Movie::Persistence::ProjectionOffsetRegressionError) do
        connection.save_projection_offset(
          Movie::Persistence::SaveProjectionOffset.new("totals", first.next_offset)
        )
      end
    ensure
      connection.try &.close
      File.delete(path) if path && File.exists?(path)
    end

    it "blocks retention while a registered projection is behind deleted events" do
      path = "/tmp/movie_projection_retention_#{UUID.random}.sqlite3"
      connection = Movie::Persistence::SQLiteBackend.new("sqlite3:#{path}").connect
      connection.ensure_event_store
      connection.append_events(
        Movie::Persistence::AppendEvents.new(
          "projected",
          0_i64,
          Movie::Persistence::OperationId.random,
          (1..3).map { |number| Movie::Persistence::SerializedEvent.new("Added", number.to_s) }
        )
      )
      connection.save_snapshot(
        Movie::Persistence::SaveSnapshot.new(
          "projected",
          Movie::Persistence::SnapshotRecord.new(3_i64, "Counter", "3")
        )
      )
      connection.save_projection_offset(
        Movie::Persistence::SaveProjectionOffset.new("slow", 0_i64)
      )

      expect_raises(Movie::Persistence::ProjectionBehindRetentionError) do
        connection.delete_events_to(Movie::Persistence::DeleteEventsTo.new("projected", 3_i64))
      end

      page = connection.query_events(Movie::Persistence::QueryEvents.new(0_i64, 10))
      connection.save_projection_offset(
        Movie::Persistence::SaveProjectionOffset.new("slow", page.next_offset)
      )
      connection.delete_events_to(Movie::Persistence::DeleteEventsTo.new("projected", 3_i64))
        .deleted_events.should eq(3_i64)
      connection.query_events(Movie::Persistence::QueryEvents.new(0_i64, 10)).events.should be_empty
    ensure
      connection.try &.close
      File.delete(path) if path && File.exists?(path)
    end
  end
end
