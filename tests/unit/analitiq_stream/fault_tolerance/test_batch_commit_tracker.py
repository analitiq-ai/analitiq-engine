"""Unit tests for BatchCommitTracker in-memory idempotency store."""

from src.state.batch_commit_tracker import BatchCommitTracker


class TestBatchCommitTracker:
    def test_check_committed_empty_returns_none(self, tmp_path):
        tracker = BatchCommitTracker(pipeline_dir=str(tmp_path), run_id="r1")
        assert tracker.check_committed(stream_id="s1", batch_seq=1) is None

    def test_record_then_check_round_trip(self, tmp_path):
        tracker = BatchCommitTracker(pipeline_dir=str(tmp_path), run_id="r1")
        tracker.record_commit(
            stream_id="s1",
            batch_seq=3,
            records_written=100,
            cursor_bytes=b'{"page": 2}',
        )

        record = tracker.check_committed(stream_id="s1", batch_seq=3)
        assert record is not None
        assert record.record_count == 100
        assert record.cursor == {"page": 2}

    def test_no_false_positive_across_batch_seq(self, tmp_path):
        tracker = BatchCommitTracker(pipeline_dir=str(tmp_path), run_id="r1")
        tracker.record_commit(stream_id="s1", batch_seq=3, records_written=50)

        assert tracker.check_committed(stream_id="s1", batch_seq=4) is None

    def test_no_false_positive_across_stream_id(self, tmp_path):
        tracker = BatchCommitTracker(pipeline_dir=str(tmp_path), run_id="r1")
        tracker.record_commit(stream_id="s1", batch_seq=3, records_written=50)

        assert tracker.check_committed(stream_id="s2", batch_seq=3) is None

    def test_multiple_streams_tracked_independently(self, tmp_path):
        tracker = BatchCommitTracker(pipeline_dir=str(tmp_path), run_id="r1")
        tracker.record_commit(stream_id="s1", batch_seq=1, records_written=10)
        tracker.record_commit(stream_id="s2", batch_seq=1, records_written=20)

        assert tracker.check_committed(stream_id="s1", batch_seq=1).record_count == 10
        assert tracker.check_committed(stream_id="s2", batch_seq=1).record_count == 20

    def test_cursor_defaults_to_none_when_no_cursor_bytes(self, tmp_path):
        tracker = BatchCommitTracker(pipeline_dir=str(tmp_path), run_id="r1")
        tracker.record_commit(stream_id="s1", batch_seq=1, records_written=5)

        assert tracker.check_committed(stream_id="s1", batch_seq=1).cursor is None
