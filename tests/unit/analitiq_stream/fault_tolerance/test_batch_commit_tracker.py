"""Unit tests for BatchCommitTracker in-memory idempotency store."""

from src.state.batch_commit_tracker import BatchCommitTracker


class TestBatchCommitTracker:
    def test_check_committed_empty_returns_none(self):
        tracker = BatchCommitTracker(pipeline_dir="/tmp", run_id="r1")
        assert tracker.check_committed("s1", 1) is None

    def test_record_then_check_round_trip(self):
        tracker = BatchCommitTracker(pipeline_dir="/tmp", run_id="r1")
        tracker.record_commit("s1", 3, records_written=100, cursor_bytes=b"\x01\x02")

        record = tracker.check_committed("s1", 3)
        assert record is not None
        assert record.records_written == 100
        assert record.cursor_bytes == b"\x01\x02"

    def test_no_false_positive_across_batch_seq(self):
        tracker = BatchCommitTracker(pipeline_dir="/tmp", run_id="r1")
        tracker.record_commit("s1", 3, records_written=50)

        assert tracker.check_committed("s1", 4) is None

    def test_no_false_positive_across_stream_id(self):
        tracker = BatchCommitTracker(pipeline_dir="/tmp", run_id="r1")
        tracker.record_commit("s1", 3, records_written=50)

        assert tracker.check_committed("s2", 3) is None

    def test_multiple_streams_tracked_independently(self):
        tracker = BatchCommitTracker(pipeline_dir="/tmp", run_id="r1")
        tracker.record_commit("s1", 1, records_written=10)
        tracker.record_commit("s2", 1, records_written=20)

        assert tracker.check_committed("s1", 1).records_written == 10
        assert tracker.check_committed("s2", 1).records_written == 20

    def test_cursor_bytes_defaults_to_empty(self):
        tracker = BatchCommitTracker(pipeline_dir="/tmp", run_id="r1")
        tracker.record_commit("s1", 1, records_written=5)

        assert tracker.check_committed("s1", 1).cursor_bytes == b""
