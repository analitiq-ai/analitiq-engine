"""In-run batch commit tracker for idempotency.

Tracks which (stream_id, batch_seq) pairs have been committed during the
current run so the engine can skip re-sending a batch that already landed
(e.g. after a retry loop where the first attempt succeeded but the ACK
was lost in transit).

State is held in memory only; it does not survive process restarts.
Cross-run idempotency uses the ``_batch_commits`` table in the destination
database instead.
"""

from typing import Dict, Tuple


class BatchCommitTracker:
    """In-memory record of batches committed during the current run."""

    def __init__(self, pipeline_dir: str, run_id: str) -> None:
        self._pipeline_dir = pipeline_dir
        self._run_id = run_id
        self._committed: Dict[Tuple[str, int], int] = {}

    def check_committed(self, stream_id: str, batch_seq: int) -> bool:
        """Return True if this batch was already committed in this run."""
        return (stream_id, batch_seq) in self._committed

    def record_commit(
        self,
        stream_id: str,
        batch_seq: int,
        records_written: int,
        cursor_bytes: bytes,
    ) -> None:
        """Record that a batch was successfully committed."""
        self._committed[(stream_id, batch_seq)] = records_written
