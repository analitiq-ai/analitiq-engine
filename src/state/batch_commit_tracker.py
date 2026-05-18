"""In-run batch commit tracker for idempotency.

Tracks which (stream_id, batch_seq) pairs have been committed during the
current run. State is held in memory only; cross-run idempotency uses the
``_batch_commits`` table in the destination database.
"""

from typing import Dict, Optional, Tuple


class BatchCommitTracker:
    """In-memory record of batches committed during the current run."""

    def __init__(
        self,
        pipeline_dir: Optional[str] = None,
        run_id: Optional[str] = None,
    ) -> None:
        # pipeline_dir and run_id are accepted for call-site compatibility
        # but not persisted — this implementation is in-memory only.
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
