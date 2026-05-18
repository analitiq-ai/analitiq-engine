"""In-run batch commit tracker for idempotency.

Tracks which (stream_id, batch_seq) pairs have been committed during the
current run. State is held in memory only. Cross-run idempotency is handled
separately by the destination connector via the ``_batch_commits`` table;
this class covers only the current run.
"""

import logging
from typing import Dict, Optional, Tuple

logger = logging.getLogger(__name__)


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
        self._cursor_warning_emitted = False

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
        """Record that a batch was successfully committed.

        Note: ``cursor_bytes`` is accepted for call-site compatibility but is
        not stored by this in-memory implementation. Cross-run cursor tracking
        relies on the destination connector's ``_batch_commits`` table.
        """
        if cursor_bytes and not self._cursor_warning_emitted:
            logger.warning(
                "BatchCommitTracker: cursor_bytes not persisted (stub "
                "implementation, tracked in issue #49); cross-run idempotency "
                "depends solely on the destination _batch_commits table"
            )
            self._cursor_warning_emitted = True
        self._committed[(stream_id, batch_seq)] = records_written
