"""In-run batch commit tracker for idempotency within a single pipeline run.

Tracks which (stream_id, batch_seq) pairs have been successfully committed
so the engine can skip re-sending batches on retry without writing duplicates.
State is held in memory and does not survive process restarts — this is
intentional: a new run starts fresh.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class _CommitRecord:
    records_written: int
    cursor_bytes: bytes


class BatchCommitTracker:
    """Tracks batch commits within a single pipeline run.

    Args:
        pipeline_dir: Path to the pipeline state directory (unused by this
            in-memory implementation; reserved for a future persistent store).
        run_id: The current run identifier.
    """

    def __init__(self, pipeline_dir: str, run_id: str) -> None:
        self.pipeline_dir = pipeline_dir
        self.run_id = run_id
        self._committed: Dict[Tuple[str, int], _CommitRecord] = {}

    def check_committed(
        self,
        stream_id: str,
        batch_seq: int,
    ) -> Optional[_CommitRecord]:
        """Return the commit record if this batch was already committed, else None."""
        return self._committed.get((stream_id, batch_seq))

    def record_commit(
        self,
        stream_id: str,
        batch_seq: int,
        records_written: int,
        cursor_bytes: bytes = b"",
    ) -> None:
        """Mark a batch as committed."""
        self._committed[(stream_id, batch_seq)] = _CommitRecord(
            records_written=records_written,
            cursor_bytes=cursor_bytes,
        )
        logger.info(
            "batch committed: stream=%s seq=%d records=%d",
            stream_id,
            batch_seq,
            records_written,
        )
