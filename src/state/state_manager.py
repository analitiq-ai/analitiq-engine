"""State management for pipeline checkpointing and recovery."""

import json
import logging
import os
import threading
from pathlib import Path
from typing import Any, Dict, Optional

from ..shared.run_id import get_or_generate_run_id
from .batch_commit_tracker import BatchCommitTracker
from .state_emission import emit_state_log
from .store import CursorStore, parse_resume_state

logger = logging.getLogger(__name__)


class StateManager:
    """
    Manages pipeline run state and emits checkpoints to structured logs.

    Checkpoints are emitted as ANALITIQ_STATE:: log lines for
    cross-run observability. Batch commit tracking uses local
    filesystem for in-run idempotency.
    """

    def __init__(
        self,
        pipeline_id: str,
        base_dir: str = "state",
    ):
        self.pipeline_id = pipeline_id
        self.base_dir = Path(base_dir)
        self.pipeline_dir = self.base_dir / pipeline_id

        # Thread safety
        self.lock = threading.RLock()

        # Current run ID (from env var RUN_ID if available)
        self.current_run_id: Optional[str] = os.environ.get("RUN_ID")

        # In-run batch commit tracker (initialized by init_commit_tracker)
        self._commit_tracker: Optional[BatchCommitTracker] = None

        # In-run cursor cache keyed by (stream_name, partition_key). Backed by
        # an on-disk checkpoint so a fresh process resumes from where the last
        # run left off instead of re-scanning the whole source.
        self._cursors: Dict[str, Dict[str, Any]] = {}
        self._cursor_store = CursorStore(self.base_dir)
        self._restore_durable_cursors()

    def _restore_durable_cursors(self) -> None:
        """Seed the cursor cache from the injected durable resume state.

        The on-disk ``state/`` checkpoint is wiped on a fresh container, so
        without this an incremental stream would find no bookmark on re-run
        and full-rescan the source. The deployment re-injects the cursors it
        harvested from the prior run's emitted state as the ``RESUME_STATE``
        env var; we decode it into the same ``{"cursor": <value>}`` shape
        :meth:`get_cursor` returns, keyed by ``stream_id`` with the empty
        partition the engine reads with.
        """
        restored = parse_resume_state(os.environ.get("RESUME_STATE"))
        seeded = 0
        for stream_id, value in restored.items():
            if value is None:
                continue
            self._cursors[self._cursor_key(stream_id, {})] = {"cursor": value}
            seeded += 1
        if seeded:
            logger.info(
                "restored durable cursor state for %d stream(s) from RESUME_STATE",
                seeded,
            )

    def init_commit_tracker(self, run_id: str) -> None:
        """Initialize batch commit tracker for the current run."""
        self._commit_tracker = BatchCommitTracker(
            pipeline_dir=str(self.pipeline_dir),
            run_id=run_id,
        )

    @property
    def commit_tracker(self) -> Optional[BatchCommitTracker]:
        """Get the batch commit tracker (if initialized)."""
        return self._commit_tracker

    def start_run(self, config: Dict[str, Any], run_id: Optional[str] = None) -> str:
        """
        Start a new pipeline run.

        Args:
            config: Pipeline configuration
            run_id: Optional run ID, generated if not provided

        Returns:
            The run ID for this execution
        """
        with self.lock:
            if not run_id:
                run_id = self.current_run_id or get_or_generate_run_id()
            self.current_run_id = run_id
            logger.info(f"Started pipeline run {run_id}")
            return run_id

    def save_stream_checkpoint(
        self,
        stream_name: str,
        partition: Dict[str, Any],
        cursor: Dict[str, Any],
        hwm: str,
        page_state: Optional[Dict[str, Any]] = None,
        http_conditionals: Optional[Dict[str, Any]] = None,
        stats: Optional[Dict[str, Any]] = None,
    ):
        """
        Emit checkpoint for a specific stream to structured logs.

        Args:
            stream_name: Name of the stream
            partition: Partition key dict (ignored)
            cursor: Cursor state with primary/tiebreaker fields
            hwm: High-water mark timestamp
            page_state: Unused, kept for call-site compatibility
            http_conditionals: Unused, kept for call-site compatibility
            stats: Unused, kept for call-site compatibility
        """
        emit_state_log(
            run_id=self.current_run_id or "",
            pipeline_id=self.pipeline_id,
            stream_id=stream_name,
            cursor_hex=json.dumps(cursor).encode().hex() if cursor else "",
            cursor_value=hwm,
        )

    @staticmethod
    def _cursor_key(stream_name: str, partition: Dict[str, Any]) -> str:
        """Build a deterministic key from stream name and partition dict."""
        suffix = json.dumps(partition, sort_keys=True) if partition else "{}"
        return f"{stream_name}::{suffix}"

    async def get_cursor(
        self, stream_name: str, partition: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """Return the last saved cursor for a stream/partition, or None.

        Falls back to the on-disk checkpoint when the in-run cache is empty so
        a newly started process resumes from the previous run's bookmark.
        """
        key = self._cursor_key(stream_name, partition or {})
        with self.lock:
            cached = self._cursors.get(key)
            if cached is not None:
                return cached
            persisted = self._cursor_store.get(self.pipeline_id, stream_name)
            if persisted is None:
                return None
            cursor = {"cursor": persisted}
            self._cursors[key] = cursor
            return cursor

    async def save_cursor(
        self,
        stream_name: str,
        partition: Optional[Dict[str, Any]],
        cursor: Dict[str, Any],
    ) -> None:
        """Persist cursor state for a stream/partition.

        Written both to the in-run cache and to the on-disk checkpoint so the
        next run resumes from it. Partitioned cursors share one document per
        ``(pipeline, stream)`` (the store's documented scope); partitions are
        not exercised today.
        """
        key = self._cursor_key(stream_name, partition or {})
        with self.lock:
            self._cursors[key] = cursor
            self._cursor_store.set(self.pipeline_id, stream_name, cursor.get("cursor"))

    def get_run_info(self) -> Dict[str, Any]:
        """Get current run information."""
        return {"run_id": self.current_run_id} if self.current_run_id else {}
