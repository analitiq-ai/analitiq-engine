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
            org_id=os.getenv("ORG_ID", ""),
        )

    def get_run_info(self) -> Dict[str, Any]:
        """Get current run information."""
        return {"run_id": self.current_run_id} if self.current_run_id else {}
