"""State management for pipeline checkpointing and recovery."""

import json
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..shared.run_id import get_or_generate_run_id
from .batch_commit_tracker import BatchCommitTracker
from .state_emission import emit_state_log
from .state_storage import (
    LocalStateStorage,
    StateStorageBackend,
    StateStorageSettings,
    create_storage_backend,
)

logger = logging.getLogger(__name__)


class StateManager:
    """
    Manages per-stream state for pipeline recovery.

    Features:
    - Single state file per stream
    - Atomic writes with locking
    - S3 storage for cloud environments (dev/prod)
    """

    def __init__(
        self,
        pipeline_id: str,
        base_dir: str = "state",
        settings: Optional[StateStorageSettings] = None,
        storage_backend: Optional[StateStorageBackend] = None,
    ):
        """
        Initialize state manager.

        The storage backend is determined automatically based on the ENV environment variable:
        - ENV=local (default): Uses local filesystem storage
        - ENV=dev or ENV=prod: Uses S3 storage

        Args:
            pipeline_id: Unique pipeline identifier
            base_dir: Base directory for state files (used for local storage)
            settings: Optional storage settings (if None, auto-detected from environment)
            storage_backend: Optional pre-configured storage backend (takes precedence over settings)
        """
        self.pipeline_id = pipeline_id

        # Determine storage backend
        if storage_backend:
            # Explicit backend provided (useful for testing)
            self.storage = storage_backend
            self._is_cloud_mode = not isinstance(storage_backend, LocalStateStorage)
            self._settings = None
        elif settings:
            # Explicit settings provided
            self.storage = create_storage_backend(settings)
            self._is_cloud_mode = settings.is_cloud_mode
            self._settings = settings
        else:
            # Auto-detect from environment
            self._settings = StateStorageSettings.from_env()
            # Override pipeline_id from parameter (takes precedence over env var)
            self._settings.pipeline_id = pipeline_id
            # Override local base dir if provided
            if base_dir != "state":
                self._settings.local_base_dir = base_dir

            self._is_cloud_mode = self._settings.is_cloud_mode
            self.storage = create_storage_backend(self._settings)

        # Path prefixes (relative to storage root)
        # For local: paths are relative to base_dir/{pipeline_id}/
        # For S3: paths are relative to {client_id}/{pipeline_id}/ prefix
        self.streams_path = "streams"
        self.state_path = None
        self.lock_path = None

        # For local storage compatibility
        self.base_dir = Path(base_dir)
        self.pipeline_dir = self.base_dir / pipeline_id
        self.streams_dir = self.pipeline_dir / "streams"
        self.state_file = None
        self.lock_file = None

        # Thread safety
        self.lock = threading.RLock()

        # Current run ID (from env var RUN_ID if available)
        self.current_run_id: Optional[str] = os.environ.get("RUN_ID")

        # In-run batch commit tracker (initialized by init_commit_tracker)
        self._commit_tracker: Optional[BatchCommitTracker] = None

        # Create directory structure
        self._ensure_directories()

    def _ensure_directories(self):
        """Create necessary directory structure."""
        self.storage.ensure_directories([self.streams_path])

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

    def _get_stream_state_path(self, stream_id: str) -> str:
        """Get state file path for a specific stream."""
        return f"{self.streams_path}/{stream_id}/state.json"

    def _load_stream_state(self, stream_id: str) -> Optional[Dict[str, Any]]:
        """Load state for a specific stream."""
        try:
            return self.storage.read_json(self._get_stream_state_path(stream_id))
        except Exception as e:
            logger.error(f"Failed to load stream state for {stream_id}: {e}")
        return None

    def _save_stream_state(self, stream_id: str, state: Dict[str, Any]) -> None:
        """Atomically save stream state."""
        self.storage.write_json(self._get_stream_state_path(stream_id), state)


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
            # Priority: explicit run_id param > env var RUN_ID (via get_or_generate_run_id)
            if not run_id:
                run_id = self.current_run_id or get_or_generate_run_id()

            # Store as current run ID
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
        Save checkpoint for a specific stream.

        Args:
            stream_name: Name of the stream (e.g., "wise.transactions")
            partition: Partition key dict (ignored; single stream state only)
            cursor: Cursor state with primary/tiebreaker fields
            hwm: High-water mark timestamp
            page_state: Optional pagination state
            http_conditionals: Optional HTTP conditional headers
            stats: Optional sync statistics
        """
        with self.lock:
            if partition:
                logger.warning(
                    f"save_stream_checkpoint received non-empty partition for {stream_name}; "
                    "partitioning is not supported and will be ignored."
                )

            stream_state = {
                "version": 1,
                "stream_id": stream_name,
                "cursor": cursor,
                "hwm": hwm,
                "last_updated": datetime.now(timezone.utc).isoformat(),
            }

            if page_state:
                stream_state["page_state"] = page_state

            if http_conditionals:
                stream_state["http_conditionals"] = http_conditionals

            if stats:
                stream_state["stats"] = stats
            else:
                stream_state["stats"] = {
                    "records_synced": 0,
                    "batches_written": 0,
                    "last_checkpoint_at": datetime.now(timezone.utc).isoformat(),
                    "errors_since_checkpoint": 0,
                }

            self._save_stream_state(stream_name, stream_state)
            logger.debug(f"Saved checkpoint for {stream_name}")

            # Emit state advance to logs
        emit_state_log(
            run_id=self.current_run_id or "",
            pipeline_id=self.pipeline_id,
            stream_id=stream_name,
            cursor_hex=json.dumps(cursor).encode().hex() if cursor else "",
            cursor_value=hwm,
            client_id=os.getenv("CLIENT_ID", ""),
        )

    def get_stream_state(self, stream_id: str) -> Optional[Dict[str, Any]]:
        """Get state for a specific stream."""
        with self.lock:
            return self._load_stream_state(stream_id)

    def get_partition_state(
        self, stream_name: str, partition: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Get state for a specific stream (partitioning not supported).

        Args:
            stream_name: Name of the stream
            partition: Partition key dict (ignored)

        Returns:
            Stream state dict or None if not found
        """
        with self.lock:
            if partition:
                logger.warning(
                    f"get_partition_state received non-empty partition for {stream_name}; "
                    "partitioning is not supported and will be ignored."
                )
            return self._load_stream_state(stream_name)

    def get_run_info(self) -> Dict[str, Any]:
        """Get current run information."""
        return {"run_id": self.current_run_id} if self.current_run_id else {}

    def list_streams(self) -> List[str]:
        """Get list of all streams with state."""
        with self.lock:
            streams_dir = self.base_dir / self.pipeline_id / self.streams_path
            if not streams_dir.exists():
                return []
            return [p.name for p in streams_dir.iterdir() if p.is_dir()]

    def clear_stream_state(self, stream_name: str):
        """Clear all state for a stream."""
        with self.lock:
            self.storage.delete_recursive(f"{self.streams_path}/{stream_name}")
            logger.info(f"Cleared state for stream {stream_name}")

    def clear_all_state(self):
        """Clear all pipeline state (use with caution)."""
        with self.lock:
            # Remove streams directory
            self.storage.delete_recursive(self.streams_path)

            logger.warning(f"Cleared all state for pipeline {self.pipeline_id}")

    def get_resume_info(self, stream_name: str) -> Dict[str, Any]:
        """
        Get information needed to resume a stream.

        Args:
            stream_name: Name of the stream

        Returns:
            Resume information dictionary
        """
        with self.lock:
            state = self._load_stream_state(stream_name)
            can_resume = bool(state)
            records_synced = state.get("stats", {}).get("records_synced", 0) if state else 0

            return {
                "can_resume": can_resume,
                "stream_id": stream_name,
                "records_synced": records_synced,
                "run_id": self.current_run_id,
                "cursor": state.get("cursor") if state else None,
                "hwm": state.get("hwm") if state else None,
            }

    async def get_cursor(
        self, stream_name: str, partition: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Get cursor state for a specific stream.

        Convenience method for database connectors that only need cursor information.

        Args:
            stream_name: Name of the stream
            partition: Partition key dict (ignored)

        Returns:
            Cursor state dict with "cursor" key, or None if not found
        """
        state = self.get_partition_state(stream_name, partition)
        if not state:
            return None

        # Extract cursor value from the full partition state
        cursor_info = state.get("cursor", {})
        primary = cursor_info.get("primary", {})
        cursor_value = primary.get("value") if primary else state.get("hwm")

        if cursor_value is not None:
            return {"cursor": cursor_value}
        return None

    async def save_cursor(
        self,
        stream_name: str,
        partition: Dict[str, Any],
        cursor_state: Dict[str, Any],
    ):
        """
        Save cursor state for a specific stream.

        Convenience method for database connectors. Wraps save_stream_checkpoint
        with simplified cursor-only interface.

        Args:
            stream_name: Name of the stream
            partition: Partition key dict (ignored)
            cursor_state: Dict with "cursor" key containing the cursor value
        """
        cursor_value = cursor_state.get("cursor")
        if cursor_value is None:
            logger.warning(f"save_cursor called with no cursor value for {stream_name}")
            return

        # Build cursor structure expected by save_stream_checkpoint
        cursor = {
            "primary": {
                "field": "replication_key",
                "value": cursor_value,
                "inclusive": True,
            }
        }

        self.save_stream_checkpoint(
            stream_name=stream_name,
            partition=partition,
            cursor=cursor,
            hwm=str(cursor_value),
        )
