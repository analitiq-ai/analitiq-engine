"""State management for pipeline checkpointing and recovery."""

import hashlib
import json
import logging
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from .state_storage import (
    LocalStateStorage,
    StateStorageBackend,
    StateStorageSettings,
    create_storage_backend,
)

logger = logging.getLogger(__name__)


class StateManager:
    """
    Manages per-stream/partition state for scalable pipeline recovery.

    Features:
    - Separate state files per stream/partition for concurrency
    - Atomic writes with locking
    - Multi-partition support for parallel processing
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
        self.state_path = "state.json"
        self.lock_path = "lock"

        # For local storage compatibility
        self.base_dir = Path(base_dir)
        self.pipeline_dir = self.base_dir / pipeline_id
        self.streams_dir = self.pipeline_dir / "streams"
        self.state_file = self.pipeline_dir / "state.json"
        self.lock_file = self.pipeline_dir / "lock"

        # Thread safety
        self.lock = threading.RLock()

        # Create directory structure
        self._ensure_directories()

    def _ensure_directories(self):
        """Create necessary directory structure."""
        self.storage.ensure_directories([self.streams_path])

    def _get_partition_path(self, stream_name: str, partition: Dict[str, Any]) -> str:
        """Get state file path for a specific stream partition."""
        if not partition:
            partition_id = "default"
        else:
            partition_str = json.dumps(partition, sort_keys=True)
            partition_id = hashlib.md5(partition_str.encode()).hexdigest()[:8]

        return f"{self.streams_path}/{stream_name}/partition-{partition_id}.json"

    def _get_partition_file(self, stream_name: str, partition: Dict[str, Any]) -> Path:
        """Get state file path for a specific stream partition (legacy compatibility)."""
        if not partition:
            partition_id = "default"
        else:
            partition_str = json.dumps(partition, sort_keys=True)
            partition_id = hashlib.md5(partition_str.encode()).hexdigest()[:8]

        return self.streams_dir / stream_name / f"partition-{partition_id}.json"

    def _load_state(self) -> Dict[str, Any]:
        """Load the pipeline state manifest."""
        try:
            state = self.storage.read_json(self.state_path)
            if state:
                return state
        except Exception as e:
            logger.warning(f"Failed to load pipeline state: {e}")

        return {"version": 1, "streams": {}, "run": {}}

    def _save_state(self, state: Dict[str, Any]):
        """Atomically save the pipeline state."""
        self.storage.write_json(self.state_path, state)

    def _load_partition_state(
        self, partition_path_or_file: Union[str, Path]
    ) -> Optional[Dict[str, Any]]:
        """Load state for a specific partition."""
        try:
            if isinstance(partition_path_or_file, Path):
                # Legacy local file path
                if partition_path_or_file.exists():
                    with open(partition_path_or_file, "r") as f:
                        return json.load(f)
            else:
                # Storage backend path
                return self.storage.read_json(partition_path_or_file)
        except Exception as e:
            logger.error(f"Failed to load partition state from {partition_path_or_file}: {e}")

        return None

    def _save_partition_state(
        self, partition_path_or_file: Union[str, Path], state: Dict[str, Any]
    ):
        """Atomically save partition state."""
        if isinstance(partition_path_or_file, Path):
            # Legacy local file handling
            partition_path_or_file.parent.mkdir(parents=True, exist_ok=True)
            temp_file = partition_path_or_file.with_suffix(".tmp")

            try:
                with open(temp_file, "w") as f:
                    json.dump(state, f, indent=2)
                temp_file.replace(partition_path_or_file)
                logger.debug(f"Saved partition state to {partition_path_or_file}")
            except Exception as e:
                if temp_file.exists():
                    temp_file.unlink()
                raise e
        else:
            # Storage backend
            self.storage.write_json(partition_path_or_file, state)


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
                run_id = f"{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}-{uuid.uuid4().hex[:4]}"

            # Load existing index
            index = self._load_state()

            # Update run metadata
            index["run"] = {
                "run_id": run_id,
                "pipeline_id": config.get("pipeline_id"),
                "lease_owner": f"worker-{threading.get_ident()}",
                "started_at": datetime.now(timezone.utc).isoformat(),
                "checkpoint_seq": index.get("run", {}).get("checkpoint_seq", 0),
            }

            self._save_state(index)
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
        Save checkpoint for a specific stream partition.

        Args:
            stream_name: Name of the stream (e.g., "wise.transactions")
            partition: Partition key dict (empty {} for single partition)
            cursor: Cursor state with primary/tiebreaker fields
            hwm: High-water mark timestamp
            page_state: Optional pagination state
            http_conditionals: Optional HTTP conditional headers
            stats: Optional sync statistics
        """
        with self.lock:
            partition_path = self._get_partition_path(stream_name, partition)

            # Build partition state
            partition_state = {
                "partition": partition,
                "cursor": cursor,
                "hwm": hwm,
                "last_updated": datetime.now(timezone.utc).isoformat(),
            }

            if page_state:
                partition_state["page_state"] = page_state

            if http_conditionals:
                partition_state["http_conditionals"] = http_conditionals

            if stats:
                partition_state["stats"] = stats
            else:
                partition_state["stats"] = {
                    "records_synced": 0,
                    "batches_written": 0,
                    "last_checkpoint_at": datetime.now(timezone.utc).isoformat(),
                    "errors_since_checkpoint": 0,
                }

            # Save partition state
            self._save_partition_state(partition_path, partition_state)

            # Update index
            index = self._load_state()

            if stream_name not in index["streams"]:
                index["streams"][stream_name] = {"partitions": []}

            # Update partition list in index
            stream_info = index["streams"][stream_name]
            partition_exists = False

            for i, p in enumerate(stream_info["partitions"]):
                if p["partition"] == partition:
                    stream_info["partitions"][i] = {
                        "partition": partition,
                        "file": partition_path,
                    }
                    partition_exists = True
                    break

            if not partition_exists:
                stream_info["partitions"].append(
                    {"partition": partition, "file": partition_path}
                )

            # Increment checkpoint sequence
            index["run"]["checkpoint_seq"] = index["run"].get("checkpoint_seq", 0) + 1

            self._save_state(index)
            logger.debug(f"Saved checkpoint for {stream_name} partition {partition}")

    def get_stream_partitions(self, stream_name: str) -> List[Dict[str, Any]]:
        """
        Get all partition states for a stream.

        Args:
            stream_name: Name of the stream

        Returns:
            List of partition state dictionaries
        """
        with self.lock:
            index = self._load_state()
            stream_info = index["streams"].get(stream_name, {})
            partitions = []

            for partition_ref in stream_info.get("partitions", []):
                partition_path = partition_ref["file"]
                partition_state = self._load_partition_state(partition_path)

                if partition_state:
                    partitions.append(partition_state)

            return partitions

    def get_partition_state(
        self, stream_name: str, partition: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Get state for a specific stream partition.

        Args:
            stream_name: Name of the stream
            partition: Partition key dict

        Returns:
            Partition state dict or None if not found
        """
        with self.lock:
            partition_path = self._get_partition_path(stream_name, partition)
            return self._load_partition_state(partition_path)

    def get_run_info(self) -> Dict[str, Any]:
        """Get current run information."""
        with self.lock:
            index = self._load_state()
            return index.get("run", {})

    def list_streams(self) -> List[str]:
        """Get list of all streams with state."""
        with self.lock:
            index = self._load_state()
            return list(index.get("streams", {}).keys())

    def clear_stream_state(self, stream_name: str):
        """Clear all state for a stream."""
        with self.lock:
            index = self._load_state()

            # Remove stream from index
            if stream_name in index["streams"]:
                stream_info = index["streams"][stream_name]

                # Delete partition files
                for partition_ref in stream_info.get("partitions", []):
                    partition_path = partition_ref["file"]
                    self.storage.delete(partition_path)

                del index["streams"][stream_name]
                self._save_state(index)

            logger.info(f"Cleared state for stream {stream_name}")

    def clear_all_state(self):
        """Clear all pipeline state (use with caution)."""
        with self.lock:
            # Remove streams directory
            self.storage.delete_recursive(self.streams_path)

            # Remove index
            self.storage.delete(self.state_path)

            # Remove lock
            self.storage.delete(self.lock_path)

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
            partitions = self.get_stream_partitions(stream_name)
            run_info = self.get_run_info()

            total_records = sum(
                p.get("stats", {}).get("records_synced", 0) for p in partitions
            )

            can_resume = bool(partitions and run_info.get("run_id"))

            return {
                "can_resume": can_resume,
                "partition_count": len(partitions),
                "total_records_synced": total_records,
                "run_id": run_info.get("run_id"),
                "last_checkpoint_seq": run_info.get("checkpoint_seq", 0),
                "partitions": [
                    {
                        "partition": p["partition"],
                        "cursor": p.get("cursor"),
                        "hwm": p.get("hwm"),
                        "records_synced": p.get("stats", {}).get("records_synced", 0),
                    }
                    for p in partitions
                ],
            }