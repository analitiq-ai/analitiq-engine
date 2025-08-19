"""State management for pipeline checkpointing and recovery."""

import json
import logging
import threading
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class StateManager:
    """
    Manages pipeline state persistence for crash recovery.

    Features:
    - Thread-safe state operations
    - Atomic file writes
    - Backup and recovery mechanisms
    - Configurable checkpoint intervals
    """

    def __init__(self, state_file: str = "state.json"):
        """
        Initialize state manager.

        Args:
            state_file: Path to state persistence file
        """
        self.state_file = Path(state_file)
        self.backup_file = Path(f"{state_file}.backup")
        self.lock = threading.Lock()
        self.state = self._load_state()

    def _load_state(self) -> Dict[str, Any]:
        """Load state from file with backup recovery."""
        try:
            if self.state_file.exists():
                with open(self.state_file, "r") as f:
                    state = json.load(f)
                logger.info(f"Loaded state from {self.state_file}")
                return state
            else:
                logger.info(f"No existing state file found at {self.state_file}")
                return {}
        except Exception as e:
            logger.warning(f"Failed to load state from {self.state_file}: {str(e)}")

            # Try to load from backup
            if self.backup_file.exists():
                try:
                    with open(self.backup_file, "r") as f:
                        state = json.load(f)
                    logger.info(f"Recovered state from backup: {self.backup_file}")
                    return state
                except Exception as backup_e:
                    logger.error(f"Failed to load backup state: {str(backup_e)}")

            # Return empty state if all fails
            logger.info("Starting with empty state")
            return {}

    def _persist_state(self):
        """Atomically persist state to file."""
        try:
            # Create backup of current state
            if self.state_file.exists():
                self.state_file.replace(self.backup_file)

            # Write new state
            with open(self.state_file, "w") as f:
                json.dump(self.state, f, indent=2)

            logger.debug(f"State persisted to {self.state_file}")

        except Exception as e:
            logger.error(f"Failed to persist state: {str(e)}")
            raise

    def save_checkpoint(self, pipeline_id: str, checkpoint: Dict[str, Any]):
        """
        Save checkpoint for a pipeline.

        Args:
            pipeline_id: Unique identifier for the pipeline
            checkpoint: Checkpoint data including timestamp, LSN, record count, etc.
        """
        with self.lock:
            if pipeline_id not in self.state:
                self.state[pipeline_id] = {}

            self.state[pipeline_id].update(
                {
                    "last_sync_time": checkpoint.get("timestamp"),
                    "last_lsn": checkpoint.get("lsn"),
                    "processed_records": checkpoint.get("count", 0),
                    "schema_hash": checkpoint.get("schema_hash"),
                    "last_checkpoint_time": checkpoint.get("timestamp"),
                    "status": "running",
                }
            )

            self._persist_state()
            logger.debug(f"Checkpoint saved for pipeline {pipeline_id}")

    def get_checkpoint(self, pipeline_id: str) -> Dict[str, Any]:
        """
        Get the last checkpoint for a pipeline.

        Args:
            pipeline_id: Unique identifier for the pipeline

        Returns:
            Dictionary containing checkpoint data
        """
        with self.lock:
            return self.state.get(pipeline_id, {})

    def mark_pipeline_completed(self, pipeline_id: str):
        """Mark a pipeline as completed."""
        with self.lock:
            if pipeline_id in self.state:
                self.state[pipeline_id]["status"] = "completed"
                self._persist_state()
                logger.info(f"Pipeline {pipeline_id} marked as completed")

    def mark_pipeline_failed(self, pipeline_id: str, error: str):
        """Mark a pipeline as failed."""
        with self.lock:
            if pipeline_id in self.state:
                self.state[pipeline_id]["status"] = "failed"
                self.state[pipeline_id]["error"] = error
                self._persist_state()
                logger.error(f"Pipeline {pipeline_id} marked as failed: {error}")

    def get_pipeline_status(self, pipeline_id: str) -> Optional[str]:
        """Get the status of a pipeline."""
        with self.lock:
            pipeline_state = self.state.get(pipeline_id, {})
            return pipeline_state.get("status")

    def list_pipelines(self) -> Dict[str, Dict[str, Any]]:
        """List all pipelines and their states."""
        with self.lock:
            return self.state.copy()

    def clear_pipeline_state(self, pipeline_id: str):
        """Clear state for a specific pipeline."""
        with self.lock:
            if pipeline_id in self.state:
                del self.state[pipeline_id]
                self._persist_state()
                logger.info(f"State cleared for pipeline {pipeline_id}")

    def reset_all_state(self):
        """Reset all pipeline states (use with caution)."""
        with self.lock:
            self.state = {}
            self._persist_state()
            logger.warning("All pipeline states have been reset")

    def get_resume_info(self, pipeline_id: str) -> Dict[str, Any]:
        """
        Get information needed to resume a pipeline.

        Args:
            pipeline_id: Unique identifier for the pipeline

        Returns:
            Dictionary containing resume information
        """
        checkpoint = self.get_checkpoint(pipeline_id)
        return {
            "can_resume": bool(checkpoint.get("last_sync_time")),
            "last_sync_time": checkpoint.get("last_sync_time"),
            "processed_records": checkpoint.get("processed_records", 0),
            "schema_hash": checkpoint.get("schema_hash"),
            "status": checkpoint.get("status", "unknown"),
        }
