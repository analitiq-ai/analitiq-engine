"""State management for pipeline checkpointing and recovery."""

import hashlib
import json
import logging
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from ..models.state import PipelineFingerprint

logger = logging.getLogger(__name__)


class StateManager:
    """
    Manages per-stream/partition state for scalable pipeline recovery.

    Features:
    - Separate state files per stream/partition for concurrency
    - Config immutability validation via fingerprint
    - Atomic writes with locking
    - Schema drift detection via hashing
    - Multi-partition support for parallel processing
    """

    def __init__(self, pipeline_id: str, base_dir: str = "state"):
        """
        Initialize state manager.

        Args:
            pipeline_id: Unique pipeline identifier
            base_dir: Base directory for state files
        """
        self.pipeline_id = pipeline_id
        self.base_dir = Path(base_dir)
        self.pipeline_dir = self.base_dir / pipeline_id / "v1"
        self.streams_dir = self.pipeline_dir / "streams"
        self.index_file = self.pipeline_dir / "index.json"
        self.lock_file = self.pipeline_dir / "lock"
        
        # Thread safety
        self.lock = threading.RLock()
        
        # Create directory structure
        self._ensure_directories()
        
    def _ensure_directories(self):
        """Create necessary directory structure."""
        self.streams_dir.mkdir(parents=True, exist_ok=True)
        
    def _compute_config_fingerprint(self, config: Dict[str, Any]) -> str:
        """Compute SHA256 fingerprint of pipeline configuration."""
        fingerprint_config = PipelineFingerprint(
            pipeline_id=config.get("pipeline_id", ""),
            version=config.get("version", ""),
            src=config.get("src", {}),
            dst=config.get("dst", {}),
            streams=config.get("streams", {})
        )
        return fingerprint_config.fingerprint()
        
    def _compute_schema_hash(self, schema: Dict[str, Any]) -> str:
        """Compute SHA256 hash of schema structure."""
        schema_str = json.dumps(schema, sort_keys=True)
        return f"sha256:{hashlib.sha256(schema_str.encode()).hexdigest()[:16]}"
        
    def _get_partition_file(self, stream_name: str, partition: Dict[str, Any]) -> Path:
        """Get state file path for a specific stream partition."""
        # Generate partition identifier
        if not partition:
            partition_id = "default"
        else:
            partition_str = json.dumps(partition, sort_keys=True)
            partition_id = hashlib.md5(partition_str.encode()).hexdigest()[:8]
            
        return self.streams_dir / stream_name / f"partition-{partition_id}.json"
        
    def _load_index(self) -> Dict[str, Any]:
        """Load the state index manifest."""
        try:
            if self.index_file.exists():
                with open(self.index_file, "r") as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load state index: {e}")
            
        return {
            "version": 1,
            "streams": {},
            "run": {}
        }
        
    def _save_index(self, index: Dict[str, Any]):
        """Atomically save the state index."""
        temp_file = self.index_file.with_suffix(".tmp")
        try:
            with open(temp_file, "w") as f:
                json.dump(index, f, indent=2)
            temp_file.replace(self.index_file)
        except Exception as e:
            if temp_file.exists():
                temp_file.unlink()
            raise e
            
    def _load_partition_state(self, partition_file: Path) -> Optional[Dict[str, Any]]:
        """Load state for a specific partition."""
        try:
            if partition_file.exists():
                with open(partition_file, "r") as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load partition state from {partition_file}: {e}")
            
        return None
        
    def _save_partition_state(self, partition_file: Path, state: Dict[str, Any]):
        """Atomically save partition state."""
        partition_file.parent.mkdir(parents=True, exist_ok=True)
        temp_file = partition_file.with_suffix(".tmp")
        
        try:
            with open(temp_file, "w") as f:
                json.dump(state, f, indent=2)
            temp_file.replace(partition_file)
            logger.debug(f"Saved partition state to {partition_file}")
        except Exception as e:
            if temp_file.exists():
                temp_file.unlink()
            raise e
            
    def start_run(self, config: Dict[str, Any], run_id: Optional[str] = None) -> str:
        """
        Start a new pipeline run and validate config compatibility.
        
        Args:
            config: Pipeline configuration
            run_id: Optional run ID, generated if not provided
            
        Returns:
            The run ID for this execution
            
        Raises:
            ValueError: If config fingerprint doesn't match existing state
        """
        with self.lock:
            if not run_id:
                run_id = f"{datetime.now(timezone.utc).isoformat()}-{uuid.uuid4().hex[:4]}"
                
            config_fingerprint = self._compute_config_fingerprint(config)
            
            # Load existing index
            index = self._load_index()
            
            # Validate config compatibility if state exists
            existing_fingerprint = index.get("run", {}).get("config_fingerprint")
            if existing_fingerprint and existing_fingerprint != config_fingerprint:
                raise ValueError(
                    f"Config fingerprint mismatch. Expected: {existing_fingerprint}, "
                    f"Got: {config_fingerprint}. Clear state or use matching config."
                )
                
            # Store config snapshot for future compatibility analysis
            fingerprint_config = PipelineFingerprint(
                pipeline_id=config.get("pipeline_id", ""),
                version=config.get("version", ""),
                src=config.get("src", {}),
                dst=config.get("dst", {}),
                streams=config.get("streams", {})
            )
            config_snapshot = fingerprint_config.model_dump(exclude_unset=True)
            
            # Update run metadata with config snapshot
            index["run"] = {
                "run_id": run_id,
                "pipeline_id": config.get("pipeline_id"),
                "lease_owner": f"worker-{threading.get_ident()}",
                "started_at": datetime.now(timezone.utc).isoformat(),
                "checkpoint_seq": index.get("run", {}).get("checkpoint_seq", 0),
                "config_fingerprint": config_fingerprint,
                "config_snapshot": config_snapshot
            }
            
            self._save_index(index)
            logger.info(f"Started pipeline run {run_id} with fingerprint {config_fingerprint}")
            
            return run_id
            
    def save_stream_checkpoint(
        self,
        stream_name: str,
        partition: Dict[str, Any],
        cursor: Dict[str, Any],
        hwm: str,
        schema: Optional[Dict[str, Any]] = None,
        page_state: Optional[Dict[str, Any]] = None,
        http_conditionals: Optional[Dict[str, Any]] = None,
        stats: Optional[Dict[str, Any]] = None
    ):
        """
        Save checkpoint for a specific stream partition.
        
        Args:
            stream_name: Name of the stream (e.g., "wise.transactions")
            partition: Partition key dict (empty {} for single partition)
            cursor: Cursor state with primary/tiebreaker fields
            hwm: High-water mark timestamp
            schema: Optional schema for drift detection
            page_state: Optional pagination state
            http_conditionals: Optional HTTP conditional headers
            stats: Optional sync statistics
        """
        with self.lock:
            partition_file = self._get_partition_file(stream_name, partition)
            
            # Build partition state
            partition_state = {
                "partition": partition,
                "cursor": cursor,
                "hwm": hwm,
                "last_updated": datetime.now(timezone.utc).isoformat()
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
                    "errors_since_checkpoint": 0
                }
                
            # Save partition state
            self._save_partition_state(partition_file, partition_state)
            
            # Update index
            index = self._load_index()
            
            if stream_name not in index["streams"]:
                index["streams"][stream_name] = {
                    "schema_hash": "",
                    "partitions": []
                }
                
            # Update schema hash if provided
            if schema:
                schema_hash = self._compute_schema_hash(schema)
                index["streams"][stream_name]["schema_hash"] = schema_hash
                
            # Update partition list in index
            stream_info = index["streams"][stream_name]
            partition_exists = False
            
            for i, p in enumerate(stream_info["partitions"]):
                if p["partition"] == partition:
                    stream_info["partitions"][i] = {
                        "partition": partition,
                        "file": str(partition_file.relative_to(self.base_dir))
                    }
                    partition_exists = True
                    break
                    
            if not partition_exists:
                stream_info["partitions"].append({
                    "partition": partition,
                    "file": str(partition_file.relative_to(self.base_dir))
                })
                
            # Increment checkpoint sequence
            index["run"]["checkpoint_seq"] = index["run"].get("checkpoint_seq", 0) + 1
            
            self._save_index(index)
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
            index = self._load_index()
            stream_info = index["streams"].get(stream_name, {})
            partitions = []
            
            for partition_ref in stream_info.get("partitions", []):
                partition_file = self.base_dir / partition_ref["file"]
                partition_state = self._load_partition_state(partition_file)
                
                if partition_state:
                    partitions.append(partition_state)
                    
            return partitions
            
    def get_partition_state(
        self, 
        stream_name: str, 
        partition: Dict[str, Any]
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
            partition_file = self._get_partition_file(stream_name, partition)
            return self._load_partition_state(partition_file)
            
    def get_run_info(self) -> Dict[str, Any]:
        """Get current run information."""
        with self.lock:
            index = self._load_index()
            return index.get("run", {})
            
    def get_stream_schema_hash(self, stream_name: str) -> Optional[str]:
        """Get schema hash for stream drift detection."""
        with self.lock:
            index = self._load_index()
            return index.get("streams", {}).get(stream_name, {}).get("schema_hash")
            
    def detect_schema_drift(self, stream_name: str, schema: Dict[str, Any]) -> bool:
        """
        Detect if schema has drifted from last checkpoint.
        
        Args:
            stream_name: Name of the stream
            schema: Current schema to check
            
        Returns:
            True if schema has drifted, False otherwise
        """
        current_hash = self._compute_schema_hash(schema)
        stored_hash = self.get_stream_schema_hash(stream_name)
        
        if not stored_hash:
            return False  # No previous schema, not a drift
            
        return current_hash != stored_hash
        
    def list_streams(self) -> List[str]:
        """Get list of all streams with state."""
        with self.lock:
            index = self._load_index()
            return list(index.get("streams", {}).keys())
            
    def clear_stream_state(self, stream_name: str):
        """Clear all state for a stream."""
        with self.lock:
            index = self._load_index()
            
            # Remove stream from index
            if stream_name in index["streams"]:
                stream_info = index["streams"][stream_name]
                
                # Delete partition files
                for partition_ref in stream_info.get("partitions", []):
                    partition_file = self.base_dir / partition_ref["file"]
                    if partition_file.exists():
                        partition_file.unlink()
                        
                # Remove stream directory if empty
                stream_dir = self.streams_dir / stream_name
                if stream_dir.exists() and not list(stream_dir.iterdir()):
                    stream_dir.rmdir()
                    
                del index["streams"][stream_name]
                self._save_index(index)
                
            logger.info(f"Cleared state for stream {stream_name}")
            
    def clear_all_state(self):
        """Clear all pipeline state (use with caution)."""
        with self.lock:
            # Remove all partition files
            if self.streams_dir.exists():
                import shutil
                shutil.rmtree(self.streams_dir)
                
            # Remove index
            if self.index_file.exists():
                self.index_file.unlink()
                
            # Remove lock
            if self.lock_file.exists():
                self.lock_file.unlink()
                
            logger.warning(f"Cleared all state for pipeline {self.pipeline_id}")
            
    def validate_config_compatibility(self, config: Dict[str, Any]) -> bool:
        """
        Validate if config is compatible with existing state.
        
        Args:
            config: Pipeline configuration to validate
            
        Returns:
            True if compatible, False otherwise
        """
        with self.lock:
            index = self._load_index()
            existing_fingerprint = index.get("run", {}).get("config_fingerprint")
            
            if not existing_fingerprint:
                return True  # No existing state, always compatible
                
            new_fingerprint = self._compute_config_fingerprint(config)
            return existing_fingerprint == new_fingerprint
            
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
                p.get("stats", {}).get("records_synced", 0) 
                for p in partitions
            )
            
            can_resume = bool(partitions and run_info.get("run_id"))
            
            return {
                "can_resume": can_resume,
                "partition_count": len(partitions),
                "total_records_synced": total_records,
                "run_id": run_info.get("run_id"),
                "config_fingerprint": run_info.get("config_fingerprint"),
                "last_checkpoint_seq": run_info.get("checkpoint_seq", 0),
                "partitions": [
                    {
                        "partition": p["partition"],
                        "cursor": p.get("cursor"),
                        "hwm": p.get("hwm"),
                        "records_synced": p.get("stats", {}).get("records_synced", 0)
                    }
                    for p in partitions
                ]
            }