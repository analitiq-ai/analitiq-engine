"""Manifest-based idempotency tracking for file destinations.

The manifest file tracks which batches have been successfully committed,
allowing the handler to detect and skip duplicate batches.
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from ..storage.base import BaseStorageBackend


logger = logging.getLogger(__name__)


@dataclass
class BatchCommit:
    """Record of a committed batch."""

    run_id: str
    stream_id: str
    batch_seq: int
    records_written: int
    cursor_bytes: bytes
    file_path: str
    committed_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "run_id": self.run_id,
            "stream_id": self.stream_id,
            "batch_seq": self.batch_seq,
            "records_written": self.records_written,
            "cursor_bytes": self.cursor_bytes.hex(),
            "file_path": self.file_path,
            "committed_at": self.committed_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BatchCommit":
        """Create from dictionary."""
        return cls(
            run_id=data["run_id"],
            stream_id=data["stream_id"],
            batch_seq=data["batch_seq"],
            records_written=data["records_written"],
            cursor_bytes=bytes.fromhex(data["cursor_bytes"]),
            file_path=data["file_path"],
            committed_at=data.get("committed_at", ""),
        )


class ManifestTracker:
    """
    Tracks committed batches using a manifest file.

    The manifest is a JSON file that records all successfully committed
    batches for a given stream. It's used to:
    1. Detect duplicate batches (idempotency)
    2. Track the cursor for each committed batch
    3. Provide audit trail of writes

    Manifest structure:
    {
        "version": 1,
        "stream_id": "...",
        "commits": [
            {
                "run_id": "...",
                "stream_id": "...",
                "batch_seq": 1,
                "records_written": 100,
                "cursor_bytes": "...",
                "file_path": "...",
                "committed_at": "2024-01-01T00:00:00"
            }
        ]
    }
    """

    MANIFEST_VERSION = 1
    MANIFEST_FILENAME = "_manifest.json"

    def __init__(self, storage: BaseStorageBackend, base_path: str) -> None:
        """
        Initialize the manifest tracker.

        Args:
            storage: Storage backend to use for manifest operations
            base_path: Base path for the manifest file
        """
        self._storage = storage
        self._base_path = base_path
        self._manifest_path = f"{base_path}/{self.MANIFEST_FILENAME}"
        self._commits: Dict[str, BatchCommit] = {}
        self._loaded = False

    def _make_key(self, run_id: str, stream_id: str, batch_seq: int) -> str:
        """Create a unique key for the batch."""
        return f"{run_id}:{stream_id}:{batch_seq}"

    async def load(self) -> None:
        """Load the manifest from storage."""
        try:
            if await self._storage.file_exists(self._manifest_path):
                data = await self._storage.read_file(self._manifest_path)
                manifest = json.loads(data.decode("utf-8"))

                # Load commits
                for commit_data in manifest.get("commits", []):
                    commit = BatchCommit.from_dict(commit_data)
                    key = self._make_key(commit.run_id, commit.stream_id, commit.batch_seq)
                    self._commits[key] = commit

                logger.info(f"Loaded manifest with {len(self._commits)} commits")
            else:
                logger.info("No existing manifest found, starting fresh")

        except Exception as e:
            logger.warning(f"Error loading manifest, starting fresh: {e}")
            self._commits = {}

        self._loaded = True

    async def save(self) -> None:
        """Save the manifest to storage."""
        manifest = {
            "version": self.MANIFEST_VERSION,
            "commits": [commit.to_dict() for commit in self._commits.values()],
        }

        data = json.dumps(manifest, indent=2).encode("utf-8")
        await self._storage.write_file(self._manifest_path, data)
        logger.debug(f"Saved manifest with {len(self._commits)} commits")

    async def check_committed(
        self,
        run_id: str,
        stream_id: str,
        batch_seq: int,
    ) -> Optional[BatchCommit]:
        """
        Check if a batch has already been committed.

        Args:
            run_id: Pipeline run identifier
            stream_id: Stream identifier
            batch_seq: Batch sequence number

        Returns:
            BatchCommit if already committed, None otherwise
        """
        if not self._loaded:
            await self.load()

        key = self._make_key(run_id, stream_id, batch_seq)
        return self._commits.get(key)

    async def record_commit(
        self,
        run_id: str,
        stream_id: str,
        batch_seq: int,
        records_written: int,
        cursor_bytes: bytes,
        file_path: str,
    ) -> None:
        """
        Record a successful batch commit.

        Args:
            run_id: Pipeline run identifier
            stream_id: Stream identifier
            batch_seq: Batch sequence number
            records_written: Number of records written
            cursor_bytes: Cursor bytes for the batch
            file_path: Path where data was written
        """
        commit = BatchCommit(
            run_id=run_id,
            stream_id=stream_id,
            batch_seq=batch_seq,
            records_written=records_written,
            cursor_bytes=cursor_bytes,
            file_path=file_path,
        )

        key = self._make_key(run_id, stream_id, batch_seq)
        self._commits[key] = commit

        # Save manifest after each commit for durability
        await self.save()

        logger.debug(f"Recorded commit: {key}")

    def get_all_commits(self) -> List[BatchCommit]:
        """Get all commits in the manifest."""
        return list(self._commits.values())

    def get_commits_for_stream(self, stream_id: str) -> List[BatchCommit]:
        """Get all commits for a specific stream."""
        return [c for c in self._commits.values() if c.stream_id == stream_id]
