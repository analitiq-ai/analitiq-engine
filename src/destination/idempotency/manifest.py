"""Manifest-based idempotency tracking for file destinations.

The manifest file tracks which batches have been successfully committed,
allowing the handler to detect and skip duplicate batches.
"""

import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

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
    committed_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> dict[str, Any]:
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
    def from_dict(cls, data: dict[str, Any]) -> "BatchCommit":
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
    """Tracks committed batches using a manifest file.

    Dedup is content-based: the key is a hash of the batch's sorted
    record_ids, not the positional (run_id, stream_id, batch_seq) tuple.
    Content-based dedup handles the same-RUN_ID restart correctly: when the
    source resumes from the committed cursor, new rows have different
    record_ids and therefore a different key — they are written rather than
    skipped.  An in-run replay of the exact same batch (ACK lost after write)
    produces the same record_ids and the same key and is correctly no-op'd.
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
        self._commits: dict[str, BatchCommit] = {}
        self._loaded = False

    def _make_key(self, run_id: str, stream_id: str, record_ids: list[str]) -> str:
        """Derive a content-based dedup key from the batch's record identities.

        Sorting before hashing makes the key independent of record order within
        the batch.  The same logical rows hash identically across an in-run
        replay (ACK lost after write) and across a same-RUN_ID restart that
        re-reads the same cursor window.  A restart that advances past the
        committed cursor produces different record_ids and therefore a different
        key, so those new rows are written rather than skipped (issue #306).
        """
        content = "|".join(sorted(record_ids)).encode()
        content_hash = hashlib.sha256(content).hexdigest()[:16]
        return f"{run_id}:{stream_id}:{content_hash}"

    async def load(self) -> None:
        """Load the manifest from storage.

        A missing manifest is a legitimate fresh start. A corrupted or
        unreadable manifest is fatal — silently emptying ``_commits``
        would let previously-committed batches re-write on the next
        run, breaking the idempotency contract.
        """
        if not await self._storage.file_exists(self._manifest_path):
            logger.info("No existing manifest found, starting fresh")
            self._loaded = True
            return

        data = await self._storage.read_file(self._manifest_path)
        try:
            manifest = json.loads(data.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise RuntimeError(
                f"Manifest at {self._manifest_path} is corrupted; refusing "
                f"to start fresh because that would re-write already-committed "
                f"batches. Inspect the file and remove or repair it manually."
            ) from e

        for commit_data in manifest.get("commits", []):
            commit = BatchCommit.from_dict(commit_data)
            key = self._make_key(commit.run_id, commit.stream_id, commit.batch_seq)
            self._commits[key] = commit

        logger.info(f"Loaded manifest with {len(self._commits)} commits")
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
        record_ids: list[str],
    ) -> BatchCommit | None:
        """Check if an identical batch has already been committed.

        Dedup is by content (record_ids), not position, so a same-RUN_ID
        restart that re-sequences the same rows returns the prior commit
        while new rows (after the committed cursor) produce a new key and
        return None.
        """
        if not self._loaded:
            await self.load()

        key = self._make_key(run_id, stream_id, record_ids)
        return self._commits.get(key)

    async def record_commit(
        self,
        run_id: str,
        stream_id: str,
        batch_seq: int,
        record_ids: list[str],
        records_written: int,
        cursor_bytes: bytes,
        file_path: str,
    ) -> None:
        """Record a successful batch commit keyed by content.

        ``batch_seq`` is stored for audit/debugging but is not the dedup key —
        content identity (``record_ids``) is.
        """
        commit = BatchCommit(
            run_id=run_id,
            stream_id=stream_id,
            batch_seq=batch_seq,
            records_written=records_written,
            cursor_bytes=cursor_bytes,
            file_path=file_path,
        )

        key = self._make_key(run_id, stream_id, record_ids)
        self._commits[key] = commit

        await self.save()

        logger.debug(f"Recorded commit: {key}")
