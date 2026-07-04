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
    content_hash: str = ""
    committed_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "run_id": self.run_id,
            "stream_id": self.stream_id,
            "batch_seq": self.batch_seq,
            "content_hash": self.content_hash,
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
            content_hash=data.get("content_hash", ""),
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

    @staticmethod
    def _content_hash(record_ids: list[str]) -> str:
        """SHA-256 of the sorted record IDs, truncated to 16 hex chars.

        Sorting before hashing makes the digest independent of record order
        within the batch.  The same logical rows produce the same digest
        across an in-run ACK-lost replay and across a same-RUN_ID restart
        that re-reads the same cursor window.  Rows past the committed cursor
        produce a different digest, so they are written rather than skipped
        (issue #306).

        The hash is persisted in the manifest so it can be rehydrated in
        ``load()`` without re-deriving it from the original ``record_ids``.
        """
        content = "|".join(sorted(record_ids)).encode()
        # 16 hex chars = 64-bit hash space.  Birthday collision probability
        # is ~1% at ~600M distinct (run_id, stream_id, content) pairs per
        # manifest — well above expected pipeline volumes.
        return hashlib.sha256(content).hexdigest()[:16]

    def _make_key(self, run_id: str, stream_id: str, record_ids: list[str]) -> str:
        """Full dedup key: ``run_id:stream_id:<content_hash>``."""
        return f"{run_id}:{stream_id}:{self._content_hash(record_ids)}"

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

        skipped = 0
        for i, commit_data in enumerate(manifest.get("commits", [])):
            try:
                commit = BatchCommit.from_dict(commit_data)
            except (KeyError, ValueError, TypeError) as e:
                raise RuntimeError(
                    f"Manifest at {self._manifest_path} contains a malformed "
                    f"commit entry at index {i}: {e!r}. Inspect the file and "
                    f"repair or remove it manually before restarting."
                ) from e

            if not commit.content_hash:
                # Entry written by the old positional-key scheme (before issue
                # #306). The original record_ids were never persisted, so the
                # content-based key cannot be reconstructed.  Skip the entry —
                # the affected batches may be re-written on this run, which is
                # preferable to the row-drop the old scheme caused.
                skipped += 1
                continue

            key = f"{commit.run_id}:{commit.stream_id}:{commit.content_hash}"
            self._commits[key] = commit

        if skipped:
            logger.warning(
                "Manifest at %s contained %d legacy positional-key entries that "
                "could not be migrated to content-based dedup; those batches may "
                "be re-written this run.",
                self._manifest_path,
                skipped,
            )
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
        content_hash = self._content_hash(record_ids)
        commit = BatchCommit(
            run_id=run_id,
            stream_id=stream_id,
            batch_seq=batch_seq,
            content_hash=content_hash,
            records_written=records_written,
            cursor_bytes=cursor_bytes,
            file_path=file_path,
        )

        key = f"{run_id}:{stream_id}:{content_hash}"
        self._commits[key] = commit

        try:
            await self.save()
        except Exception as e:
            raise RuntimeError(
                f"Batch data was written successfully but the manifest at "
                f"{self._manifest_path} could not be updated (key={key}). "
                f"The next run may re-write this batch. Manual inspection required."
            ) from e

        logger.debug(f"Recorded commit: {key}")
