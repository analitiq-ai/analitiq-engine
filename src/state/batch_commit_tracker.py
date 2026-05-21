"""Batch commit idempotency tracker.

Tracks committed ``(run_id, stream_id, batch_seq)`` triples on disk so a
retried pipeline can skip batches that have already been written. The
runtime layer is responsible for calling :meth:`record` after each
successful destination write and :meth:`is_committed` before re-sending
a batch.
"""

from __future__ import annotations

import json
import logging
import threading
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BatchCommitRecord:
    """One committed batch. Used as the persisted line shape."""

    run_id: str
    stream_id: str
    batch_seq: int
    committed_at: str
    record_count: int = 0
    cursor: Optional[Dict[str, Any]] = None


class BatchCommitTracker:
    """File-backed idempotency tracker for batch commits.

    State is persisted as one JSON line per committed batch under
    ``{pipeline_dir}/state/batch_commits/{run_id}.jsonl``. The format is
    intentionally append-only: a crash in the middle of a write loses
    only the partially-written record.
    """

    def __init__(self, *, pipeline_dir: str, run_id: str) -> None:
        self._pipeline_dir = Path(pipeline_dir)
        self._run_id = run_id
        self._dir = self._pipeline_dir / "state" / "batch_commits"
        self._dir.mkdir(parents=True, exist_ok=True)
        self._file = self._dir / f"{run_id}.jsonl"
        self._lock = threading.RLock()
        self._committed: set[tuple[str, int]] = set()
        self._records: Dict[tuple[str, int], BatchCommitRecord] = {}
        self._load()

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def file_path(self) -> Path:
        return self._file

    def _load(self) -> None:
        if not self._file.is_file():
            return
        try:
            with self._file.open() as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError as err:
                        logger.warning(
                            "BatchCommitTracker: skipping malformed line in %s: %s",
                            self._file,
                            err,
                        )
                        continue
                    key = (record["stream_id"], int(record["batch_seq"]))
                    self._committed.add(key)
                    self._records[key] = BatchCommitRecord(
                        run_id=record.get("run_id", self._run_id),
                        stream_id=record["stream_id"],
                        batch_seq=int(record["batch_seq"]),
                        committed_at=record.get("committed_at", ""),
                        record_count=int(record.get("record_count", 0)),
                        cursor=record.get("cursor"),
                    )
        except OSError as err:
            logger.warning("BatchCommitTracker: failed to read %s: %s", self._file, err)

    def check_committed(
        self, *, stream_id: str, batch_seq: int
    ) -> Optional[BatchCommitRecord]:
        """Return the recorded commit if ``(stream_id, batch_seq)`` is already
        committed in this run, or ``None`` otherwise.

        The engine uses the truthy return value as a "skip duplicate" signal,
        so we intentionally return the record (or ``None``) rather than a
        plain boolean.
        """
        with self._lock:
            return self._records.get((stream_id, batch_seq))

    # Back-compat alias — older internal callers used ``is_committed``.
    def is_committed(self, stream_id: str, batch_seq: int) -> bool:
        return self.check_committed(stream_id=stream_id, batch_seq=batch_seq) is not None

    def record_commit(
        self,
        *,
        stream_id: str,
        batch_seq: int,
        records_written: int = 0,
        cursor_bytes: bytes = b"",
        committed_at: Optional[str] = None,
    ) -> BatchCommitRecord:
        """Persist a successful batch commit so retries can detect it."""
        from datetime import datetime, timezone

        cursor_payload: Optional[Dict[str, Any]] = None
        if cursor_bytes:
            try:
                cursor_payload = json.loads(cursor_bytes.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                # Cursor is opaque bytes from gRPC — store hex when not JSON.
                cursor_payload = {"hex": cursor_bytes.hex()}

        record = BatchCommitRecord(
            run_id=self._run_id,
            stream_id=stream_id,
            batch_seq=batch_seq,
            committed_at=committed_at or datetime.now(timezone.utc).isoformat(),
            record_count=records_written,
            cursor=cursor_payload,
        )
        with self._lock:
            with self._file.open("a") as fh:
                fh.write(json.dumps(asdict(record)) + "\n")
            self._committed.add((stream_id, batch_seq))
            self._records[(stream_id, batch_seq)] = record
        return record

    def list_committed(self) -> List[tuple[str, int]]:
        with self._lock:
            return sorted(self._committed)
