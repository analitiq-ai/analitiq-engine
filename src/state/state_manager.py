"""State management for pipeline checkpointing and recovery."""

import json
import logging
import os
import threading
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from ..shared.run_id import get_or_generate_run_id
from .batch_commit_tracker import BatchCommitTracker
from .state_emission import emit_state_log
from .store import CursorStore, load_resume_file, write_resume_file

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
        # The consolidated resume bookmark: the cloud deployment delivers it in
        # the config bundle, a local run writes it at the end (write_resume_snapshot).
        # It sits at the top of state/ (not beside the per-stream checkpoints,
        # which own the state/{pipeline_id}/{stream_id}.json namespace) so a
        # stream named "resume" can never collide with it.
        self._resume_path = self.base_dir / "resume.json"

        # Thread safety
        self.lock = threading.RLock()

        # Current run ID (from env var RUN_ID if available)
        self.current_run_id: str | None = os.environ.get("RUN_ID")

        # In-run batch commit tracker (initialized by init_commit_tracker)
        self._commit_tracker: BatchCommitTracker | None = None

        # In-run cursor cache keyed by (stream_name, partition_key). Backed by
        # an on-disk checkpoint so a fresh process resumes from where the last
        # run left off instead of re-scanning the whole source.
        self._cursors: dict[str, dict[str, Any]] = {}
        self._cursor_store = CursorStore(self.base_dir)
        self._restore_durable_cursors()

    def _restore_durable_cursors(self) -> None:
        """Seed the cursor cache from the delivered resume-state file.

        The per-stream ``state/`` checkpoints are wiped on a fresh container, so
        without this an incremental stream would find no bookmark on re-run
        and full-rescan the source. The deployment delivers the cursors it
        harvested from the prior run's emitted state as ``state/resume.json``
        in the config bundle (a local run writes the same file itself, see
        :meth:`write_resume_snapshot`); we decode it into the same
        ``{"cursor": <value>}`` shape
        :meth:`get_cursor` returns, keyed by ``stream_id`` with the empty
        partition the engine reads with. The seeded value wins over any stale
        per-stream checkpoint left on disk, since the delivered file is the
        authoritative bookmark.
        """
        restored = load_resume_file(self._resume_path)
        seeded = 0
        for stream_id, value in restored.items():
            if value is None:
                continue
            self._cursors[self._cursor_key(stream_id, {})] = {"cursor": value}
            seeded += 1
        if seeded:
            logger.info(
                "restored durable cursor state for %d stream(s) from %s",
                seeded,
                self._resume_path,
            )

    def write_resume_snapshot(self, stream_ids: Iterable[str]) -> None:
        """Persist the run's resume cursors as the consolidated resume file.

        Called once the pipeline finishes so the next local run resumes from the
        same ``state/resume.json`` the cloud deployment delivers, instead of
        re-reading the per-stream checkpoints. Each stream's current
        cursor is taken from the in-run cache, falling back to its on-disk
        checkpoint; a stream with no cursor yet is omitted (nothing to resume).
        In the cloud the durable channel stays the emitted ``ANALITIQ_STATE``
        log lines, so this file is a harmless local artifact there.
        """
        snapshot: dict[str, Any] = {}
        with self.lock:
            for stream_id in stream_ids:
                cursor = self._current_cursor(stream_id)
                if cursor is not None:
                    snapshot[stream_id] = cursor
        write_resume_file(self._resume_path, snapshot)

    def _current_cursor(self, stream_id: str) -> Any | None:
        """Latest cursor value for a stream: in-run cache, then on-disk checkpoint.

        Mirrors :meth:`get_cursor`'s resolution (cache wins over disk) but
        returns the bare cursor value for the empty partition the engine reads
        with, for building the resume snapshot.
        """
        cached = self._cursors.get(self._cursor_key(stream_id, {}))
        if cached is not None:
            return cached.get("cursor")
        return self._cursor_store.get(self.pipeline_id, stream_id)

    def init_commit_tracker(self, run_id: str) -> None:
        """Initialize batch commit tracker for the current run."""
        self._commit_tracker = BatchCommitTracker(
            pipeline_dir=str(self.pipeline_dir),
            run_id=run_id,
        )

    @property
    def commit_tracker(self) -> BatchCommitTracker | None:
        """Get the batch commit tracker (if initialized)."""
        return self._commit_tracker

    def start_run(self, config: dict[str, Any], run_id: str | None = None) -> str:
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
        partition: dict[str, Any],
        cursor: dict[str, Any],
        hwm: str,
        stream_version: int,
        page_state: dict[str, Any] | None = None,
        http_conditionals: dict[str, Any] | None = None,
        stats: dict[str, Any] | None = None,
    ) -> None:
        """
        Emit checkpoint for a specific stream to structured logs.

        Args:
            stream_name: Name of the stream
            partition: Partition key dict (ignored)
            cursor: Cursor state with primary/tiebreaker fields
            hwm: High-water mark timestamp
            stream_version: Version the manifest pins for this stream. Carried
                on the line so the deployment can scope the durable bookmark to
                the version that produced it; the engine never acts on it.
            page_state: Unused, kept for call-site compatibility
            http_conditionals: Unused, kept for call-site compatibility
            stats: Unused, kept for call-site compatibility

        The engine still emits and keys its in-run cursor by bare
        ``stream_id``; ``stream_version`` is pass-through metadata. The
        emission-time ``emitted_at`` ordering key is not stamped here -- it is
        added to every record centrally by
        :func:`src.state.log_emitter.emit_log`.
        """
        emit_state_log(
            run_id=self.current_run_id or "",
            pipeline_id=self.pipeline_id,
            stream_id=stream_name,
            stream_version=stream_version,
            cursor_hex=json.dumps(cursor).encode().hex() if cursor else "",
            cursor_value=hwm,
        )

    @staticmethod
    def _cursor_key(stream_name: str, partition: dict[str, Any]) -> str:
        """Build a deterministic key from stream name and partition dict."""
        suffix = json.dumps(partition, sort_keys=True) if partition else "{}"
        return f"{stream_name}::{suffix}"

    async def get_cursor(
        self, stream_name: str, partition: dict[str, Any] | None = None
    ) -> dict[str, Any] | None:
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
        partition: dict[str, Any] | None,
        cursor: dict[str, Any],
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

    def get_run_info(self) -> dict[str, Any]:
        """Get current run information."""
        return {"run_id": self.current_run_id} if self.current_run_id else {}
