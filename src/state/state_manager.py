"""State management for pipeline checkpointing and recovery."""

import json
import logging
import os
import threading
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
        # The consolidated resume bookmark, scoped per pipeline like every other
        # state file: the cloud deployment delivers it in the config bundle, a
        # local run writes it at the end (write_resume_snapshot). Pipeline-scoping
        # stops a second pipeline that shares the local state/ dir from
        # overwriting this pipeline's bookmark. It lives in its own ``resume/``
        # sub-directory, not directly under the pipeline dir, so it can never
        # collide with a per-stream checkpoint -- those own the
        # ``state/{pipeline_id}/{stream_id}.json`` namespace, and a stream whose
        # id was ``resume`` would otherwise write its ``{"cursor": ...}`` file
        # over this map.
        self._resume_path = self.pipeline_dir / "resume" / "cursors.json"

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
        # The committed (destination-ACKed) high-water mark per stream -- the
        # value write_resume_snapshot persists. Kept apart from _cursors, which
        # the source advances as it yields batches, ahead of the ACK: snapshotting
        # that pre-ACK position would let a failed or un-ACKed stream skip rows
        # that never landed. Seeded from the delivered resume file so a stream
        # that does not advance this run keeps its prior bookmark, then updated
        # only from save_stream_checkpoint -- the same ACKed watermark the
        # ANALITIQ_STATE log emits, so the local snapshot matches the cloud one.
        self._committed_cursors: dict[str, Any] = {}
        self._cursor_store = CursorStore(self.base_dir)
        self._restore_durable_cursors()

    def _restore_durable_cursors(self) -> None:
        """Seed the cursor cache from the delivered resume-state file.

        The per-stream ``state/`` checkpoints are wiped on a fresh container, so
        without this an incremental stream would find no bookmark on re-run
        and full-rescan the source. The deployment delivers the cursors it
        harvested from the prior run's emitted state as
        ``state/{pipeline_id}/resume/cursors.json`` in the config bundle (a local
        run writes the same file itself, see :meth:`write_resume_snapshot`); we
        decode it into the same ``{"cursor": <value>}`` shape
        :meth:`get_cursor` returns, keyed by ``stream_id`` with the empty
        partition the engine reads with. The seeded value wins over any stale
        per-stream checkpoint left on disk, since the delivered file is the
        authoritative bookmark. It also seeds ``_committed_cursors`` so a stream
        that does not advance this run still carries its bookmark into the next
        snapshot.
        """
        # Whether a resume file was delivered/left for this run. When one is
        # present it is the authoritative committed bookmark, so get_cursor must
        # not fall back to a pre-ACK per-stream checkpoint for a stream the file
        # omits (that checkpoint can be ahead of the ACK and would skip rows).
        self._resume_file_present = self._resume_path.exists()
        restored = load_resume_file(self._resume_path)
        seeded = 0
        for stream_id, value in restored.items():
            if value is None:
                continue
            self._cursors[self._cursor_key(stream_id, {})] = {"cursor": value}
            self._committed_cursors[stream_id] = value
            seeded += 1
        if seeded:
            logger.info(
                "restored durable cursor state for %d stream(s) from %s",
                seeded,
                self._resume_path,
            )

    def write_resume_snapshot(self) -> None:
        """Persist the committed resume cursors as the consolidated resume file.

        Called once the pipeline finishes so the next run resumes from the same
        ``state/{pipeline_id}/resume/cursors.json`` the cloud deployment
        delivers. The snapshot is the committed (destination-ACKed) high-water
        mark per stream
        (``_committed_cursors``, advanced only by :meth:`save_stream_checkpoint`)
        merged with whatever this run resumed from -- never the source's pre-ACK
        position. So a stream that failed or never ACKed a batch keeps its last
        safe bookmark instead of skipping un-landed rows, a stream that did not
        advance this run keeps the value it resumed from, and the file matches
        the cloud one, which the deployment builds from the same emitted ACKed
        watermarks. In the cloud the durable channel stays the emitted
        ``ANALITIQ_STATE`` log lines, so this file is a harmless local artifact
        there.
        """
        with self.lock:
            snapshot = dict(self._committed_cursors)
        write_resume_file(self._resume_path, snapshot)

    def record_committed_value(self, stream_id: str, value: Any) -> None:
        """Advance a stream's committed high-water mark from a recorded watermark.

        For the engine's in-run idempotency skip path: a batch already committed
        in a prior attempt is not re-sent, but the resume snapshot must still
        carry its committed watermark so a retry does not regress the bookmark.
        ``value`` is the value the commit tracker recorded when the batch first
        committed -- what actually landed -- in the same cursor-token form
        :meth:`save_stream_checkpoint` records (a JSON-native scalar or the
        tagged datetime/date/decimal form). A ``None`` value (a non-incremental
        batch) is ignored.
        """
        if value is not None:
            with self.lock:
                self._committed_cursors[stream_id] = value

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

        This runs only on a destination ACK (the engine's committed-watermark
        path), so it is also where the resume snapshot's bookmark advances:
        recording the committed value here, rather than reading the source's
        pre-ACK cursor at snapshot time, is what keeps a failed stream from
        persisting a watermark for rows that never landed.
        """
        emit_state_log(
            run_id=self.current_run_id or "",
            pipeline_id=self.pipeline_id,
            stream_id=stream_name,
            stream_version=stream_version,
            cursor_hex=json.dumps(cursor).encode().hex() if cursor else "",
            cursor_value=hwm,
        )
        committed_value = cursor.get("primary", {}).get("value") if cursor else None
        if committed_value is not None:
            with self.lock:
                self._committed_cursors[stream_name] = committed_value

    @staticmethod
    def _cursor_key(stream_name: str, partition: dict[str, Any]) -> str:
        """Build a deterministic key from stream name and partition dict."""
        suffix = json.dumps(partition, sort_keys=True) if partition else "{}"
        return f"{stream_name}::{suffix}"

    async def get_cursor(
        self, stream_name: str, partition: dict[str, Any] | None = None
    ) -> dict[str, Any] | None:
        """Return the last saved cursor for a stream/partition, or None.

        Resolution: the in-run cache (seeded at startup from the resume file,
        updated by :meth:`save_cursor`) wins. When a resume file was present this
        run it is the authoritative committed bookmark, so a stream the file
        omits resumes from nothing (a full re-scan) -- never from the per-stream
        on-disk checkpoint, which the source advances ahead of the destination
        ACK and could therefore point past rows that never landed. The on-disk
        checkpoint is consulted only when no resume file exists at all (a true
        first run, where it is absent too), so it can no longer shadow the
        committed bookmark.
        """
        key = self._cursor_key(stream_name, partition or {})
        with self.lock:
            cached = self._cursors.get(key)
            if cached is not None:
                return cached
            if self._resume_file_present:
                return None
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
