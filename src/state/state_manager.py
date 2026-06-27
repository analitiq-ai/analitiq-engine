"""State management for pipeline checkpointing and recovery."""

import json
import logging
import os
import threading
from pathlib import Path
from typing import Any

from ..shared.run_id import get_or_generate_run_id
from .state_emission import emit_state_log
from .store import CursorStore

logger = logging.getLogger(__name__)


class StateManager:
    """
    Manages pipeline run state and emits checkpoints to structured logs.

    Checkpoints are emitted as ANALITIQ_STATE:: log lines for
    cross-run observability. Idempotency is enforced at the destination on
    content-derived row identity, not by an engine-side batch ledger
    (issue #282), so this manager owns only cursor state.
    """

    def __init__(
        self,
        pipeline_id: str,
        base_dir: str = "state",
    ):
        self.pipeline_id = pipeline_id
        self.base_dir = Path(base_dir)
        self.pipeline_dir = self.base_dir / pipeline_id

        # Thread safety
        self.lock = threading.RLock()

        # Current run ID (from env var RUN_ID if available)
        self.current_run_id: str | None = os.environ.get("RUN_ID")

        # In-run cursor cache keyed by (stream_name, partition_key). Read by
        # get_cursor and backed by the per-stream committed checkpoint, so a fresh
        # process resumes from the prior run's ACKed bookmark instead of
        # re-scanning the whole source.
        self._cursors: dict[str, dict[str, Any]] = {}
        # Per-stream committed-cursor checkpoint files: one
        # state/{pipeline_id}/{stream_id}.json per stream, written on each
        # destination ACK and read at the next run's start. Each stream owns its
        # file (no contention), and the value is the ACKed watermark -- never the
        # source's pre-ACK position. On a fresh container the config bundle
        # delivers these same files; there is no env var and no single shared
        # file.
        self._cursor_store = CursorStore(self.base_dir)

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
        path), so it is also where the per-stream checkpoint advances: persisting
        the committed value here, never the source's pre-ACK cursor, is what
        keeps a failed stream from resuming past rows that never landed. The
        write goes to this stream's own file, so concurrent streams never
        contend.
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
                self._cursor_store.set(self.pipeline_id, stream_name, committed_value)

    @staticmethod
    def _cursor_key(stream_name: str, partition: dict[str, Any]) -> str:
        """Build a deterministic key from stream name and partition dict."""
        suffix = json.dumps(partition, sort_keys=True) if partition else "{}"
        return f"{stream_name}::{suffix}"

    async def get_cursor(
        self, stream_name: str, partition: dict[str, Any] | None = None
    ) -> dict[str, Any] | None:
        """Return the resume cursor for a stream/partition, or None.

        Falls back to the per-stream committed checkpoint when the in-run cache
        is empty, so a newly started process (or a fresh container holding only
        the bundle-delivered files) resumes from the previous run's ACKed
        bookmark. A stream with no checkpoint resumes with a full re-scan.
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
        """Update the in-run cursor cache for a stream/partition.

        Records the source's latest (pre-ACK) cursor for the duration of the run
        only; it is never persisted. The durable per-stream checkpoint advances
        solely on a destination ACK (see :meth:`save_stream_checkpoint`), so the
        source running ahead of the ACK can't push the on-disk bookmark past rows
        that have not landed.
        """
        key = self._cursor_key(stream_name, partition or {})
        with self.lock:
            self._cursors[key] = cursor

    def get_run_info(self) -> dict[str, Any]:
        """Get current run information."""
        return {"run_id": self.current_run_id} if self.current_run_id else {}
