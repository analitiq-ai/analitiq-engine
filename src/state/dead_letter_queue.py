"""Dead Letter Queue for failed Arrow batches.

Stores failed batches as Arrow IPC files plus a JSON metadata sidecar.
The DLQ is intentionally payload-format-naive: it persists the Arrow
buffers byte-for-byte without inspecting types. Decimal precision,
timestamp time zones, JSON columns, and any other Arrow-native types
round-trip losslessly because no serialisation runs over the payload.

Layout per failed batch:
    dlq_<ts>_<run>_<stream>_<seq>.arrow   Arrow IPC stream of the batch
    dlq_<ts>_<run>_<stream>_<seq>.json    Metadata only (primitives)

Cloud persistence is the deployment layer's job (mounted volume or
sidecar log shipper). Lightweight summaries hit stdout via
``ANALITIQ_DLQ`` for observability; payloads never go to stdout.
"""

from __future__ import annotations

import asyncio
import json
import logging
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pyarrow as pa
import pyarrow.ipc as pa_ipc

from src.state.log_emitter import emit_log

logger = logging.getLogger(__name__)


def emit_dlq_log(
    pipeline_id: str,
    stream_id: Optional[str],
    added: int,
    total: int,
) -> None:
    """Emit a lightweight DLQ summary to stdout for observability.

    Never emits record payloads -- only counts.
    """
    data: Dict[str, Any] = {
        "type": "dlq",
        "pipeline_id": pipeline_id,
        "added": added,
        "total": total,
    }
    if stream_id:
        data["stream_id"] = stream_id
    emit_log("dlq", data)


def _sanitize(value: str) -> str:
    """Strip filename-unfriendly characters from a path component."""
    return "".join(
        ch if (ch.isalnum() or ch in ("-", "_")) else "_" for ch in value
    )


def _error_payload(error: Union[BaseException, str]) -> Dict[str, Any]:
    """Render an exception or message into a JSON-safe metadata dict."""
    if isinstance(error, BaseException):
        try:
            tb = traceback.format_exception(type(error), error, error.__traceback__)
        except Exception:
            tb = None
        return {
            "type": type(error).__name__,
            "message": str(error),
            "traceback": tb,
        }
    return {"type": "str", "message": str(error), "traceback": None}


class LocalDLQStorage:
    """Local filesystem DLQ storage.

    One file pair per failed batch:

    * ``<stem>.arrow`` -- Arrow IPC stream (payload).
    * ``<stem>.json``  -- metadata sidecar (primitives only).

    The sidecar is written last so a half-written pair is detectable:
    readers skip any ``.arrow`` lacking a sibling ``.json``.
    """

    SUFFIX_PAYLOAD = ".arrow"
    SUFFIX_META = ".json"

    def __init__(self, dlq_path: str, max_files: int = 100):
        self.dlq_path = Path(dlq_path)
        self.max_files = max_files
        self.dlq_path.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()

    def _stem(self, run_id: str, stream_id: str, batch_seq: int) -> str:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%f")
        return (
            f"dlq_{ts}_{_sanitize(run_id)}_"
            f"{_sanitize(stream_id)}_{batch_seq:09d}"
        )

    async def write_batch(
        self,
        batch: pa.RecordBatch,
        metadata: Dict[str, Any],
        run_id: str,
        stream_id: str,
        batch_seq: int,
    ) -> None:
        async with self._lock:
            stem = self._stem(run_id, stream_id, batch_seq)
            payload_path = self.dlq_path / f"{stem}{self.SUFFIX_PAYLOAD}"
            meta_path = self.dlq_path / f"{stem}{self.SUFFIX_META}"

            await asyncio.to_thread(_write_arrow_ipc, payload_path, batch)
            await asyncio.to_thread(_write_json, meta_path, metadata)

    async def list_entries(
        self, pipeline_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        entries: List[Dict[str, Any]] = []
        for meta_path in sorted(self.dlq_path.glob(f"dlq_*{self.SUFFIX_META}")):
            try:
                with open(meta_path, "r", encoding="utf-8") as f:
                    meta = json.load(f)
            except (OSError, json.JSONDecodeError):
                logger.warning("Skipping unreadable DLQ sidecar %s", meta_path)
                continue
            if pipeline_id and meta.get("pipeline_id") != pipeline_id:
                continue
            meta["payload_path"] = str(
                meta_path.with_suffix(self.SUFFIX_PAYLOAD)
            )
            entries.append(meta)
        return entries

    async def get_stats(self) -> Dict[str, Any]:
        stats: Dict[str, Any] = {
            "total_batches": 0,
            "total_records": 0,
            "batches_by_pipeline": {},
            "batches_by_stream": {},
            "batches_by_error_type": {},
            "total_files": 0,
            "total_size_bytes": 0,
            "oldest_batch": None,
            "newest_batch": None,
        }
        for meta_path in self.dlq_path.glob(f"dlq_*{self.SUFFIX_META}"):
            stats["total_files"] += 1
            try:
                stats["total_size_bytes"] += meta_path.stat().st_size
            except OSError:
                pass
            payload_path = meta_path.with_suffix(self.SUFFIX_PAYLOAD)
            try:
                stats["total_size_bytes"] += payload_path.stat().st_size
            except OSError:
                pass
            try:
                with open(meta_path, "r", encoding="utf-8") as f:
                    meta = json.load(f)
            except (OSError, json.JSONDecodeError):
                continue
            stats["total_batches"] += 1
            stats["total_records"] += int(meta.get("record_count", 0))

            pid = meta.get("pipeline_id", "unknown")
            stats["batches_by_pipeline"][pid] = (
                stats["batches_by_pipeline"].get(pid, 0) + 1
            )
            sid = meta.get("stream_id", "unknown")
            stats["batches_by_stream"][sid] = (
                stats["batches_by_stream"].get(sid, 0) + 1
            )
            etype = (meta.get("error") or {}).get("type", "unknown")
            stats["batches_by_error_type"][etype] = (
                stats["batches_by_error_type"].get(etype, 0) + 1
            )

            ts = meta.get("timestamp")
            if ts:
                if stats["oldest_batch"] is None or ts < stats["oldest_batch"]:
                    stats["oldest_batch"] = ts
                if stats["newest_batch"] is None or ts > stats["newest_batch"]:
                    stats["newest_batch"] = ts
        return stats

    async def clear(self) -> None:
        for path in self.dlq_path.glob("dlq_*"):
            try:
                path.unlink()
            except OSError:
                pass

    async def cleanup_old(self, retention_days: int) -> None:
        pairs = sorted(
            self.dlq_path.glob(f"dlq_*{self.SUFFIX_META}"),
            key=lambda p: p.stat().st_ctime,
            reverse=True,
        )
        for meta in pairs[self.max_files:]:
            _delete_pair(meta)

        cutoff = datetime.now(timezone.utc).timestamp() - retention_days * 86400
        for meta in pairs[: self.max_files]:
            try:
                ctime = meta.stat().st_ctime
            except OSError:
                continue
            if ctime < cutoff:
                _delete_pair(meta)


def _delete_pair(meta_path: Path) -> None:
    payload_path = meta_path.with_suffix(LocalDLQStorage.SUFFIX_PAYLOAD)
    for p in (meta_path, payload_path):
        try:
            p.unlink()
        except OSError:
            pass


def _write_arrow_ipc(path: Path, batch: pa.RecordBatch) -> None:
    with pa.OSFile(str(path), "wb") as sink:
        with pa_ipc.new_stream(sink, batch.schema) as writer:
            writer.write_batch(batch)


def _write_json(path: Path, data: Dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


class DeadLetterQueue:
    """Persist failed Arrow batches as IPC files plus JSON sidecars.

    The DLQ does no transformation. Callers hand a ``pa.RecordBatch``
    plus failure context; the Arrow buffers are written byte-for-byte
    and the metadata sidecar carries only primitive types.
    """

    def __init__(
        self,
        dlq_path: str = "./deadletter/",
        max_files: int = 100,
        retention_days: int = 30,
    ):
        self.dlq_path = Path(dlq_path)
        self.max_files = max_files
        self.retention_days = retention_days
        self._batch_count = 0
        self.storage = LocalDLQStorage(
            dlq_path=dlq_path,
            max_files=max_files,
        )

    async def send_batch(
        self,
        batch: pa.RecordBatch,
        error: Union[BaseException, str],
        pipeline_id: str,
        stream_id: str,
        run_id: str,
        batch_seq: int,
        additional_context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Store the failed batch and its failure context."""
        record_count = batch.num_rows
        metadata: Dict[str, Any] = {
            "pipeline_id": pipeline_id,
            "stream_id": stream_id,
            "run_id": run_id,
            "batch_seq": batch_seq,
            "record_count": record_count,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "error": _error_payload(error),
        }
        if additional_context:
            metadata["additional_context"] = additional_context

        await self.storage.write_batch(
            batch=batch,
            metadata=metadata,
            run_id=run_id,
            stream_id=stream_id,
            batch_seq=batch_seq,
        )
        self._batch_count += 1

        logger.warning(
            "Sent batch %s/%s/%s (%d records) to DLQ for pipeline %s",
            run_id, stream_id, batch_seq, record_count, pipeline_id,
        )
        emit_dlq_log(
            pipeline_id=pipeline_id,
            stream_id=stream_id,
            added=record_count,
            total=self._batch_count,
        )

    async def list_entries(
        self, pipeline_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        return await self.storage.list_entries(pipeline_id)

    async def get_dlq_stats(self) -> Dict[str, Any]:
        return await self.storage.get_stats()

    async def clear_dlq(self) -> None:
        await self.storage.clear()

    async def cleanup(self) -> None:
        await self.storage.cleanup_old(self.retention_days)

    async def flush(self) -> None:
        return None