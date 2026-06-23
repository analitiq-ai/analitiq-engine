"""
Filesystem-backed cursor checkpoint store.

State for an incremental stream is one JSON document per
``(pipeline, stream)`` pair. The schema contract leaves run-log/state
ownership to a future contract; until then the engine writes minimal
``{"cursor": <value>}`` documents under
``state/{pipeline_id}/{stream_id}.json``. The store is
deliberately tiny — no persistence backend, no schema migrations —
because the engine must remain cloud-agnostic per the brief.

Cursor values are usually scalars (an ``int`` id, a ``str``), but a timestamp
cursor arrives as a ``datetime`` — which JSON cannot represent and which the
source rebinds verbatim into ``cursor_field >= ?`` with no cast. asyncpg infers
that bind as the column's type and rejects a plain string for a timestamp
param, so a reloaded timestamp cursor must come back as a ``datetime``, not its
ISO text. ``encode_value``/``decode_value`` tag ``datetime``/``date`` as
``{"__type__": ..., "value": ...}`` to round-trip them losslessly; JSON-native
scalars (``int``, ``str``) pass through unchanged. This same tagging travels
the gRPC cursor token and the durable ``RESUME_STATE`` payload (see
``src/grpc/cursor.py`` and ``parse_resume_state``), so the type is carried
end-to-end and never guessed from a string's shape — a ``str`` cursor whose
value looks like a date stays a ``str``.

The resume bound is inclusive (``>=``): the stored cursor is re-read on the
next run (see the read path in ``cdk/cdk/sql/generic.py``). This is what keeps
a non-unique cursor lossless — a row that arrives at the last committed value
between runs is still read, where an exclusive ``>`` would filter it out at the
source and drop it for good. The re-read is deduped by the default upsert
write mode against its conflict_keys; under insert mode a unique/primary key
rejects the re-read duplicate loudly, while a keyless insert stream has nothing
to dedup against and would append a duplicate boundary row (insert + an
incremental cursor without a uniqueness key is unsafe). A lost or
unreadable checkpoint costs a one-time full re-scan, never data, so both the
read and write paths degrade to that re-scan on I/O failure rather than
aborting a load — but say so loudly, since a corrupt checkpoint usually means a
prior crash an operator should see.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Optional, Set

logger = logging.getLogger(__name__)

_TYPE_KEY = "__type__"
_VALUE_KEY = "value"


def parse_resume_state(raw: Optional[str]) -> Dict[str, Any]:
    """Decode the durable resume-state env payload into per-stream cursors.

    The engine emits its cursor checkpoints as ``ANALITIQ_STATE`` log lines
    (see :mod:`src.state.state_emission`); the deployment harvests those into
    durable storage and re-injects them on the next run as the ``RESUME_STATE``
    environment variable -- a JSON object mapping ``stream_id`` to the
    high-water-mark cursor value. A fresh container's local ``state/``
    directory is empty, so this injected value is the only bookmark an
    incremental stream can resume from. Reading it here keeps the engine
    cloud-agnostic: it consumes a resolved value the same way it consumes any
    other secret/config input, and never reaches for cloud storage itself.

    Each value carries its type the same way the on-disk checkpoint does: a
    ``datetime``/``date`` is the tagged ``{"__type__": ..., "value": ...}`` form
    and is decoded back to the real type (asyncpg rejects a plain string for a
    timestamp bind); a JSON-native ``int``/``str`` is used verbatim. Because the
    type is carried, not inferred, a ``str`` cursor whose value happens to look
    like a date stays a ``str`` -- no shape-guessing.

    Corrupt state degrades to a full re-scan -- loudly -- rather than aborting
    the run: a missing or non-object payload yields an empty mapping, and a
    single malformed tagged value (e.g. a bad ISO string) skips just that
    stream, the same way :meth:`CursorStore.get` degrades on disk.
    """
    if not raw:
        return {}
    try:
        decoded = json.loads(raw)
    except ValueError as exc:
        logger.warning(
            "RESUME_STATE is not valid JSON (%s); incremental streams resume "
            "with a full re-scan",
            exc,
        )
        return {}
    if not isinstance(decoded, dict):
        logger.warning(
            "RESUME_STATE must be a JSON object {stream_id: cursor}; got %s; "
            "incremental streams resume with a full re-scan",
            type(decoded).__name__,
        )
        return {}
    restored: Dict[str, Any] = {}
    for stream_id, value in decoded.items():
        try:
            restored[str(stream_id)] = decode_value(value)
        except (ValueError, TypeError) as exc:
            # A malformed tagged datetime/date (corrupt durable state or a bad
            # manual injection) must not abort StateManager construction. Skip
            # the stream so it resumes with a full re-scan -- loudly -- exactly
            # as CursorStore.get does for an unreadable on-disk checkpoint.
            logger.warning(
                "RESUME_STATE cursor for stream %r is unreadable (%s); that "
                "stream resumes with a full re-scan",
                stream_id,
                exc,
            )
    return restored


def encode_cursor_state(cursor: Dict[str, Any]) -> Dict[str, Any]:
    """JSON-safe form of a cursor-state dict (tags ``datetime``/``date``).

    The worker wire protocol relays cursor saves as JSON; this applies the
    same tagging the on-disk store uses so a timestamp cursor survives the
    hop as a ``datetime``, not its ISO text.
    """
    return {key: encode_value(value) for key, value in cursor.items()}


def decode_cursor_state(cursor: Dict[str, Any]) -> Dict[str, Any]:
    """Inverse of :func:`encode_cursor_state`."""
    return {key: decode_value(value) for key, value in cursor.items()}


class CursorStore:
    def __init__(self, root: Path) -> None:
        self._root = root
        self._write_failures_warned: Set[Path] = set()

    def _path(self, pipeline_id: str, stream_id: str) -> Path:
        return self._root / pipeline_id / f"{stream_id}.json"

    def get(self, pipeline_id: str, stream_id: str) -> Any | None:
        path = self._path(pipeline_id, stream_id)
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text())
            return decode_value(payload.get("cursor"))
        except (OSError, ValueError, TypeError) as exc:
            # Torn write, unparseable JSON, or a bad ISO value (decode_value raises
            # ValueError/TypeError; JSONDecodeError is a ValueError). Treat any
            # unreadable checkpoint the same: resume with a full re-scan, loudly.
            logger.warning(
                "cursor checkpoint %s is unreadable (%s); stream resumes with a "
                "full re-scan",
                path,
                exc,
            )
            return None

    def set(self, pipeline_id: str, stream_id: str, cursor: Any | None) -> None:
        if cursor is None:
            return
        path = self._path(pipeline_id, stream_id)
        tmp = path.parent / f"{path.name}.tmp"
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp.write_text(json.dumps({"cursor": encode_value(cursor)}, default=str))
            # Atomic swap so a concurrent/next-run reader never sees a torn file.
            os.replace(tmp, path)
        except OSError as exc:
            # Don't leave a stale partial temp file behind if the swap failed.
            with contextlib.suppress(OSError):
                tmp.unlink(missing_ok=True)
            # A write failure must not abort an otherwise-successful load; warn
            # once per path so the silent degradation to full re-scan stays
            # visible without spamming a line per batch, while a distinct second
            # path's failure is still reported.
            if path not in self._write_failures_warned:
                self._write_failures_warned.add(path)
                logger.warning(
                    "failed to persist cursor checkpoint %s (%s); incremental "
                    "resume is disabled until the path is writable",
                    path,
                    exc,
                )


def encode_value(value: Any) -> Any:
    """Tag a ``datetime``/``date`` so JSON can round-trip its type.

    Shared by the on-disk checkpoint, the worker cursor-state wire, and the
    gRPC cursor token (``src/grpc/cursor.py``) so every place a cursor is
    serialized carries the type the same way. JSON-native scalars are returned
    unchanged.
    """
    # datetime is a subclass of date, so check it first.
    if isinstance(value, datetime):
        return {_TYPE_KEY: "datetime", _VALUE_KEY: value.isoformat()}
    if isinstance(value, date):
        return {_TYPE_KEY: "date", _VALUE_KEY: value.isoformat()}
    return value


def decode_value(value: Any) -> Any:
    """Inverse of :func:`encode_value`; untagged scalars pass through."""
    if isinstance(value, dict) and _TYPE_KEY in value:
        kind = value[_TYPE_KEY]
        raw = value.get(_VALUE_KEY)
        if kind == "datetime":
            return datetime.fromisoformat(raw)
        if kind == "date":
            return date.fromisoformat(raw)
    return value
