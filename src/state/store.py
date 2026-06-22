"""
Filesystem-backed cursor checkpoint store.

State for an incremental stream is one JSON document per
``(pipeline, stream)`` pair. The schema contract leaves run-log/state
ownership to a future contract; until then the engine writes minimal
``{"cursor": <value>}`` documents under
``state/{pipeline_id}/{stream_id}.json``. The store is
deliberately tiny — no persistence backend, no schema migrations —
because the engine must remain cloud-agnostic per the brief.

Cursor values are usually scalars (an ``int`` id, an ISO ``str``), but a
timestamp cursor arrives as a ``datetime`` — which JSON cannot represent and
which the source rebinds verbatim into ``cursor_field >= ?`` with no cast.
asyncpg infers that bind as the column's type and rejects a plain string for a
timestamp param, so a reloaded timestamp cursor must come back as a
``datetime``, not its ISO text. ``_encode``/``_decode`` tag ``datetime``/``date``
to round-trip them losslessly; JSON-native scalars (``int``, ``str``) pass
through unchanged.

Checkpointing is an optimization, not a correctness guarantee: incremental
writes are idempotent (inclusive ``>=`` cursor + upsert), so a lost or
unreadable checkpoint costs a one-time full re-scan, never data. Both the read
and write paths therefore degrade to that re-scan on I/O failure rather than
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

    A timestamp cursor crosses the wire as an ISO-8601 string but must rebind
    as a ``datetime`` -- asyncpg infers the bind as the column's type and
    rejects a plain string for a timestamp param (the same constraint
    :func:`_decode` documents). Strings carrying a time component are
    reconstructed to ``datetime``; ints, bare dates, and non-temporal strings
    pass through unchanged.

    Returns an empty mapping on a missing or unparseable payload so a startup
    with corrupt state degrades to a full re-scan -- loudly -- rather than
    aborting the run.
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
    return {
        str(stream_id): _reconstruct_cursor_value(value)
        for stream_id, value in decoded.items()
    }


def _reconstruct_cursor_value(value: Any) -> Any:
    """Invert the datetime -> ISO-string serialization the wire applied.

    Only strings with a time separator (``T`` or ``:``) are candidates, so a
    bare date or an opaque string cursor is never misread as a timestamp; a
    string that looks like a datetime but does not parse is left as-is.
    """
    if isinstance(value, str) and ("T" in value or ":" in value):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return value
    return value


def encode_cursor_state(cursor: Dict[str, Any]) -> Dict[str, Any]:
    """JSON-safe form of a cursor-state dict (tags ``datetime``/``date``).

    The worker wire protocol relays cursor saves as JSON; this applies the
    same tagging the on-disk store uses so a timestamp cursor survives the
    hop as a ``datetime``, not its ISO text.
    """
    return {key: _encode(value) for key, value in cursor.items()}


def decode_cursor_state(cursor: Dict[str, Any]) -> Dict[str, Any]:
    """Inverse of :func:`encode_cursor_state`."""
    return {key: _decode(value) for key, value in cursor.items()}


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
            return _decode(payload.get("cursor"))
        except (OSError, ValueError, TypeError) as exc:
            # Torn write, unparseable JSON, or a bad ISO value (_decode raises
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
            tmp.write_text(json.dumps({"cursor": _encode(cursor)}, default=str))
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


def _encode(value: Any) -> Any:
    # datetime is a subclass of date, so check it first.
    if isinstance(value, datetime):
        return {_TYPE_KEY: "datetime", _VALUE_KEY: value.isoformat()}
    if isinstance(value, date):
        return {_TYPE_KEY: "date", _VALUE_KEY: value.isoformat()}
    return value


def _decode(value: Any) -> Any:
    if isinstance(value, dict) and _TYPE_KEY in value:
        kind = value[_TYPE_KEY]
        raw = value.get(_VALUE_KEY)
        if kind == "datetime":
            return datetime.fromisoformat(raw)
        if kind == "date":
            return date.fromisoformat(raw)
    return value
