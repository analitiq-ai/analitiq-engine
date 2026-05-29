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
from typing import Any, Set

logger = logging.getLogger(__name__)

_TYPE_KEY = "__type__"
_VALUE_KEY = "value"


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
