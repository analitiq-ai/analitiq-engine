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
which the source rebinds verbatim into ``WHERE cursor_field >= :cursor`` with
no cast. asyncpg infers that bind as the column's type and rejects a plain
string, so a reloaded timestamp cursor must come back as a ``datetime``, not
its ISO text. ``_encode``/``_decode`` tag ``datetime``/``date`` to round-trip
them losslessly; every other value passes through unchanged.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

_TYPE_KEY = "__type__"
_VALUE_KEY = "value"


class CursorStore:
    def __init__(self, root: Path) -> None:
        self._root = root

    def _path(self, pipeline_id: str, stream_id: str) -> Path:
        return self._root / pipeline_id / f"{stream_id}.json"

    def get(self, pipeline_id: str, stream_id: str) -> Any | None:
        path = self._path(pipeline_id, stream_id)
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text())
        except json.JSONDecodeError:
            return None
        return _decode(payload.get("cursor"))

    def set(self, pipeline_id: str, stream_id: str, cursor: Any | None) -> None:
        if cursor is None:
            return
        path = self._path(pipeline_id, stream_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"cursor": _encode(cursor)}, default=str))


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
