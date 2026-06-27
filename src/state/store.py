"""
Per-stream cursor checkpoint store and cursor-value encoding.

An incremental stream's durable bookmark is the committed (destination-ACKed)
high-water mark, written as one minimal ``{"cursor": <value>}`` JSON document
per ``(pipeline, stream)`` under ``state/{pipeline_id}/{stream_id}.json``
(:class:`CursorStore`). Each stream owns its own file and writes it on every
destination ACK, so concurrent streams never contend on a shared file and a
crash loses at most the last un-ACKed batch. The store is deliberately tiny — no
persistence backend, no schema migrations — because the engine must remain
cloud-agnostic: the same per-stream files are written by a local run and
delivered in the config bundle on a fresh container (whose local ``state/`` is
otherwise empty), and the engine only ever reads a resolved local file. The
cursor written here is the ACKed watermark, never the source's pre-ACK position,
so a stream that fails after extraction raced ahead never resumes past rows that
never landed.

Cursor values are usually scalars (an ``int`` id, a ``str``), but a timestamp
cursor arrives as a ``datetime`` — which JSON cannot represent and which the
source rebinds verbatim into ``cursor_field >= ?`` with no cast. asyncpg infers
that bind as the column's type and rejects a plain string for a timestamp
param, so a reloaded timestamp cursor must come back as a ``datetime``, not its
ISO text. ``encode_value``/``decode_value`` tag ``datetime``/``date``/``time``/
``Decimal`` as ``{"__type__": ..., "value": ...}`` to round-trip them
losslessly; JSON-native scalars (``int``, ``str``) pass through unchanged. This
same tagging travels the gRPC cursor token and the on-disk checkpoint (see
``src/grpc/cursor.py``), so the type is carried end-to-end and never guessed
from a string's shape — a ``str`` cursor whose value looks like a date stays a
``str``.

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
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_TYPE_KEY = "__type__"
_VALUE_KEY = "value"


class CursorStore:
    """Per-stream committed-cursor checkpoint files under ``root/{pipeline}/``.

    ``set`` is called on every destination ACK with the committed watermark, so
    each stream's file holds only ACKed progress; ``get`` reads it back at the
    start of the next run. A lost or unreadable checkpoint costs a one-time full
    re-scan, never data, so both paths degrade to that re-scan -- loudly --
    rather than aborting the load.
    """

    def __init__(self, root: Path) -> None:
        self._root = root
        self._write_failures_warned: set[Path] = set()

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
            # Torn write, unparseable JSON, a bad ISO value (decode_value raises
            # ValueError/TypeError; JSONDecodeError is a ValueError), or a
            # non-UTF-8 file (UnicodeDecodeError is a ValueError). Treat any
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
        except (OSError, TypeError, ValueError) as exc:
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


def encode_cursor_state(cursor: dict[str, Any]) -> dict[str, Any]:
    """JSON-safe form of a cursor-state dict (tags ``datetime``/``date``).

    The worker wire protocol relays cursor saves as JSON; this applies the
    same tagging the resume file uses so a timestamp cursor survives the
    hop as a ``datetime``, not its ISO text.
    """
    return {key: encode_value(value) for key, value in cursor.items()}


def decode_cursor_state(cursor: dict[str, Any]) -> dict[str, Any]:
    """Inverse of :func:`encode_cursor_state`."""
    return {key: decode_value(value) for key, value in cursor.items()}


def encode_value(value: Any) -> Any:
    """Tag a ``datetime``/``date``/``time``/``Decimal`` so JSON round-trips it.

    Shared by the resume file, the worker cursor-state wire, and the gRPC cursor
    token (``src/grpc/cursor.py``) so every place a cursor is serialized carries
    the type the same way. JSON-native scalars are returned unchanged.

    Every JSON-unsupported scalar the engine can present as a cursor value is
    tagged so it round-trips losslessly into the resume bind: ``datetime`` /
    ``date`` / ``time`` (timestamp/date/time columns) and ``Decimal`` (a
    ``NUMERIC`` / ``DECIMAL`` column, which the source rebinds verbatim into
    ``cursor_field >= ?``). Flattening to ``float`` would lose precision and
    flattening to ``str`` would force the read path to guess the type back.
    """
    # datetime is a subclass of date, so check it first.
    if isinstance(value, datetime):
        return {_TYPE_KEY: "datetime", _VALUE_KEY: value.isoformat()}
    if isinstance(value, date):
        return {_TYPE_KEY: "date", _VALUE_KEY: value.isoformat()}
    if isinstance(value, time):
        return {_TYPE_KEY: "time", _VALUE_KEY: value.isoformat()}
    if isinstance(value, Decimal):
        return {_TYPE_KEY: "decimal", _VALUE_KEY: str(value)}
    return value


def decode_value(value: Any) -> Any:
    """Inverse of :func:`encode_value`; untagged scalars pass through.

    A malformed tagged value raises ``ValueError`` (never an arithmetic or
    other exception type) so the tolerant restore callers — which catch
    ``ValueError``/``TypeError`` to degrade a corrupt checkpoint to a full
    re-scan — handle every tag the same way.
    """
    if isinstance(value, dict) and _TYPE_KEY in value:
        kind = value[_TYPE_KEY]
        raw = value.get(_VALUE_KEY)
        if not isinstance(raw, str):
            raise ValueError(f"malformed tagged cursor value {value!r}")
        if kind == "datetime":
            return datetime.fromisoformat(raw)
        if kind == "date":
            return date.fromisoformat(raw)
        if kind == "time":
            return time.fromisoformat(raw)
        if kind == "decimal":
            # Decimal(raw) raises decimal.InvalidOperation (an ArithmeticError,
            # not ValueError) on a malformed value; normalize it so a corrupt
            # decimal checkpoint degrades to a re-scan like a bad datetime would,
            # instead of escaping the tolerant callers and aborting state load.
            try:
                return Decimal(raw)
            except InvalidOperation as exc:
                raise ValueError(f"malformed decimal cursor value {raw!r}") from exc
    return value
