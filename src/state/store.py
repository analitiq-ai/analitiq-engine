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
ISO text. ``encode_value``/``decode_value`` tag ``datetime``/``date``/``time``/
``Decimal`` as ``{"__type__": ..., "value": ...}`` to round-trip them
losslessly; JSON-native scalars (``int``, ``str``) pass through unchanged. This
same tagging travels the gRPC cursor token and the durable resume-state file
(see
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
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_TYPE_KEY = "__type__"
_VALUE_KEY = "value"


def parse_resume_state(raw: str | None) -> dict[str, Any]:
    """Decode a resume-state JSON payload into per-stream cursors.

    The engine emits its cursor checkpoints as ``ANALITIQ_STATE`` log lines
    (see :mod:`src.state.state_emission`); the deployment harvests those into
    durable storage and delivers them back on the next run as a resume-state
    file inside the config bundle (``state/resume.json``) -- a JSON object
    mapping ``stream_id`` to the high-water-mark cursor value. A
    fresh container's local ``state/`` directory is otherwise empty, so this
    delivered file is the only bookmark an incremental stream can resume from.
    A local run produces the same file itself at the end of a run (see
    :func:`write_resume_file`), so the restore path is identical in both
    environments. Reading a local file here keeps the engine cloud-agnostic: it
    consumes a resolved value the same way it consumes any other config input,
    and never reaches for cloud storage itself.

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
            "resume state is not valid JSON (%s); incremental streams resume "
            "with a full re-scan",
            exc,
        )
        return {}
    if not isinstance(decoded, dict):
        logger.warning(
            "resume state must be a JSON object {stream_id: cursor}; got %s; "
            "incremental streams resume with a full re-scan",
            type(decoded).__name__,
        )
        return {}
    restored: dict[str, Any] = {}
    for stream_id, value in decoded.items():
        try:
            restored[str(stream_id)] = decode_value(value)
        except (ValueError, TypeError) as exc:
            # A malformed tagged datetime/date (corrupt durable state or a bad
            # manual injection) must not abort StateManager construction. Skip
            # the stream so it resumes with a full re-scan -- loudly -- exactly
            # as CursorStore.get does for an unreadable on-disk checkpoint.
            logger.warning(
                "resume-state cursor for stream %r is unreadable (%s); that "
                "stream resumes with a full re-scan",
                stream_id,
                exc,
            )
    return restored


def load_resume_file(path: Path) -> dict[str, Any]:
    """Read the resume-state file and decode it into per-stream cursors.

    The resume file is delivered in the config bundle (cloud) or written by the
    previous local run (see :func:`write_resume_file`). An absent file is the
    normal first-run case and yields an empty mapping; an unreadable file
    degrades to a full re-scan -- loudly -- like every other resume-state defect
    (see :func:`parse_resume_state`), rather than aborting the run.
    """
    if not path.exists():
        return {}
    try:
        raw = path.read_text()
    except OSError as exc:
        logger.warning(
            "resume-state file %s is unreadable (%s); incremental streams "
            "resume with a full re-scan",
            path,
            exc,
        )
        return {}
    return parse_resume_state(raw)


def write_resume_file(path: Path, cursors: dict[str, Any]) -> None:
    """Atomically write per-stream cursors as the resume-state file.

    The map is ``{stream_id: cursor}`` with each value tagged by
    :func:`encode_value`, the exact shape :func:`parse_resume_state` decodes and
    the cloud deployment delivers -- so a local run's output and a cloud-injected
    file are byte-for-byte the same contract. Writing it lets the next local run
    resume without re-reading the per-stream checkpoints.

    A write failure must not abort an otherwise-successful run, so an
    ``OSError`` degrades to a full re-scan on the next run -- loudly -- exactly
    as :meth:`CursorStore.set` does for a per-stream checkpoint.
    """
    encoded = {stream_id: encode_value(value) for stream_id, value in cursors.items()}
    tmp = path.parent / f"{path.name}.tmp"
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_text(json.dumps(encoded, default=str))
        # Atomic swap so a concurrent/next-run reader never sees a torn file.
        os.replace(tmp, path)
    except OSError as exc:
        with contextlib.suppress(OSError):
            tmp.unlink(missing_ok=True)
        logger.warning(
            "failed to write resume-state file %s (%s); the next run resumes "
            "from the per-stream checkpoints instead",
            path,
            exc,
        )


def encode_cursor_state(cursor: dict[str, Any]) -> dict[str, Any]:
    """JSON-safe form of a cursor-state dict (tags ``datetime``/``date``).

    The worker wire protocol relays cursor saves as JSON; this applies the
    same tagging the on-disk store uses so a timestamp cursor survives the
    hop as a ``datetime``, not its ISO text.
    """
    return {key: encode_value(value) for key, value in cursor.items()}


def decode_cursor_state(cursor: dict[str, Any]) -> dict[str, Any]:
    """Inverse of :func:`encode_cursor_state`."""
    return {key: decode_value(value) for key, value in cursor.items()}


class CursorStore:
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
    """Tag a ``datetime``/``date``/``time``/``Decimal`` so JSON round-trips it.

    Shared by the on-disk checkpoint, the worker cursor-state wire, and the
    gRPC cursor token (``src/grpc/cursor.py``) so every place a cursor is
    serialized carries the type the same way. JSON-native scalars are returned
    unchanged.

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
