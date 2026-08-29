"""Binding a stream's incremental cursor to the params the endpoint declares.

The contract's cursor-mapping union parses into two classes, so which
form a mapping is comes out of the parse and is read with ``isinstance``
-- never re-derived from the keys the mapping happens to carry. A single
mapping binds one lower bound: the stored cursor moved back by the safety
window. A window mapping binds that lower bound to ``start_param`` and the
run's upper bound -- now, UTC -- to ``end_param``, so a provider that only
answers a bounded range (Stripe ``created[gte]``/``created[lte]``, Xero
``DateFrom``/``DateTo``) gets the range the author declared. Each bound is
rendered in the mapping's declared ``format``; the vocabulary is read from
the contract model, never restated here. How the stored cursor reads back
is decided by the JSON type the endpoint's record schema declares for the
cursor field, never sniffed from the value's shape.

A bound is a value, not a spelling: an epoch bound and an id bound are
``int`` so a JSON body carries a number, and the request builder spells a
query or path value when it puts it on the wire.

The operators say which way each bound faces. The engine sends the same
value whether a bound is inclusive or not -- inclusiveness is the
provider's fact, and the safety window already re-reads the boundary --
but a bound facing the wrong way is refused: a single mapping whose
operator is ``lt``/``lte`` bounds the read from above and leaves an
incremental stream nothing to resume from.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from functools import partial
from types import NoneType
from typing import Any, get_args

from analitiq.contracts.endpoints import (
    Replication,
    SingleCursorMapping,
    WindowCursorMapping,
)
from pydantic import BaseModel

from ..exceptions import ReadError

__all__ = ["check_mapping_direction", "cursor_bounds", "cursor_mapping_for"]

CursorMapping = SingleCursorMapping | WindowCursorMapping


def _literal_values(model: type[BaseModel], field: str) -> tuple[str, ...]:
    """Read the ``Literal`` vocabulary of an optional field off the model."""
    annotation = model.model_fields[field].annotation
    for member in get_args(annotation):
        if member is not NoneType:
            return tuple(get_args(member))
    raise TypeError(f"{model.__name__}.{field} declares no Literal vocabulary")


_FORMATS = _literal_values(SingleCursorMapping, "format")
if _FORMATS != _literal_values(WindowCursorMapping, "format"):
    raise TypeError(
        "the two cursor-mapping forms declare different format vocabularies"
    )

_LOWER: frozenset[str] = frozenset({"gt", "gte"})
_UPPER: frozenset[str] = frozenset({"lt", "lte"})


_UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

#: The one place an epoch unit is declared: a bound is rendered as whole
#: units since the epoch, and an integer cursor under the same format is
#: read back as that many units after it.
_EPOCH_UNIT: dict[str, timedelta] = {
    "epoch_seconds": timedelta(seconds=1),
    "epoch_milliseconds": timedelta(milliseconds=1),
}


def _iso(moment: datetime) -> str:
    return moment.isoformat().replace("+00:00", "Z")


def _epoch(moment: datetime, unit: timedelta) -> int:
    """Render a moment as whole epoch units, truncated toward the past.

    Truncation, never rounding: a lower bound that lands after the stored
    cursor opens a gap, one that lands before it only widens the replay
    the safety window already allows. Integer arithmetic on the timedelta
    rather than ``timestamp()`` so a float cannot carry the moment across
    a unit boundary.
    """
    return (moment - _UNIX_EPOCH) // unit


# One renderer per contract format. The check below fails the import when
# the contract grows a format this table cannot render, rather than sending
# a value the provider parses as something else.
_RENDER: dict[str, Callable[[datetime], str | int]] = {
    "date-time": _iso,
    "date": lambda moment: moment.date().isoformat(),
    **{fmt: partial(_epoch, unit=unit) for fmt, unit in _EPOCH_UNIT.items()},
}
if set(_RENDER) != set(_FORMATS):
    raise TypeError(
        f"cursor formats {sorted(set(_FORMATS) ^ set(_RENDER))} have no renderer"
    )


def cursor_mapping_for(
    replication: Replication | None, cursor_field: str
) -> CursorMapping | None:
    """Return the mapping declared for the stream's cursor field, or ``None``.

    ``None`` means the endpoint declares no mapping for this cursor field,
    which the caller reports as running full replication -- loudly, because
    an incremental stream silently reading everything is the failure mode
    this answer exists to make visible.
    """
    if replication is None:
        return None
    for mapping in replication.cursor_mappings:
        if mapping.cursor_field == cursor_field:
            return mapping
    return None


def _parse_cursor(cursor: Any, field_type: str, fmt: str | None) -> datetime | int:
    """Read a stored cursor the way the record field it came from declares.

    The cursor is the last record's value for the stream's cursor field,
    and the endpoint document declares that field's JSON type -- so the
    type says how to read it; nothing is guessed from the value's shape.
    A ``string`` field holds an ISO-8601 moment, whatever format the
    request param takes: a provider may answer ISO timestamps and take an
    epoch ``since``. An ``integer`` field under an epoch format holds ticks
    in that unit; under no format it holds a monotonic id, which comes back
    as the ``int``. Any other case cannot be a moment and is refused
    naming the field's type.

    The ISO parser is dateutil's rather than the stdlib's because a cursor
    string is provider data, and narrowing what parses would start failing
    cursors that work today. Every moment comes back in UTC so a date
    rendered off the cursor and one rendered off ``now`` share one
    calendar.
    """
    if field_type == "string":
        return _parse_iso(cursor, fmt)
    if field_type == "integer":
        try:
            ticks = int(str(cursor))
        except ValueError as err:
            raise ReadError(
                f"cursor value {cursor!r} is not an integer; the cursor field is "
                f"declared as type 'integer'"
            ) from err
        unit = _EPOCH_UNIT.get(fmt) if fmt is not None else None
        if unit is None:
            if fmt is not None:
                raise ReadError(
                    f"cursor field is declared as type 'integer' but the mapping "
                    f"declares format {fmt!r}; an integer is a moment only under "
                    f"an epoch format"
                )
            return ticks
        try:
            return _UNIX_EPOCH + ticks * unit
        except OverflowError as err:
            # A checkpoint fact, not a transient one: the same integer is
            # out of range on every retry.
            raise ReadError(
                f"cursor value {cursor!r} is outside the range a moment can "
                f"hold; the mapping declares format {fmt!r}"
            ) from err
    raise ReadError(
        f"cursor field is declared as type {field_type!r}, which cannot hold a "
        f"cursor; an incremental cursor field is a string moment or an integer"
    )


def _parse_iso(cursor: Any, fmt: str | None) -> datetime:
    from dateutil.parser import isoparse

    try:
        moment: datetime = isoparse(str(cursor))
    except (ValueError, TypeError) as err:
        raise ReadError(
            f"cursor value {cursor!r} is not an ISO timestamp; the cursor field "
            f"is declared as type 'string'"
            + (f" and the mapping declares format {fmt!r}" if fmt else "")
        ) from err
    if moment.tzinfo is None:
        return moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc)


def _render(moment: datetime, fmt: str | None) -> str | int:
    return _RENDER[fmt](moment) if fmt is not None else _iso(moment)


def check_mapping_direction(mapping: CursorMapping) -> None:
    """Refuse a mapping whose bounds face the wrong way.

    Decidable from the document alone, so the read path asks before it
    looks for a stored cursor: a first run has none and would otherwise
    read everything, establish a checkpoint, and only then fail.
    """
    if isinstance(mapping, SingleCursorMapping):
        if mapping.operator not in _LOWER:
            raise ReadError(
                f"cursor mapping for {mapping.cursor_field!r} binds {mapping.param!r} "
                f"with operator {mapping.operator!r}, an upper bound; an "
                f"incremental read needs a lower bound to resume from -- declare "
                f"a gt/gte operator or a start/end window mapping"
            )
        return
    if mapping.start_operator not in _LOWER or mapping.end_operator not in _UPPER:
        raise ReadError(
            f"cursor mapping for {mapping.cursor_field!r} declares "
            f"start_operator {mapping.start_operator!r} and end_operator "
            f"{mapping.end_operator!r}; a window's start must be gt/gte and "
            f"its end lt/lte"
        )


def cursor_bounds(
    mapping: CursorMapping,
    cursor: Any,
    safety_window_seconds: int,
    *,
    cursor_field_type: str,
    now: datetime,
) -> dict[str, str | int]:
    """Compute the param values an incremental run sends for its stored cursor.

    ``cursor_field_type`` is the JSON type the endpoint document declares
    for the cursor field, which decides how the stored value is read. The
    lower bound is the cursor moved back by the safety window, in its own
    vocabulary: a moment moves back by seconds, a monotonic id by that
    many ids, floored at zero. A window's upper bound is ``now``, rendered
    the same way -- an id cursor has no "now", so a window over one is
    refused.
    """
    check_mapping_direction(mapping)
    fmt = mapping.format
    parsed = _parse_cursor(cursor, cursor_field_type, fmt)
    if isinstance(parsed, int):
        if isinstance(mapping, WindowCursorMapping):
            raise ReadError(
                f"cursor value {cursor!r} is an integer id; a start/end window "
                f"mapping needs a timestamp cursor, or a declared epoch format"
            )
        return {mapping.param: max(0, parsed - safety_window_seconds)}
    lower = _render(parsed - timedelta(seconds=safety_window_seconds), fmt)
    if isinstance(mapping, SingleCursorMapping):
        return {mapping.param: lower}
    return {
        mapping.start_param: lower,
        mapping.end_param: _render(now.astimezone(timezone.utc), fmt),
    }
