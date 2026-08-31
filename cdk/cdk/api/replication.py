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
is decided by what the endpoint's record schema declares for the cursor
field -- its JSON type, and for an integer moment its format, which names
the unit the RECORD carries and may differ from the unit the request param
takes -- never sniffed from the value's shape.

A bound is a value, not a spelling: an epoch bound and an id bound are
``int`` so a JSON body carries a number, and the request builder spells a
query or path value when it puts it on the wire.

The operators say which way each bound faces. Under ``date-time`` the
engine sends the same value whether a bound is inclusive or not --
inclusiveness is the provider's fact, and the safety window already
re-reads the boundary. Under a format that truncates -- ``date`` and the
epoch formats -- an exclusive lower bound is rendered one unit earlier:
``gt`` on the floored value would exclude the whole unit the cursor sits
in, and every record after the cursor inside that unit with it. A bound
facing the wrong way is refused: a single mapping whose operator is
``lt``/``lte`` bounds the read from above and leaves an incremental
stream nothing to resume from. A cursor field the endpoint maps no
param for at all is refused on the same grounds and for a sharper
reason: without a mapping every run re-reads the whole collection and
reports success, so the defect never surfaces on its own.
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
from .response_schema import FieldDeclaration

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

#: The format a bound goes out in when the mapping declares none.
_DEFAULT_FORMAT = "date-time"


_UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

#: The one place an epoch unit is declared: a bound is rendered as whole
#: units since the epoch, and an integer cursor whose record field declares
#: the same format is read back as that many units after it.
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

#: The formats that truncate a moment, and the unit each truncates to. An
#: exclusive lower bound under one of these moves back a unit before it is
#: rendered, so ``gt`` on the floored value cannot exclude the cursor's
#: own unit. ``date-time`` truncates nothing and is absent.
_TRUNCATION_UNIT: dict[str, timedelta] = {"date": timedelta(days=1), **_EPOCH_UNIT}


def cursor_mapping_for(
    replication: Replication | None, cursor_field: str, *, endpoint: str
) -> CursorMapping:
    """Return the mapping the endpoint declares for the stream's cursor field.

    There is no "not declared" answer. A mapping is what names the param a
    cursor bound goes out in, so an incremental stream whose cursor field
    has none reads the whole collection on every run -- and reports success
    doing it, forever, because a full read always succeeds. That is a broken
    pairing between two documents, not a degraded mode to fall back to: the
    endpoint says it serves ``incremental`` (the engine refuses the stream at
    configure time when it does not) and then declares nothing to serve it
    with. Refusing here makes the defect visible on the first run instead of
    hiding it in a checkpoint that never moves.

    The refusal names the fields the endpoint DOES map, because the usual
    cause is the stream naming a sibling field -- ``modified_at`` against a
    document that maps ``updated_at`` -- and the answer is in that list.
    """
    if replication is None:
        raise ReadError(
            f"endpoint {endpoint!r} declares no read replication block, so it "
            f"maps no cursor field; the stream reads incrementally on "
            f"{cursor_field!r} and has no param to send a cursor bound in"
        )
    for mapping in replication.cursor_mappings:
        if mapping.cursor_field == cursor_field:
            return mapping
    raise ReadError(
        f"endpoint {endpoint!r} declares no replication.cursor_mappings entry "
        f"for cursor field {cursor_field!r}; it maps "
        f"{sorted(m.cursor_field for m in replication.cursor_mappings)}"
    )


def _parse_cursor(cursor: Any, field: FieldDeclaration) -> datetime | int:
    """Read a stored cursor the way the record field it came from declares.

    The cursor is the last record's value for the stream's cursor field,
    and the endpoint document declares that field's JSON type and format
    -- so the declaration says how to read it; nothing is guessed from the
    value's shape. A ``string`` field holds an ISO-8601 moment, whatever
    format the request param takes: a provider may answer ISO timestamps
    and take an epoch ``since``. An ``integer`` field whose format is an
    epoch format holds ticks in THAT unit -- the record's, not the request
    param's, which may render another; under a calendar format it is
    contradictory and refused; under no epoch format it holds a monotonic
    id, which comes back as the ``int``. Any other type cannot be a moment
    and is refused naming the field's type.

    The ISO parser is dateutil's rather than the stdlib's because a cursor
    string is provider data, and narrowing what parses would start failing
    cursors that work today. Every moment comes back in UTC so a date
    rendered off the cursor and one rendered off ``now`` share one
    calendar.
    """
    if field.json_type == "string":
        return _parse_iso(cursor)
    if field.json_type == "integer":
        try:
            ticks = int(str(cursor))
        except ValueError as err:
            raise ReadError(
                f"cursor value {cursor!r} is not an integer; the cursor field is "
                f"declared as type 'integer'"
            ) from err
        unit = _EPOCH_UNIT.get(field.format) if field.format is not None else None
        if unit is None:
            if field.format in _FORMATS:
                raise ReadError(
                    f"cursor field is declared as type 'integer' with format "
                    f"{field.format!r}; an integer is a moment only under an "
                    f"epoch format"
                )
            return ticks
        try:
            return _UNIX_EPOCH + ticks * unit
        except OverflowError as err:
            # A checkpoint fact, not a transient one: the same integer is
            # out of range on every retry.
            raise ReadError(
                f"cursor value {cursor!r} is outside the range a moment can "
                f"hold; the cursor field declares format {field.format!r}"
            ) from err
    raise ReadError(
        f"cursor field is declared as type {field.json_type!r}, which cannot "
        f"hold a cursor; an incremental cursor field is a string moment or an "
        f"integer"
    )


def _parse_iso(cursor: Any) -> datetime:
    from dateutil.parser import isoparse

    try:
        moment: datetime = isoparse(str(cursor))
    except (ValueError, TypeError) as err:
        raise ReadError(
            f"cursor value {cursor!r} is not an ISO timestamp; the cursor field "
            f"is declared as type 'string'"
        ) from err
    if moment.tzinfo is None:
        return moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc)


def _bound_format(mapping: CursorMapping, cursor_field: FieldDeclaration) -> str:
    """Pick the format a bound is rendered in.

    The mapping's declared format wins. With none declared the bound keeps
    the cursor's own vocabulary: a record field declaring an epoch format
    renders epoch ticks in that unit, and a string moment renders ISO.
    """
    if mapping.format is not None:
        return mapping.format
    if cursor_field.format in _EPOCH_UNIT:
        return cursor_field.format
    return _DEFAULT_FORMAT


def _lands_after(lower: str | int, upper: str | int) -> bool:
    """Whether the rendered lower bound sorts after the rendered upper bound.

    Compared as rendered, not as moments: under a truncating format two
    moments a few seconds apart render to one value, and a window whose
    ends render equal is a valid single-unit request, not a reversed one.
    Both come from one renderer, so they share a type, and in every format
    the rendered order is the moments' order.
    """
    if isinstance(lower, int) and isinstance(upper, int):
        return lower > upper
    if isinstance(lower, str) and isinstance(upper, str):
        return lower > upper
    raise TypeError(
        f"bounds {lower!r} and {upper!r} were rendered in different formats"
    )


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
    cursor_field: FieldDeclaration,
    now: datetime,
) -> dict[str, str | int]:
    """Compute the param values an incremental run sends for its stored cursor.

    ``cursor_field`` is what the endpoint document declares for the record
    field the cursor came from, which decides how the stored value is
    read; the mapping's ``format`` decides how each bound is rendered. The
    lower bound is the cursor moved back by the safety window, in its own
    vocabulary: a moment moves back by seconds, a monotonic id by that
    many ids, floored at zero. A window's upper bound is ``now``, rendered
    the same way -- an id cursor has no "now", so a window over one is
    refused. An exclusive lower bound under a truncating format goes out
    one unit earlier, so the provider's ``gt`` cannot skip the unit the
    cursor sits in. A window whose rendered start lands after its
    rendered end -- a cursor ahead of this clock by more than the safety
    window -- is refused rather than sent: a reversed range is a request
    a provider answers empty or rejects, and the checkpoint would never
    move.
    """
    check_mapping_direction(mapping)
    parsed = _parse_cursor(cursor, cursor_field)
    if isinstance(parsed, int):
        if isinstance(mapping, WindowCursorMapping):
            raise ReadError(
                f"cursor value {cursor!r} is an integer id; a start/end window "
                f"mapping needs a timestamp cursor, or a record field declaring "
                f"an epoch format"
            )
        if mapping.format is not None:
            raise ReadError(
                f"cursor value {cursor!r} is an integer id, but the mapping "
                f"declares format {mapping.format!r}; an id is sent as itself, "
                f"and a moment needs the record field to declare an epoch format"
            )
        return {mapping.param: max(0, parsed - safety_window_seconds)}
    fmt = _bound_format(mapping, cursor_field)
    render = _RENDER[fmt]
    start = parsed - timedelta(seconds=safety_window_seconds)
    if isinstance(mapping, SingleCursorMapping):
        start_operator: str = mapping.operator
    else:
        start_operator = mapping.start_operator
    if start_operator == "gt" and fmt in _TRUNCATION_UNIT:
        start -= _TRUNCATION_UNIT[fmt]
    lower = render(start)
    if isinstance(mapping, SingleCursorMapping):
        return {mapping.param: lower}
    upper = render(now.astimezone(timezone.utc))
    if _lands_after(lower, upper):
        raise ReadError(
            f"cursor value {cursor!r} is ahead of the run clock by more than "
            f"the safety window: the window start {lower!r} lands after its "
            f"end {upper!r}"
        )
    return {mapping.start_param: lower, mapping.end_param: upper}
