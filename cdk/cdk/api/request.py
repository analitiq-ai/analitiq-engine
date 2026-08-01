"""Everything between the declared params and the bytes on the wire.

One param table serves both roles: the read builds it from
``operations.read.params`` and layers the pagination exclusion, the
declared placements and the stream's filter overrides on top; the write
builds it from ``operations.write.<mode>.params``. Both resolve defaults
through the same CDK helper, so an unresolved default is omitted and
warned about identically.

The request builder is a named unit rather than a closure so a page's
``(query, body)`` can be tested without a session.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from ..request_binding import (
    bind_param_refs,
    bind_record_inputs,
    resolve_param_defaults,
)
from ..resolver import Resolver

__all__ = ["ParamTable", "RequestBuilder", "build_write_body"]

#: Where a declared param lands. The raw contract key is ``in`` -- reading
#: it as ``location`` (the model's Python attribute name) finds nothing and
#: silently places every param in the query string.
_PLACEMENT_KEY = "in"


@dataclass
class ParamTable:
    """The declared params' resolved values, and where each one lands.

    ``values`` is the single table a page request materialises from: the
    body's ``from_param`` binding reads it whole, and the query string
    takes everything not declared ``in: body``.
    """

    values: dict[str, Any] = field(default_factory=dict)
    placements: dict[str, str] = field(default_factory=dict)

    @classmethod
    def for_read(
        cls,
        declared: Mapping[str, Any],
        resolver: Resolver,
        *,
        filters: Iterable[Mapping[str, Any]] = (),
    ) -> ParamTable:
        """Build the read role's table: defaults, then the stream's filters.

        Params declared ``controlled_by`` pagination or replication are left
        out of the defaults: their loops set them, and a resolved default
        would be overwritten on the first page anyway -- or worse, survive
        as a stale value the loop never touched.
        """
        uncontrolled = {
            name: decl
            for name, decl in declared.items()
            if isinstance(decl, Mapping) and not decl.get("controlled_by")
        }
        table = cls(
            values=resolve_param_defaults(uncontrolled, resolver),
            placements={
                name: decl[_PLACEMENT_KEY]
                for name, decl in declared.items()
                if isinstance(decl, Mapping) and decl.get(_PLACEMENT_KEY)
            },
        )
        for declared_filter in filters:
            target = declared_filter.get("field")
            value = declared_filter.get("value")
            if target and value is not None:
                table.values[target] = value
        return table

    @classmethod
    def for_write(cls, declared: Mapping[str, Any], resolver: Resolver) -> ParamTable:
        """Build the write role's table.

        Write params are request-construction inputs feeding the body's
        ``from_param`` bindings, not stream filters, so nothing is layered
        on top.
        """
        return cls(
            values=resolve_param_defaults(declared, resolver, context="write param"),
            placements={
                name: decl[_PLACEMENT_KEY]
                for name, decl in declared.items()
                if isinstance(decl, Mapping) and decl.get(_PLACEMENT_KEY)
            },
        )


def _require_body(body: Any, endpoint: str) -> Any:
    """Refuse a declared body that resolved to nothing.

    Both roles refuse it: sending ``null`` writes nothing, and reading with
    a null body sends a request the endpoint did not describe. Either way
    it is an authoring defect in the body's expressions.
    """
    if body is None:
        raise ValueError(
            f"request body for endpoint {endpoint!r} resolved to nothing; "
            f"check the endpoint's request.body expressions"
        )
    return body


def _body_number(name: str, value: Any, endpoint: str) -> Any:
    """Return a page param as the JSON number the provider sent it as.

    A page param's ``Decimal`` only ever comes from the lossless parse of a
    number the provider itself put in a response -- a keyset key, a cursor
    token. It goes back as a number, because a body schema typing that
    field as a number rejects a quoted string and the read dies after the
    first page.

    Refusing the values float cannot hold is the other half of the same
    rule. Rounding a continuation token silently is worse than not sending
    it: the provider answers a position slightly off the one the last page
    ended at, so records are skipped or repeated and nothing reports it.
    JSON has no wider number, so this is the transport's real ceiling and
    the author has to hear about it.

    Record data is a different population and keeps its exact decimal
    string (see ``encode_body``) -- that precision is the source column's,
    not a round trip of a number the provider chose.
    """
    if not isinstance(value, Decimal):
        return value
    narrowed = float(value)
    if Decimal(str(narrowed)) != value:
        raise ValueError(
            f"pagination value {name!r} for endpoint {endpoint!r} is "
            f"{value}, which cannot go into a JSON body without losing "
            f"digits; a continuation token has to survive the round trip"
        )
    return narrowed


class RequestBuilder:
    """Turns one page's param table into the query and body actually sent."""

    def __init__(
        self,
        table: ParamTable,
        *,
        raw_body: Any | None,
        resolver: Resolver,
        endpoint: str,
    ) -> None:
        self._table = table
        # Only the contract's POST read request declares a body; a GET read
        # structurally has none, so ``None`` here means "send no body".
        self._raw_body = raw_body
        self._resolver = resolver
        self._endpoint = endpoint

    def for_page(self, page_params: Mapping[str, Any]) -> tuple[dict[str, Any], Any]:
        """Return the ``(query, body)`` for a page's full param table.

        Built per page, not once: a body-paginated endpoint must see the
        values the pagination loop set (limit, offset, cursor) rather than
        their initial values frozen at the first request.
        """
        query = {
            name: value
            for name, value in page_params.items()
            if self._table.placements.get(name) != "body"
        }
        if self._raw_body is None:
            return query, None
        bound = bind_param_refs(
            self._raw_body,
            {
                name: _body_number(name, value, self._endpoint)
                for name, value in page_params.items()
            },
        )
        return query, _require_body(
            self._resolver.resolve_for_request(bound), self._endpoint
        )


def build_write_body(
    *,
    body_spec: Any | None,
    endpoint: str,
    params: Mapping[str, Any],
    resolver: Resolver,
    record: dict[str, Any] | None = None,
    records: list[dict[str, Any]] | None = None,
) -> Any:
    """Build one write request body for the in-flight record(s).

    No declared body spec: the record(s) are the body, unchanged. With a
    spec: bind ``from_param`` nodes to the declared write params and
    ``from_input`` nodes to the record data, then resolve the value
    expressions -- an unresolved expression omits its field rather than
    going onto the wire raw.
    """
    if body_spec is None:
        return record if record is not None else records
    bound = bind_param_refs(body_spec, dict(params))
    bound = bind_record_inputs(bound, record=record, records=records)
    return _require_body(resolver.resolve_for_request(bound), endpoint)
