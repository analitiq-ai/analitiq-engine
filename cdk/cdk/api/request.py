"""Everything between the declared params and the bytes on the wire.

One param table serves both roles: the read builds it from
``operations.read.params`` and layers the pagination exclusion, the
declared placements and the stream's filter overrides on top; the write
builds it from ``operations.write.<mode>.params``. Both resolve defaults
through the same CDK helper, so an unresolved default is omitted and
warned about identically.

The contract's three request binding maps -- ``headers``, ``query`` and
``path_params`` -- share one grammar, so they share one reader
(:func:`bind_request_values`). Where a bound value lands is the only thing
that differs between them, and that is the caller's business.

The request builder is a named unit rather than a closure so a page's
:class:`PreparedRequest` can be tested without a session.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Collection, Iterable, Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any
from urllib.parse import quote

from ..request_binding import (
    bind_param_refs,
    bind_record_inputs,
    resolve_param_defaults,
)
from ..resolver import Resolver

__all__ = [
    "JSON_CONTENT_TYPE",
    "ParamTable",
    "PreparedRequest",
    "RequestBuilder",
    "bind_query_and_headers",
    "bind_request_values",
    "build_write_body",
    "request_block_problem",
    "substitute_path",
]

logger = logging.getLogger(__name__)

#: Where a declared param lands. The raw contract key is ``in`` -- reading
#: it as ``location`` (the model's Python attribute name) finds nothing and
#: silently places every param in the query string.
_PLACEMENT_KEY = "in"

#: The placement that puts a param in the header map rather than the query
#: string. Named once because the request build, the reserved-header refusal
#: and the query filter all have to agree on what it means.
_HEADER_PLACEMENT = "header"

#: Placements that keep a param off the query string. ``path`` was already
#: substituted into the URL, ``header`` lands in the header map, and ``body``
#: is bound into the request body.
_OFF_QUERY_PLACEMENTS = frozenset({"body", "path", _HEADER_PLACEMENT})

#: The derived function that percent-encodes a value. The engine encodes
#: every substituted path segment itself, so this one inside ``path_params``
#: encodes twice.
_URL_ENCODE = "url_encode"

#: The media type the engine sends for a JSON body. ``cdk.api.http`` adds it
#: when a body is present and the endpoint declared none; ``write_plan``
#: reads it to decide whether a declared Content-Type is the engine's own
#: value or a conflicting one.
JSON_CONTENT_TYPE = "application/json"

#: One ``{name}`` placeholder in a declared path.
_PLACEHOLDER = re.compile(r"\{([^{}]+)\}")


@dataclass(frozen=True)
class PreparedRequest:
    """One page's request: the query string, the headers, and the body."""

    query: dict[str, Any] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)
    body: Any = None


@dataclass
class ParamTable:
    """The declared params' resolved values, and where each one lands.

    ``values`` is the single table a request materialises from: every
    ``{from_param}`` binding reads it, and each param also reaches the sink
    its declared ``in`` names.
    """

    values: dict[str, Any] = field(default_factory=dict)
    placements: dict[str, str] = field(default_factory=dict)
    #: Params the pagination loop owns (``controlled_by: pagination``). A
    #: declared fact recorded the same way ``placements`` is, and read by
    #: exactly one caller: :func:`request_block_problem`, which refuses a
    #: path placeholder bound to one. Replication-controlled params are
    #: deliberately not here -- their value is written into ``values``
    #: before the first request, so the path substitution can see it, while
    #: a page's value does not exist until the loop is running.
    pagination_controlled: frozenset[str] = frozenset()

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
            placements=_placements(declared),
            pagination_controlled=_pagination_controlled(declared),
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
            placements=_placements(declared),
            pagination_controlled=_pagination_controlled(declared),
        )

    def places_in_header(self, name: str) -> bool:
        """Whether the param *name* lands in the header map."""
        return self.placements.get(name) == _HEADER_PLACEMENT

    def places_in_query(self, name: str) -> bool:
        """Whether the param *name* lands on the query string.

        The contract requires ``in`` on every param, so a name with no
        placement here is one this table never saw declared -- a value the
        pagination loop or a stream filter put in. The query string is the
        only sink such a value can reach without a declaration to route it.
        """
        return self.placements.get(name) not in _OFF_QUERY_PLACEMENTS

    def header_params(self) -> dict[str, Any]:
        """Name the params declared ``in: header``, with the value each sends.

        A param placed here lands in the same map as ``request.headers``, so
        the reserved-header rule has to see both. The value is the resolved
        default when there is one -- ``None`` when the declaration alone is
        all there is, which is enough to judge the name.
        """
        return {
            name: self.values.get(name)
            for name, placement in self.placements.items()
            if placement == _HEADER_PLACEMENT
        }


def _placements(declared: Mapping[str, Any]) -> dict[str, str]:
    """Where each declared param lands, read from the raw contract key."""
    return {
        name: decl[_PLACEMENT_KEY]
        for name, decl in declared.items()
        if isinstance(decl, Mapping) and decl.get(_PLACEMENT_KEY)
    }


def _pagination_controlled(declared: Mapping[str, Any]) -> frozenset[str]:
    """Name the declared params whose value only the pagination loop supplies."""
    return frozenset(
        name
        for name, decl in declared.items()
        if isinstance(decl, Mapping) and decl.get("controlled_by") == "pagination"
    )


def bind_request_values(
    # Typed ``Any`` on purpose: the document is raw JSON at this point, so
    # "not an object" is a real state to refuse rather than one the
    # annotation can rule out.
    declared: Any,
    *,
    params: Mapping[str, Any],
    resolver: Resolver,
    block: str,
    endpoint: str,
) -> dict[str, Any]:
    """Resolve one declared request map (headers / query / path_params).

    The three blocks share one grammar, so they share one reader: bind
    ``{from_param}`` against the params in flight, then run the value
    expressions under the per-request policy (an unresolved value omits its
    key rather than going out raw).

    Each declared VALUE is resolved on its own. Handing the whole map to the
    resolver would put the map itself in a value position, and a header,
    query or path-param whose NAME is ``ref``, ``template``, ``literal`` or
    ``function`` would then be read as an expression marker -- ``ref`` is a
    real query parameter name, so that mis-read breaks a working endpoint
    with an error no caller classifies.
    """
    if declared is None:
        return {}
    if not isinstance(declared, Mapping):
        raise ValueError(
            f"request.{block} for endpoint {endpoint!r} must be a JSON object, "
            f"got {type(declared).__name__}"
        )
    resolved: dict[str, Any] = {}
    for name, value in bind_param_refs(dict(declared), params).items():
        bound = resolver.resolve_for_request(value)
        if bound is None and Resolver.is_expression_node(value):
            # The per-request policy: an expression with nothing to resolve
            # omits its key rather than going onto the wire raw.
            logger.warning(
                "request.%s for endpoint %r: dropping %r -- its expression "
                "resolved to nothing",
                block,
                endpoint,
                name,
            )
            continue
        resolved[name] = bound
    return resolved


def substitute_path(path: str, values: Mapping[str, Any], *, endpoint: str) -> str:
    """Replace every ``{name}`` in *path* with its bound value.

    Each value is percent-encoded as a single path segment: it crosses a
    trust boundary (config or provider data), and a value carrying ``/``,
    ``?`` or ``#`` would otherwise rewrite the URL's structure.

    An empty segment is refused alongside a missing one. ``/Contact/{id}``
    with an empty ``id`` addresses the whole collection: a read then fetches
    every record instead of one, and a PUT or PATCH targets the collection.
    ``url_encode`` returns ``""`` for an unbound input, so the empty case is
    reachable without anyone declaring it.
    """

    def bound(match: re.Match[str]) -> str:
        name = match.group(1)
        value = values.get(name)
        segment = "" if value is None else str(value)
        if not segment:
            raise ValueError(
                f"path {path!r} for endpoint {endpoint!r} has no value for the "
                f"placeholder {{{name}}}; bind it in request.path_params to "
                f"something that resolves to a non-empty value"
            )
        return quote(segment, safe="")

    return _PLACEHOLDER.sub(bound, path)


def request_block_problem(
    request_block: Mapping[str, Any],
    *,
    reserved_headers: frozenset[str] | set[str],
    paged_params: Collection[str] = (),
    header_params: Mapping[str, Any] | None = None,
) -> str | None:
    """Why this request block cannot be sent as declared, or ``None``.

    ``paged_params`` and ``header_params`` are two facts about the same
    declared param table (:meth:`ParamTable.header_params` builds the
    second), passed in because this check runs before the table's values are
    complete and must judge the declarations rather than the values.
    """
    removals = request_block.get("headers_remove")
    if removals:
        return (
            f"request.headers_remove {list(removals)} cannot be honoured: the "
            f"connection's default headers live on the shared HTTP session, "
            f"and a per-request header can only add to or override them, never "
            f"delete one. Remove the key, or move the header off the "
            f"transport's defaults."
        )
    problem = _header_map_problem(
        request_block.get("headers"),
        header_params or {},
        reserved_headers=reserved_headers,
    )
    if problem is not None:
        return problem
    problem = _path_encoding_problem(request_block)
    if problem is not None:
        return problem
    return _paged_placeholder_problem(request_block, paged_params)


def _header_map_problem(
    declared: Any,
    header_params: Mapping[str, Any],
    *,
    reserved_headers: frozenset[str] | set[str],
) -> str | None:
    """Why the headers this request sends may not go out, or ``None``.

    Two declarations reach the same header map -- ``request.headers`` and any
    param placed ``in: header`` -- so one rule judges the MERGED result. A
    rule that read one route only would refuse ``Authorization`` spelled as a
    header and permit the byte-identical param, and the param route wins on
    the wire, so the bypass is the one that ships.

    Content-Type is the engine's own: it is permitted only where the author
    declared exactly what the engine already sends, which makes the collision
    a no-op rather than a conflict nobody can see. Every other reserved name
    carries the connection's values (auth and friends), and the request build
    never sees those values -- only their names -- so an endpoint
    re-declaring one can only shadow it.
    """
    routes: list[tuple[str, str, Any]] = []
    if isinstance(declared, Mapping):
        routes += [
            ("request.headers declares", name, value)
            for name, value in declared.items()
        ]
    routes += [
        ("the param declared in: header sends", name, value)
        for name, value in header_params.items()
    ]
    for source, name, value in routes:
        lowered = str(name).lower()
        if lowered == "content-type":
            if isinstance(value, str) and value.strip().lower() == JSON_CONTENT_TYPE:
                continue
            return (
                f"{source} {name!r}, which the engine owns: it sends "
                f"{JSON_CONTENT_TYPE!r} with every JSON body. Declare that "
                f"exact value or remove the header."
            )
        if lowered in reserved_headers:
            return (
                f"{source} {name!r}, which the connection already sends; an "
                f"endpoint cannot shadow the connection's own header. Remove "
                f"it, or change the transport's headers."
            )
    return None


def _path_encoding_problem(request_block: Mapping[str, Any]) -> str | None:
    """Why a path binding cannot be substituted as declared, or ``None``.

    :func:`substitute_path` percent-encodes every segment it substitutes, so
    a binding that encodes the value itself sends ``a%252Fb`` where the
    provider expects ``a%2Fb`` and answers 404.
    """
    bindings = request_block.get("path_params")
    if not isinstance(bindings, Mapping):
        return None
    for name, binding in bindings.items():
        if _calls_url_encode(binding):
            return (
                f"request.path_params binds {{{name}}} through the "
                f"{_URL_ENCODE!r} function, which encodes the value a second "
                f"time: the engine already percent-encodes every substituted "
                f"path segment. Bind the raw value."
            )
    return None


def _calls_url_encode(node: Any) -> bool:
    """Whether *node* reaches the ``url_encode`` function anywhere inside it."""
    if isinstance(node, Mapping):
        if node.get("function") == _URL_ENCODE:
            return True
        return any(_calls_url_encode(child) for child in node.values())
    if isinstance(node, list):
        return any(_calls_url_encode(item) for item in node)
    return False


def _paged_placeholder_problem(
    request_block: Mapping[str, Any], paged_params: Collection[str]
) -> str | None:
    """Why a path placeholder cannot be substituted, or ``None``.

    A pagination-owned param has no value until the loop is running, and the
    path is substituted once before the first request. Freezing page one's
    value into the URL forever is a read that reports success while fetching
    the same page, so it is refused by name.
    """
    path = request_block.get("path")
    if not isinstance(path, str) or not paged_params:
        return None
    bindings = request_block.get("path_params")
    if not isinstance(bindings, Mapping):
        return None
    for match in _PLACEHOLDER.finditer(path):
        name = match.group(1)
        binding = bindings.get(name)
        if not isinstance(binding, Mapping):
            continue
        source = binding.get("from_param")
        if isinstance(source, str) and source in paged_params:
            return (
                f"path {path!r} binds the placeholder {{{name}}} to the param "
                f"{source!r}, which the pagination loop owns; the path is "
                f"substituted once per read, so a per-page value can never "
                f"reach it"
            )
    return None


def bind_query_and_headers(
    *,
    table: ParamTable,
    params: Mapping[str, Any],
    declared_query: Any,
    declared_headers: Any,
    resolver: Resolver,
    endpoint: str,
) -> tuple[dict[str, Any], dict[str, str]]:
    """Build the query string and the header map one request sends.

    Both roles call this. The placement vocabulary is declared once in the
    endpoint document, so it has to be honoured once: a write that read
    ``request.query`` but ignored ``in: query`` would drop the API key the
    identical read sends, and nothing would report it.
    """
    headers = {
        str(name): str(value)
        for name, value in bind_request_values(
            declared_headers,
            params=params,
            resolver=resolver,
            block="headers",
            endpoint=endpoint,
        ).items()
    }
    # A param declared ``in: header`` is the more specific statement, so it
    # wins over the endpoint-wide headers map.
    headers.update(
        {
            str(name): str(value)
            for name, value in params.items()
            if table.places_in_header(name)
        }
    )
    query = {
        name: value for name, value in params.items() if table.places_in_query(name)
    }
    # request.query is the endpoint's explicit statement of what goes on the
    # query string, so it wins a key collision with a param.
    query.update(
        bind_request_values(
            declared_query,
            params=params,
            resolver=resolver,
            block="query",
            endpoint=endpoint,
        )
    )
    return query, headers


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
        declared_query: Mapping[str, Any] | None = None,
        declared_headers: Mapping[str, Any] | None = None,
    ) -> None:
        self._table = table
        # Only the contract's POST read request declares a body; a GET read
        # structurally has none, so ``None`` here means "send no body".
        self._raw_body = raw_body
        self._resolver = resolver
        self._endpoint = endpoint
        self._declared_query = declared_query
        self._declared_headers = declared_headers

    def for_page(
        self, page_params: Mapping[str, Any], *, sends_declared_body: bool = True
    ) -> PreparedRequest:
        """Return what one page actually sends: query, headers and body.

        Built per page, not once: a body-paginated endpoint must see the
        values the pagination loop set (limit, offset, cursor) rather than
        their initial values frozen at the first request.

        ``sends_declared_body`` is False on a provider-supplied continuation,
        which the contract says replaces the whole request: it carries its
        own query and takes no declared body. The endpoint's headers still
        go out -- they describe how this connection talks to the provider,
        not which page is being asked for.
        """
        # A link continuation arrives with no params of its own, so the
        # bindings read the table too: a header-placed param must carry the
        # same value on page two as on page one.
        binding_params = {**self._table.values, **page_params}
        query, headers = bind_query_and_headers(
            table=self._table,
            params=binding_params,
            # A continuation replaces the whole request, query string
            # included, so the endpoint's own query map does not apply to it.
            declared_query=self._declared_query if sends_declared_body else None,
            declared_headers=self._declared_headers,
            resolver=self._resolver,
            endpoint=self._endpoint,
        )
        if not sends_declared_body:
            query = dict(page_params)

        if self._raw_body is None or not sends_declared_body:
            return PreparedRequest(query=query, headers=headers, body=None)
        bound = bind_param_refs(
            self._raw_body,
            {
                name: _body_number(name, value, self._endpoint)
                for name, value in page_params.items()
            },
        )
        return PreparedRequest(
            query=query,
            headers=headers,
            body=_require_body(
                self._resolver.resolve_for_request(bound), self._endpoint
            ),
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
