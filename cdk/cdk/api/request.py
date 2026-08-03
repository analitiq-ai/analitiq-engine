"""Everything between the declared params and the bytes on the wire.

One param table serves both roles: the read builds it from
``operations.read.params`` and layers the pagination exclusion and the
stream's filter overrides on top; the write builds it from
``operations.write.<mode>.params``. Both resolve defaults through the same
CDK helper, so an unresolved default is omitted and warned about
identically.

The contract's three request binding maps -- ``headers``, ``query`` and
``path_params`` -- share one grammar, so they share one reader
(:func:`bind_request_values`). Where a bound value lands is the only thing
that differs between them, and that is the caller's business.

Those three maps are also the ONLY route from a declared param to the
wire. The contract requires every declared param to be referenced by
exactly one binding, in the map its ``in`` names, so the param name is the
endpoint's internal handle and the binding-map KEY is the wire name. A
second route that emitted params under their own names sent every value
twice -- the second time under a name the provider never declared, which
for a secret-valued param put a credential on the query string. ``in`` is
a consistency fact the contract already checks, not a routing mechanism,
so nothing here reads it.

Every way a declaration here fails to become a request leaves as one
:class:`~cdk.api.exceptions.RequestSpecError`. This module is where the
resolver's exception vocabulary stops: past it, the read, the write and
the conformance kit each catch a single class and say what a failure means
for them.

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
from .exceptions import RequestSpecError, request_spec_errors

__all__ = [
    "JSON_CONTENT_TYPE",
    "ParamTable",
    "PreparedRequest",
    "RequestBuilder",
    "bind_query_and_headers",
    "bind_request_values",
    "build_write_body",
    "path_placeholders",
    "request_block_problem",
    "substitute_path",
]

logger = logging.getLogger(__name__)

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
    """The declared params' resolved values.

    ``values`` is the single table a request materialises from: every
    ``{from_param}`` binding in the three request maps reads it, and that
    is the only way a param reaches the wire.
    """

    values: dict[str, Any] = field(default_factory=dict)
    #: Params the pagination loop owns (``controlled_by: pagination``), read
    #: by exactly one caller: :func:`request_block_problem`, which refuses a
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
        # A default is a declared expression like any other, so a defect in
        # one leaves through the same door the binding maps use rather than
        # as whichever builtin the resolver happened to raise.
        with request_spec_errors("a read param's declared default"):
            values = resolve_param_defaults(uncontrolled, resolver)
        table = cls(
            values=values,
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
        with request_spec_errors("a write param's declared default"):
            values = resolve_param_defaults(declared, resolver, context="write param")
        return cls(
            values=values,
            pagination_controlled=_pagination_controlled(declared),
        )


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

    Resolving one key at a time is also what lets the failure name that
    key: every defect leaves here as a :class:`RequestSpecError` saying
    which block, which key and which endpoint.
    """
    if declared is None:
        return {}
    if not isinstance(declared, Mapping):
        raise RequestSpecError(
            f"request.{block} for endpoint {endpoint!r} must be a JSON object, "
            f"got {type(declared).__name__}"
        )
    resolved: dict[str, Any] = {}
    for name, value in declared.items():
        with request_spec_errors(f"request.{block}.{name} for endpoint {endpoint!r}"):
            # ``bind_param_refs`` turns a ``{from_param}`` node into a
            # ``{literal}`` one, so the omit rule below has to judge the
            # BOUND node: a binding whose param has no value is exactly the
            # case the rule exists for, and reading the raw declaration
            # instead would send the key with a null value.
            node = bind_param_refs(value, params)
            bound = resolver.resolve_for_request(node)
        if bound is None and Resolver.is_expression_node(node):
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


def path_placeholders(path: str) -> list[str]:
    """Name every ``{name}`` placeholder in a declared path, in order.

    The one reader of the placeholder grammar. Substitution, the refusals
    below and the conformance kit all ask this rather than each carrying a
    pattern of its own -- a second pattern is a second answer to "what is a
    placeholder", and the two would disagree the day the grammar moves.
    """
    return _PLACEHOLDER.findall(path)


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
            raise RequestSpecError(
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
    resolver: Resolver,
    paged_params: Collection[str] = (),
) -> str | None:
    """Why this request block cannot be sent as declared, or ``None``.

    ``paged_params`` is a fact about the declared param table, passed in
    because this check runs before the table's values are complete and must
    judge the declarations rather than the values.

    ``resolver`` is the one the request build itself uses, so the header
    rule below judges the value that would go out rather than the spelling
    it was declared in.
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
        reserved_headers=reserved_headers,
        resolver=resolver,
    )
    if problem is not None:
        return problem
    problem = _path_encoding_problem(request_block)
    if problem is not None:
        return problem
    return _paged_placeholder_problem(request_block, paged_params)


def _header_map_problem(
    declared: Any,
    *,
    reserved_headers: frozenset[str] | set[str],
    resolver: Resolver,
) -> str | None:
    """Why the headers this request sends may not go out, or ``None``.

    ``request.headers`` is the whole header map an endpoint can declare:
    its keys are the only names that reach the wire, so judging them is
    judging what goes out. A param declared ``in: header`` is named by one
    of these keys, and the key is what the provider sees.

    Content-Type is the engine's own: it is permitted only where the author
    declared exactly what the engine already sends, which makes the collision
    a no-op rather than a conflict nobody can see. What counts is the value
    that would reach the wire, not the spelling it was declared in -- the
    contract lets a header value be a literal or an expression, so
    ``{"literal": "application/json"}`` sends exactly what the plain string
    does and has to be read the same way.

    Every other reserved name carries the connection's values (auth and
    friends), and the request build never sees those values -- only their
    names -- so an endpoint re-declaring one can only shadow it.

    A name is all THAT rule ever has, which is why its refusal speaks of
    what the transport declares rather than of what a particular connection
    sends. The two differ: a transport header whose value resolves to
    nothing is dropped, so one connection sends it and another does not.
    Permitting the endpoint's copy for the connections that drop it would
    make the shadowing depend on a connection document nobody reads while
    authoring the endpoint.
    """
    if not isinstance(declared, Mapping):
        return None
    for name, value in declared.items():
        lowered = str(name).lower()
        if lowered == "content-type":
            sent = _header_value_sent(name, value, resolver)
            if sent is None or sent.strip().lower() == JSON_CONTENT_TYPE:
                continue
            return (
                f"request.headers declares {name!r} as {sent!r}, and the "
                f"engine owns that header: it sends {JSON_CONTENT_TYPE!r} "
                f"with every JSON body. Declare that exact value or remove "
                f"the header."
            )
        if lowered in reserved_headers:
            return (
                f"request.headers declares {name!r}, which the connection's "
                f"transport declares. An endpoint cannot shadow a header the "
                f"connection sends, and whether a given connection fills this "
                f"one in is the connection's business rather than the "
                f"endpoint's. Remove it, or change the transport's headers."
            )
    return None


def _header_value_sent(name: str, declared: Any, resolver: Resolver) -> str | None:
    """Return the value this header would carry, or ``None`` if nothing can say.

    Three answers, because the declaration has three shapes and only two of
    them settle here:

    * a plain literal is already the value :func:`bind_query_and_headers`
      stringifies onto the wire;
    * an expression node is resolved through the same call the request build
      makes, so the two cannot disagree about what it says. One that resolves
      to nothing sends no header at all -- ``bind_request_values`` drops the
      key -- so there is nothing to judge and nothing to refuse;
    * anything else is a ``{from_param}`` binding, whose value is the param
      table's and arrives per run. Judging it from the declaration would be
      guessing.

    A malformed expression leaves as a :class:`RequestSpecError` naming the
    header, the same way the request build reports it, rather than as
    whichever builtin the resolver happened to raise.
    """
    if not isinstance(declared, Mapping):
        return str(declared)
    if not Resolver.is_expression_node(declared):
        return None
    with request_spec_errors(f"request.headers.{name}"):
        resolved = resolver.resolve_for_request(declared)
    return None if resolved is None else str(resolved)


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
    for name in path_placeholders(path):
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
    params: Mapping[str, Any],
    declared_query: Any,
    declared_headers: Any,
    resolver: Resolver,
    endpoint: str,
) -> tuple[dict[str, Any], dict[str, str]]:
    """Build the query string and the header map one request sends.

    Both roles call this, and both send exactly the keys the endpoint's
    ``request.query`` and ``request.headers`` maps declare -- the params in
    flight reach the wire through the ``{from_param}`` bindings inside those
    maps and through nothing else.
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
    query = bind_request_values(
        declared_query,
        params=params,
        resolver=resolver,
        block="query",
        endpoint=endpoint,
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
        # bindings read the table too: a header bound to a declared param
        # must carry the same value on page two as on page one.
        binding_params = {**self._table.values, **page_params}
        query, headers = bind_query_and_headers(
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
        with request_spec_errors(f"request.body for endpoint {self._endpoint!r}"):
            bound = bind_param_refs(
                self._raw_body,
                {
                    name: _body_number(name, value, self._endpoint)
                    for name, value in page_params.items()
                },
            )
            body = _require_body(
                self._resolver.resolve_for_request(bound), self._endpoint
            )
        return PreparedRequest(query=query, headers=headers, body=body)


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
