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
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from types import MappingProxyType
from typing import Any
from urllib.parse import quote

from ..request_binding import (
    bind_param_refs,
    bind_record_inputs,
    resolve_param_defaults,
)
from ..resolver import Resolver, scope_paths
from .exceptions import RequestSpecError, request_spec_errors

__all__ = [
    "JSON_CONTENT_TYPE",
    "REQUEST_SUPPLIED_CONNECTION_SCOPES",
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
    #: Every param a loop owns, mapped to the loop that owns it
    #: (``controlled_by``). Read by exactly one caller:
    #: :func:`request_block_problem`, which refuses a path placeholder bound
    #: to one -- see :func:`_controlled_placeholder_problem` for why both
    #: loops are the same defect there.
    controlled_by: dict[str, str] = field(default_factory=dict)

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

        A filter names a param, and a param reaches the provider only
        through a request binding that names it in turn. One naming no
        declared param therefore narrows nothing: the stream reads the whole
        collection while reporting success, which for a filter is a
        correctness failure rather than a slow read. Nothing in the contract
        links a filter to a param declaration, so this is the only place it
        can be caught, and it is caught loudly.
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
        table = cls(values=values, controlled_by=_controlled_by(declared))
        for declared_filter in filters:
            target = declared_filter.get("field")
            if not target:
                continue
            if target not in declared:
                raise RequestSpecError(
                    f"the stream filters on {target!r}, which "
                    f"operations.read.params does not declare, so nothing "
                    f"can send it: the filter narrows nothing and the stream "
                    f"reads the whole collection. Declared params: "
                    f"{sorted(declared)}"
                )
            value = declared_filter.get("value")
            if value is not None:
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
        return cls(values=values, controlled_by=_controlled_by(declared))


def _controlled_by(declared: Mapping[str, Any]) -> dict[str, str]:
    """Map each declared param a loop owns to the loop that owns it."""
    return {
        name: str(decl["controlled_by"])
        for name, decl in declared.items()
        if isinstance(decl, Mapping) and decl.get("controlled_by")
    }


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
        resolved[name] = _sendable_value(name, bound, block=block, endpoint=endpoint)
    return resolved


def _sendable_value(name: str, value: Any, *, block: str, endpoint: str) -> Any:
    """Normalize one bound value to what the wire can carry, or refuse it.

    The declared document is JSON, so a boolean goes out in its JSON
    spelling: the URL builder refuses the Python object outright, and
    ``str()`` would send ``True`` -- a spelling no JSON document contains.
    A bare null is refused loud: unlike an expression resolving to nothing
    (a per-connection fact the omit rule above drops), a declared null is
    static -- it names a key nothing can ever send, on any connection. A
    container is refused for the reason the contract demands a declared
    wire serialization for container params: there is no one way to send
    it, so guessing one would be the engine's guess on the provider's wire.
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        raise RequestSpecError(
            f"request.{block}.{name} for endpoint {endpoint!r} declares "
            f"null; nothing can send it -- remove the key or declare a value"
        )
    if isinstance(value, (Mapping, list)):
        raise RequestSpecError(
            f"request.{block}.{name} for endpoint {endpoint!r} resolves to "
            f"a {type(value).__name__}; a request value must be a scalar -- "
            f"declare how the container serializes, or flatten it"
        )
    return value


def path_placeholders(path: str) -> list[str]:
    """Name every ``{name}`` placeholder in a declared path, in order.

    The one reader of :data:`_PLACEHOLDER`. Substitution, the refusals
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

    Which spans of *path* are placeholders is :func:`path_placeholders`'s
    answer, not a second reading of the pattern here.
    """
    substituted = path
    for name in path_placeholders(path):
        value = values.get(name)
        segment = "" if value is None else str(value)
        if not segment:
            raise RequestSpecError(
                f"path {path!r} for endpoint {endpoint!r} has no value for the "
                f"placeholder {{{name}}}; bind it in request.path_params to "
                f"something that resolves to a non-empty value"
            )
        # Encoding before replacing is what makes replacing by name safe:
        # ``quote`` percent-encodes braces, so no bound value can spell a
        # placeholder for a later turn of this loop to substitute again.
        substituted = substituted.replace(f"{{{name}}}", quote(segment, safe=""))
    return substituted


def request_block_problem(
    request_block: Mapping[str, Any],
    *,
    reserved_headers: frozenset[str] | set[str],
    resolver: Resolver,
    params: Mapping[str, Any],
    controlled_by: Mapping[str, str] = MappingProxyType({}),
    declared_params: Mapping[str, Any] = MappingProxyType({}),
    pagination: Mapping[str, Any] | None = None,
) -> str | None:
    """Why this request block cannot be sent as declared, or ``None``.

    ``controlled_by`` is a fact about the declarations rather than about
    the values: a loop-owned param HAS no value here, which is exactly what
    :func:`_controlled_placeholder_problem` judges.

    ``params`` and ``resolver`` are what the request build itself uses, in
    the order it uses them -- bind, then resolve -- so the header rule below
    judges the value that would go out rather than the spelling it was
    declared in. Both roles call this after their param table is built, so
    the values are the run's own.

    ``declared_params`` and ``pagination`` are the operation's other
    expression carriers: a param ``default`` or a pagination value reading
    a never-request-time scope is the same silent omission a request slot's
    would be, so the one secret-read walk covers all of them.
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
    problem = _secret_read_problem(request_block, declared_params, pagination, resolver)
    if problem is not None:
        return problem
    problem = _header_map_problem(
        request_block.get("headers"),
        reserved_headers=reserved_headers,
        resolver=resolver,
        params=params,
    )
    if problem is not None:
        return problem
    problem = _path_encoding_problem(request_block)
    if problem is not None:
        return problem
    return _controlled_placeholder_problem(request_block, controlled_by)


#: The connection subtrees per-request resolution supplies --
#: ``ConnectionRuntime.request_resolver`` builds exactly these three. The
#: ONE statement of this fact: the conformance kit's request-phase deferral
#: imports it rather than restating it, so the kit's verdict and the
#: engine's behavior cannot disagree about what a run will fill.
REQUEST_SUPPLIED_CONNECTION_SCOPES = (
    "connection.parameters.",
    "connection.selections.",
    "connection.discovered.",
)


#: The request slots a declaration can put an expression in.
_REQUEST_SLOTS = ("headers", "query", "body", "path_params")


def _secret_read_problem(
    request_block: Mapping[str, Any],
    declared_params: Mapping[str, Any],
    pagination: Mapping[str, Any] | None,
    resolver: Resolver,
) -> str | None:
    """Why an operation reads what no request can carry, or ``None``.

    Not an error a run would surface: request-time resolution omits an
    unresolved value rather than failing, so a request slot, a param
    ``default`` or a pagination value reading what request-time resolution
    does not supply builds a request WITHOUT the declared
    value -- every request, every connection, both roles -- and the run
    stays green while the provider sees the credential-less (or filter-less,
    or unversioned) shape. The refusal therefore happens here, where the
    declarations are read, naming the phase fact. One walk over every
    expression the operation authors, so a new never-fillable spelling
    cannot slip in through a slot the scan does not name. The pagination
    block alone also reads ``response.*``: the page loop supplies that
    scope, page by page.
    """
    # The runtime keys THIS phase supplies, read off the resolver the phase
    # built rather than restated: the read passes batch_size, the write does
    # not, and a key outside the set (`runtime.batchsize`, or batch_size on
    # a write) is a typo that would be warn-and-omitted forever.
    supplied_runtime = {f"runtime.{key}" for key in resolver.context.runtime}

    def unfillable(path: str, *, page: bool) -> bool:
        if path.startswith(REQUEST_SUPPLIED_CONNECTION_SCOPES):
            return False
        if path in supplied_runtime:
            return False
        return not (page and path.startswith("response."))

    reads = sorted(
        {
            path
            for block in [request_block.get(slot) for slot in _REQUEST_SLOTS]
            + [declared_params]
            for path in scope_paths(block)
            if unfillable(path, page=False)
        }
        | {path for path in scope_paths(pagination) if unfillable(path, page=True)}
    )
    if not reads:
        return None
    return (
        f"the operation reads {', '.join(repr(path) for path in reads)}, "
        f"which request-time resolution never supplies -- it builds exactly "
        f"connection.parameters/selections/discovered and the engine's "
        f"runtime values; secrets and auth resolve once, engine-side, at "
        f"transport materialization -- so the value would be dropped from "
        f"every request ever sent. Route it through a declared param, a "
        f"connection parameter, or the transport's headers."
    )


def _header_map_problem(
    declared: Any,
    *,
    reserved_headers: frozenset[str] | set[str],
    resolver: Resolver,
    params: Mapping[str, Any],
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
            sent = _header_value_sent(name, value, resolver, params)
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


def _header_value_sent(
    name: str, declared: Any, resolver: Resolver, params: Mapping[str, Any]
) -> str | None:
    """Return the value this header would carry, or ``None`` if nothing can say.

    The build's own two steps, in the build's own order: bind
    ``{from_param}`` against the params in flight, then resolve the bound
    node. Resolving the raw declaration instead reads a binding node as an
    expression and refuses a connector that works -- a ``lookup`` over a
    ``{from_param}`` input is a correct Content-Type declaration, and the
    raw node makes it "input must resolve to a scalar; got dict".

    Binding first is also what lets the rule judge a plain
    ``{"from_param": "ct"}``: its value is the param table's, and by the
    time this runs the table is built. Only two things are left unjudged,
    and neither is a spelling of the media type:

    * a plain literal is already the value :func:`bind_query_and_headers`
      stringifies onto the wire, so it needs no resolving;
    * a bound node that resolves to nothing sends no header at all --
      ``bind_request_values`` drops the key -- so there is nothing to judge
      and nothing to refuse.

    A malformed expression leaves as a :class:`RequestSpecError` naming the
    header, the same way the request build reports it, rather than as
    whichever builtin the resolver happened to raise.
    """
    if not isinstance(declared, Mapping):
        return str(declared)
    with request_spec_errors(f"request.headers.{name}"):
        node = bind_param_refs(declared, params)
        if not Resolver.is_expression_node(node):
            return None
        resolved = resolver.resolve_for_request(node)
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


def _controlled_placeholder_problem(
    request_block: Mapping[str, Any], controlled_by: Mapping[str, str]
) -> str | None:
    """Why a path placeholder cannot be substituted, or ``None``.

    A loop-owned param has no value when the path is substituted, and the
    path is substituted once, before the first request. Both loops leave the
    placeholder empty there, so both are the same authoring defect:

    * pagination produces its first value from page one's response, so
      freezing it into the URL would read one page forever;
    * replication produces its value from the stored cursor, which does not
      exist on the first run and is never consulted at all by a
      full-refresh stream. The read fails on the URL it builds, blaming a
      binding that is correct, and then succeeds on the next run -- a
      document that works or not depending on stored state.

    Refused deterministically at plan time for both, because the value a
    loop owns is a position in a traversal and a position cannot address a
    resource.
    """
    path = request_block.get("path")
    if not isinstance(path, str) or not controlled_by:
        return None
    bindings = request_block.get("path_params")
    if not isinstance(bindings, Mapping):
        return None
    for name in path_placeholders(path):
        binding = bindings.get(name)
        if not isinstance(binding, Mapping):
            continue
        source = binding.get("from_param")
        controller = controlled_by.get(source) if isinstance(source, str) else None
        if controller is not None:
            return (
                f"path {path!r} binds the placeholder {{{name}}} to the param "
                f"{source!r}, which the {controller} loop owns; the path is "
                f"substituted once, before that loop has a value, so a "
                f"loop-owned value can never address this resource. Bind the "
                f"placeholder to a param the connection or the stream fills, "
                f"and let the {controller} loop keep {source!r} in a request "
                f"binding"
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
        which the contract says replaces the whole request: the URL carries
        its own query, so this one sends none and takes no declared body.
        The endpoint's headers still go out -- they describe how this
        connection talks to the provider, not which page is being asked for.
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

    Inside the same boundary the read's body build sits behind: one defect
    class in a declared body -- an unknown scope, a conflicting pair of
    markers, a function handed the wrong type, a binding with siblings --
    used to leave here as four different exceptions, and the write's catch
    sites classified two of them as a rejected record and let the other two
    tear down the batch. What went wrong inside a body cannot decide what
    the failure MEANS.
    """
    if body_spec is None:
        return record if record is not None else records
    with request_spec_errors(f"request.body for endpoint {endpoint!r}"):
        bound = bind_param_refs(body_spec, dict(params))
        bound = bind_record_inputs(bound, record=record, records=records)
        return _require_body(resolver.resolve_for_request(bound), endpoint)
