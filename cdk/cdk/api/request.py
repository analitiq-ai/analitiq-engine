"""Everything between the declared params and the bytes on the wire.

One param table serves both roles: the read builds it from
``operations.read.params``, leaving out what the pagination and replication
loops own and taking the stream's filters on top; the write builds it from
``operations.write.<mode>.params``. Both resolve defaults through the same
CDK helper, so an unresolved default is omitted and warned about
identically -- and both carry the same
:class:`~cdk.api.param_constraints.ParamChecker`, which is what turns that
omission into a refusal for a param the endpoint declared ``required``.

A filter is not an override. It names a param the endpoint declared, under
an operator that param declared, exactly once; each of those three is a
refusal here rather than a value quietly reaching the wire under a
comparison, or in a quantity, the stream did not ask for.

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
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from functools import partial
from types import MappingProxyType
from typing import Any
from urllib.parse import quote

from analitiq.contracts.endpoints import (
    ApiEndpointDoc,
    Expression,
    Pagination,
    Param,
    ReadOperation,
    ReadRequest,
    WriteOperation,
    WriteRequest,
)
from analitiq.contracts.stream import Filter

from ..request_binding import (
    bind_param_refs,
    bind_record_inputs,
    collect_from_param_names,
    resolve_param_defaults,
)
from ..resolver import REQUEST_CONNECTION_SUBTREES, Resolver, scope_paths
from ..transport_factory import require_wire_safe_header
from .body import unsupported_media_type
from .exceptions import RequestSpecError, request_spec_errors
from .param_constraints import ParamChecker
from .query_style import (
    QueryStyle,
    declared_query_styles,
    declared_style,
    reaches_the_wire,
    serialize_query_value,
    unserializable_style_problem,
)
from .strategies import PRE_PAGE_VALUE_PATHS

__all__ = [
    "REQUEST_SUPPLIED_CONNECTION_ROOTS",
    "REQUEST_SUPPLIED_CONNECTION_SCOPES",
    "ROLE_OPERATIONS",
    "ParamTable",
    "PreparedRequest",
    "RequestBuilder",
    "bind_query_and_headers",
    "bind_request_values",
    "build_write_body",
    "endpoint_transport_refs",
    "param_bindings",
    "path_placeholders",
    "request_block_problem",
    "request_supplies",
    "substitute_path",
]

logger = logging.getLogger(__name__)

#: One ``{name}`` placeholder in a declared path.
_PLACEHOLDER = re.compile(r"\{([^{}]+)\}")

#: The two path segments RFC 3986 resolves away rather than sends.
#:
#: The one URL rule here that is NOT delegated to ``yarl``, because yarl is
#: what performs it: ``URL('https://h/acc') / '..'`` answers
#: ``https://h/items``. Encoding cannot prevent it either -- ``.`` is
#: unreserved, so ``quote`` returns it unchanged. A guard against what the
#: library does, then, rather than a copy of a rule the library knows.
_DOT_SEGMENTS = frozenset({".", ".."})


@dataclass(frozen=True)
class PreparedRequest:
    """One page's request: the query string, the headers, and the body."""

    query: dict[str, Any] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)
    body: Any = None
    #: ``request.content_type``, or ``None`` for the JSON the engine sends
    #: when an endpoint declares none. It selects the encoding and is the
    #: header sent with it -- the two cannot disagree because one field
    #: decides both.
    content_type: str | None = None


@dataclass
class ParamTable:
    """The declared params' resolved values.

    ``values`` is the single table a request materialises from: every
    ``{from_param}`` binding in the three request maps reads it, and that
    is the only way a param reaches the wire.
    """

    #: What the declaring operation says every one of these values may be,
    #: compiled once. It travels ON the table because the table is what the
    #: request is built from, so no request can be built without the
    #: declarations that judge it in hand; required and first, because a
    #: default would be a checker that refuses nothing.
    #:
    #: That reaches the VALUES on every page, through
    #: :meth:`RequestBuilder.for_page`. Presence -- is a required param here
    #: at all -- is asked once, by the two callers that can answer it, and a
    #: connector overriding the whole read on the CDK base takes that call
    #: over along with everything else :meth:`_plan_read` does for it.
    checker: ParamChecker
    values: dict[str, Any] = field(default_factory=dict)
    #: The params the author declared ``required``, minus the ones a loop
    #: owns. Read by the binding walk: a key that resolves to nothing is
    #: dropped, unless it is the wire route of one of these.
    required: frozenset[str] = frozenset()
    #: Every param a loop owns, mapped to the loop that owns it
    #: (``controlled_by``). Read by exactly one caller:
    #: :func:`request_block_problem`, which refuses a path placeholder bound
    #: to one -- see :func:`_controlled_placeholder_problem` for why both
    #: loops are the same defect there.
    controlled_by: dict[str, str] = field(default_factory=dict)

    @classmethod
    def for_read(
        cls,
        declared: Mapping[str, Param],
        resolver: Resolver,
        *,
        endpoint: str,
        filters: Iterable[Filter] = (),
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

        A filter's OPERATOR is checked here and never rendered, because on
        this transport it has no rendering: a param reaches the provider as
        a value bound into a query key, a header or a path segment, and
        ``?updated_since=X`` carries no spelling of "greater than" the
        engine could choose -- the provider's own parameter decides what
        comparing it means. So the endpoint declares the comparison each
        param stands for, in ``Param.operators``, and the honest reading of
        a filter's operator is to hold the stream to that declaration.
        Silently binding the value under a different operator is the defect
        this replaces: ``amount gt 100`` and ``amount lt 100`` built the
        identical request, and one of the two streams was always wrong.

        A param declaring no ``operators`` at all is not stream-filterable
        (the contract says so, and a ``controlled_by`` param is forbidden to
        declare any -- so a filter aimed at one the pagination or
        replication loop owns lands here rather than being overwritten on
        page one). ``is_null``, ``is_not_null``, ``like`` and ``ilike`` never
        reach this check at all: they are the SQL path's operators, and the
        stream document's own scope validator refuses them on a connector
        source before a ``StreamSource`` exists.

        What this cannot do, and does not claim to: tell two members of one
        declared set apart. A param declaring ``["gt", "lt"]`` admits both,
        and both still bind the same value to the same key -- the engine has
        no wire form for the difference, so at most one of those two is
        truthful and the document is the only place that can say which. The
        engine holds a filter to the set; whether a SET is honest is
        decidable from the endpoint document alone and belongs to the
        contract's validator. Some sets genuinely are honest -- ``["eq",
        "in"]`` is told apart by the value's own shape, a scalar against a
        collection -- which is why this is not a cardinality rule here.

        Two filters on ONE param is the case this can answer, and does: a
        param carries one value, so the second silently overwrote the first
        and the read narrowed by half of what the stream declared.

        So is a value that serializes to nothing, for the same reason one
        step further on. ``in []`` selects no records, and under an exploded
        ``form`` array it produces no query pairs at all -- so the request
        goes out carrying no filter and the provider answers everything, the
        exact inversion of what the stream asked for. Which shapes vanish is
        the serializer's answer, not a list kept here: the same empty array
        under ``explode: false`` sends ``ids=`` and does reach.

        ``filters`` are the stream document's contract ``Filter`` models,
        so the field a filter names is a required attribute rather than a
        key that might be missing -- an unnamed filter never reaches here,
        the stream's parse refuses it.

        ``endpoint`` names the document in every refusal this table's checker
        raises later, on a page that may be the hundredth: by then the caller
        has long stopped being able to say which endpoint it was reading.
        """
        uncontrolled = {
            name: decl for name, decl in declared.items() if decl.controlled_by is None
        }
        # A default is a declared expression like any other, so a defect in
        # one leaves through the same door the binding maps use rather than
        # as whichever builtin the resolver happened to raise.
        with request_spec_errors("a read param's declared default"):
            values = resolve_param_defaults(uncontrolled, resolver)
        table = cls(
            values=values,
            controlled_by=_controlled_by(declared),
            checker=ParamChecker.for_params(declared, endpoint=endpoint),
            required=_required_names(declared),
        )
        seen: dict[str, str] = {}
        for declared_filter in filters:
            target = declared_filter.field
            if target not in declared:
                raise RequestSpecError(
                    f"the stream filters on {target!r}, which "
                    f"operations.read.params does not declare, so nothing "
                    f"can send it: the filter narrows nothing and the stream "
                    f"reads the whole collection. Declared params: "
                    f"{sorted(declared)}"
                )
            declaration = declared[target]
            if declaration.operators is None:
                owner = declaration.controlled_by
                raise RequestSpecError(
                    f"the stream filters on {target!r} with operator "
                    f"{declared_filter.operator!r}, but "
                    f"operations.read.params[{target!r}] declares no "
                    f"`operators`, which is how the endpoint says the param "
                    f"is not stream-filterable"
                    + (
                        f" -- it is owned by the {owner} loop, which the "
                        f"contract forbids from declaring any"
                        if owner is not None
                        else ""
                    )
                )
            if declared_filter.operator not in declaration.operators:
                raise RequestSpecError(
                    f"the stream filters on {target!r} with operator "
                    f"{declared_filter.operator!r}, which "
                    f"operations.read.params[{target!r}] does not declare; "
                    f"the param stands for {sorted(declaration.operators)} "
                    f"and the value would otherwise go out under a "
                    f"comparison the provider never agreed to"
                )
            if target in seen:
                raise RequestSpecError(
                    f"the stream filters on {target!r} twice, with operators "
                    f"{seen[target]!r} and {declared_filter.operator!r}; a param "
                    f"carries one value, so the second would overwrite the "
                    f"first and the read would narrow by half of what the "
                    f"stream declared while reporting success. Two bounds on "
                    f"one field need two params for the endpoint to bind them "
                    f"to -- a start/end pair"
                )
            seen[target] = declared_filter.operator
            value = declared_filter.value
            if value is not None and not reaches_the_wire(
                value, declared_style(declaration)
            ):
                raise RequestSpecError(
                    f"the stream filters on {target!r} with operator "
                    f"{declared_filter.operator!r} and a value that serializes "
                    f"to nothing under the param's declared spelling, so the "
                    f"request would carry no filter at all and the provider "
                    f"would answer the whole collection -- the inversion of "
                    f"what the filter asked for. There is no wire spelling of "
                    f"'match nothing' to send instead"
                )
            if value is None:
                # Reachable: the contract lets a non-unary filter carry a
                # null value because pydantic cannot tell an omitted key
                # from an explicit ``None``, and the unary operators that
                # legitimately have no value are already refused above.
                # Dropping it silently is the whole-collection read again.
                raise RequestSpecError(
                    f"the stream filters on {target!r} with operator "
                    f"{declared_filter.operator!r} and no value, so nothing "
                    f"can be sent for it and the stream would read the whole "
                    f"collection while reporting success"
                )
            table.values[target] = value
        return table

    @classmethod
    def for_write(
        cls, declared: Mapping[str, Param], resolver: Resolver, *, endpoint: str
    ) -> ParamTable:
        """Build the write role's table.

        Write params are request-construction inputs feeding the body's
        ``from_param`` bindings, not stream filters, so nothing is layered
        on top. ``endpoint`` names the document in the checker's refusals,
        as it does for the read role.
        """
        with request_spec_errors("a write param's declared default"):
            values = resolve_param_defaults(declared, resolver, context="write param")
        table = cls(
            values=values,
            controlled_by=_controlled_by(declared),
            checker=ParamChecker.for_params(declared, endpoint=endpoint),
        )
        return table


def _required_names(declared: Mapping[str, Param]) -> frozenset[str]:
    """Return the declared params whose absence from the request is a defect.

    A ``controlled_by`` param is left out for the reason it is exempt from
    the presence check: the pagination and replication loops decide when
    their param is in flight, so a binding of theirs resolving to nothing on
    page one is the scheme working, not a narrowing lost.
    """
    return frozenset(
        name
        for name, decl in declared.items()
        if decl.required and decl.controlled_by is None
    )


def _controlled_by(declared: Mapping[str, Param]) -> dict[str, str]:
    """Map each declared param a loop owns to the loop that owns it."""
    return {
        name: decl.controlled_by
        for name, decl in declared.items()
        if decl.controlled_by is not None
    }


def bind_request_values(
    # Typed ``Any`` on purpose. An operation's own maps arrive parsed, so
    # the contract has already ruled a non-object out for them -- but this
    # is an exported CDK helper, and a connector overriding a method on the
    # CDK base calls it with a map of its own making. "Not an object" is a
    # real state on that route, refused here rather than reaching the
    # resolver as whichever builtin error a walk over it raises.
    declared: Any,
    *,
    params: Mapping[str, Any],
    resolver: Resolver,
    block: str,
    endpoint: str,
    styles: Mapping[str, QueryStyle] = MappingProxyType({}),
    required: frozenset[str] = frozenset(),
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

    ``styles`` carries the declared wire serialization of the query keys
    that have one (:func:`~cdk.api.query_style.declared_query_styles`).
    A collection is sendable exactly where one says how, which is why the
    query is the only block that passes any: the contract requires the
    declaration on a query param and defines it nowhere else, so a
    container in a header or a path segment keeps the refusal below.
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
        sendable = partial(_sendable_value, block=block, endpoint=endpoint)
        style = styles.get(str(name))
        if style is not None and bound is not None:
            # A declared style is the one thing that turns one key into
            # several, so it is the only branch that produces more than a
            # single pair. What it produces still goes through the one
            # insertion below.
            with request_spec_errors(
                f"request.{block}.{name} for endpoint {endpoint!r}"
            ):
                spelled = serialize_query_value(
                    str(name), bound, style, endpoint=endpoint, sendable=sendable
                )
        elif bound is None and Resolver.is_expression_node(node):
            # The per-request policy: an expression with nothing to resolve
            # omits its key rather than going onto the wire raw.
            #
            # Unless the key is the wire route of a param the author declared
            # REQUIRED. The value can be present and admissible in the table
            # and still never reach the provider -- a binding that wraps
            # {from_param} in a function that cannot resolve drops the whole
            # key -- and dropping it is the declared narrowing disappearing
            # from a request that then answers the full collection. Omission
            # is right for an optional binding and wrong for this one, so the
            # rule reads the declaration rather than applying to both.
            lost = sorted(collect_from_param_names(value) & required)
            if lost:
                raise RequestSpecError(
                    f"request.{block}[{name!r}] for endpoint {endpoint!r} is "
                    f"the wire route of required param(s) {lost}, and its "
                    f"expression resolved to nothing -- so the key is not sent "
                    f"and the narrowing those params declare is absent from a "
                    f"request the provider answers in full"
                )
            logger.warning(
                "request.%s for endpoint %r: dropping %r -- its expression "
                "resolved to nothing",
                block,
                endpoint,
                name,
            )
            continue
        else:
            spelled = {name: sendable(name, bound)}
        # One insertion site, whichever branch produced the pairs, and it
        # does one thing: guard the name. Each branch has already put its
        # values through the sendable rule, so a list here is the exploded
        # array that means "this name repeats" -- never a container that
        # slipped past the scalar refusal. Two insertion sites is how the
        # collision refusal came to depend on which key the document
        # happened to declare first.
        for wire_name, wire_value in spelled.items():
            if wire_name in resolved:
                # A deepObject or an exploded object writes key names of
                # its own, so it can land on one the map already declared.
                # Whichever won, the other would be dropped without a word
                # -- the exact silence honouring the key map exists to end.
                raise RequestSpecError(
                    f"request.{block}.{name} for endpoint {endpoint!r} "
                    f"sends {wire_name!r}, which this request already "
                    f"carries; one of the two would be dropped silently. "
                    f"Rename the key, or declare a style that keeps its "
                    f"own name"
                )
            resolved[wire_name] = wire_value
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
    Reachable without anyone writing an empty literal: a template whose
    placeholder resolves to nothing renders the rest of itself, so a path
    param bound to one arrives as a shorter string or an empty one.

    A dot segment is refused for the same reason one step further on.
    Percent-encoding does not contain ``.`` -- it is unreserved, so ``quote``
    hands it back unchanged -- and the client's URL layer then removes the
    dot segment per RFC 3986, which is not encoding the value but deleting
    the segment around it: ``/accounts/{id}/items`` with ``id`` bound to
    ``..`` is SENT as ``/items``. The request addresses a different resource
    and succeeds there. Only ``.`` and ``..`` do this -- ``...`` and ``..a``
    are ordinary segments, and a value carrying a slash is already encoded
    into one segment before any of this applies.

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
        if segment in _DOT_SEGMENTS:
            raise RequestSpecError(
                f"path {path!r} for endpoint {endpoint!r} binds the "
                f"placeholder {{{name}}} to {segment!r}, which is not a value "
                f"but a relative-path step: the client removes it and the "
                f"segment before it, so the request would address a different "
                f"resource and succeed there. Bind it to a resource identifier"
            )
        # Encoding before replacing is what makes replacing by name safe:
        # ``quote`` percent-encodes braces, so no bound value can spell a
        # placeholder for a later turn of this loop to substitute again.
        substituted = substituted.replace(f"{{{name}}}", quote(segment, safe=""))
    return substituted


#: Which operations a worker in each role executes, and so which
#: ``transport_ref`` values that role's run can dispatch through. Named
#: here because the shell packs a document for one role at a time.
ROLE_OPERATIONS = {"source": ("read",), "destination": ("write",)}


def endpoint_transport_refs(
    document: ApiEndpointDoc, *, role: str, write_modes: Iterable[str] | None = None
) -> set[str]:
    """Name every transport this run's operations in *document* dispatch through.

    The read's for a source, and for a destination the write modes in
    *write_modes* -- the modes its streams actually selected -- or every
    declared mode when the caller cannot say. Scoped, because the
    transports named here are the ones whose specs (credentials resolved
    into them) travel to that worker and whose origins widen what its
    requests may reach. A worker given an operation it never executes is
    handed secrets it never sends and an allowlist wider than its own
    requests, and the bootstrap fails outright if that operation's
    transport needs a credential this run does not carry.

    Read twice from two sides of the process boundary and answered here
    once: the trusted shell asks so it can resolve those transports while
    the secrets are still in reach, and the connector asks so it
    dispatches through the one the operation named. A second reading of
    the key is a second answer, and the two disagreeing means a request
    resolved against one transport and sent on another.

    The document is parsed before it gets here -- both callers validate it
    at the boundary they receive it on -- so every level below is a named
    attribute the contract guarantees rather than a shape this function
    re-checks. What is still optional here is optional in the contract: an
    operation the document does not declare, and a request naming no
    transport. A document with no transport_ref anywhere answers the empty
    set, which is what a single-transport connector always answers.
    """
    operations = document.operations
    blocks: list[ReadOperation | WriteOperation | None] = []
    for name in ROLE_OPERATIONS.get(role, ()):
        if name != "write":
            # ``read`` is one block; ``write`` is a map of modes.
            blocks.append(operations.read)
            continue
        declared_modes = operations.write or {}
        # Selected by membership rather than by looking each name up: the
        # mode keys are the contract's closed vocabulary and the caller
        # passes plain strings, so a lookup would have to widen the key
        # type to ask a question a filter answers as it stands.
        selected = None if write_modes is None else set(write_modes)
        blocks.extend(
            operation
            for mode, operation in declared_modes.items()
            if selected is None or mode in selected
        )
    refs: set[str] = set()
    for block in blocks:
        if block is None:
            continue
        ref = block.request.transport_ref
        if ref:
            refs.add(ref)
    return refs


def request_block_problem(
    request_block: ReadRequest | WriteRequest,
    *,
    reserved_headers: frozenset[str] | set[str],
    resolver: Resolver,
    endpoint: str,
    controlled_by: Mapping[str, str] = MappingProxyType({}),
    declared_params: Mapping[str, Param] = MappingProxyType({}),
    pagination: Pagination | None = None,
    metadata: Mapping[str, Expression] | None = None,
) -> str | None:
    """Why this request block cannot be sent as declared, or ``None``.

    ``controlled_by`` is a fact about the declarations rather than about
    the values: a loop-owned param HAS no value here, which is exactly what
    :func:`_controlled_placeholder_problem` judges.

    ``resolver`` is the one the request build itself resolves through, so
    the never-fillable walk judges what this phase actually supplies rather
    than a restatement of it.

    ``declared_params``, ``pagination`` and ``metadata`` (a read's
    ``response.metadata``) are the operation's other expression carriers: a
    param ``default``, a pagination value or a metadata value reading a
    never-request-time scope is the same silent omission a request slot's
    would be, so the one secret-read walk covers all of them.
    """
    removals = request_block.headers_remove
    if removals:
        return (
            f"request.headers_remove {list(removals)} cannot be honoured: the "
            f"connection's default headers live on the shared HTTP session, "
            f"and a per-request header can only add to or override them, never "
            f"delete one. Remove the key, or move the header off the "
            f"transport's defaults."
        )
    # Judged here, before anything is sent, rather than where the body is
    # encoded: the write encodes per record inside the send, where a refusal
    # would surface as a failed batch instead of a refused schema. The media
    # type is the same on every record, so it settles once, with the rest of
    # the request block.
    #
    # ``content_type`` is declared on the branches that carry a body and
    # nowhere else, so a GET read has no attribute to read: getattr's
    # default says "this request sends no body" for every branch at once.
    problem = unsupported_media_type(getattr(request_block, "content_type", None))
    if problem is not None:
        return problem
    problem = _secret_read_problem(
        request_block, declared_params, pagination, metadata, resolver
    )
    if problem is not None:
        return problem
    problem = _header_map_problem(
        request_block.headers, reserved_headers=reserved_headers
    )
    if problem is not None:
        return problem
    problem = _query_style_problem(request_block, declared_params, endpoint)
    if problem is not None:
        return problem
    return _controlled_placeholder_problem(request_block, controlled_by)


def _query_style_problem(
    request_block: ReadRequest | WriteRequest,
    declared_params: Mapping[str, Param],
    endpoint: str,
) -> str | None:
    """Why a declared query serialization cannot be sent, or ``None``.

    The pair is defined or it is not, on every connection and for every
    value, so it is settled with the rest of the block rather than on the
    first page whose value happens to be a collection -- which for a
    param the pagination or replication loop fills would be page two of a
    read that already committed rows.
    """
    for key, style in declared_query_styles(
        request_block.query, declared_params
    ).items():
        problem = unserializable_style_problem(key, style, endpoint=endpoint)
        if problem is not None:
            return problem
    return None


#: The connection paths per-request resolution supplies, as prefixes --
#: DERIVED from the subtree names ``ConnectionRuntime.request_resolver``
#: builds its scope from, never restated, so the guard and the runtime
#: cannot disagree about what a run will fill.
REQUEST_SUPPLIED_CONNECTION_SCOPES = tuple(
    f"connection.{subtree}." for subtree in REQUEST_CONNECTION_SUBTREES
)

#: The same subtrees as exact paths. ``request_resolver`` puts each one in
#: scope as a whole mapping, so ``connection.parameters`` resolves to that
#: mapping -- a body may legitimately BE it.
REQUEST_SUPPLIED_CONNECTION_ROOTS = frozenset(
    f"connection.{subtree}" for subtree in REQUEST_CONNECTION_SUBTREES
)


def request_supplies(path: str) -> bool:
    """Whether request-time resolution puts *path* in scope.

    A prefix test alone answers no for the subtree ROOT it is a prefix of:
    ``connection.parameters`` does not start with ``connection.parameters.``
    while being exactly what the resolver supplies. Both readers -- this
    module's never-fillable guard and the conformance kit's deferral -- ask
    here rather than each spelling the test, which is how the two came to
    report a body that resolves perfectly well as one nothing can fill.

    Whether the value is SENDABLE where it lands is a different rule, and
    :func:`_sendable_value` still owns it: a whole mapping is refused in a
    header, a query value or a path segment, and permitted in a body, which
    is the only slot with a serialization for it.
    """
    return (
        path.startswith(REQUEST_SUPPLIED_CONNECTION_SCOPES)
        or path in REQUEST_SUPPLIED_CONNECTION_ROOTS
    )


#: The request slots a declaration can put an expression in. ``body`` is
#: declared on the branches that carry one and nowhere else, so the walk
#: below reads every slot with a default rather than asking which branch
#: this request is: a slot the branch does not declare has no expression in
#: it, which is the same answer as a slot it declares and leaves out.
_REQUEST_SLOTS = ("headers", "query", "body", "path_params")


def _pagination_value(pagination: Pagination | None, path: tuple[str, ...]) -> Any:
    """Return the declared value at *path* in the pagination block, or ``None``.

    The path names attributes: the pagination models spell every field the
    way the wire does, and a strategy that declares no ``limit`` (or no
    ``page``, being another strategy entirely) simply has no attribute
    there -- the same "nothing declared" the paths are read for.
    """
    node: Any = pagination
    for key in path:
        node = getattr(node, key, None)
    return node


def _declared_expressions(
    request_block: ReadRequest | WriteRequest, declared_params: Mapping[str, Param]
) -> Iterator[Any]:
    """Yield each declared expression an operation carries, one at a time.

    One at a time is the whole point. Handing a binding MAP to a scanner
    puts the map itself in a value position, and a header, query key or
    path param whose NAME is ``ref``, ``template`` or ``literal`` is then
    read as an expression marker -- so the scanner answers about that one
    key and never sees its siblings. ``ref`` is a real query parameter
    name, and a map containing one hid an ``api_key`` reading
    ``secrets.api_key`` from this guard entirely: request-time resolution
    then dropped the key, silently, on every request.

    :func:`bind_request_values` resolves these maps one value at a time for
    exactly this reason, and says so. The scan has to read them the same
    way or it is not scanning what the build will resolve.

    ``request.body`` is handed over whole: it IS one value, and the
    resolver refuses a marker with siblings there
    (:func:`~cdk.resolver.expression_node_problem`), so the same shadowing
    cannot arise. A param declaration is not an expression either -- only
    its ``default`` is.
    """
    for slot in _REQUEST_SLOTS:
        declared = getattr(request_block, slot, None)
        if slot == "body" or not isinstance(declared, Mapping):
            yield declared
        else:
            yield from declared.values()
    for declaration in declared_params.values():
        yield declaration.default


def _secret_read_problem(
    request_block: ReadRequest | WriteRequest,
    declared_params: Mapping[str, Param],
    pagination: Pagination | None,
    metadata: Mapping[str, Expression] | None,
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
    scope, page by page -- except for the values resolved BEFORE the first
    page exists (:data:`~cdk.api.strategies.PRE_PAGE_VALUE_PATHS`), which
    are judged like any other request-time read. A page size reading
    ``response.*`` resolves to nothing on every run, and ``resolve_page_size``
    answers that by warning and taking the engine's batch size instead, so
    the stream pages at a size nobody authored and still reports success.
    A read's ``response.metadata`` resolves per page through the same
    page scope, so it is judged like the pagination block: a value reading
    ``secrets.*`` would be ``None`` on every page the read ever served.
    """
    # The runtime keys THIS phase supplies, read off the resolver the phase
    # built rather than restated: the read passes batch_size, the write does
    # not, and a key outside the set (`runtime.batchsize`, or batch_size on
    # a write) is a typo that would be warn-and-omitted forever.
    supplied_runtime = {f"runtime.{key}" for key in resolver.context.runtime}

    def unfillable(path: str, *, page: bool) -> bool:
        if request_supplies(path):
            return False
        if path in supplied_runtime:
            return False
        return not (page and path.startswith("response."))

    pre_page = [_pagination_value(pagination, path) for path in PRE_PAGE_VALUE_PATHS]
    declared_values = list(_declared_expressions(request_block, declared_params))
    reads = sorted(
        {
            path
            for block in declared_values + pre_page
            for path in scope_paths(block)
            if unfillable(path, page=False)
        }
        | {path for path in scope_paths(pagination) if unfillable(path, page=True)}
        # Walked per VALUE: the map's keys are author names, and a key
        # named ``ref`` or ``literal`` would otherwise make the whole map
        # read as one expression node, hiding every sibling.
        | {
            path
            for expression in (metadata or {}).values()
            for path in scope_paths(expression)
            if unfillable(path, page=True)
        }
    )
    if not reads:
        return None
    # The message names what IS supplied from the same tuples the check
    # reads, so an author is never told to use a scope the guard refuses.
    supplied = ", ".join(
        [scope.rstrip(".") for scope in REQUEST_SUPPLIED_CONNECTION_SCOPES]
        + sorted(supplied_runtime)
    )
    return (
        f"the operation reads {', '.join(repr(path) for path in reads)}, "
        f"which request-time resolution never supplies -- it builds exactly "
        f"{supplied}; secrets and auth resolve once, engine-side, at "
        f"transport materialization -- so the value would be dropped from "
        f"every request ever sent. Route it through a declared param, a "
        f"connection parameter, or the transport's headers."
    )


def _header_map_problem(
    declared: Any, *, reserved_headers: frozenset[str] | set[str]
) -> str | None:
    """Why the headers this request sends may not go out, or ``None``.

    ``request.headers`` is the whole header map an endpoint can declare:
    its keys are the only names that reach the wire, so judging them is
    judging what goes out. A param declared ``in: header`` is named by one
    of these keys, and the key is what the provider sees.

    One rule is left here, and it is the only one a document cannot settle.
    The engine-owned names went to the contract in 1.0.0rc23 --
    ``Content-Length`` by RULE-HTTP-002 and ``Content-Type`` by
    RULE-HTTP-003, both case-insensitive, in every block that names a
    header: an endpoint's ``request.headers``, a transport's,
    ``transport_defaults`` and an ``idempotency.name``. Four routes to one
    wire, which is why they belong somewhere that sees all four at once
    rather than here, where the engine closed them one review round at a
    time. The media type is ``request.content_type`` now, and
    :mod:`cdk.api.body` is what reads it.

    What is left carries the CONNECTION's values (auth and friends), and no
    document can know them. The request build never sees those values --
    only their names -- so an endpoint re-declaring one can only shadow it.

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
    for name in declared:
        lowered = str(name).lower()
        if lowered in reserved_headers:
            return (
                f"request.headers declares {name!r}, which the connection's "
                f"transport declares. An endpoint cannot shadow a header the "
                f"connection sends, and whether a given connection fills this "
                f"one in is the connection's business rather than the "
                f"endpoint's. Remove it, or change the transport's headers."
            )
    return None


def param_bindings(node: Any) -> Iterator[str]:
    """Yield the param every ``{from_param}`` inside *node* names.

    Anywhere inside it, because a binding is a binding at any depth: the
    contract's own wiring walk reaches nested ones, so a declaration it
    accepts can carry a param through a ``function``'s input. Reading only
    the top of a value is how a rule about bindings comes to miss one.

    A ``literal`` payload is not walked, for the reason the resolver hands
    one back untouched: a binding spelled inside one is data the engine
    never resolves, so treating it as a binding would judge a declaration
    on something that never happens.

    One walk, because both readers ask the same question of the same
    grammar and differ only in what they do with the answer -- one refuses a
    loop-owned param under an engine-owned header, the other withholds a
    body a definition-only run cannot fill.
    """
    if isinstance(node, list):
        for item in node:
            yield from param_bindings(item)
        return
    if not isinstance(node, Mapping) or "literal" in node:
        return
    bound = node.get("from_param")
    if isinstance(bound, str):
        yield bound
        return
    for child in node.values():
        yield from param_bindings(child)


def _controlled_placeholder_problem(
    request_block: ReadRequest | WriteRequest, controlled_by: Mapping[str, str]
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
    path = request_block.path
    if not controlled_by:
        return None
    bindings = request_block.path_params
    if bindings is None:
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
    query_styles: Mapping[str, QueryStyle] = MappingProxyType({}),
    required: frozenset[str] = frozenset(),
) -> tuple[dict[str, Any], dict[str, str]]:
    """Build the query string and the header map one request sends.

    Both roles call this, and both send exactly the keys the endpoint's
    ``request.query`` and ``request.headers`` maps declare -- the params in
    flight reach the wire through the ``{from_param}`` bindings inside those
    maps and through nothing else.

    Exactly the keys, with one exception the contract names: a key whose
    param declares a ``style`` that writes its own names -- ``deepObject``,
    or an exploded object -- sends those instead of the key it was
    declared under. That IS the declared spelling, which is what
    ``style`` and ``explode`` exist to say.
    """
    # An endpoint's headers reach the wire by a different route than the
    # transport's, and the HTTP client judges both the same way: one rule,
    # applied wherever a header is built, so a name or value the client
    # will refuse fails here rather than on the connector's first request.
    with request_spec_errors(f"request.headers for endpoint {endpoint!r}"):
        headers = {
            str(name): require_wire_safe_header(str(name), str(value))
            for name, value in bind_request_values(
                declared_headers,
                params=params,
                resolver=resolver,
                block="headers",
                endpoint=endpoint,
                required=required,
            ).items()
        }
    query = bind_request_values(
        declared_query,
        params=params,
        resolver=resolver,
        block="query",
        endpoint=endpoint,
        styles=query_styles,
        required=required,
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
        content_type: str | None = None,
        query_styles: Mapping[str, QueryStyle] = MappingProxyType({}),
    ) -> None:
        self._table = table
        # Only the contract's POST read request declares a body; a GET read
        # structurally has none, so ``None`` here means "send no body".
        self._raw_body = raw_body
        self._resolver = resolver
        self._endpoint = endpoint
        self._declared_query = declared_query
        self._declared_headers = declared_headers
        self._content_type = content_type
        self._query_styles = query_styles

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
        # Values only, and per page: a page carries the loop's values for THIS
        # page, so it is the only point at which they can be judged at all --
        # but it is also a caller that cannot read absence, because page one
        # of a cursor scheme carries no cursor. Presence is answered once, by
        # the caller holding everything a run can produce.
        self._table.checker.check_values(binding_params)
        query, headers = bind_query_and_headers(
            params=binding_params,
            # A continuation replaces the whole request, query string
            # included, so the endpoint's own query map does not apply to it.
            declared_query=self._declared_query if sends_declared_body else None,
            declared_headers=self._declared_headers,
            resolver=self._resolver,
            endpoint=self._endpoint,
            query_styles=self._query_styles,
            required=self._table.required,
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
        return PreparedRequest(
            query=query,
            headers=headers,
            body=body,
            content_type=self._content_type,
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
