"""The transport every read opens, and whether a read may open it at all.

One place is left where the published contract is wider than the CDK's api
path: ``request.transport_ref``. The path opens ``default_transport`` at
connect time and dispatches every read through it, so a request naming
another transport goes out against the wrong origin with the wrong headers
-- and still succeeds. There is no execution to drive there, so reading the
declaration is the only way to report it.

The rest of the block is about the transport itself: it has to exist, be
HTTP, and carry a base URL that resolves to something a session can be
built on.

Also home to the readers both api modules share, so "which endpoints does
this connector read" and "what resolver does a definition-only run resolve
through" are each answered once.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from cdk.api.exceptions import RequestSpecError, request_spec_errors
from cdk.api.request import request_supplies
from cdk.connection_runtime import (
    MATERIALIZATION_CONNECTION_SCALARS,
    MATERIALIZATION_CONNECTION_SUBTREES,
    MATERIALIZATION_SECRET_SCOPES,
    ConnectionRuntime,
)
from cdk.derived_functions import DEFAULT_FUNCTIONS
from cdk.exceptions import TransportSpecError
from cdk.resolver import RUNTIME_CONNECTION_ID, ResolutionContext, Resolver, scope_paths
from cdk.transport_factory import require_http_base_url, resolve_http_spec

from .fakes import NoSecretsResolver
from .target import ConformanceTarget
from .violations import Violation

__all__ = [
    "api_base_url",
    "check_api_has_reads",
    "check_read_transport_selection",
    "definition_resolver",
    "fillable_at_request_time",
    "read_operations",
    "unknown_function_problem",
]

TRANSPORT_CHECK = "api-read-transport-selection"
READS_CHECK = "api-has-reads"

#: The transport type the api path materializes.
HTTP_TRANSPORT_TYPE = "http"

#: What only a CONNECTION brings to transport materialization: exactly the
#: connection-document fields ``_build_resolution_context`` puts in scope
#: (derived from its own statement, never restated -- an unknown field like
#: ``connection.hostname`` is stray, not deferred), the secrets and auth it
#: carries, and, as an exact key, the per-connection
#: ``runtime.connection_id``. An expression reading one is deferred: a
#: definition-only run cannot say the value, and that says nothing about
#: the connector.
_TRANSPORT_DEFERRED_SCOPES = tuple(
    f"connection.{subtree}." for subtree in MATERIALIZATION_CONNECTION_SUBTREES
) + tuple(f"{scope}." for scope in MATERIALIZATION_SECRET_SCOPES)
_TRANSPORT_DEFERRED_KEYS = tuple(
    f"connection.{scalar}" for scalar in MATERIALIZATION_CONNECTION_SCALARS
) + (f"runtime.{RUNTIME_CONNECTION_ID}",)


def _definition_settled(path: str) -> bool:
    """Whether the definition-only run itself supplies *path* at connect.

    Only ``connector.*``: the kit holds the very definition production
    materialization puts in scope, so a value reading it resolves to the
    same answer here. Everything else materialization supplies is
    per-connection (``runtime.connection_id`` included) and defers.
    """
    return path.startswith("connector.")


#: Paths that ARE a scope rather than a value in it. Deferring one would
#: certify a field that resolves to a whole mapping on every connection --
#: never the scalar the transport needs.
_TRANSPORT_MAPPING_ROOTS = (
    ("connection",)
    + MATERIALIZATION_SECRET_SCOPES
    + tuple(f"connection.{subtree}" for subtree in MATERIALIZATION_CONNECTION_SUBTREES)
)


def _transport_deferral(
    target: ConformanceTarget, declared: Any, *, drops_if_null: bool = False
) -> tuple[list[str], bool]:
    """Classify a transport value's reads: ``(problems, defers)``.

    The one spelling of the materialization deferral rule, called by the
    base-url and header ladders alike so the two cannot drift. Each read
    is judged on its own, so one deferrable path never carries a broken
    one past the check, and a broken one never gets the connection-supplied
    half of a mixed node blamed:

    * a connection-supplied read (the scope prefixes, plus exact keys
      matched exactly -- ``runtime.connection_identifier`` is a typo, not
      a supply) defers;
    * a whole-scope read (``connection.parameters`` and friends) is a
      problem: it resolves to a mapping on every connection, never the
      scalar the field needs;
    * a ``connector.*`` read is definition-settled and verified path by
      path against the definition the kit holds -- production resolves the
      same document, so a path it does not declare fails connect() on
      every connection;
    * anything else is a scope no phase supplies.

    ``defers`` is True only when nothing is a problem and at least one
    read is connection-supplied.

    ``drops_if_null`` says what the BUILD does with a settled value that
    resolves to nothing, which is the one thing the two callers disagree
    about: ``resolve_http_spec`` skips a header whose value is ``None`` and
    opens the connection, so an optional header pointed at a null field in
    the definition is a working connector -- while a base URL that resolves
    to nothing is the connect() failure this check exists to catch. Judging
    both the strict way reported a green connector as broken.
    """

    def deferred(path: str) -> bool:
        return (
            path.startswith(_TRANSPORT_DEFERRED_SCOPES)
            or path in _TRANSPORT_DEFERRED_KEYS
        )

    paths = list(dict.fromkeys(scope_paths(declared)))
    problems: list[str] = []
    resolver = materialization_resolver(target)
    for path in paths:
        if path in _TRANSPORT_MAPPING_ROOTS:
            problems.append(
                f"{path!r} is a whole scope, not a value in one -- it "
                f"resolves to a mapping on every connection"
            )
        elif deferred(path):
            continue
        elif _definition_settled(path):
            try:
                with request_spec_errors(f"transport read {path!r}"):
                    settled = resolver.resolve({"ref": path})
            except RequestSpecError:
                problems.append(
                    f"{path!r} names nothing in the connector definition "
                    f"production materializes with"
                )
            else:
                # Resolving is not enough, the value has to be substitutable:
                # `connector.transports` resolves to the whole block, which
                # nothing can put in a field. A value resolving to nothing is
                # the same dead end for a base URL, and NOT one for a header
                # the build simply drops.
                if isinstance(settled, (Mapping, list)):
                    problems.append(
                        f"{path!r} resolves to a whole "
                        f"{type(settled).__name__}, not a value -- nothing "
                        f"can substitute it"
                    )
                elif settled is None and not drops_if_null:
                    problems.append(
                        f"{path!r} resolves to nothing (the definition "
                        f"declares it null), not a value -- nothing can "
                        f"substitute it"
                    )
        else:
            problems.append(
                f"{path!r} is not a scope transport materialization supplies"
            )
    return problems, (not problems and any(deferred(path) for path in paths))


def read_operations(target: ConformanceTarget) -> list[tuple[str, dict[str, Any]]]:
    """Every endpoint document's read operation, labelled for messages.

    The label is the document's own ``endpoint_id`` when it declares one,
    so a violation names what a stream's ``endpoint_ref`` names, and the
    file stem otherwise. Documents with no read operation are write-only
    and carry nothing the read-path checks certify.
    """
    reads: list[tuple[str, dict[str, Any]]] = []
    for stem, document in sorted(target.endpoints.items()):
        operations = document.get("operations")
        read = operations.get("read") if isinstance(operations, Mapping) else None
        if isinstance(read, dict):
            label = document.get("endpoint_id")
            reads.append((str(label) if label else stem, read))
    return reads


def definition_resolver(
    target: ConformanceTarget, *, runtime_values: Mapping[str, Any] | None = None
) -> Resolver:
    """Build the resolver a definition-only run resolves declarations through.

    Built the way the engine builds it -- through
    :meth:`~cdk.connection_runtime.ConnectionRuntime.request_resolver`, so
    the scope set and the registered functions are the read's own -- over
    an empty connection config and a secrets resolver that supplies
    nothing. One construction site, so a check reading a declaration and a
    drive executing one resolve identically.
    """
    runtime = ConnectionRuntime(
        raw_config={},
        connection_id="conformance-definition",
        connector_id=target.connector_id,
        connector_type=target.kind,
        resolver=NoSecretsResolver(),
        connector_definition=target.definition,
    )
    return runtime.request_resolver(runtime_values=runtime_values)


def fillable_at_request_time(declared: Any) -> bool:
    """Whether a real run's request resolution could still fill *declared*.

    The supply fact is the engine's own
    (:func:`~cdk.api.request.request_supplies`), asked rather than
    restated: an expression defers exactly when everything it reads is a
    connection value a run's request resolution will fill. One reading
    ``secrets.*``, ``auth.*`` -- or a connection field outside the supplied
    subtrees, like ``connection.name`` -- resolves on no run, so it is a
    defect to report, never a value to defer.
    """
    paths = scope_paths(declared)
    return bool(paths) and all(request_supplies(path) for path in paths)


def materialization_resolver(target: ConformanceTarget) -> Resolver:
    """Build the transport-phase resolver a definition-only run has.

    Exactly the one scope the kit itself settles: the connector's own
    definition -- the same document production materialization puts in
    scope, so a ``connector.*`` read resolves to production's answer.
    Everything a connection brings (``runtime.connection_id`` included --
    it is per-connection) stays absent, so resolving it raises rather
    than inventing a value.
    """
    return Resolver(
        ResolutionContext(connector=target.definition),
        functions=DEFAULT_FUNCTIONS,
    )


def unknown_function_problem(node: Any, resolver: Resolver) -> str | None:
    """Name the first ``function`` node *resolver* has no function for, or ``None``.

    The one thing about an expression's grammar the contract does NOT
    settle. It refuses a malformed node -- two markers, a stray sibling on
    a ``ref``, an undocumented sibling on a ``function`` -- before a
    document reaches the kit, so reading any of that here would be a second
    answer to a question already answered. It does not know the function
    REGISTRY, which is engine-owned and closed: a name outside it resolves
    on no run, and a deferred node the kit never resolves would otherwise
    carry the typo past every check and die on the connector's first
    request.

    The whole declaration is walked, because a name can sit at any depth.
    A ``literal`` is opaque data, so a function spelled inside one is not
    one.
    """
    walk = lambda child: unknown_function_problem(child, resolver)  # noqa: E731
    if isinstance(node, list):
        return next((p for p in map(walk, node) if p), None)
    if not isinstance(node, Mapping):
        return None
    if "literal" in node:
        return None
    name = node.get("function")
    if name is not None and not resolver.knows_function(name):
        return (
            f"unknown derived function {name!r}; the registry is closed "
            f"and engine-owned, so this resolves on no run "
            f"(registered: {resolver.function_names})"
        )
    return next((p for p in map(walk, node.values()) if p), None)


def api_base_url(target: ConformanceTarget) -> str | None:
    """Return the base URL the default http transport settles by itself.

    A plain string, or a value expression the definition alone resolves --
    ``{"literal": "https://..."}`` settles the same origin production's
    connect() resolves, and arming the link-origin guard with a stand-in
    instead would refuse an absolute same-origin link a real run follows.

    ``None`` only for what a definition-only run genuinely cannot say: an
    expression reading a scope the connection supplies, one that is
    malformed or does not resolve (each a transport-check finding in its
    own right, never silently absorbed here), and a missing or non-http
    default transport (reported by
    :func:`check_read_transport_selection`). The read-path checks
    substitute a stand-in origin then, because what they certify (that a
    path segment joins, that an off-origin link is refused) holds for
    whatever origin the connection supplies.
    """
    ref = target.definition.get("default_transport")
    block = target.declared_transports().get(ref) if isinstance(ref, str) else None
    if not isinstance(block, Mapping):
        return None
    if block.get("transport_type") != HTTP_TRANSPORT_TYPE:
        return None
    base_url = block.get("base_url")
    if isinstance(base_url, str):
        return base_url or None
    if unknown_function_problem(base_url, materialization_resolver(target)) is not None:
        return None
    # The same classification the base-url ladder reports on, rather than a
    # second reading of it: anything that does not settle from the definition
    # alone -- deferred to a connection, or condemned -- leaves the origin to
    # the stand-in, and a category added to the ladder cannot quietly stop
    # arming the guard with the connector's real origin.
    problems, defers = _transport_deferral(target, base_url)
    if problems or defers:
        return None
    try:
        with request_spec_errors("transport base_url"):
            resolved = materialization_resolver(target).resolve(base_url)
    except RequestSpecError:
        return None
    return resolved if isinstance(resolved, str) and resolved else None


def check_read_transport_selection(target: ConformanceTarget) -> list[Violation]:
    """Certify that the one transport every read opens exists and is HTTP.

    ``connect()`` materializes ``default_transport`` with no
    ``transport_ref`` and every read goes out on it, which makes two
    demands on the definition.

    It must name an http transport. A connector whose default is absent, or
    points at a block of another type, materializes no session and no base
    URL, so every read fails at ``connect()`` -- before a stream reads a
    row. Reported here rather than tolerated, because the read-path checks
    substitute a stand-in origin to certify what they are about, and a
    silent stand-in for a transport that does not exist would let a
    connector that cannot connect at all pass tier 1.

    And nothing may ask for another. The contract lets a request name the
    transport it dispatches through
    (``operations.read.request.transport_ref``), which is contract-valid
    and unexecutable -- and unexecutable silently, since the request still
    succeeds against the wrong origin with the wrong headers.
    """
    violations: list[Violation] = []
    default_ref = target.definition.get("default_transport")
    transports = target.declared_transports()
    block = transports.get(default_ref) if isinstance(default_ref, str) else None
    if not isinstance(block, Mapping):
        violations.append(
            Violation(
                TRANSPORT_CHECK,
                f"connector.json names default_transport {default_ref!r}, "
                f"which is not one of the declared transports "
                f"{sorted(transports)}. Every read opens the default "
                f"transport at connect time, so no stream on this connector "
                f"reaches its first request.",
            )
        )
    elif block.get("transport_type") != HTTP_TRANSPORT_TYPE:  # noqa: SIM114
        violations.append(
            Violation(
                TRANSPORT_CHECK,
                f"default_transport {default_ref!r} declares transport_type "
                f"{block.get('transport_type')!r}, not "
                f"{HTTP_TRANSPORT_TYPE!r}. An api connector's reads go out on "
                f"an HTTP session built from this block; there is no session "
                f"and no base URL to read from without one.",
            )
        )
    else:
        violations.extend(_base_url_violations(target, default_ref, block))
        violations.extend(_transport_header_violations(target, default_ref, block))
        violations.extend(_transport_spec_violations(target, default_ref, block))
    for label, read in read_operations(target):
        request = read.get("request")
        ref = request.get("transport_ref") if isinstance(request, Mapping) else None
        if ref is None or ref == default_ref:
            continue
        violations.append(
            Violation(
                TRANSPORT_CHECK,
                f"endpoint {label!r}: the read requests transport_ref {ref!r}, "
                f"but the api path opens one connection at connect time and "
                f"dispatches every read through default_transport "
                f"({default_ref!r}). This read would go out on the wrong "
                f"origin with the wrong headers and still succeed. Move the "
                f"endpoint onto the default transport, or split the connector.",
            )
        )
    return violations


def _base_url_violations(
    target: ConformanceTarget, default_ref: Any, block: Mapping[str, Any]
) -> list[Violation]:
    """Resolve the declared base URL and require a non-empty string.

    Resolved, not merely present: ``{"literal": ""}`` is a mapping and a
    truthy one, and it certifies a connector whose ``connect()`` cannot
    open a session. Only what the connection supplies is deferred, and
    only the ``base_url`` is resolved -- a transport whose headers carry
    ``${auth.access_token}`` is perfectly well-formed, so resolving the
    whole block strictly would refuse it and take the base-url check down
    with it.

    Resolving is what makes the classification matter: every way a
    declaration can be malformed arrives here as an exception, and one that
    escaped would not merely lose the base-url finding -- it would abandon
    the ``transport_ref`` loop that runs after it, so a second, unrelated
    defect would go unreported because of the first. That is why this
    catches the engine's own resolution boundary rather than a list of
    exception types kept in step by hand: the list here once named both
    ``UnresolvedValueError`` and the ``KeyError`` it subclasses, and still
    could not say what it was missing.
    """
    declared = block.get("base_url")
    grammar = unknown_function_problem(declared, materialization_resolver(target))
    if grammar is not None:
        # Judged before the deferral: a malformed node is malformed whatever
        # scope it reads, and resolve_http_spec() raises on it at connect()
        # with any connection at all. Deferring it on the scope alone
        # certified exactly that connector.
        return [
            Violation(
                TRANSPORT_CHECK,
                f"default_transport {default_ref!r} declares no usable "
                f"base_url ({declared!r}, which is malformed: {grammar}). "
                f"The transport build resolves this declaration at "
                f"connect(), so every connection fails before any stream "
                f"reaches its first request.",
            )
        ]
    problems, defers = _transport_deferral(target, declared)
    if problems:
        return [
            Violation(
                TRANSPORT_CHECK,
                f"default_transport {default_ref!r} declares no usable "
                f"base_url ({declared!r}: {'; '.join(problems)}). The build "
                f"resolves the whole declaration at connect(), so every "
                f"connection fails before any stream reaches its first "
                f"request.",
            )
        ]
    if defers:
        return []
    # No scope paths, or only definition-settled ones: the declaration is
    # the kit's to resolve, to the same answer production materializes.
    try:
        with request_spec_errors("transport base_url"):
            resolved = materialization_resolver(target).resolve(declared)
        # The engine's own definition of a usable base URL (non-empty,
        # absolute http(s)), so the kit and connect() cannot disagree.
        require_http_base_url(resolved)
    except RequestSpecError as err:
        unusable = f"({declared!r}, which does not resolve: {err})"
    except TransportSpecError as err:
        unusable = f"({declared!r}: {err})"
    else:
        return []
    return [
        Violation(
            TRANSPORT_CHECK,
            f"default_transport {default_ref!r} declares no usable base_url "
            f"{unusable}. The transport build requires one that resolves to "
            f"an absolute http(s) URL, so connect() fails before any stream "
            f"reaches its first request. A reference the connection supplies "
            f"is fine -- a definition-only run cannot say what it will be -- "
            f"but nothing else may be absent, empty or schemeless.",
        )
    ]


def _transport_header_violations(
    target: ConformanceTarget, default_ref: Any, block: Mapping[str, Any]
) -> list[Violation]:
    """Judge the default transport's headers the way connect() will.

    ``resolve_http_spec`` requires ``headers`` to be an object and resolves
    every value in it before any read goes out, so a non-object block or a
    value that cannot resolve fails the whole connector at ``connect()`` --
    while a check reading only the base URL reports the transport usable.

    Each value gets the base-url treatment: grammar judged always, the
    value deferred only when materialization supplies everything it reads
    (connection, secrets and auth -- and a mixed value naming any other
    scope is refused by the stray path's name), resolved otherwise. A
    value resolving to ``None`` is fine: the build drops that header
    rather than sending it empty.
    """
    declared = block.get("headers")
    if declared is None:
        return []
    if not isinstance(declared, Mapping):
        return [
            Violation(
                TRANSPORT_CHECK,
                f"default_transport {default_ref!r} declares headers as "
                f"{declared!r}. The transport build requires an object of "
                f"name -> value, so connect() fails before any stream "
                f"reaches its first request.",
            )
        ]
    violations: list[Violation] = []
    for name, value in sorted(declared.items()):
        grammar = unknown_function_problem(value, materialization_resolver(target))
        if grammar is not None:
            violations.append(
                Violation(
                    TRANSPORT_CHECK,
                    f"default_transport {default_ref!r} header {name!r} is "
                    f"malformed ({value!r}: {grammar}). The transport build "
                    f"resolves every header value at connect(), so every "
                    f"connection fails before any stream reaches its first "
                    f"request.",
                )
            )
            continue
        problems, defers = _transport_deferral(target, value, drops_if_null=True)
        if problems:
            violations.append(
                Violation(
                    TRANSPORT_CHECK,
                    f"default_transport {default_ref!r} header {name!r} "
                    f"({value!r}: {'; '.join(problems)}). The build "
                    f"resolves the whole value at connect(), so every "
                    f"connection fails before any stream reaches its first "
                    f"request.",
                )
            )
            continue
        if defers:
            continue
        try:
            with request_spec_errors(f"transport header {name!r}"):
                materialization_resolver(target).resolve(value)
        except RequestSpecError as err:
            violations.append(
                Violation(
                    TRANSPORT_CHECK,
                    f"default_transport {default_ref!r} header {name!r} "
                    f"({value!r}) does not resolve: {err}. The transport "
                    f"build resolves every header value at connect(), and "
                    f"nothing a connection supplies is read here, so every "
                    f"connection fails the same way.",
                )
            )
    return violations


#: Stands in for a transport value whose real one a connection supplies, so
#: the spec drive below can materialize the REST of the block around it.
_STAND_IN_SPEC_VALUE = "https://conformance.invalid"


def _spec_value(
    target: ConformanceTarget, declared: Any, *, drops_if_null: bool = False
) -> Any:
    """Hand the build the real declaration, or a stand-in if it cannot judge it.

    Stood in for a value a CONNECTION supplies -- a definition-only run
    cannot say what it will be, and inventing one would make the verdict
    the kit's -- and for one the ladders have already condemned, whose
    finding is theirs to report in terms of the declaration rather than as
    a second failure out of the build.

    Everything else (a literal, or an expression the connector's own
    definition settles) goes to the build as written, so the engine judges
    the connector's actual value rather than the kit's placeholder. A
    header settling to nothing is one of those: the build drops it, and
    standing a URL in for it would certify a header the connector does not
    send (``drops_if_null``, as in :func:`_transport_deferral`).
    """
    resolver = materialization_resolver(target)
    if unknown_function_problem(declared, resolver) is not None:
        return _STAND_IN_SPEC_VALUE
    problems, defers = _transport_deferral(
        target, declared, drops_if_null=drops_if_null
    )
    return _STAND_IN_SPEC_VALUE if (problems or defers) else declared


def _transport_spec_violations(
    target: ConformanceTarget, default_ref: Any, block: Mapping[str, Any]
) -> list[Violation]:
    """Drive ``resolve_http_spec`` itself over the whole block.

    The kit does not restate what a usable transport is; it runs the build
    connect() runs. A value stands in ONLY where a connection supplies it
    -- everything a definition settles by itself is handed over verbatim,
    so the engine's own rules (an absolute http(s) origin with a host, a
    header an HTTP client will send, a coercible timeout and rate limit)
    judge the connector's real declarations. Anything the build learns to
    check is thereby checked here the same day, with no second statement
    to drift.

    The base-url and header ladders still run first: they name a deferred
    value's own defect (a malformed node, a scope no phase supplies) in
    terms of the declaration, which a stand-in would otherwise hide.
    """
    spec = dict(block)
    spec["base_url"] = _spec_value(target, block.get("base_url"))
    headers = block.get("headers")
    if isinstance(headers, Mapping):
        spec["headers"] = {
            str(name): _spec_value(target, value, drops_if_null=True)
            for name, value in headers.items()
        }
    elif headers is not None:
        # The header ladder already reported the shape; stand an empty map
        # in so the one defect does not hide the rest of the block.
        spec["headers"] = {}
    try:
        # The engine's own boundary converts the resolver's exception
        # vocabulary (UnresolvedValueError and the KeyError it subclasses
        # included, and the ArithmeticError an unnarrowable JSON number
        # raises out of `float(timeout_seconds)` / `int(max_requests)`), so
        # no defect leaves as a raw traceback that would abandon the
        # transport_ref loop after this call.
        with request_spec_errors("transport spec"):
            resolve_http_spec(spec, resolver=materialization_resolver(target))
    except RequestSpecError as err:
        return [
            Violation(
                TRANSPORT_CHECK,
                f"default_transport {default_ref!r} does not materialize: "
                f"{err}. connect() runs this exact build, so every "
                f"connection fails before any stream reaches its first "
                f"request.",
            )
        ]
    return []


def check_api_has_reads(target: ConformanceTarget) -> list[Violation]:
    """Certify that this connector gives the api checks something to drive.

    Every check here and next door iterates the read operations, so a
    connector shipping no endpoint documents -- or only write-only ones --
    satisfies all of them by having nothing to fail. The applicability gate
    does not catch it either: those modules do declare they apply to kind
    ``api``, so the run reports itself as having assessed the kind.

    That is the kit's own founding rule one level down. A green tier 1 has
    to mean the read path was exercised, not that there was no read path to
    exercise.
    """
    if target.kind != "api" or read_operations(target):
        return []
    endpoints = sorted(target.endpoints)
    carried = (
        f"the endpoint documents it does ship ({', '.join(endpoints)}) declare "
        f"no operations.read"
        if endpoints
        else "it ships no endpoint documents at all"
    )
    return [
        Violation(
            READS_CHECK,
            f"connector {target.connector_id!r} is kind 'api', but "
            f"{carried}. Every api check drives a read, so all of them pass "
            f"here by having nothing to drive -- a green run that certifies "
            f"nothing about this connector.",
        )
    ]
