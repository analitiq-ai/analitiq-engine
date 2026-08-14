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
from cdk.connection_runtime import ConnectionRuntime
from cdk.resolver import Resolver, expression_node_problem, scope_paths

from .fakes import NoSecretsResolver
from .target import ConformanceTarget
from .violations import Violation

__all__ = [
    "api_base_url",
    "check_api_has_reads",
    "check_read_transport_selection",
    "definition_resolver",
    "expression_grammar_problem",
    "fillable_at_request_time",
    "read_operations",
    "unsupplied_paths",
]

TRANSPORT_CHECK = "api-read-transport-selection"
READS_CHECK = "api-has-reads"

#: The transport type the api path materializes.
HTTP_TRANSPORT_TYPE = "http"

#: Scopes transport materialization fills in, on the trusted side -- what
#: ``ConnectionRuntime._build_resolution_context`` supplies at connect(): the
#: connection document, the secrets and auth it carries, the connector's own
#: definition, and ``runtime.connection_id``. An expression reading one
#: cannot be resolved from a definition-only run, and that says nothing
#: about the connector.
_CONNECTION_SCOPES = (
    "connection.",
    "secrets.",
    "auth.",
    "connector.",
    "runtime.connection_id",
)

#: The one scope per-request resolution supplies from a connection
#: (``ConnectionRuntime.request_resolver``). Secrets and auth are resolved
#: once, engine-side, at transport materialization, and never reach a
#: request-time scope -- a request slot reading them resolves on no run.
_REQUEST_TIME_SCOPES = ("connection.",)


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


def unsupplied_paths(declared: Any, scopes: tuple[str, ...]) -> list[str]:
    """Return the scope paths *declared* reads that *scopes* does not supply.

    The deferral primitive both phases share: a declaration defers exactly
    when it reads something (``scope_paths`` is non-empty) and everything
    it reads is supplied at that phase (this list is empty). Any path left
    over resolves on no run at that phase, so it is judged now, by name --
    one path outside the set must never let the ones inside it carry the
    whole node past the check.
    """
    return [path for path in scope_paths(declared) if not path.startswith(scopes)]


def fillable_at_request_time(declared: Any) -> bool:
    """Whether a real run's request resolution could still fill *declared*.

    Per-request resolution (``ConnectionRuntime.request_resolver``) supplies
    the connection document and nothing secret, so an expression defers
    exactly when everything it reads is ``connection.``-scoped. One reading
    ``secrets.*`` or ``auth.*`` resolves on no run -- those scopes exist
    only at transport materialization, engine-side -- so it is a defect to
    report, never a value to defer.
    """
    paths = scope_paths(declared)
    return bool(paths) and not unsupplied_paths(declared, _REQUEST_TIME_SCOPES)


def expression_grammar_problem(node: Any) -> str | None:
    """Return the first malformed expression node in *node*, or ``None``.

    The shape rules are the resolver's own
    (:func:`~cdk.resolver.expression_node_problem`), applied to a
    declaration rather than to a resolution -- which is what lets a
    deferred branch still be judged. A ``literal`` is opaque data, so an
    expression spelled inside one is not one.
    """
    if isinstance(node, list):
        return next((p for p in map(expression_grammar_problem, node) if p), None)
    if not isinstance(node, Mapping):
        return None
    if Resolver.is_expression_node(node):
        problem = expression_node_problem(node)
        if problem is not None:
            return problem
        if "literal" in node:
            return None
    return next((p for p in map(expression_grammar_problem, node.values()) if p), None)


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
    if scope_paths(base_url) or expression_grammar_problem(base_url) is not None:
        return None
    try:
        with request_spec_errors("transport base_url"):
            resolved = definition_resolver(target).resolve(base_url)
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
    grammar = expression_grammar_problem(declared)
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
    paths = scope_paths(declared)
    if paths:
        stray = unsupplied_paths(declared, _CONNECTION_SCOPES)
        if not stray:
            return []
        # Named rather than resolved: resolving over the kit's empty
        # connection would blame the connection-supplied half of a mixed
        # node -- the half production fills fine -- while the defect is the
        # path no phase ever supplies.
        return [
            Violation(
                TRANSPORT_CHECK,
                f"default_transport {default_ref!r} declares no usable "
                f"base_url ({declared!r}, which reads "
                f"{', '.join(repr(path) for path in stray)} -- not a scope "
                f"transport materialization supplies). The build resolves "
                f"the whole declaration at connect(), so every connection "
                f"fails before any stream reaches its first request.",
            )
        ]
    unusable = f"({declared!r})"
    try:
        with request_spec_errors("transport base_url"):
            resolved = definition_resolver(target).resolve(declared)
    except RequestSpecError as err:
        resolved = None
        unusable = f"({declared!r}, which does not resolve: {err})"
    if isinstance(resolved, str) and resolved:
        return []
    return [
        Violation(
            TRANSPORT_CHECK,
            f"default_transport {default_ref!r} declares no usable base_url "
            f"{unusable}. The transport build requires one that resolves to "
            f"a non-empty string, so connect() fails before any stream "
            f"reaches its first request. A reference the connection supplies "
            f"is fine -- a definition-only run cannot say what it will be -- "
            f"but nothing else may be absent or empty.",
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
        grammar = expression_grammar_problem(value)
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
        paths = scope_paths(value)
        if paths:
            stray = unsupplied_paths(value, _CONNECTION_SCOPES)
            if not stray:
                continue
            violations.append(
                Violation(
                    TRANSPORT_CHECK,
                    f"default_transport {default_ref!r} header {name!r} "
                    f"({value!r}) reads "
                    f"{', '.join(repr(path) for path in stray)} -- not a "
                    f"scope transport materialization supplies. The build "
                    f"resolves the whole value at connect(), so every "
                    f"connection fails before any stream reaches its first "
                    f"request.",
                )
            )
            continue
        try:
            with request_spec_errors(f"transport header {name!r}"):
                definition_resolver(target).resolve(value)
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
