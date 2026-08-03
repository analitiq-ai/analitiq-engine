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
from cdk.resolver import Resolver, scope_paths

from .fakes import NoSecretsResolver
from .target import ConformanceTarget
from .violations import Violation

__all__ = [
    "api_base_url",
    "check_api_has_reads",
    "check_read_transport_selection",
    "definition_resolver",
    "read_operations",
    "reads_a_connection_scope",
]

TRANSPORT_CHECK = "api-read-transport-selection"
READS_CHECK = "api-has-reads"

#: The transport type the api path materializes.
HTTP_TRANSPORT_TYPE = "http"

#: Scopes a connection document fills in. An expression reading one cannot
#: be resolved from a definition alone, and that says nothing about the
#: connector.
_CONNECTION_SCOPES = ("connection.", "secrets.", "auth.")


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


def reads_a_connection_scope(declared: Any) -> bool:
    """Whether *declared* reads a scope only a connection document supplies."""
    return any(path.startswith(_CONNECTION_SCOPES) for path in scope_paths(declared))


def api_base_url(target: ConformanceTarget) -> str | None:
    """Return the base URL the default http transport declares literally.

    ``None`` when the ``base_url`` is a value expression rather than a
    literal -- a reference resolves from the connection document, which a
    definition-only run does not have -- and when there is no http default
    transport to read one from at all. The read-path checks substitute a
    stand-in origin either way, because what they certify (that a path
    segment joins, that an off-origin link is refused) holds for whatever
    origin the connection supplies; an absent or non-http default transport
    is a failure in its own right, reported by
    :func:`check_read_transport_selection`.
    """
    ref = target.definition.get("default_transport")
    block = target.declared_transports().get(ref) if isinstance(ref, str) else None
    if not isinstance(block, Mapping):
        return None
    if block.get("transport_type") != HTTP_TRANSPORT_TYPE:
        return None
    base_url = block.get("base_url")
    return base_url if isinstance(base_url, str) and base_url else None


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
    if reads_a_connection_scope(declared):
        return []
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
