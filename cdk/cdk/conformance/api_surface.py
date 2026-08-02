"""What an endpoint document may declare that the api read path never sends.

The published contract is wider than the CDK's api path in two places. An
endpoint may name the transport its request dispatches through, and it may
map query keys onto param names. The path implements neither: it opens one
connection at connect time and sends every non-body param under the param's
own name.

Both gaps fail *silently* -- the request still goes out, just not the one
the author wrote -- so neither shows up in the executed drives next door
(:mod:`cdk.conformance.api_read_path`), which certify that what the path
does execute is buildable. A declaration the path ignores has no execution
to certify; the only way to report it is to read it. That is why these two
are enumeration and the rest is not: they are the statement "the contract
permits this and the engine drops it", which no amount of driving the read
can discover.

Also home to the readers both api modules share, so "which endpoints does
this connector read" is answered once.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .target import ConformanceTarget
from .violations import Violation

__all__ = [
    "api_base_url",
    "check_api_query_bindings",
    "check_read_transport_selection",
    "read_operations",
]

TRANSPORT_CHECK = "api-read-transport-selection"
QUERY_CHECK = "api-query-bindings"

#: The transport type the api path materializes.
HTTP_TRANSPORT_TYPE = "http"


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


def api_base_url(target: ConformanceTarget) -> str | None:
    """Return the base URL the default http transport declares literally.

    ``None`` when the connector declares no http default transport, or
    when its ``base_url`` is a value expression rather than a literal: a
    reference resolves from the connection document, which a definition-only
    run does not have. The read-path checks substitute a stand-in origin in
    that case, because what they certify -- that a path segment joins and
    that an off-origin link is refused -- holds for whatever origin the
    connection supplies.
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
    """Certify that no read asks for a transport the api path will not open.

    The contract lets a request name the transport it dispatches through
    (``operations.read.request.transport_ref``, defaulting to
    ``default_transport``). The CDK's api path does not implement that
    selection: ``connect()`` materializes one connection with no
    ``transport_ref`` and every read goes out on it. A definition naming
    any other transport is contract-valid and unexecutable -- and
    unexecutable silently, since the request still succeeds against the
    wrong origin with the wrong headers.
    """
    default_ref = target.definition.get("default_transport")
    violations: list[Violation] = []
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


def check_api_query_bindings(target: ConformanceTarget) -> list[Violation]:
    """Certify that no read renames a param through ``request.query``.

    The contract lets a request map query keys to params
    (``request.query.<key>: {"from_param": <name>}``). The api path does
    not materialize that map: ``RequestBuilder.for_page`` sends every
    non-body param under the param's own name. A binding whose key matches
    the param it names is a no-op and harmless; any other is a request the
    engine will not send, and it fails silently -- the provider simply
    never sees the key the connector meant to send.
    """
    violations: list[Violation] = []
    for label, read in read_operations(target):
        request = read.get("request")
        query = request.get("query") if isinstance(request, Mapping) else None
        if not isinstance(query, Mapping):
            continue
        for key, binding in query.items():
            bound = binding.get("from_param") if isinstance(binding, Mapping) else None
            if bound == key:
                continue
            violations.append(
                Violation(
                    QUERY_CHECK,
                    f"endpoint {label!r}: request.query declares {key!r} as "
                    f"{binding!r}, but the api path does not materialize the "
                    f"query map -- it sends every non-body param under the "
                    f"param's own name. The provider never sees {key!r}, and "
                    f"the request goes out anyway. Name the param what the "
                    f"provider expects instead.",
                )
            )
    return violations
