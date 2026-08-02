"""What an endpoint document may declare that the api read path never sends.

The published contract is wider than the CDK's api path in four places, and
a read declaring any of them still succeeds -- against the wrong host, at
the wrong URL, or without the header the author wrote:

* ``request.transport_ref``. The path opens one connection at connect time
  and dispatches every read through it.
* ``request.path_params``, and the ``in: path`` params they bind. The path
  is joined to the base URL verbatim, so a ``{placeholder}`` goes onto the
  wire as those literal characters.
* ``request.headers``, and the ``in: header`` params they bind. A read
  sends the connection's own session headers and no others.
* ``request.query``'s key map. Every non-body param is sent under the
  param's own name, so a key that renames one is never seen by the
  provider.

Every one of them fails *silently*, which is why none shows up in the
executed drives next door (:mod:`cdk.conformance.api_read_path`): those
certify that what the path does execute is buildable, and a declaration the
path ignores has no execution to certify. Reading the declaration is the
only way to report it. That is what makes these checks enumeration where
the rest is not -- they are the statement "the contract permits this and
the engine drops it", which no amount of driving the read can discover.

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
    "check_api_request_placements",
    "check_read_transport_selection",
    "read_operations",
]

TRANSPORT_CHECK = "api-read-transport-selection"
QUERY_CHECK = "api-query-bindings"
PLACEMENT_CHECK = "api-request-placements"

#: The transport type the api path materializes.
HTTP_TRANSPORT_TYPE = "http"

#: The two param placements the api path does not implement, the request
#: block that binds each, and what the path does instead.
_UNSENT_PLACEMENTS: tuple[tuple[str, str, str], ...] = (
    (
        "path",
        "path_params",
        "joins the declared path to the base URL verbatim and substitutes no "
        "placeholder, so the request goes to a URL carrying the braces.",
    ),
    (
        "header",
        "headers",
        "sends the connection's session headers and no others, so the "
        "declared header never reaches the provider.",
    ),
)


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


def check_api_request_placements(target: ConformanceTarget) -> list[Violation]:
    """Certify that no read places a param where the path cannot send it.

    The contract's ``in`` vocabulary is ``path``, ``query``, ``header`` and
    ``body``, and it cross-checks each param against the binding that names
    it -- so a document reaching here has already been told its placements
    are coherent. What it has not been told is that the api path implements
    two of the four.

    A ``path`` param binds through ``request.path_params`` to a
    ``{placeholder}`` in the path. The read joins the declared path to the
    base URL and substitutes nothing, so the request goes to a URL carrying
    the braces -- a 404 at best, and at worst a provider that treats the
    literal segment as an identifier.

    A ``header`` param binds through ``request.headers``. The read sends
    the connection's session headers and an otherwise empty header map, so
    the declared header never leaves the process. Where that header carries
    a tenant, an API version or a scope, the provider answers about
    something other than what the stream asked for -- and answers 200.

    Both then compound: every param not placed ``in: body`` is sent as a
    query parameter, so the value the author routed into a path or a header
    arrives on the query string under its param name.
    """
    violations: list[Violation] = []
    for label, read in read_operations(target):
        request = read.get("request")
        if not isinstance(request, Mapping):
            continue
        for placement, binding_key, consequence in _UNSENT_PLACEMENTS:
            names = _params_placed(read, placement)
            binding = request.get(binding_key)
            if not names and not isinstance(binding, Mapping):
                continue
            violations.append(
                Violation(
                    PLACEMENT_CHECK,
                    f"endpoint {label!r}: the read declares {binding_key} "
                    f"{sorted(binding) if isinstance(binding, Mapping) else []} "
                    f"and params {sorted(names)} placed {placement!r}, but the "
                    f"api path {consequence} Each of those values is sent as a "
                    f"query parameter under its own param name instead, and "
                    f"the request succeeds.",
                )
            )
    return violations


def _params_placed(read: Mapping[str, Any], placement: str) -> list[str]:
    """Return the read's declared params placed at *placement*."""
    declared = read.get("params")
    if not isinstance(declared, Mapping):
        return []
    return [
        name
        for name, spec in declared.items()
        if isinstance(spec, Mapping) and spec.get("in") == placement
    ]
