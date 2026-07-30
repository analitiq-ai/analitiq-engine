"""The declared transport, and the auth it is supposed to carry.

An API connector has no class to audit — the CDK's own generic path
executes its definition — so the artifact under test is that definition
against what :mod:`cdk.transport_factory` can actually build from it:

* **the transport materializes** — each declared block resolves through
  the CDK's real http resolve phase, against the connection its own
  ``connection_contract`` promises. A ``${secrets.api_key}`` header with
  no input declaring ``storage: secrets`` under that name is a connection
  that can never be built; today it fails on the customer's first
  connect attempt, with the connector already published.
* **the declared auth reaches the wire** — the only auth behaviour the
  CDK executes is resolving credential material into the request the
  transport opens. A connector declaring an auth type whose transport
  reads no credential authenticates nothing; one declaring
  ``type: "none"`` while reading a secret contradicts the contract the
  control plane provisions its connections from.
"""

from __future__ import annotations

from collections.abc import Container, Mapping
from typing import TYPE_CHECKING, Any

from cdk.transport_factory import (
    HTTP_TRANSPORT_TYPE,
    registered_transport_kinds,
    resolve_http_spec,
)

from .api_endpoints import read_operations
from .declared_connection import (
    CREDENTIAL_PREFIXES,
    RESOLVE_FAILURES,
    AskedPath,
    connect_probe,
    declared_connection,
    guaranteed_connection,
    optional_paths,
    unsatisfied,
)
from .violations import Violation

if TYPE_CHECKING:
    from .target import ConformanceTarget

CHECK = "api-transport"
AUTH_CHECK = "api-auth"


def _http_transports(target: ConformanceTarget) -> dict[str, Mapping[str, Any]]:
    """Every declared transport the CDK's API path can materialize."""
    return {
        ref: block
        for ref, block in target.declared_transports().items()
        if block.get("transport_type") == HTTP_TRANSPORT_TYPE
    }


def selected_transport(
    target: ConformanceTarget,
) -> tuple[str, Mapping[str, Any]] | None:
    """Return the transport the CDK's API path actually opens, or ``None``.

    ``APIConnector.connect`` materializes the runtime with no
    ``transport_ref``, so every request a read sends goes out on
    ``default_transport`` and on nothing else. The other declared
    transports belong to auth operations and resource discovery, which the
    control plane runs — so they are what the API path *can* build, not
    what it *uses*.
    """
    ref = target.definition.get("default_transport")
    block = target.declared_transports().get(ref) if isinstance(ref, str) else None
    if block is None or block.get("transport_type") != HTTP_TRANSPORT_TYPE:
        return None
    return str(ref), block


def check_read_transport_selection(target: ConformanceTarget) -> list[Violation]:
    """Certify that no read asks for a transport the API path will not open.

    The published contract lets a request name the transport it dispatches
    through (``operations.read.request.transport_ref``, defaulting to
    ``default_transport``). The CDK's generic API path does not implement
    that selection: it materializes one connection at connect time and
    every read goes out on it. A definition naming any other transport is
    therefore contract-valid and unexecutable — and unexecutable
    *silently*, since the request still succeeds against the wrong origin
    with the wrong headers.

    Reported here rather than tolerated: a check that stayed quiet would
    certify a connector whose every read reaches the wrong host.
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
                CHECK,
                f"endpoint {label!r}: the read requests transport_ref {ref!r}, "
                f"but the CDK's API path opens one connection at connect time "
                f"and dispatches every read through default_transport "
                f"({default_ref!r}). This read would go out on the wrong "
                f"origin with the wrong headers and still succeed. Move the "
                f"endpoint onto the default transport, or split the connector.",
            )
        )
    return violations


def probe_transports(
    target: ConformanceTarget,
    raw_config: Mapping[str, Any],
    refs: Container[str] | None = None,
) -> tuple[list[Violation], list[AskedPath]]:
    """Resolve http transports against *raw_config*, with failures and paths.

    One walk serving both checks here: the transport check reports the
    failures, the auth check reads the paths to decide whether a
    credential reached the request it opens. Both go through this function
    so they cannot resolve the same block against different connections
    and reach contradictory verdicts.

    *refs* narrows the walk to named transports; ``None`` walks every
    declared http block.
    """
    resolver, asked = connect_probe(target.definition, raw_config)
    violations: list[Violation] = []
    for ref, block in _http_transports(target).items():
        if refs is not None and ref not in refs:
            continue
        try:
            resolve_http_spec(block, resolver=resolver)
        except RESOLVE_FAILURES as err:
            violations.append(
                Violation(
                    CHECK,
                    f"transport {ref!r} does not resolve into an HTTP "
                    f"transport: {err}. The engine builds every API "
                    f"connection through this exact call, so no connection "
                    f"on this connector can be opened.",
                )
            )
    optional = optional_paths(target.definition)
    for entry in unsatisfied(asked):
        if entry.path in optional:
            violations.append(
                Violation(
                    CHECK,
                    f"a declared transport reads {entry.path!r}, which "
                    f"connection_contract declares optional and gives no "
                    f"default — so a user can leave it blank and a connection "
                    f"without it is valid. A transport resolves strictly: the "
                    f"connect fails outright for those users. Make the input "
                    f"required, give it a default, or stop reading it here.",
                )
            )
            continue
        violations.append(
            Violation(
                CHECK,
                f"a declared transport reads {entry.path!r}, which the "
                f"connection this connector declares does not carry "
                f"({entry.detail}). Every value a transport resolves comes "
                f"from the connection the control plane provisions from "
                f"connection_contract, so declare the input (or post-auth "
                f"output) that supplies it, or drop the reference.",
            )
        )
    return violations, asked


def check_api_transport(target: ConformanceTarget) -> list[Violation]:
    """Certify that the CDK can build a connection from this definition."""
    violations: list[Violation] = []
    declared = target.declared_transports()
    if not declared:
        return [
            Violation(
                CHECK,
                "connector.json declares no transports; an API connector is "
                "reached only through a declared transport block.",
            )
        ]

    default_ref = target.definition.get("default_transport")
    if not isinstance(default_ref, str) or default_ref not in declared:
        violations.append(
            Violation(
                CHECK,
                f"default_transport is {default_ref!r}, which is not one of "
                f"the declared transports {sorted(declared)}; a read that "
                f"names no transport_ref falls back to it and fails.",
            )
        )
    elif declared[default_ref].get("transport_type") != HTTP_TRANSPORT_TYPE:
        violations.append(
            Violation(
                CHECK,
                f"default_transport {default_ref!r} is of type "
                f"{declared[default_ref].get('transport_type')!r}, not "
                f"{HTTP_TRANSPORT_TYPE!r}. The API path materializes the "
                f"connection from the default transport and reads its session "
                f"and base_url, so every read on this connector fails at "
                f"connect however many other http transports it declares.",
            )
        )

    registered = registered_transport_kinds()
    for ref, block in declared.items():
        transport_type = block.get("transport_type")
        if transport_type not in registered:
            violations.append(
                Violation(
                    CHECK,
                    f"transport {ref!r} declares transport_type "
                    f"{transport_type!r}, which the CDK does not register "
                    f"(registered: {registered}); the engine refuses to "
                    f"materialize it.",
                )
            )

    if not _http_transports(target):
        violations.append(
            Violation(
                CHECK,
                f"no declared transport is of type {HTTP_TRANSPORT_TYPE!r}; "
                f"the CDK's API path materializes an API connection from an "
                f"HTTP transport and from nothing else.",
            )
        )
        return violations

    # Against the narrowest connection the contract admits, so an http field
    # reading an entry a user may leave blank is reported rather than
    # papered over by a stand-in the contract never promised.
    transport_violations, _asked = probe_transports(
        target, guaranteed_connection(target.definition)
    )
    violations.extend(transport_violations)
    violations.extend(check_read_transport_selection(target))
    return violations


def check_api_auth(target: ConformanceTarget) -> list[Violation]:
    """Certify that the declared auth type matches what the transport carries.

    The CDK executes no auth flow — ``authorize`` / ``token_exchange`` /
    ``refresh`` are the control plane's, and ``test`` is its connection
    probe. What it executes is the resolution of credential material into
    the transport it opens, so that is what the declaration is checked
    against.
    """
    auth = target.definition.get("auth")
    if not isinstance(auth, Mapping):
        return [
            Violation(
                AUTH_CHECK,
                "connector.json declares no auth block; every connector "
                'declares one (`{"type": "none"}` for a system that '
                "needs no credential), and the control plane provisions "
                "connections from it.",
            )
        ]
    auth_type = auth.get("type")
    if not isinstance(auth_type, str) or not auth_type:
        return [
            Violation(
                AUTH_CHECK,
                f"the auth block declares no type (got {auth_type!r}); the "
                f"type is what selects the credential flow.",
            )
        ]

    selected = selected_transport(target)
    if selected is None:
        # Reported by check_api_transport with the actionable message;
        # failing again here would only bury it.
        return []

    # Only the transport the API path opens. A credential resolved into a
    # transport nothing selects — an auth-operation block the control plane
    # drives, a discovery origin — authenticates none of the requests a read
    # sends, so counting it would pass a connector that reads unauthenticated.
    #
    # Against the WIDEST connection the contract admits: an optional
    # credential is still a credential the connector reads, and whether a
    # strict field may read one is check_api_transport's question, reported
    # there. Any resolve failure here is likewise reported there; this walk
    # keeps only what the resolver was asked for on the way.
    ref, _block = selected
    _violations, asked = probe_transports(
        target, declared_connection(target.definition), refs={ref}
    )
    credentials = sorted({entry.path for entry in asked if entry.is_credential})

    if auth_type == "none" and credentials:
        return [
            Violation(
                AUTH_CHECK,
                f"auth declares type 'none' but the default transport {ref!r} "
                f"reads credential material {credentials}; a connection with "
                f"no auth flow never carries those values. Declare the auth "
                f"type this connector actually uses, or drop the reference.",
            )
        ]
    if auth_type != "none" and not credentials:
        return [
            Violation(
                AUTH_CHECK,
                f"auth declares type {auth_type!r} but the default transport "
                f"{ref!r} — the one every read opens — reads no credential "
                f"material (nothing under "
                f"{list(CREDENTIAL_PREFIXES)}); every request this connector "
                f"opens would go out unauthenticated. Resolve the credential "
                f"into the transport's headers, base_url, or query — an "
                f"endpoint param default cannot carry it, because secrets "
                f"are not in scope at request time.",
            )
        ]
    return []
