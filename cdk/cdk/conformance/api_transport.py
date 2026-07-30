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

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from cdk.exceptions import TransportSpecError
from cdk.transport_factory import (
    HTTP_TRANSPORT_TYPE,
    registered_transport_kinds,
    resolve_http_spec,
)

from .declared_connection import (
    CREDENTIAL_PREFIXES,
    AskedPath,
    connect_probe,
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


def probe_transports(
    target: ConformanceTarget,
) -> tuple[list[Violation], list[AskedPath]]:
    """Resolve every http transport, returning its failures and asked paths.

    One walk serving both checks here: the transport check reports the
    failures, the auth check reads the paths to decide whether any
    credential reached the request.
    """
    resolver, asked = connect_probe(target.definition)
    violations: list[Violation] = []
    for ref, block in _http_transports(target).items():
        try:
            resolve_http_spec(block, resolver=resolver)
        except (TransportSpecError, KeyError, TypeError, ValueError) as err:
            violations.append(
                Violation(
                    CHECK,
                    f"transport {ref!r} does not resolve into an HTTP "
                    f"transport: {err}. The engine builds every API "
                    f"connection through this exact call, so no connection "
                    f"on this connector can be opened.",
                )
            )
    for entry in unsatisfied(asked):
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

    transport_violations, _asked = probe_transports(target)
    violations.extend(transport_violations)
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

    if not _http_transports(target):
        # Reported by check_api_transport with the actionable message;
        # failing again here would only bury it.
        return []

    _violations, asked = probe_transports(target)
    credentials = sorted({entry.path for entry in asked if entry.is_credential})

    if auth_type == "none" and credentials:
        return [
            Violation(
                AUTH_CHECK,
                f"auth declares type 'none' but a declared transport reads "
                f"credential material {credentials}; a connection with no "
                f"auth flow never carries those values. Declare the auth "
                f"type this connector actually uses, or drop the reference.",
            )
        ]
    if auth_type != "none" and not credentials:
        return [
            Violation(
                AUTH_CHECK,
                f"auth declares type {auth_type!r} but no declared transport "
                f"reads any credential material (nothing under "
                f"{list(CREDENTIAL_PREFIXES)}); every request this connector "
                f"opens would go out unauthenticated. Resolve the credential "
                f"into the transport's headers, base_url, or query — an "
                f"endpoint param default cannot carry it, because secrets "
                f"are not in scope at request time.",
            )
        ]
    return []
