"""Resolve a connector's expressions against the connection it declares.

An API connector is executed almost entirely from its declarations: the
CDK builds the transport from ``transports.<ref>``, binds each request
from the endpoint's ``params`` and ``body``, and resolves each page's
continuation from the strategy's value expressions. Every one of those
steps runs the same grammar against a :class:`~cdk.resolver.Resolver`,
and every one of them can only reach the scopes the CDK populates *at
that step*.

The published contract checks that a ref or placeholder begins with a
known scope, and stops there — "sub-path existence and per-phase
availability are the runtime resolver's concern". This module is that
concern, made checkable offline: it builds the connection the connector's
own ``connection_contract`` promises, hands it to the CDK's real scope
assembly, and records every path an expression asks for and whether the
scope set of that phase actually carries it.

The three phases the CDK offers, and what each carries:

* **connect** (:func:`~cdk.connection_runtime.transport_resolution_context`)
  — the definition, the connection's stored blocks, the resolved secrets,
  the connection's auth material.
* **request** (:meth:`~cdk.connection_runtime.ConnectionRuntime.request_resolver`)
  — ``connection.{parameters,selections,discovered}`` and ``runtime`` only.
  Secrets are deliberately absent: per-request resolution runs
  connector-side, where the secret store never is.
* **page** — the request scopes plus the ``response`` scope a paging loop
  builds (:func:`~cdk.api_paging.page_response_scope`).

A path a phase does not carry is not an error at resolution time — the
per-request policy omits the field with a warning — so the connector
simply sends a request without it. That is the defect this module exists
to name: an ``{"ref": "secrets.api_key"}`` param default drops the
credential silently, and a ``stop_when`` on ``response.headers`` ends
every read after one page.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from cdk.api_paging import page_response_scope
from cdk.connection_runtime import ConnectionRuntime, transport_resolution_context
from cdk.derived_functions import DEFAULT_FUNCTIONS
from cdk.exceptions import UnresolvedValueError
from cdk.resolver import ResolutionContext, Resolver
from cdk.secrets.protocol import SecretsResolver

#: The value every stand-in scope entry carries. A string so it survives
#: template substitution (which refuses non-scalars) and DSN encoding.
STAND_IN = "conformance-stand-in"

#: The connection id the stand-in connection is built under.
STAND_IN_CONNECTION_ID = "conformance-declared-connection"

#: Storage targets a ``connection_contract`` entry may name, mapped to the
#: connection block the CDK reads that storage from. The vocabulary is the
#: published contract's (``ContractInputStorage`` /
#: ``PostAuthOutputStorage``); an entry naming anything else lands in no
#: block, and every expression addressing it is reported unsatisfied —
#: which is what such a definition does at runtime.
_STORAGE_BLOCKS: Mapping[str, str] = {
    "connection.parameters": "parameters",
    "connection.selections": "selections",
    "connection.discovered": "discovered",
    "secrets": "secrets",
}

#: Prefixes whose contents no connection contract can promise, per phase.
#:
#: ``response.body`` is the provider's payload — its shape lives in the
#: endpoint's response schema, not in any scope the kit can enumerate.
#: ``auth`` and ``connection.auth_state`` are what the control plane's auth
#: flow produced for a live connection (an OAuth access token); a
#: connector legitimately reads them at connect time and no declaration
#: describes their keys.
_CONNECT_OPAQUE: tuple[str, ...] = ("auth", "connection.auth_state", "response.body")
_REQUEST_OPAQUE: tuple[str, ...] = ("response.body",)

#: Scope prefixes that carry credential material. What makes a declared
#: auth type more than a label: unless the connection's transport reads
#: one of these, nothing authenticates the requests it opens.
CREDENTIAL_PREFIXES: tuple[str, ...] = ("secrets", "auth", "connection.auth_state")


@dataclass(frozen=True)
class AskedPath:
    """One dotted path an expression asked the resolver for.

    ``detail`` is empty when the phase's scopes carried the path; it names
    what was missing otherwise.
    """

    path: str
    detail: str = ""

    @property
    def satisfied(self) -> bool:
        """Whether the phase's scopes carried this path."""
        return not self.detail

    @property
    def is_credential(self) -> bool:
        """Whether this path addresses credential material."""
        return _has_prefix(self.path, CREDENTIAL_PREFIXES)


def _has_prefix(path: str, prefixes: tuple[str, ...]) -> bool:
    """Whether *path* is one of *prefixes* or sits under one."""
    return any(path == prefix or path.startswith(f"{prefix}.") for prefix in prefixes)


class _RecordingContext(ResolutionContext):
    """A resolution context that answers every path and records the misses.

    Resolution stops at the first missing path, so a context that raised
    would report one defect per run and hide the rest. This one records
    the miss, answers with :data:`STAND_IN`, and lets the walk continue —
    so one run reports every unreachable path in a connector's
    declarations.
    """

    def __init__(
        self,
        scopes: ResolutionContext,
        asked: list[AskedPath],
        opaque: tuple[str, ...],
    ) -> None:
        super().__init__(
            connector=scopes.connector,
            connection=scopes.connection,
            secrets=scopes.secrets,
            auth=scopes.auth,
            runtime=scopes.runtime,
            state=scopes.state,
            derived=scopes.derived,
            request=scopes.request,
            response=scopes.response,
        )
        self._asked = asked
        self._opaque = opaque

    def lookup(self, dotted_path: str) -> Any:
        """Answer *dotted_path*, recording whether the scopes carried it."""
        if isinstance(dotted_path, str) and _has_prefix(dotted_path, self._opaque):
            self._asked.append(AskedPath(dotted_path))
            return STAND_IN
        try:
            value = super().lookup(dotted_path)
        except UnresolvedValueError as err:
            # Well-formed path, nothing there: the connection contract does
            # not promise it, or this phase does not carry the scope.
            self._asked.append(AskedPath(str(dotted_path), str(err)))
            return STAND_IN
        except KeyError as err:
            # An unknown scope name — an authoring defect the resolver
            # refuses to absorb even at request time.
            self._asked.append(AskedPath(str(dotted_path), str(err)))
            return STAND_IN
        self._asked.append(AskedPath(dotted_path))
        return STAND_IN if value is None else value

    def with_response(self, response: Mapping[str, Any]) -> _RecordingContext:
        """Return a page-phase context that records into the same list."""
        return _RecordingContext(
            super().with_response(response), self._asked, self._opaque
        )

    def with_runtime(self, runtime: Mapping[str, Any]) -> _RecordingContext:
        """Return a context with a replaced runtime scope, still recording."""
        return _RecordingContext(
            super().with_runtime(runtime), self._asked, self._opaque
        )


class _StandInSecretsResolver(SecretsResolver):
    """Return a stand-in value for every secret the connection declares."""

    async def resolve(
        self, connection_id: str, secret_refs: Mapping[str, str]
    ) -> dict[str, str]:
        """Answer every declared ref with :data:`STAND_IN`."""
        return {name: STAND_IN for name in secret_refs}

    async def close(self) -> None:
        """Nothing to release."""


def declared_connection(definition: Mapping[str, Any]) -> dict[str, Any]:
    """Build the connection a connector's ``connection_contract`` promises.

    Every declared input and post-auth output becomes a stand-in value in
    the connection block its ``storage`` names — so a value expression
    resolves exactly when the contract promises what it addresses, and
    fails to resolve exactly when it does not.
    """
    contract = definition.get("connection_contract")
    contract = contract if isinstance(contract, Mapping) else {}
    blocks: dict[str, dict[str, Any]] = {
        block: {} for block in dict.fromkeys(_STORAGE_BLOCKS.values())
    }
    for group in ("inputs", "post_auth_outputs"):
        declared = contract.get(group)
        if not isinstance(declared, Mapping):
            continue
        for name, spec in declared.items():
            storage = spec.get("storage") if isinstance(spec, Mapping) else None
            block = _STORAGE_BLOCKS.get(storage) if isinstance(storage, str) else None
            if block is not None:
                blocks[block][str(name)] = STAND_IN
    return {
        "connection_id": STAND_IN_CONNECTION_ID,
        "name": STAND_IN,
        "status": STAND_IN,
        "parameters": blocks["parameters"],
        "selections": blocks["selections"],
        "discovered": blocks["discovered"],
        # The declared secret names, as the refs a connection would carry.
        # _StandInSecretsResolver answers each with a stand-in value.
        "secret_refs": {name: STAND_IN for name in blocks["secrets"]},
    }


def connect_probe(definition: Mapping[str, Any]) -> tuple[Resolver, list[AskedPath]]:
    """Return a connect-phase resolver over the declared connection, and its record.

    The scopes come from the CDK's own connect-phase assembly, so what the
    kit certifies and what the engine resolves a transport against cannot
    be different scope sets.
    """
    raw_config = declared_connection(definition)
    asked: list[AskedPath] = []
    scopes = transport_resolution_context(
        raw_config=raw_config,
        connector_definition=definition,
        secrets={name: STAND_IN for name in raw_config["secret_refs"]},
        connection_id=STAND_IN_CONNECTION_ID,
    )
    context = _RecordingContext(scopes, asked, _CONNECT_OPAQUE)
    return Resolver(context, functions=DEFAULT_FUNCTIONS), asked


def request_probe(
    definition: Mapping[str, Any], *, batch_size: int = 1000
) -> tuple[Resolver, list[AskedPath]]:
    """Return a request-phase resolver over the declared connection, and its record.

    Built through :meth:`ConnectionRuntime.request_resolver` — the same
    call the API connector makes once per read — so the narrower
    request-time scope set is the CDK's statement of it, not a copy.
    """
    raw_config = declared_connection(definition)
    runtime = ConnectionRuntime(
        raw_config=raw_config,
        connection_id=STAND_IN_CONNECTION_ID,
        connector_id=str(definition.get("connector_id") or "conformance-target"),
        connector_type=str(definition.get("kind") or "api"),
        resolver=_StandInSecretsResolver(),
        connector_definition=dict(definition),
    )
    asked: list[AskedPath] = []
    context = _RecordingContext(
        runtime.request_resolver(runtime_values={"batch_size": batch_size}).context,
        asked,
        _REQUEST_OPAQUE,
    )
    return Resolver(context, functions=DEFAULT_FUNCTIONS), asked


def page_probe(
    definition: Mapping[str, Any], *, batch_size: int = 1000
) -> tuple[Resolver, list[AskedPath]]:
    """Return a page-phase resolver over the declared connection, and its record.

    The request scopes plus the ``response`` scope a paging loop builds.
    One stand-in record, so ``response.record_count`` resolves to the
    positive integer a real page carries.
    """
    resolver, asked = request_probe(definition, batch_size=batch_size)
    return resolver.with_response(page_response_scope(STAND_IN, [{}])), asked


def unsatisfied(asked: list[AskedPath]) -> list[AskedPath]:
    """Return the recorded paths the phase's scopes did not carry, deduplicated."""
    seen: dict[str, AskedPath] = {}
    for entry in asked:
        if not entry.satisfied and entry.path not in seen:
            seen[entry.path] = entry
    return list(seen.values())
