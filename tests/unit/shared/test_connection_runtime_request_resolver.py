"""Tests for :meth:`cdk.connection_runtime.ConnectionRuntime.request_resolver`.

The per-request resolver carries the connection scopes
(``parameters``/``selections``/``discovered``) plus a ``runtime`` scope,
with the default derived functions registered. Secrets are deliberately
absent: per-request resolution runs connector-side where the secret
store never exists, so the scope set must be identical on the trusted
and worker sides.
"""

from __future__ import annotations

import base64

from analitiq.contracts.connection import ConnectionInput
from contract_documents import connection_document

from cdk.connection_runtime import ConnectionRuntime
from cdk.secrets import InMemorySecretsResolver


def _runtime(connection: ConnectionInput | None = None) -> ConnectionRuntime:
    return ConnectionRuntime(
        connection=connection
        or connection_document(
            parameters={"region": "eu", "plan": "gold"},
            selections={"org": "acme"},
            discovered={"account_id": "a-1"},
        ),
        connection_id="conn-1",
        connector_id="testapi",
        connector_type="api",
        resolver=InMemorySecretsResolver({"conn-1": {"api_key": "shh"}}),
    )


class TestRequestResolverScopes:
    def test_connection_scopes_resolve(self):
        resolver = _runtime().request_resolver()
        assert resolver.resolve({"ref": "connection.parameters.region"}) == "eu"
        assert resolver.resolve({"ref": "connection.selections.org"}) == "acme"
        assert resolver.resolve({"ref": "connection.discovered.account_id"}) == "a-1"

    def test_runtime_scope_carries_connection_id_and_caller_values(self):
        resolver = _runtime().request_resolver(runtime_values={"batch_size": 500})
        assert resolver.resolve({"ref": "runtime.connection_id"}) == "conn-1"
        assert resolver.resolve({"ref": "runtime.batch_size"}) == 500

    def test_secrets_scope_is_empty(self):
        # Secrets are consumed at transport materialization on the trusted
        # side; a per-request ref into secrets.* must not resolve — under
        # the request policy the field is dropped.
        resolver = _runtime().request_resolver()
        out = resolver.resolve_for_request({"key": {"ref": "secrets.api_key"}})
        assert out == {}

    def test_default_functions_registered(self):
        resolver = _runtime().request_resolver()
        out = resolver.resolve_for_request(
            {
                "function": "base64_encode",
                "input": {"ref": "connection.parameters.plan"},
            }
        )
        assert out == base64.b64encode(b"gold").decode("ascii")

    def test_missing_config_blocks_yield_empty_scopes(self):
        resolver = _runtime(
            connection=connection_document(parameters={"host": "h"})
        ).request_resolver()
        out = resolver.resolve_for_request(
            {"v": {"ref": "connection.parameters.anything"}}
        )
        assert out == {}
