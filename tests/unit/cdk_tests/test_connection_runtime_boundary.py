"""Tests for the trusted-side resolve / worker-side rebuild secret boundary.

This is the core promise of worker isolation: ``resolve_spec()`` runs on
the trusted side and produces a JSON-safe payload of values only; a runtime
rebuilt from that payload (``from_resolved_payload``) carries a resolver
that refuses every secret-store access. A regression in either direction
hands untrusted connector code a path to the secret store.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from cdk.connection_runtime import ConnectionRuntime, _PreResolvedSecretsResolver
from cdk.exceptions import TransportSpecError


def _resolver(secrets=None):
    return AsyncMock(resolve=AsyncMock(return_value=secrets or {}))


def _transportless_runtime(**overrides):
    kwargs = dict(
        raw_config={
            "path": "/tmp/out",
            "secret_refs": {"API_TOKEN": "connections/x/token"},
        },
        connection_id="my-file",
        connector_id="filedrop",
        connector_type="file",
        resolver=_resolver({"API_TOKEN": "tok-123"}),
    )
    kwargs.update(overrides)
    return ConnectionRuntime(**kwargs)


def _http_runtime():
    return ConnectionRuntime(
        raw_config={
            "parameters": {"region": "eu"},
            "secret_refs": {"API_TOKEN": "connections/x/token"},
        },
        connection_id="my-api",
        connector_id="demo",
        connector_type="api",
        resolver=_resolver({"API_TOKEN": "tok-123"}),
        connector_definition={
            "connector_id": "demo",
            "default_transport": "api",
            "transports": {
                "api": {
                    "transport_type": "http",
                    "base_url": "https://api.example.com",
                    "headers": {
                        "Authorization": {"template": "Bearer ${secrets.API_TOKEN}"}
                    },
                }
            },
        },
    )


class TestResolveSpec:
    async def test_transportless_payload_is_json_safe_with_values_only(self):
        runtime = _transportless_runtime()
        payload = await runtime.resolve_spec()

        # JSON-safe: the whole payload survives a round trip.
        assert json.loads(json.dumps(payload)) == payload
        assert payload["connection_id"] == "my-file"
        assert payload["connector_id"] == "filedrop"
        assert payload["connector_type"] == "file"
        assert payload["transport_spec"] is None
        # Secrets arrive as resolved values, not references.
        assert payload["resolved_config"]["API_TOKEN"] == "tok-123"

    async def test_transport_payload_resolves_secrets_into_values(self):
        runtime = _http_runtime()
        payload = await runtime.resolve_spec()

        assert json.loads(json.dumps(payload)) == payload
        spec = payload["transport_spec"]
        assert spec["transport_type"] == "http"
        assert spec["headers"]["Authorization"] == "Bearer tok-123"
        assert payload["resolved_config"] is None

    async def test_missing_declared_secret_fails_loudly(self):
        # A resolver that fails loud on a missing source propagates through
        # resolve_spec -- _load_secrets never swallows the failure into an
        # empty secret.
        from cdk.secrets.exceptions import SecretNotFoundError

        failing = AsyncMock(
            resolve=AsyncMock(
                side_effect=SecretNotFoundError(
                    "my-file", detail="secret_ref 'API_TOKEN' -> env:X not set"
                )
            )
        )
        runtime = _transportless_runtime(resolver=failing)
        with pytest.raises(SecretNotFoundError, match="API_TOKEN"):
            await runtime.resolve_spec()

    async def test_connection_config_crosses_without_secret_bearing_blocks(self):
        runtime = ConnectionRuntime(
            raw_config={
                "parameters": {"account_id": "acc-42"},
                "selections": {"profile": "main"},
                "max_retries": 7,
                "secret_refs": {"API_TOKEN": "connections/x/token"},
                "auth": {"type": "oauth2"},
                "auth_state": {"refresh_token": "rt-secret"},
            },
            connection_id="my-api",
            connector_id="demo",
            connector_type="api",
            resolver=_resolver({"API_TOKEN": "tok-123"}),
        )
        payload = await runtime.resolve_spec()

        config = payload["connection_config"]
        # Non-secret scopes and top-level settings cross: connector code
        # resolves connection.parameters.* refs and handler settings from
        # the worker runtime's raw_config.
        assert config["parameters"] == {"account_id": "acc-42"}
        assert config["selections"] == {"profile": "main"}
        assert config["max_retries"] == 7
        # Secret pointers and auth material never cross.
        assert "secret_refs" not in config
        assert "auth" not in config
        assert "auth_state" not in config


class TestWorkerSideRuntime:
    async def test_rebuilt_runtime_refuses_secret_resolution(self):
        payload = await _transportless_runtime().resolve_spec()
        worker_runtime = ConnectionRuntime.from_resolved_payload(payload)

        assert isinstance(worker_runtime._resolver, _PreResolvedSecretsResolver)
        with pytest.raises(RuntimeError, match="never\\s+access the secret store"):
            await worker_runtime._resolver.resolve("my-file", {})

    async def test_rebuilt_runtime_materializes_from_payload_without_secrets(self):
        payload = await _transportless_runtime().resolve_spec()
        worker_runtime = ConnectionRuntime.from_resolved_payload(payload)

        # materialize() must take the pre-resolved branch: if it fell
        # through to secret loading, the placeholder resolver would raise.
        await worker_runtime.materialize()
        assert worker_runtime.resolved_config["API_TOKEN"] == "tok-123"
        assert worker_runtime.connector_type == "file"
        assert worker_runtime.connection_id == "my-file"

    async def test_rebuilt_runtime_refuses_smuggled_secret_ref(self):
        # resolve_spec strips secret_refs from the payload, so a worker never
        # has any to resolve. If a malformed/malicious payload smuggles a
        # secret_ref into connection_config, materialize() takes the trusted
        # path (no transport_spec / resolved_config) and must hit the refusing
        # resolver rather than reaching a real secret store.
        worker_runtime = ConnectionRuntime.from_resolved_payload(
            {
                "connection_id": "my-file",
                "connector_id": "filedrop",
                "connector_type": "file",
                "connection_config": {
                    "secret_refs": {"API_TOKEN": "sidecar:API_TOKEN"}
                },
                "transport_spec": None,
                "resolved_config": None,
            }
        )
        with pytest.raises(RuntimeError, match="pre-resolved worker runtime"):
            await worker_runtime.materialize()

    async def test_rebuilt_runtime_restores_connection_config_as_raw_config(self):
        runtime = ConnectionRuntime(
            raw_config={
                "parameters": {"account_id": "acc-42"},
                "secret_refs": {"API_TOKEN": "connections/x/token"},
            },
            connection_id="my-api",
            connector_id="demo",
            connector_type="api",
            resolver=_resolver({"API_TOKEN": "tok-123"}),
        )
        payload = await runtime.resolve_spec()
        worker_runtime = ConnectionRuntime.from_resolved_payload(payload)

        # connection.parameters.* refs must resolve inside the worker.
        assert worker_runtime.raw_config["parameters"] == {"account_id": "acc-42"}
        assert "secret_refs" not in worker_runtime.raw_config

    async def test_driver_hint_survives_the_round_trip(self):
        payload = {
            "connection_id": "my-pg",
            "connector_id": "postgres",
            "connector_type": "database",
            "driver_hint": "postgresql",
            "transport_spec": None,
            "resolved_config": {"host": "db"},
        }
        worker_runtime = ConnectionRuntime.from_resolved_payload(payload)
        assert worker_runtime.driver == "postgresql"


class TestConnectionContractValidation:
    """Required inputs are enforced once, at the connection boundary, from the
    connector's ``connection_contract`` — the published schema's authoritative
    optionality signal (``required`` = "whether resolution must produce a
    value"). This is what lets transport resolution omit an absent optional
    binding rather than fail."""

    def _runtime(self, *, parameters, contract_inputs):
        return ConnectionRuntime(
            raw_config={"parameters": parameters, "secret_refs": {}},
            connection_id="c1",
            connector_id="demo",
            connector_type="database",
            resolver=_resolver({}),
            connector_definition={
                "connector_id": "demo",
                "connection_contract": {"inputs": contract_inputs},
            },
        )

    def test_missing_required_parameter_raises(self):
        runtime = self._runtime(
            parameters={"warehouse": "wh"},
            contract_inputs={
                "account": {
                    "required": True,
                    "source": "user",
                    "storage": "connection.parameters",
                },
                "warehouse": {
                    "required": False,
                    "source": "user",
                    "storage": "connection.parameters",
                },
            },
        )
        with pytest.raises(TransportSpecError, match="account"):
            runtime._validate_connection_contract({})

    def test_absent_optional_input_passes(self):
        runtime = self._runtime(
            parameters={"account": "abc"},
            contract_inputs={
                "account": {
                    "required": True,
                    "source": "user",
                    "storage": "connection.parameters",
                },
                "role": {
                    "required": False,
                    "source": "user",
                    "storage": "connection.parameters",
                },
            },
        )
        runtime._validate_connection_contract({})  # no raise

    def test_required_secret_checked_against_secret_store(self):
        runtime = self._runtime(
            parameters={"account": "abc"},
            contract_inputs={
                "account": {
                    "required": True,
                    "source": "user",
                    "storage": "connection.parameters",
                },
                "password": {"required": True, "source": "user", "storage": "secrets"},
            },
        )
        with pytest.raises(TransportSpecError, match="password"):
            runtime._validate_connection_contract({})
        # Present in the secret store -> passes.
        runtime._validate_connection_contract({"password": "pw"})

    def test_required_platform_input_enforced(self):
        # ``required`` is enforced regardless of ``source``: a platform-
        # provisioned input lands in the same scopes as a user one, so a
        # missing required platform input must fail just like a user input.
        # (The transport-resolution drop logic relies on *every* required
        # input being present, not just the user-sourced ones.)
        inputs = {
            "account": {
                "required": True,
                "source": "user",
                "storage": "connection.parameters",
            },
            "region": {
                "required": True,
                "source": "platform",
                "storage": "connection.parameters",
            },
        }
        with pytest.raises(TransportSpecError, match="region"):
            self._runtime(
                parameters={"account": "abc"}, contract_inputs=inputs
            )._validate_connection_contract({})
        # Present -> passes.
        self._runtime(
            parameters={"account": "abc", "region": "eu"},
            contract_inputs=inputs,
        )._validate_connection_contract(
            {}
        )  # no raise

    def test_required_input_unknown_storage_fails_loud(self):
        # A required input whose storage is not one the connection carries
        # (the schema's storage enum is closed to connection.parameters /
        # secrets) is a malformed connector definition -- fail loud, never
        # silently skip.
        runtime = self._runtime(
            parameters={"account": "abc"},
            contract_inputs={
                "account": {
                    "required": True,
                    "source": "user",
                    "storage": "connection.parameters",
                },
                "bogus": {
                    "required": True,
                    "source": "user",
                    "storage": "connection.discovered",
                },
            },
        )
        with pytest.raises(TransportSpecError, match="unknown storage"):
            runtime._validate_connection_contract({})

    def test_null_parameters_block_still_enforces_required(self):
        # A connection with no (or null) parameters block must still raise
        # for a required connection.parameters input -- this exercises the
        # ``self._raw_config.get("parameters") or {}`` coalescing.
        runtime = self._runtime(
            parameters=None,
            contract_inputs={
                "account": {
                    "required": True,
                    "source": "user",
                    "storage": "connection.parameters",
                },
            },
        )
        with pytest.raises(TransportSpecError, match="account"):
            runtime._validate_connection_contract({})

    async def test_resolve_spec_enforces_contract_at_boundary(self):
        # The contract check runs through the public boundary, after secrets
        # load -- so a connection missing a required input fails resolve_spec,
        # not just the private helper.
        runtime = self._runtime(
            parameters={},
            contract_inputs={
                "account": {
                    "required": True,
                    "source": "user",
                    "storage": "connection.parameters",
                },
            },
        )
        with pytest.raises(TransportSpecError, match="account"):
            await runtime.resolve_spec()

    def test_no_contract_is_unconstrained(self):
        runtime = ConnectionRuntime(
            raw_config={"parameters": {}},
            connection_id="c1",
            connector_id="demo",
            connector_type="database",
            resolver=_resolver({}),
            connector_definition={"connector_id": "demo"},
        )
        runtime._validate_connection_contract({})  # no raise
