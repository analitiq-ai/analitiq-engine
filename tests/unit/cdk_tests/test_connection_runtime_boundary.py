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
                        "Authorization": {
                            "template": "Bearer ${secrets.API_TOKEN}"
                        }
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
        from cdk.secrets.exceptions import SecretNotFoundError

        runtime = _transportless_runtime(resolver=_resolver({}))
        with pytest.raises(SecretNotFoundError, match="secret_refs"):
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
            await worker_runtime._resolver.resolve("my-file")

    async def test_rebuilt_runtime_materializes_from_payload_without_secrets(self):
        payload = await _transportless_runtime().resolve_spec()
        worker_runtime = ConnectionRuntime.from_resolved_payload(payload)

        # materialize() must take the pre-resolved branch: if it fell
        # through to secret loading, the placeholder resolver would raise.
        await worker_runtime.materialize()
        assert worker_runtime.resolved_config["API_TOKEN"] == "tok-123"
        assert worker_runtime.connector_type == "file"
        assert worker_runtime.connection_id == "my-file"

    async def test_rebuilt_runtime_without_payload_blocks_trusted_path(self):
        # A payload with neither transport_spec nor resolved_config forces
        # materialize() onto the trusted path, which must hit the refusing
        # resolver instead of silently succeeding with empty secrets.
        worker_runtime = ConnectionRuntime.from_resolved_payload(
            {
                "connection_id": "my-file",
                "connector_id": "filedrop",
                "connector_type": "file",
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
                "account": {"required": True, "source": "user",
                            "storage": "connection.parameters"},
                "warehouse": {"required": False, "source": "user",
                              "storage": "connection.parameters"},
            },
        )
        with pytest.raises(TransportSpecError, match="account"):
            runtime._validate_connection_contract({})

    def test_absent_optional_input_passes(self):
        runtime = self._runtime(
            parameters={"account": "abc"},
            contract_inputs={
                "account": {"required": True, "source": "user",
                            "storage": "connection.parameters"},
                "role": {"required": False, "source": "user",
                         "storage": "connection.parameters"},
            },
        )
        runtime._validate_connection_contract({})  # no raise

    def test_required_secret_checked_against_secret_store(self):
        runtime = self._runtime(
            parameters={"account": "abc"},
            contract_inputs={
                "account": {"required": True, "source": "user",
                            "storage": "connection.parameters"},
                "password": {"required": True, "source": "user",
                             "storage": "secrets"},
            },
        )
        with pytest.raises(TransportSpecError, match="password"):
            runtime._validate_connection_contract({})
        # Present in the secret store -> passes.
        runtime._validate_connection_contract({"password": "pw"})

    def test_non_user_required_input_not_enforced_here(self):
        # A discovered / post-auth required input is not present when the
        # connection materializes and is not the connection's to carry.
        runtime = self._runtime(
            parameters={"account": "abc"},
            contract_inputs={
                "account": {"required": True, "source": "user",
                            "storage": "connection.parameters"},
                "table_id": {"required": True, "source": "discovered",
                             "storage": "connection.discovered"},
            },
        )
        runtime._validate_connection_contract({})  # no raise

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
