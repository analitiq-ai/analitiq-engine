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
from analitiq.contracts.connection import ConnectionInput
from analitiq.contracts.connector import Connector
from contract_documents import (
    adbc_transport,
    connection_document,
    connector_document,
    contract_input,
    http_transport,
    sqlalchemy_transport,
)
from pydantic import ValidationError

from cdk.connection_runtime import ConnectionRuntime, _PreResolvedSecretsResolver
from cdk.exceptions import TransportSpecError


def _resolver(secrets=None):
    return AsyncMock(resolve=AsyncMock(return_value=secrets or {}))


def _transportless_runtime(**overrides):
    kwargs = dict(
        connection=connection_document(
            parameters={"path": "/tmp/out"},
            secret_refs={"API_TOKEN": "env:API_TOKEN"},
        ),
        connection_id="my-file",
        connector_id="filedrop",
        connector_type="file",
        resolver=_resolver({"API_TOKEN": "tok-123"}),
    )
    kwargs.update(overrides)
    return ConnectionRuntime(**kwargs)


def _http_runtime():
    return ConnectionRuntime(
        connection=connection_document(
            parameters={"region": "eu"},
            secret_refs={"API_TOKEN": "env:API_TOKEN"},
        ),
        connection_id="my-api",
        connector_id="demo",
        connector_type="api",
        resolver=_resolver({"API_TOKEN": "tok-123"}),
        connector=connector_document(
            "api",
            connector_id="demo",
            transports={
                "api": http_transport(
                    headers={
                        "Authorization": {"template": "Bearer ${secrets.API_TOKEN}"}
                    }
                )
            },
        ),
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
        assert payload["transport_specs"] is None
        assert payload["default_transport_ref"] is None
        # Secrets arrive as resolved values, not references, beside the
        # connection's authored parameters.
        assert payload["resolved_config"] == {
            "parameters": {"path": "/tmp/out"},
            "secrets": {"API_TOKEN": "tok-123"},
        }

    async def test_transport_payload_resolves_secrets_into_values(self):
        runtime = _http_runtime()
        payload = await runtime.resolve_spec()

        assert json.loads(json.dumps(payload)) == payload
        # The default transport travels under its own ref: the worker has no
        # secret store, so a transport whose spec is not in this payload can
        # never be opened there.
        assert payload["default_transport_ref"] == "api"
        spec = payload["transport_specs"]["api"]
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

    async def test_connection_config_crosses_without_secret_pointers(self):
        runtime = ConnectionRuntime(
            connection=connection_document(
                parameters={"account_id": "acc-42"},
                selections={"profile": "main"},
                secret_refs={"API_TOKEN": "env:API_TOKEN"},
            ),
            connection_id="my-api",
            connector_id="demo",
            connector_type="api",
            resolver=_resolver({"API_TOKEN": "tok-123"}),
        )
        payload = await runtime.resolve_spec()

        config = payload["connection_config"]
        # The authored scopes cross, as the contract document the worker
        # parses again: connector code resolves connection.parameters.* refs
        # from the worker runtime's connection.
        assert config["parameters"] == {"account_id": "acc-42"}
        assert config["selections"] == {"profile": "main"}
        assert ConnectionInput.model_validate(config).connector_id == "test-connector"
        # Secret pointers never cross.
        assert "secret_refs" not in config

    def test_a_connection_field_outside_the_contract_cannot_be_read(self):
        # The connection document is the contract's model: a handler setting
        # the contract does not declare (``max_retries``, ``auth``) has no
        # place on it, so no engine read of it can exist.
        with pytest.raises(ValidationError, match="max_retries"):
            ConnectionInput.model_validate({"connector_id": "demo", "max_retries": 7})
        with pytest.raises(ValidationError, match="auth"):
            ConnectionInput.model_validate(
                {"connector_id": "demo", "auth": {"type": "oauth2"}}
            )


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
        assert worker_runtime.resolved_config["secrets"]["API_TOKEN"] == "tok-123"
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
                    "connector_id": "filedrop",
                    "secret_refs": {"API_TOKEN": "sidecar:API_TOKEN"},
                },
                "transport_specs": None,
                "resolved_config": None,
            }
        )
        with pytest.raises(RuntimeError, match="pre-resolved worker runtime"):
            await worker_runtime.materialize()

    @pytest.mark.asyncio
    async def test_worker_without_artifacts_rejected(self):
        # A malformed payload with neither transport_spec nor resolved_config
        # (and no secret_refs) must not materialize a degenerate worker runtime:
        # the trusted path always consults the refusing resolver.
        worker_runtime = ConnectionRuntime.from_resolved_payload(
            {
                "connection_id": "my-file",
                "connector_id": "filedrop",
                "connector_type": "file",
                "connection_config": {
                    "connector_id": "filedrop",
                    "parameters": {"path": "/tmp/out"},
                },
                "transport_specs": None,
                "resolved_config": None,
            }
        )
        with pytest.raises(RuntimeError, match="pre-resolved worker runtime"):
            await worker_runtime.materialize()

    def test_worker_refuses_a_connection_config_outside_the_contract(self):
        # The payload's connection_config is parsed as the contract's
        # connection document; a payload carrying anything else is
        # malformed and refused at the boundary, not read as settings.
        with pytest.raises(ValueError, match="my-file.*connection_config"):
            ConnectionRuntime.from_resolved_payload(
                {
                    "connection_id": "my-file",
                    "connector_id": "filedrop",
                    "connector_type": "file",
                    "connection_config": {"path": "/tmp/out"},
                    "transport_specs": None,
                    "resolved_config": None,
                }
            )

    async def test_rebuilt_runtime_restores_the_connection_document(self):
        runtime = ConnectionRuntime(
            connection=connection_document(
                parameters={"account_id": "acc-42"},
                secret_refs={"API_TOKEN": "env:API_TOKEN"},
            ),
            connection_id="my-api",
            connector_id="demo",
            connector_type="api",
            resolver=_resolver({"API_TOKEN": "tok-123"}),
        )
        payload = await runtime.resolve_spec()
        worker_runtime = ConnectionRuntime.from_resolved_payload(payload)

        # connection.parameters.* refs must resolve inside the worker.
        assert worker_runtime.connection.parameters == {"account_id": "acc-42"}
        assert worker_runtime.connection.secret_refs == {}
        assert worker_runtime.connector is None

    async def test_driver_hint_survives_the_round_trip(self):
        payload = {
            "connection_id": "my-pg",
            "connector_id": "postgres",
            "connector_type": "database",
            "connection_config": {"connector_id": "postgres"},
            "driver_hint": "postgresql",
            "transport_spec": None,
            "resolved_config": {"parameters": {"host": "db"}, "secrets": {}},
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
            connection=connection_document(parameters=parameters, secret_refs={}),
            connection_id="c1",
            connector_id="demo",
            connector_type="database",
            resolver=_resolver({}),
            connector=connector_document(
                "database",
                connector_id="demo",
                connection_contract={"inputs": contract_inputs},
            ),
        )

    def test_missing_required_parameter_raises(self):
        runtime = self._runtime(
            parameters={"warehouse": "wh"},
            contract_inputs={
                "account": contract_input(required=True),
                "warehouse": contract_input(required=False),
            },
        )
        with pytest.raises(TransportSpecError, match="account"):
            runtime._validate_connection_contract({})

    def test_absent_optional_input_passes(self):
        runtime = self._runtime(
            parameters={"account": "abc"},
            contract_inputs={
                "account": contract_input(required=True),
                "role": contract_input(required=False),
            },
        )
        runtime._validate_connection_contract({})  # no raise

    def test_required_secret_checked_against_secret_store(self):
        runtime = self._runtime(
            parameters={"account": "abc"},
            contract_inputs={
                "account": contract_input(required=True),
                "password": contract_input(required=True, storage="secrets"),
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
            "account": contract_input(required=True),
            "region": contract_input(required=True, source="platform"),
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

    def test_an_input_stored_outside_the_two_scopes_is_refused_by_the_contract(
        self,
    ):
        # The contract closes ``storage`` to the two scopes a connection
        # carries; a definition storing a required input anywhere else is
        # refused at validation, before a runtime exists to check it.
        with pytest.raises(ValidationError, match="storage"):
            connector_document(
                "database",
                connection_contract={
                    "inputs": {
                        "bogus": contract_input(
                            required=True, storage="connection.discovered"
                        )
                    }
                },
            )

    def test_empty_parameters_block_still_enforces_required(self):
        # A connection with no parameters block must still raise for a
        # required connection.parameters input.
        runtime = self._runtime(
            parameters=None,
            contract_inputs={"account": contract_input(required=True)},
        )
        with pytest.raises(TransportSpecError, match="account"):
            runtime._validate_connection_contract({})

    async def test_resolve_spec_enforces_contract_at_boundary(self):
        # The contract check runs through the public boundary, after secrets
        # load -- so a connection missing a required input fails resolve_spec,
        # not just the private helper.
        runtime = self._runtime(
            parameters={},
            contract_inputs={"account": contract_input(required=True)},
        )
        with pytest.raises(TransportSpecError, match="account"):
            await runtime.resolve_spec()

    def test_no_inputs_is_unconstrained(self):
        runtime = ConnectionRuntime(
            connection=connection_document(parameters={}),
            connection_id="c1",
            connector_id="demo",
            connector_type="database",
            resolver=_resolver({}),
            connector=connector_document("database", connector_id="demo"),
        )
        runtime._validate_connection_contract({})  # no raise


class TestDriverDerivation:
    """``ConnectionRuntime.driver`` derives through the merged transports."""

    def _runtime(self, connector: Connector) -> ConnectionRuntime:
        return ConnectionRuntime(
            connection=connection_document(parameters={}),
            connection_id="c1",
            connector_id="demo",
            connector_type="database",
            resolver=_resolver({}),
            connector=connector,
        )

    def test_transport_type_supplied_via_transport_defaults_derives(self):
        """Defaults-supplied fields must derive exactly as they materialize.

        The materialization path merges ``transport_defaults`` before
        resolving a transport; the pre-materialization derivation reads
        through the same merged view, so the defaults block's fields
        (``transport_type`` is the one it may carry) apply to the derived
        driver too.
        """
        runtime = self._runtime(
            connector_document(
                "database",
                transport_defaults={"transport_type": "sqlalchemy"},
                transports={
                    "database": {
                        "transport_type": "sqlalchemy",
                        "driver": "postgresql+asyncpg",
                    }
                },
            )
        )
        assert runtime.driver == "postgresql"

    def test_adbc_driver_derives_as_declared(self):
        runtime = self._runtime(
            connector_document(
                "database", transports={"adbc": adbc_transport("snowflake")}
            )
        )
        assert runtime.driver == "snowflake"

    def test_sqlalchemy_driver_splits_off_the_dbapi(self):
        runtime = self._runtime(
            connector_document(
                "database",
                transports={"database": sqlalchemy_transport("mysql+aiomysql")},
            )
        )
        assert runtime.driver == "mysql"
