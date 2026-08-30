"""Contract-valid connection and connector documents for unit tests.

``ConnectionRuntime`` holds the contract's own models, so a test that builds
one needs documents the published contract accepts. These builders produce
the smallest valid document of each shape and let a test override exactly
the fact it exercises; every result is validated, so a test cannot author a
shape the engine would refuse.
"""

from __future__ import annotations

from typing import Any

from analitiq.contracts.connection import ConnectionInput
from analitiq.contracts.connector import Connector
from pydantic import TypeAdapter

from cdk.conformance.fakes import minimal_connector_definition

_CONNECTOR_ADAPTER: TypeAdapter[Connector] = TypeAdapter(Connector)


def connection_document(
    *,
    connector_id: str = "test-connector",
    parameters: dict[str, Any] | None = None,
    selections: dict[str, Any] | None = None,
    discovered: dict[str, Any] | None = None,
    secret_refs: dict[str, str] | None = None,
    connection_id: str | None = None,
) -> ConnectionInput:
    """A validated connection document with the given scope blocks."""
    document: dict[str, Any] = {"connector_id": connector_id}
    if parameters is not None:
        document["parameters"] = parameters
    if selections is not None:
        document["selections"] = selections
    if discovered is not None:
        document["discovered"] = discovered
    if secret_refs is not None:
        document["secret_refs"] = secret_refs
    if connection_id is not None:
        document["connection_id"] = connection_id
    return ConnectionInput.model_validate(document)


def sqlalchemy_transport(
    driver: str = "postgresql+asyncpg",
    template: str | None = None,
    bindings: dict[str, Any] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """A sqlalchemy transport block with a DSN template.

    The default template binds a literal host, so a connection with no
    parameters still materializes; a test exercising a connection value
    passes its own ``template`` and ``bindings``.
    """
    return {
        "transport_type": "sqlalchemy",
        "driver": driver,
        "dsn": {
            "kind": "url_template",
            "template": template or f"{driver}://u:p@{{host}}:5432/d",
            "bindings": bindings or {"host": {"value": "h", "encoding": "host"}},
        },
        **extra,
    }


def adbc_transport(driver: str = "snowflake", **extra: Any) -> dict[str, Any]:
    """An adbc transport block; the contract requires some connection state."""
    return {
        "transport_type": "adbc",
        "driver": driver,
        "db_kwargs": {"account": {"ref": "connection.parameters.account"}},
        **extra,
    }


def http_transport(
    base_url: str = "https://api.example.com", **extra: Any
) -> dict[str, Any]:
    return {"transport_type": "http", "base_url": base_url, **extra}


def connector_document(
    kind: str = "database",
    *,
    connector_id: str = "test-connector",
    transports: dict[str, dict[str, Any]] | None = None,
    default_transport: str | None = None,
    **extra: Any,
) -> Connector:
    """The smallest valid connector definition of *kind*, plus *extra* fields.

    ``transports`` replaces the kind's default block; ``default_transport``
    defaults to the first ref given.
    """
    if transports is not None:
        extra["transports"] = transports
        extra["default_transport"] = default_transport or next(iter(transports), kind)
    elif default_transport is not None:
        extra["default_transport"] = default_transport
    return _CONNECTOR_ADAPTER.validate_python(
        minimal_connector_definition(kind, connector_id, **extra)
    )


def contract_input(
    *,
    required: bool,
    storage: str = "connection.parameters",
    source: str = "user",
) -> dict[str, Any]:
    """One ``connection_contract.inputs`` entry, as connector.json declares it."""
    return {
        "source": source,
        "phase": "pre_auth",
        "storage": storage,
        "type": "string",
        "required": required,
        # The contract ties the two together: an input stored in secrets is
        # a secret, and only such an input may be.
        "secret": storage == "secrets",
    }
