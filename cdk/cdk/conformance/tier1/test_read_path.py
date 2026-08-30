"""The read path compiles under this connector's dialect flavour.

Every database read renders through the CDK's ``QueryBuilder`` using
the connector's declared driver and the dialect's
``sqlalchemy_registry_name`` (issue #105 unified the cursor filter
there). Each declared transport is probed with the exact construction
its read path uses: the SQLAlchemy read binds in the driver's native
style and consults the dialect's paging fallback; the ADBC-only read
forces qmark binding, quotes every identifier, and inlines LIMIT/OFFSET
— the shape where quoting and literal-paging dialect quirks actually
bite. A wrong registry name, a missing SQLAlchemy dialect package in
the connector's requirements, or a broken transports block breaks every
read at runtime, so a database connector that cannot even derive a
driver *fails* these tests rather than skipping them (the gate's input
must not be supplied by the defect it gates).
"""

from __future__ import annotations

import pytest
from analitiq.contracts.connection import ConnectionInput

from cdk.conformance.fakes import NoSecretsResolver
from cdk.conformance.skips import require_dialect
from cdk.conformance.target import ConformanceTarget
from cdk.connection_runtime import ConnectionRuntime
from cdk.query_builder import QueryBuilder, QueryConfig
from cdk.sql.capabilities import SQL_TRANSPORT_TYPES
from cdk.sql.dialects import SqlDialect
from cdk.sql.generic import read_query_builder

#: Every check here renders SQL through a dialect (see
#: cdk.conformance.applicability).
APPLIES_TO_KINDS = ("database",)


def _declared_driver(target: ConformanceTarget) -> str:
    """The SQL dialect string the engine would derive, fail-loud.

    A database connector whose definition cannot yield a driver can
    never build a connection; that is a definition defect, not an
    inapplicable check.
    """
    runtime = ConnectionRuntime(
        connection=ConnectionInput(connector_id=target.connector_id),
        connection_id="conformance-definition",
        connector_id=target.connector_id,
        connector_type=target.kind,
        resolver=NoSecretsResolver(),
        connector=target.connector,
    )
    driver = runtime.driver
    if not driver:
        transports = target.definition.get("transports")
        if not isinstance(transports, dict) or not transports:
            problem = "connector.json declares no transports block"
        else:
            problem = (
                f"no SQL driver is derivable from the declared transports "
                f"{sorted(transports)} (check default_transport, each "
                f"block's transport_type, and its driver field)"
            )
        pytest.fail(
            f"{problem}; the engine cannot build a connection for a "
            f"database connector without one"
        )
    return driver


def _declared_sql_transports(target: ConformanceTarget) -> list[tuple[str, str]]:
    """Collect ``(transport_type, dialect string)`` per declared SQL transport.

    Each transport is probed with its own block's driver — the engine
    derives the read dialect from the transport actually materialized,
    so probing a non-default ADBC transport with the default SQLAlchemy
    transport's driver would certify SQL the ADBC read never compiles.
    The dialect string is the driver up to any ``+`` flavour suffix
    (``postgresql+asyncpg`` reads as ``postgresql``; ADBC driver names
    carry none). Fail-loud when nothing SQL-shaped is declared or a
    block declares no driver.
    """
    declared: list[tuple[str, str]] = []
    for ref, block in target.declared_transports().items():
        transport_type = block.get("transport_type")
        if transport_type not in SQL_TRANSPORT_TYPES:
            continue
        raw_driver = block.get("driver")
        if not isinstance(raw_driver, str) or not raw_driver:
            pytest.fail(
                f"transport {ref!r} ({transport_type}) declares no driver; "
                f"the engine cannot materialize it"
            )
        shape = (transport_type, raw_driver.split("+", 1)[0])
        if shape not in declared:
            declared.append(shape)
    if not declared:
        pytest.fail(
            f"connector.json declares no SQL transport (sqlalchemy/adbc) in "
            f"transports {sorted(target.declared_transports())}; a database "
            f"connector needs one for the engine's read path"
        )
    return declared


def _query_builder(
    dialect: SqlDialect, transport_type: str, driver: str
) -> QueryBuilder:
    """Build the read-path query builder exactly as the engine does.

    Calls the engine's own :func:`~cdk.sql.generic.read_query_builder`
    — the single construction both read flavors use — so the shape the
    suite certifies and the shape the engine executes cannot drift.
    """
    try:
        return read_query_builder(dialect, driver, adbc_only=transport_type == "adbc")
    except Exception as err:  # noqa: BLE001 - reported as a conformance failure
        pytest.fail(
            f"QueryBuilder cannot resolve a SQLAlchemy dialect for driver "
            f"{driver!r} in the {transport_type} read shape (registry_name="
            f"{dialect.sqlalchemy_registry_name!r}): {err}. The engine "
            f"renders every read on this declared transport through this "
            f"shape; add the SQLAlchemy dialect package to the connector's "
            f"requirements or fix sqlalchemy_registry_name."
        )


def test_definition_derives_a_driver(
    conformance_target: ConformanceTarget,
) -> None:
    """The default transport yields a driver the engine can materialize."""
    assert _declared_driver(conformance_target)


def test_read_query_compiles_on_every_declared_transport(
    conformance_target: ConformanceTarget,
) -> None:
    """A plain projection renders in each declared transport's shape."""
    dialect = require_dialect(conformance_target)
    for transport_type, driver in _declared_sql_transports(conformance_target):
        builder = _query_builder(dialect, transport_type, driver)
        sql, _params = builder.build_select_query(
            QueryConfig(
                schema_name="conformance",
                table_name="conformance_target",
                columns=["id", "seq"],
            )
        )
        assert "conformance_target" in sql


def test_cursor_read_orders_by_the_cursor_field(
    conformance_target: ConformanceTarget,
) -> None:
    """Incremental reads filter on and order by the cursor.

    Checkpoint advancement takes the page's last row, which is the
    maximum only when pages are ordered by the cursor — the ordering is
    what makes saved cursors monotonic.
    """
    dialect = require_dialect(conformance_target)
    for transport_type, driver in _declared_sql_transports(conformance_target):
        builder = _query_builder(dialect, transport_type, driver)
        sql, _params = builder.build_select_query(
            QueryConfig(
                schema_name="conformance",
                table_name="conformance_target",
                columns=["id", "seq"],
                cursor_field="seq",
                cursor_value=5,
                limit=10,
            )
        )
        rendered = sql.upper()
        assert "ORDER BY" in rendered, (
            f"cursor-filtered read renders no ORDER BY; cursor checkpoints "
            f"would not be monotonic: {sql!r}"
        )
        ordering = rendered.split("ORDER BY", 1)[1]
        assert (
            "SEQ" in ordering
        ), f"the read is ordered, but not by the cursor field: {sql!r}"
