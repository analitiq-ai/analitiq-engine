"""The read path compiles under this connector's dialect flavour.

Every database read — both transports — renders through the CDK's
``QueryBuilder`` using the connector's declared driver and the dialect's
``sqlalchemy_registry_name`` (issue #105 unified the cursor filter
there). A wrong registry name, a missing SQLAlchemy dialect package in
the connector's requirements, or a broken ``paging_order_fallback``
breaks every read at runtime; these tests catch it in the connector's CI
with its own installed requirements.
"""

from __future__ import annotations

import pytest

from cdk.conformance.fakes import NoSecretsResolver
from cdk.conformance.skips import require_database, require_dialect
from cdk.conformance.target import ConformanceTarget
from cdk.connection_runtime import ConnectionRuntime
from cdk.query_builder import QueryBuilder, QueryConfig
from cdk.sql.dialects import SqlDialect


def _declared_driver(target: ConformanceTarget) -> str:
    """The SQL dialect string the engine would derive for this connector."""
    runtime = ConnectionRuntime(
        raw_config={},
        connection_id="conformance-definition",
        connector_id=target.connector_id,
        connector_type=target.kind,
        resolver=NoSecretsResolver(),
        connector_definition=target.definition,
    )
    driver = runtime.driver
    if not driver:
        pytest.skip(
            "connector.json declares no SQL transport to derive a dialect "
            "string from; the QueryBuilder checks do not apply"
        )
    return driver


def _query_builder(target: ConformanceTarget, dialect: SqlDialect) -> QueryBuilder:
    """Build the read-path query builder as the engine builds it.

    Tried with the driver's native parameter style first, then with the
    qmark style the ADBC-only read path forces; a connector is conformant
    when at least one of the engine's two construction shapes resolves
    against the connector's own installed requirements.
    """
    driver = _declared_driver(target)
    last_error: Exception | None = None
    for paramstyle in (None, "qmark"):
        try:
            return QueryBuilder(
                driver,
                paramstyle=paramstyle,
                registry_name=dialect.sqlalchemy_registry_name,
                paging_order_fallback=dialect.paging_order_fallback,
            )
        except Exception as err:  # noqa: BLE001 - reported as a conformance failure
            last_error = err
    pytest.fail(
        f"QueryBuilder cannot resolve a SQLAlchemy dialect for driver "
        f"{driver!r} (registry_name="
        f"{dialect.sqlalchemy_registry_name!r}): {last_error}. The engine "
        f"renders every read through this dialect; add the SQLAlchemy "
        f"dialect package to the connector's requirements or fix "
        f"sqlalchemy_registry_name."
    )


def test_read_query_compiles_under_the_connector_dialect(
    conformance_target: ConformanceTarget,
) -> None:
    """A plain projection over a schema-qualified table renders."""
    require_database(conformance_target)
    dialect = require_dialect(conformance_target)
    builder = _query_builder(conformance_target, dialect)
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
    require_database(conformance_target)
    dialect = require_dialect(conformance_target)
    builder = _query_builder(conformance_target, dialect)
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
    assert "SEQ" in rendered
