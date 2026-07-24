"""The read path compiles under this connector's dialect flavour.

Every database read renders through the CDK's ``QueryBuilder`` using
the connector's declared driver and the dialect's
``sqlalchemy_registry_name`` (issue #105 unified the cursor filter
there) — with the driver's native parameter style on the SQLAlchemy
transport and the forced ``qmark`` style on the ADBC transport. The
shapes checked here are exactly the ones the connector's declared
transports make the engine use; a wrong registry name, a missing
SQLAlchemy dialect package in the connector's requirements, or a broken
transports block breaks every read at runtime, so a database connector
that cannot even derive a driver *fails* these tests rather than
skipping them (the gate's input must not be supplied by the defect it
gates).
"""

from __future__ import annotations

import pytest

from cdk.conformance.fakes import NoSecretsResolver
from cdk.conformance.skips import require_database, require_dialect
from cdk.conformance.target import ConformanceTarget
from cdk.connection_runtime import ConnectionRuntime
from cdk.query_builder import QueryBuilder, QueryConfig
from cdk.sql.dialects import SqlDialect

#: transport_type -> the paramstyle the engine builds QueryBuilder with
#: on that transport (None = the driver's native style).
_TRANSPORT_PARAMSTYLES = {"sqlalchemy": None, "adbc": "qmark"}


def _declared_driver(target: ConformanceTarget) -> str:
    """The SQL dialect string the engine would derive, fail-loud.

    A database connector whose definition cannot yield a driver can
    never build a connection; that is a definition defect, not an
    inapplicable check.
    """
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


def _required_paramstyles(target: ConformanceTarget) -> list[str | None]:
    """The QueryBuilder parameter styles the declared transports need."""
    transports = target.definition.get("transports") or {}
    styles: list[str | None] = []
    for block in transports.values():
        transport_type = (
            block.get("transport_type") if isinstance(block, dict) else None
        )
        if transport_type in _TRANSPORT_PARAMSTYLES:
            style = _TRANSPORT_PARAMSTYLES[transport_type]
            if style not in styles:
                styles.append(style)
    if not styles:
        pytest.fail(
            f"connector.json declares no SQL transport (sqlalchemy/adbc) in "
            f"transports {sorted(transports)}; a database connector needs "
            f"one for the engine's read path"
        )
    return styles


def _query_builder(
    target: ConformanceTarget, dialect: SqlDialect, paramstyle: str | None
) -> QueryBuilder:
    """Build the read-path query builder in one engine construction shape."""
    driver = _declared_driver(target)
    try:
        return QueryBuilder(
            driver,
            paramstyle=paramstyle,
            registry_name=dialect.sqlalchemy_registry_name,
            paging_order_fallback=dialect.paging_order_fallback,
        )
    except Exception as err:  # noqa: BLE001 - reported as a conformance failure
        shape = "native" if paramstyle is None else paramstyle
        pytest.fail(
            f"QueryBuilder cannot resolve a SQLAlchemy dialect for driver "
            f"{driver!r} with the {shape} parameter style (registry_name="
            f"{dialect.sqlalchemy_registry_name!r}): {err}. The engine "
            f"renders every read on this declared transport through this "
            f"shape; add the SQLAlchemy dialect package to the connector's "
            f"requirements or fix sqlalchemy_registry_name."
        )


def test_read_query_compiles_on_every_declared_transport(
    conformance_target: ConformanceTarget,
) -> None:
    """A plain projection renders in each declared transport's shape."""
    require_database(conformance_target)
    dialect = require_dialect(conformance_target)
    for paramstyle in _required_paramstyles(conformance_target):
        builder = _query_builder(conformance_target, dialect, paramstyle)
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
    for paramstyle in _required_paramstyles(conformance_target):
        builder = _query_builder(conformance_target, dialect, paramstyle)
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
