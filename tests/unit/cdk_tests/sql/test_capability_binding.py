"""One binding site: a dialect carries its declaration from construction (#427).

The declared ``sql_capabilities`` block used to be attached to a dialect by
every entry point that received a runtime — the facade plus each standalone
control-plane helper — so a dialect's capability state depended on how many
callers had pushed it through the binding function.

Now the declaration becomes a dialect exactly once, in
:meth:`SqlDialect.for_runtime`, and :attr:`SqlDialect.capabilities` is
read-only afterwards. These tests pin both halves: the single site
(structurally, the way ``test_cdk_boundary`` pins the import rule) and the
consumer-visible behavior — every path that hands a dialect on hands one that
already carries its connector's declaration.
"""

from __future__ import annotations

import ast
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow as pa
import pytest

import cdk
from cdk.contract import ColumnDef
from cdk.sql.capabilities import SqlCapabilities, SqlCapabilitiesError
from cdk.sql.dialects import SqlDialect
from cdk.sql.exceptions import CatalogAddressingError
from cdk.sql.generic import GenericSQLConnector

from .conftest import FakeAdbcRuntime, caps_block

_CDK_DIR = Path(cdk.__file__).resolve().parent

#: The module that owns the attribute — it publishes the declaration onto the
#: runtime, so its own reads are the source, not a binding site.
_DECLARATION_OWNER = "connection_runtime.py"


def _declaration_reads() -> list[str]:
    """Every ``<x>.declared_sql_capabilities`` read outside the owning module."""
    reads: list[str] = []
    for path in sorted(_CDK_DIR.rglob("*.py")):
        if path.name == _DECLARATION_OWNER:
            continue
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Attribute)
                and node.attr == "declared_sql_capabilities"
            ):
                reads.append(f"{path.relative_to(_CDK_DIR.parent)}:{node.lineno}")
    return reads


class TestOneBindingSite:
    def test_the_declaration_becomes_a_dialect_in_exactly_one_place(self):
        reads = _declaration_reads()
        assert len(reads) == 1, (
            "a runtime's declared sql_capabilities must be read in exactly one "
            "place (SqlDialect.for_runtime); every other consumer reads the "
            "already-bound dialect. Found:\n  " + "\n  ".join(reads)
        )
        assert reads[0].startswith("cdk/sql/dialects.py:")

    def test_capabilities_cannot_be_established_after_construction(self):
        # The half that makes single-site binding structural rather than a
        # convention: a consumer handed a dialect cannot re-establish what
        # the system can do.
        dialect = SqlDialect(SqlCapabilities.from_declaration(caps_block()))
        with pytest.raises(AttributeError):
            dialect.capabilities = None

    def test_a_class_body_capability_is_refused_where_it_is_written(self):
        # The route around the read-only property: a class attribute shadows
        # it outright, so for_runtime's parse would land on _capabilities and
        # never be read again — the dialect would answer gates with a fact
        # its connector.json never declared.
        with pytest.raises(TypeError, match="class body"):

            class _SelfDeclaringDialect(SqlDialect):
                capabilities = SqlCapabilities.from_declaration(
                    caps_block(catalog="full")
                )

    def test_a_constructor_that_drops_the_declaration_is_refused(self):
        # A dialect the CDK cannot hand a declaration to is named where it is
        # defined, not as a bare TypeError at connect.
        with pytest.raises(TypeError, match="forward it to super"):

            class _ClosedDialect(SqlDialect):
                def __init__(self) -> None:
                    super().__init__()

    def test_a_constructor_that_forwards_the_declaration_is_allowed(self):
        class _ExtraStateDialect(SqlDialect):
            def __init__(self, capabilities=None) -> None:
                super().__init__(capabilities)
                self.landings = 0

        dialect = _ExtraStateDialect(SqlCapabilities.from_declaration(caps_block()))
        assert dialect.capabilities is not None
        assert dialect.landings == 0

    def test_for_runtime_keeps_the_connector_package_class(self):
        class _PackageDialect(SqlDialect):
            name = "packaged"

        runtime = FakeAdbcRuntime(
            "packaged", declared_sql_capabilities=caps_block(catalog="full")
        )
        dialect = _PackageDialect.for_runtime(runtime)
        assert isinstance(dialect, _PackageDialect)
        assert dialect.capabilities is not None
        assert dialect.capabilities.catalog == "full"

    def test_for_runtime_names_the_connector_in_a_malformed_declaration(self):
        runtime = FakeAdbcRuntime(
            "demo", declared_sql_capabilities=caps_block(catalog="everything")
        )
        with pytest.raises(SqlCapabilitiesError, match="connector 'demo'"):
            SqlDialect.for_runtime(runtime)

    def test_a_for_runtime_override_is_refused_where_it_is_written(self):
        # The third route around the binding site: an override replaces the
        # parse itself, so the facade would render through a dialect that
        # never went through it. The sanctioned connector surface is
        # dialect_class plus the rendering hooks.
        with pytest.raises(TypeError, match="for_runtime"):

            class _SelfBindingDialect(SqlDialect):
                @classmethod
                def for_runtime(cls, runtime):
                    return cls()


def _connectable_runtime():
    """A runtime the facade's ``connect()`` accepts, declaring full catalog."""
    runtime = MagicMock()
    runtime.connector_id = "demo"
    runtime.declared_sql_capabilities = caps_block(catalog="full")
    runtime.declared_error_map = None
    runtime.is_adbc = False
    runtime.is_sync_sqlalchemy = False
    runtime.driver = "postgresql"
    return runtime


class _RequiredDeclarationDialect(SqlDialect):
    """A package dialect whose constructor demands the declaration.

    The shape the class-definition guard admits: nothing may build this
    dialect without a declaration to hand it.
    """

    name = "strict"

    def __init__(self, capabilities: SqlCapabilities | None) -> None:
        super().__init__(capabilities)


class _RequiredDeclarationConnector(GenericSQLConnector):
    dialect_class = _RequiredDeclarationDialect


async def _write_a_batch(connector: GenericSQLConnector):
    """Push a batch at *connector* and return its result (never raises).

    The write gate is the externally visible answer to "can this handler
    still be written through": it names which guard refused.
    """
    return await connector.write_batch(
        run_id="r1",
        stream_id="s1",
        batch_seq=1,
        record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
        record_ids=["1"],
        cursor=None,
        emitted_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


def _route(rows_by_view):
    def responder(sql, params):
        for view, rows in rows_by_view.items():
            if f"information_schema.{view}" in sql:
                return rows
        return []

    return responder


class TestEveryConsumerPathCarriesTheDeclaration:
    """The facade's entry points hand the standalone helpers a dialect that
    already carries the declaration — visible in the SQL that runs, since a
    catalog-scoped statement is only composable off a declaring dialect."""

    @pytest.mark.asyncio
    async def test_discovery_through_the_facade_scopes_the_declared_catalog(self):
        connector = GenericSQLConnector()
        runtime = FakeAdbcRuntime(
            "cataloged",
            responder=_route({"tables": [{"table_name": "orders"}]}),
            declared_sql_capabilities=caps_block(catalog="read"),
        )
        assert await connector.list_tables(runtime, "ds", catalog="proj") == ["orders"]
        sql, params = runtime.connections[-1].executed[-1]
        # The declared catalog reaches both halves of the statement: the
        # information_schema reference it composes and the predicate it binds.
        assert '"proj".information_schema.tables' in sql
        assert params == ["ds", "proj"]
        assert connector.dialect.capabilities is not None

    @pytest.mark.asyncio
    async def test_discovery_through_the_facade_refuses_an_undeclared_catalog(self):
        connector = GenericSQLConnector()
        runtime = FakeAdbcRuntime("plain", responder=_route({}))
        with pytest.raises(CatalogAddressingError, match="sql_capabilities.catalog"):
            await connector.list_tables(runtime, "ds", catalog="proj")

    @pytest.mark.asyncio
    async def test_create_table_through_the_facade_reads_the_declared_catalog(self):
        connector = GenericSQLConnector()
        runtime = FakeAdbcRuntime(
            "cataloged",
            responder=_route({}),
            declared_sql_capabilities=caps_block(catalog="read"),
        )
        # 'read' addresses a catalog for discovery but not for DDL: the gate
        # fires off the dialect the facade built, with no binding of its own.
        with pytest.raises(CatalogAddressingError, match="requires 'full'"):
            await connector.create_table(
                runtime, "ds", "orders", [ColumnDef("id", "Int64")], [], catalog="proj"
            )

    @pytest.mark.asyncio
    async def test_connect_leaves_the_dialect_carrying_the_declaration(self):
        connector = GenericSQLConnector()
        runtime = _connectable_runtime()
        with patch("cdk.sql.generic.materialize_runtime", new=AsyncMock()):
            await connector.connect(runtime)
        assert connector.dialect.capabilities is connector._capabilities
        assert connector.dialect.capabilities.catalog == "full"

    @pytest.mark.asyncio
    async def test_connect_builds_the_transport_with_the_declaring_dialect(self):
        # The transport factory keeps the dialect it is handed and calls it
        # later (verify_tls_state on every new DBAPI connection), so the
        # dialect must already carry the declaration when the transport is
        # built — the order read_batches uses.
        connector = GenericSQLConnector()
        runtime = _connectable_runtime()
        materialize = AsyncMock()
        with patch("cdk.sql.generic.materialize_runtime", new=materialize):
            await connector.connect(runtime)
        handed = materialize.await_args.kwargs["sql_dialect"]
        assert handed is connector.dialect
        assert handed.capabilities.catalog == "full"

    @pytest.mark.asyncio
    async def test_a_failed_connect_leaves_no_handler_to_write_through(self):
        # connect() replaces the dialect before it acquires the transport,
        # so a materialization that raises must not leave the new runtime's
        # declaration rendering against the previous runtime's live
        # connection. The handler tears down instead: the write gate that
        # let a batch past before now refuses it.
        connector = GenericSQLConnector()
        with patch("cdk.sql.generic.materialize_runtime", new=AsyncMock()):
            await connector.connect(_connectable_runtime())
        assert (await _write_a_batch(connector)).failure_summary == (
            "Schema not configured"
        )

        failing = AsyncMock(side_effect=OSError("host unreachable"))
        with patch("cdk.sql.generic.materialize_runtime", new=failing), pytest.raises(
            ConnectionError
        ):
            await connector.connect(_connectable_runtime())
        assert (await _write_a_batch(connector)).failure_summary == (
            "Handler not connected"
        )


class TestNoDialectBeforeADeclaration:
    """The facade holds no dialect until a runtime's declaration builds one.

    Constructing one at ``__init__`` would manufacture exactly what the
    ticket says must be unrepresentable — a dialect with no declaration
    behind it — before any runtime exists to declare anything."""

    def test_a_connector_carries_no_dialect_before_one_is_bound(self):
        connector = GenericSQLConnector()
        with pytest.raises(AttributeError, match="dialect"):
            connector.dialect  # noqa: B018

    @pytest.mark.asyncio
    async def test_a_dialect_that_requires_the_declaration_works_end_to_end(self):
        # The class-definition guard admits a constructor that takes the
        # declaration as a required parameter, so every path that builds
        # this connector's dialect must supply one.
        connector = _RequiredDeclarationConnector()
        runtime = FakeAdbcRuntime(
            "strict",
            responder=_route({"tables": [{"table_name": "orders"}]}),
            declared_sql_capabilities=caps_block(catalog="read"),
        )
        assert await connector.list_tables(runtime, "ds", catalog="proj") == ["orders"]
        assert connector.dialect.capabilities.catalog == "read"

    def test_the_advertised_write_modes_answer_before_a_runtime_binds(self):
        # GetCapabilities reads these off the handler; which hooks a dialect
        # implements is a class fact, so they answer without a dialect
        # instance rather than raising.
        connector = GenericSQLConnector()
        assert connector.supports_insert is False
        assert connector.supports_upsert is False
        assert connector.supports_truncate is False
