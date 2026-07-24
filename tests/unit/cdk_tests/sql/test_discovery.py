"""Discovery: list_schemas / list_tables / list_columns.

Driven through the fake ADBC runtime with the synthetic postgres-/
snowflake-shaped type-maps from conftest, so the native -> canonical mapping
(and its failure mode) is exercised end to end. The fake ignores the WHERE
filters and just returns canned rows keyed by which catalog view the query
hits.

The dialect is now an explicit ``dialect=`` keyword. The generic-machinery
tests pass the ANSI-neutral ``SqlDialect`` base. Dialect-specific mechanisms
(schema normalization reaching the binds; a structural FROM with no params) are
exercised with small fixture dialects defined here — vendor-specific dialects
live in the connector packages, not in the engine repo.
"""

from __future__ import annotations

import pytest

from cdk.contract import ColumnDef
from cdk.sql.dialects import SqlDialect
from cdk.sql.discovery import list_columns, list_schemas, list_tables
from cdk.sql.exceptions import DiscoveryError
from cdk.type_map.exceptions import UnmappedTypeError

from .conftest import FakeAdbcRuntime, caps_block


def _route(rows_by_view):
    """Build a responder that returns rows based on the catalog view hit.

    Case-insensitive so it also routes upper-cased ``INFORMATION_SCHEMA.*``
    and dataset-qualified queries.
    """

    def responder(sql, params):
        lowered = sql.lower()
        if "key_column_usage" in lowered:
            return rows_by_view.get("pks", [])
        if "information_schema.columns" in lowered:
            return rows_by_view.get("columns", [])
        if "information_schema.tables" in lowered:
            return rows_by_view.get("tables", [])
        if "information_schema.schemata" in lowered:
            return rows_by_view.get("schemas", [])
        return []

    return responder


# --- fixture dialects (stand in for connector-package dialects) -------------


class _UpperSchemaDialect(SqlDialect):
    """Folds identifiers upper-case before they reach the query binds."""

    name = "upper"

    # Overrides an instance method of the dialect interface; a staticmethod
    # would diverge from the shape connector dialects implement.
    def normalize_ident(self, name: str) -> str:
        return name.upper()


class _StructuralFromDialect(SqlDialect):
    """Puts the schema in the FROM path (quoted identifier), not in a bind."""

    name = "structural"

    def tables_query(self, schema, catalog=""):
        return (
            f"SELECT table_name FROM {self.quote_ident(schema)}"
            ".information_schema.tables ORDER BY table_name",
            [],
        )


class TestListSchemas:
    @pytest.mark.asyncio
    async def test_returns_schema_names_in_order(self, pg_mapper):
        runtime = FakeAdbcRuntime(
            "postgresql",
            mapper=pg_mapper,
            responder=_route(
                {"schemas": [{"schema_name": "public"}, {"schema_name": "sales"}]}
            ),
        )
        assert await list_schemas(runtime, dialect=SqlDialect()) == [
            "public",
            "sales",
        ]

    @pytest.mark.asyncio
    async def test_tolerates_uppercased_column_names(self, sf_mapper):
        # A dialect (e.g. Snowflake) folds unquoted output column names to
        # upper case; ``_col`` resolves them case-insensitively.
        runtime = FakeAdbcRuntime(
            "snowflake",
            mapper=sf_mapper,
            responder=_route(
                {"schemas": [{"SCHEMA_NAME": "PUBLIC"}, {"SCHEMA_NAME": "RAW"}]}
            ),
        )
        assert await list_schemas(runtime, dialect=SqlDialect()) == [
            "PUBLIC",
            "RAW",
        ]


class TestListTables:
    @pytest.mark.asyncio
    async def test_returns_table_names(self, pg_mapper):
        runtime = FakeAdbcRuntime(
            "postgresql",
            mapper=pg_mapper,
            responder=_route(
                {"tables": [{"table_name": "orders"}, {"table_name": "customers"}]}
            ),
        )
        assert await list_tables(runtime, "public", dialect=SqlDialect()) == [
            "orders",
            "customers",
        ]
        # The schema reached the query as a bind parameter.
        sql, params = runtime.connections[-1].executed[-1]
        assert params == ["public"]

    @pytest.mark.asyncio
    async def test_normalized_schema_reaches_the_binds(self, pg_mapper):
        # A dialect that normalizes schema names: the normalized value is what
        # the query binds, not the caller's literal.
        runtime = FakeAdbcRuntime(
            "postgresql",
            mapper=pg_mapper,
            responder=_route({"tables": [{"table_name": "orders"}]}),
        )
        assert await list_tables(runtime, "public", dialect=_UpperSchemaDialect()) == [
            "orders"
        ]
        _, params = runtime.connections[-1].executed[-1]
        assert params == ["PUBLIC"]

    @pytest.mark.asyncio
    async def test_structural_from_takes_no_params(self):
        # A dialect that qualifies the schema in the FROM path emits the schema
        # as a quoted identifier and binds no parameters.
        runtime = FakeAdbcRuntime(
            "structural",
            responder=_route({"tables": [{"table_name": "orders"}]}),
        )
        assert await list_tables(runtime, "ds", dialect=_StructuralFromDialect()) == [
            "orders"
        ]
        sql, params = runtime.connections[-1].executed[-1]
        assert '"ds".information_schema.tables' in sql
        assert params == []

    @pytest.mark.asyncio
    async def test_catalog_scope_reaches_the_query(self):
        # A connector declaring catalog addressing ('read' suffices for
        # discovery) scopes the FROM path and the binds to the requested
        # catalog (issue #348 item 4). The declaration rides the runtime;
        # list_tables binds it to the dialect itself.
        class _CatalogDialect(SqlDialect):
            name = "cataloged"

        runtime = FakeAdbcRuntime(
            "cataloged",
            responder=_route({"tables": [{"table_name": "orders"}]}),
            declared_sql_capabilities=caps_block(catalog="read"),
        )
        assert await list_tables(
            runtime, "ds", dialect=_CatalogDialect(), catalog="proj"
        ) == ["orders"]
        sql, params = runtime.connections[-1].executed[-1]
        assert '"proj".information_schema.tables' in sql
        assert params == ["ds", "proj"]


class TestListColumns:
    @pytest.mark.asyncio
    async def test_maps_native_types_and_flags_pks(self, pg_mapper):
        runtime = FakeAdbcRuntime(
            "postgresql",
            mapper=pg_mapper,
            responder=_route(
                {
                    "pks": [{"column_name": "id"}],
                    "columns": [
                        {
                            "column_name": "id",
                            "data_type": "bigint",
                            "is_nullable": "NO",
                        },
                        {
                            "column_name": "name",
                            "data_type": "character varying",
                            "is_nullable": "YES",
                        },
                        {
                            "column_name": "amount",
                            "data_type": "numeric",
                            "is_nullable": "YES",
                        },
                        {
                            "column_name": "created",
                            "data_type": "timestamp with time zone",
                            "is_nullable": "NO",
                        },
                    ],
                }
            ),
        )
        columns, primary_keys = await list_columns(
            runtime, "public", "orders", dialect=SqlDialect()
        )
        assert primary_keys == ["id"]
        assert columns == [
            ColumnDef("id", "Int64", nullable=False, primary_key=True),
            ColumnDef("name", "Utf8", nullable=True, primary_key=False),
            ColumnDef("amount", "Decimal128(38, 9)", nullable=True, primary_key=False),
            ColumnDef(
                "created",
                "Timestamp(MICROSECOND, UTC)",
                nullable=False,
                primary_key=False,
            ),
        ]

    @pytest.mark.asyncio
    async def test_composite_primary_key(self, pg_mapper):
        runtime = FakeAdbcRuntime(
            "postgresql",
            mapper=pg_mapper,
            responder=_route(
                {
                    "pks": [{"column_name": "a"}, {"column_name": "b"}],
                    "columns": [
                        {
                            "column_name": "a",
                            "data_type": "integer",
                            "is_nullable": "YES",
                        },
                        {
                            "column_name": "b",
                            "data_type": "integer",
                            "is_nullable": "YES",
                        },
                        {"column_name": "c", "data_type": "text", "is_nullable": "YES"},
                    ],
                }
            ),
        )
        columns, primary_keys = await list_columns(
            runtime, "public", "t", dialect=SqlDialect()
        )
        assert primary_keys == ["a", "b"]
        assert [c.primary_key for c in columns] == [True, True, False]

    @pytest.mark.asyncio
    async def test_unmapped_native_type_raises_with_context(self, pg_mapper):
        runtime = FakeAdbcRuntime(
            "postgresql",
            mapper=pg_mapper,
            responder=_route(
                {
                    "pks": [],
                    "columns": [
                        {
                            "column_name": "shape",
                            "data_type": "geometry",
                            "is_nullable": "YES",
                        },
                    ],
                }
            ),
        )
        with pytest.raises(DiscoveryError) as exc:
            await list_columns(runtime, "public", "spatial", dialect=SqlDialect())
        assert "public.spatial.shape" in str(exc.value)
        assert "geometry" in str(exc.value)
        # The underlying type-map error is chained, not swallowed.
        assert isinstance(exc.value.__cause__, UnmappedTypeError)

    @pytest.mark.asyncio
    async def test_missing_expected_column_raises(self, pg_mapper):
        # A row that lacks the expected key (exact + case-insensitive) is a hard
        # DiscoveryError, not a silent KeyError or wrong-column read.
        runtime = FakeAdbcRuntime(
            "postgresql",
            mapper=pg_mapper,
            responder=_route({"schemas": [{"unexpected_key": "x"}]}),
        )
        with pytest.raises(DiscoveryError, match="expected column 'schema_name'"):
            await list_schemas(runtime, dialect=SqlDialect())


class TestConnectionScopedTypeMap:
    """``list_columns`` resolves through the connection-composed read map (#368).

    A connection-authored ``type-map-read.json`` must take effect during
    control-plane discovery exactly as it does at stream run time: connection
    rules first, connector rules filling the gaps.
    """

    @pytest.mark.asyncio
    async def test_connection_only_native_resolves(
        self, pg_mapper, pg_connection_mapper
    ):
        # CITEXT exists only in the connection map — the exact case the map
        # was authored for. Before the fix this raised DiscoveryError.
        runtime = FakeAdbcRuntime(
            "postgresql",
            mapper=pg_mapper,
            connection_mapper=pg_connection_mapper,
            responder=_route(
                {
                    "pks": [],
                    "columns": [
                        {
                            "column_name": "email",
                            "data_type": "citext",
                            "is_nullable": "YES",
                        },
                    ],
                }
            ),
        )
        columns, _ = await list_columns(
            runtime, "public", "users", dialect=SqlDialect()
        )
        assert columns == [ColumnDef("email", "Utf8", nullable=True, primary_key=False)]

    @pytest.mark.asyncio
    async def test_connection_rule_wins_and_connector_fills_gaps(
        self, pg_mapper, pg_connection_mapper
    ):
        # TEXT is mapped by both: the connection's rule (LargeUtf8) is
        # authoritative over the connector's (Utf8). BIGINT has no connection
        # rule and falls through to the connector map.
        runtime = FakeAdbcRuntime(
            "postgresql",
            mapper=pg_mapper,
            connection_mapper=pg_connection_mapper,
            responder=_route(
                {
                    "pks": [],
                    "columns": [
                        {
                            "column_name": "body",
                            "data_type": "text",
                            "is_nullable": "YES",
                        },
                        {
                            "column_name": "id",
                            "data_type": "bigint",
                            "is_nullable": "NO",
                        },
                    ],
                }
            ),
        )
        columns, _ = await list_columns(
            runtime, "public", "posts", dialect=SqlDialect()
        )
        assert [c.canonical_type for c in columns] == ["LargeUtf8", "Int64"]


class TestNormalizingDialectDiscovery:
    """A normalizing dialect's binds propagate through list_columns too."""

    @pytest.mark.asyncio
    async def test_list_columns_normalizes_schema_in_binds(self, sf_mapper):
        runtime = FakeAdbcRuntime(
            "snowflake",
            mapper=sf_mapper,
            responder=_route(
                {
                    "pks": [{"COLUMN_NAME": "ID"}],
                    "columns": [
                        {
                            "COLUMN_NAME": "ID",
                            "DATA_TYPE": "NUMBER",
                            "IS_NULLABLE": "NO",
                        },
                    ],
                }
            ),
        )
        columns, pks = await list_columns(
            runtime, "public", "ORDERS", dialect=_UpperSchemaDialect()
        )
        assert pks == ["ID"]
        assert columns == [
            ColumnDef("ID", "Decimal128(38, 0)", nullable=False, primary_key=True)
        ]
        # Both the PK query and the column query bound the normalized schema.
        for _sql, params in runtime.connections[-1].executed:
            assert params[0] == "PUBLIC"


class TestStructuralFromDiscovery:
    """A dialect that puts the schema in the FROM path binds no schema param."""

    @pytest.mark.asyncio
    async def test_list_tables_uses_structural_from(self):
        runtime = FakeAdbcRuntime(
            "structural",
            responder=_route({"tables": [{"table_name": "orders"}]}),
        )
        assert await list_tables(runtime, "ds", dialect=_StructuralFromDialect()) == [
            "orders"
        ]
        sql, params = runtime.connections[-1].executed[-1]
        assert '"ds".information_schema.tables' in sql
        assert params == []
