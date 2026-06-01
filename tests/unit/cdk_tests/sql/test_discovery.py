"""Discovery: list_schemas / list_tables / list_columns.

Driven through the fake ADBC runtime with the *real* postgres type-map, so the
native -> canonical mapping (and its failure mode) is exercised end to end. The
fake ignores the WHERE filters and just returns canned rows keyed by which
catalog view the query hits.
"""

from __future__ import annotations

import pytest

from cdk.contract import ColumnDef
from cdk.sql.discovery import list_columns, list_schemas, list_tables
from cdk.sql.exceptions import DiscoveryError
from cdk.type_map.exceptions import UnmappedTypeError

from .conftest import FakeAdbcRuntime


def _route(rows_by_view):
    """Build a responder that returns rows based on the catalog view hit."""

    def responder(sql, params):
        if "key_column_usage" in sql:
            return rows_by_view.get("pks", [])
        if "information_schema.columns" in sql:
            return rows_by_view.get("columns", [])
        if "information_schema.tables" in sql:
            return rows_by_view.get("tables", [])
        if "information_schema.schemata" in sql:
            return rows_by_view.get("schemas", [])
        return []

    return responder


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
        assert await list_schemas(runtime) == ["public", "sales"]

    @pytest.mark.asyncio
    async def test_tolerates_uppercased_column_names(self, sf_mapper):
        # Snowflake folds unquoted output column names to upper case.
        runtime = FakeAdbcRuntime(
            "snowflake",
            mapper=sf_mapper,
            responder=_route(
                {"schemas": [{"SCHEMA_NAME": "PUBLIC"}, {"SCHEMA_NAME": "RAW"}]}
            ),
        )
        assert await list_schemas(runtime) == ["PUBLIC", "RAW"]


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
        assert await list_tables(runtime, "public") == ["orders", "customers"]
        # The schema reached the query as a bind parameter.
        sql, params = runtime.connections[-1].executed[-1]
        assert params == ["public"]


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
                        {"column_name": "id", "data_type": "bigint",
                         "is_nullable": "NO"},
                        {"column_name": "name", "data_type": "character varying",
                         "is_nullable": "YES"},
                        {"column_name": "amount", "data_type": "numeric",
                         "is_nullable": "YES"},
                        {"column_name": "created",
                         "data_type": "timestamp with time zone",
                         "is_nullable": "NO"},
                    ],
                }
            ),
        )
        columns, primary_keys = await list_columns(runtime, "public", "orders")
        assert primary_keys == ["id"]
        assert columns == [
            ColumnDef("id", "Int64", nullable=False, primary_key=True),
            ColumnDef("name", "Utf8", nullable=True, primary_key=False),
            ColumnDef("amount", "Decimal128(38, 9)", nullable=True, primary_key=False),
            ColumnDef(
                "created", "Timestamp(MICROSECOND, UTC)",
                nullable=False, primary_key=False,
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
                        {"column_name": "a", "data_type": "integer",
                         "is_nullable": "YES"},
                        {"column_name": "b", "data_type": "integer",
                         "is_nullable": "YES"},
                        {"column_name": "c", "data_type": "text",
                         "is_nullable": "YES"},
                    ],
                }
            ),
        )
        columns, primary_keys = await list_columns(runtime, "public", "t")
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
                        {"column_name": "shape", "data_type": "geometry",
                         "is_nullable": "YES"},
                    ],
                }
            ),
        )
        with pytest.raises(DiscoveryError) as exc:
            await list_columns(runtime, "public", "spatial")
        assert "public.spatial.shape" in str(exc.value)
        assert "geometry" in str(exc.value)
        # The underlying type-map error is chained, not swallowed.
        assert isinstance(exc.value.__cause__, UnmappedTypeError)
