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
    """Build a responder that returns rows based on the catalog view hit.

    Case-insensitive so it also routes BigQuery's upper-cased
    ``INFORMATION_SCHEMA.*`` and dataset-qualified queries.
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
            await list_schemas(runtime)


class TestSnowflakeDiscovery:
    """The snowflake dialect's normalize + flat-schema queries run end to end."""

    @pytest.mark.asyncio
    async def test_list_tables_normalizes_public_schema(self, sf_mapper):
        runtime = FakeAdbcRuntime(
            "snowflake",
            mapper=sf_mapper,
            responder=_route({"tables": [{"TABLE_NAME": "ORDERS"}]}),
        )
        assert await list_tables(runtime, "public") == ["ORDERS"]
        # public -> PUBLIC reached the query as the bind value.
        _, params = runtime.connections[-1].executed[-1]
        assert params == ["PUBLIC"]

    @pytest.mark.asyncio
    async def test_list_columns_maps_native_number(self, sf_mapper):
        runtime = FakeAdbcRuntime(
            "snowflake",
            mapper=sf_mapper,
            responder=_route(
                {
                    "pks": [{"COLUMN_NAME": "ID"}],
                    "columns": [
                        {"COLUMN_NAME": "ID", "DATA_TYPE": "NUMBER",
                         "IS_NULLABLE": "NO"},
                    ],
                }
            ),
        )
        columns, pks = await list_columns(runtime, "public", "ORDERS")
        assert pks == ["ID"]
        assert columns == [
            ColumnDef("ID", "Decimal128(38, 0)", nullable=False, primary_key=True)
        ]


class TestBigQueryDiscovery:
    """The bigquery dialect's dataset-qualified queries run end to end."""

    @pytest.mark.asyncio
    async def test_list_tables_uses_dataset_qualified_from(self):
        runtime = FakeAdbcRuntime(
            "bigquery",
            responder=_route({"tables": [{"table_name": "orders"}]}),
        )
        assert await list_tables(runtime, "ds") == ["orders"]
        sql, params = runtime.connections[-1].executed[-1]
        # Dataset is structural (no bind); table list takes no params.
        assert "`ds`.INFORMATION_SCHEMA.TABLES" in sql
        assert params == []

    @pytest.mark.asyncio
    async def test_list_columns_maps_native_and_binds_table(self, bq_mapper):
        runtime = FakeAdbcRuntime(
            "bigquery",
            mapper=bq_mapper,
            responder=_route(
                {
                    "pks": [],
                    "columns": [
                        {"column_name": "id", "data_type": "INT64",
                         "is_nullable": "NO"},
                        {"column_name": "ok", "data_type": "BOOL",
                         "is_nullable": "YES"},
                    ],
                }
            ),
        )
        columns, pks = await list_columns(runtime, "ds", "orders")
        assert pks == []
        assert columns == [
            ColumnDef("id", "Int64", nullable=False, primary_key=False),
            ColumnDef("ok", "Boolean", nullable=True, primary_key=False),
        ]
        # The table name is bound; the dataset is in the FROM path.
        col_sql, col_params = runtime.connections[-1].executed[-1]
        assert col_params == ["orders"]
