"""Tests for :mod:`src.source.drivers.adbc_reader`."""

from __future__ import annotations

from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from src.source.drivers.adbc_reader import (
    AdbcReadPlan,
    AdbcReader,
    _build_select_sql,
)


class TestBuildSelectSql:
    def test_quotes_identifiers(self):
        sql, params = _build_select_sql(
            AdbcReadPlan(
                schema_name="public",
                table_name="orders",
                columns=["id", "name"],
            ),
            batch_size=100,
            offset=0,
        )
        assert '"public"."orders"' in sql
        assert '"id"' in sql and '"name"' in sql
        assert params == ()

    def test_handles_identifier_quote_escape(self):
        sql, _ = _build_select_sql(
            AdbcReadPlan(
                schema_name='public"weird',
                table_name="orders",
                columns=["id"],
            ),
            batch_size=10,
            offset=0,
        )
        # A literal ``"`` inside an identifier must be doubled.
        assert '"public""weird"' in sql

    def test_inclusive_cursor_uses_gte(self):
        sql, params = _build_select_sql(
            AdbcReadPlan(
                schema_name="",
                table_name="orders",
                columns=["id", "updated_at"],
                cursor_field="updated_at",
                cursor_value="2024-01-01",
                cursor_mode="inclusive",
            ),
            batch_size=100,
            offset=0,
        )
        assert '"updated_at" >= ?' in sql
        assert 'ORDER BY "updated_at" ASC' in sql
        assert params == ("2024-01-01",)

    def test_exclusive_cursor_uses_gt(self):
        sql, params = _build_select_sql(
            AdbcReadPlan(
                schema_name="",
                table_name="orders",
                columns=["id"],
                cursor_field="id",
                cursor_value=42,
                cursor_mode="exclusive",
            ),
            batch_size=10,
            offset=0,
        )
        assert '"id" > ?' in sql
        assert params == (42,)

    def test_no_cursor_emits_no_where_no_order(self):
        sql, params = _build_select_sql(
            AdbcReadPlan(
                schema_name="public",
                table_name="orders",
                columns=["id"],
            ),
            batch_size=50,
            offset=200,
        )
        assert "WHERE" not in sql
        assert "ORDER BY" not in sql
        assert "LIMIT 50 OFFSET 200" in sql
        assert params == ()

    def test_select_star_when_no_columns(self):
        sql, _ = _build_select_sql(
            AdbcReadPlan(
                schema_name="",
                table_name="orders",
                columns=[],
            ),
            batch_size=1,
            offset=0,
        )
        assert sql.startswith("SELECT * FROM ")


class TestAdbcReadPlan:
    def test_rejects_invalid_cursor_mode(self):
        with pytest.raises(ValueError, match="must be 'inclusive' or 'exclusive'"):
            AdbcReadPlan(
                schema_name="public",
                table_name="t",
                cursor_mode="Inclusive",  # case mismatch
            )

    def test_is_frozen(self):
        plan = AdbcReadPlan(schema_name="public", table_name="t")
        with pytest.raises(Exception):
            plan.table_name = "other"  # type: ignore[misc]


class TestAdbcReader:
    def test_rejects_none_connection(self):
        with pytest.raises(ValueError, match="open ADBC connection"):
            AdbcReader(None)

    @pytest.mark.asyncio
    async def test_paginates_until_short_page(self):
        schema = pa.schema([pa.field("id", pa.int64())])
        full_page = pa.record_batch([pa.array(list(range(10)))], schema=schema)
        short_page = pa.record_batch([pa.array([100, 101])], schema=schema)

        # ``RecordBatchReader`` is iterable per the Arrow C++ binding;
        # any iterable that yields RecordBatch instances is enough for
        # ``_fetch_page_sync`` since it materializes via ``list(reader)``.
        page_readers = [iter([full_page]), iter([short_page])]

        cursor = MagicMock()
        cursor.fetch_record_batch.side_effect = page_readers

        connection = MagicMock()
        connection.cursor.return_value = cursor

        reader = AdbcReader(connection)
        plan = AdbcReadPlan(
            schema_name="public", table_name="t", columns=["id"]
        )

        collected = []
        async for batch in reader.read_batches(plan, batch_size=10):
            collected.append(batch)

        assert sum(b.num_rows for b in collected) == 12
        # Second cursor.execute fires for the next page (offset = 10).
        assert cursor.execute.call_count == 2
        first_sql = cursor.execute.call_args_list[0].args[0]
        second_sql = cursor.execute.call_args_list[1].args[0]
        assert "OFFSET 0" in first_sql
        assert "OFFSET 10" in second_sql
