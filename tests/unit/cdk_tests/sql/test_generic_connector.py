"""GenericSQLConnector: the Readable + Discoverable + TableCreator surface.

The Writable (destination) half is covered by the relocated destination
suite. These tests pin the read path promoted onto the unified class and
the thin control-plane delegators:

* ``read_batches`` materializes the runtime it is handed (no prior
  ``connect()``), pages via ``QueryBuilder``, advances the checkpoint,
  and releases the runtime on exit.
* the ADBC branch composes filters + cursor into one quoted, qmark,
  inline-paged SELECT and guards an empty projection.
* ``list_schemas`` / ``list_tables`` / ``list_columns`` / ``create_table``
  delegate to the standalone ``cdk.sql`` helpers.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, patch

import pyarrow as pa
import pytest

from cdk.secrets.exceptions import PlaceholderExpansionError
from cdk.sql.exceptions import ReadError
from cdk.sql.generic import GenericSQLConnector


class _FakeRuntime:
    """Minimal materialized runtime for the read path."""

    def __init__(
        self, *, is_adbc: bool, driver: str = "postgresql", engine: Any = None
    ):
        self.is_adbc = is_adbc
        self.is_sync_sqlalchemy = False
        self.driver = driver
        self.connector_id = driver
        self.declared_sql_capabilities = None
        self.declared_error_map = None
        self.engine = engine
        self.close = AsyncMock()


class _RecordingReader:
    """Fake AdbcReader: records SQL/params, returns one page then drains."""

    def __init__(self, pages: list[list[pa.RecordBatch]]) -> None:
        self._pages = pages
        self.calls: list[tuple[str, Any]] = []

    async def fetch_page(self, sql: str, params: Any = ()) -> list[pa.RecordBatch]:
        self.calls.append((sql, list(params)))
        return self._pages.pop(0) if self._pages else []


def _checkpoint(cursor: dict | None = None) -> AsyncMock:
    cp = AsyncMock()
    cp.get_cursor = AsyncMock(return_value=cursor)
    cp.save_cursor = AsyncMock()
    return cp


def _endpoint_config(
    filters=None,
    replication=None,
    columns=("id", "updated_at"),
    database_pagination=None,
):
    return {
        "endpoint_document": {
            "database_object": {"name": "orders", "schema": "public"},
            "columns": [{"name": c} for c in columns],
        },
        "stream_source": {
            "filters": filters or [],
            "replication": replication or {},
            "database_pagination": database_pagination or {},
        },
    }


async def _drain(connector, runtime, config, checkpoint, batch_size=2):
    out = []
    async for batch in connector.read_batches(
        runtime,
        config,
        checkpoint=checkpoint,
        stream_name="s",
        batch_size=batch_size,
    ):
        out.append(batch)
    return out


class TestReadGuards:
    @pytest.mark.asyncio
    async def test_missing_endpoint_document_raises(self):
        connector = GenericSQLConnector()
        runtime = _FakeRuntime(is_adbc=False)
        with pytest.raises(ReadError, match="missing 'endpoint_document'"):
            await _drain(connector, runtime, {}, _checkpoint())

    @pytest.mark.asyncio
    async def test_missing_table_name_raises(self):
        connector = GenericSQLConnector()
        runtime = _FakeRuntime(is_adbc=False)
        config = {"endpoint_document": {"database_object": {}, "columns": []}}
        with pytest.raises(ReadError, match="database_object.name"):
            await _drain(connector, runtime, config, _checkpoint())

    @pytest.mark.asyncio
    async def test_incremental_cursor_field_not_in_projection_raises(self):
        # An incremental stream whose projection drops the cursor column
        # would silently degrade to full-scan + upsert every run; the
        # misconfiguration must fail before any extraction work.
        connector = GenericSQLConnector()
        runtime = _FakeRuntime(is_adbc=True)
        with patch("cdk.sql.generic.materialize_runtime", new=AsyncMock()), patch(
            "cdk.sql.generic.SchemaContract"
        ):
            config = _endpoint_config(
                replication={"method": "incremental", "cursor_field": "deleted_at"},
            )
            with pytest.raises(ReadError, match="cursor_field 'deleted_at'"):
                await _drain(connector, runtime, config, _checkpoint())
        runtime.close.assert_awaited()

    def test_build_filters_missing_field_raises(self):
        # A filter dict without 'field' used to be skipped, silently
        # widening the result set.
        with pytest.raises(ReadError, match="missing 'field'"):
            GenericSQLConnector._build_filters([{"operator": "eq", "value": 1}])

    @pytest.mark.asyncio
    async def test_incremental_wildcard_projection_passes_cursor_check(self):
        # selected_columns ['*'] compiles to SELECT *, which always carries
        # the cursor column — the projection guard must not reject it.
        runtime = _FakeRuntime(is_adbc=True)
        page = [pa.RecordBatch.from_pydict({"id": [1], "updated_at": ["2024-01-01"]})]
        reader = _RecordingReader([page, []])

        class _CM:
            async def __aenter__(self):
                return reader

            async def __aexit__(self, *exc):
                return False

        connector = GenericSQLConnector()
        with patch("cdk.sql.generic.materialize_runtime", new=AsyncMock()), patch(
            "cdk.sql.generic.open_adbc_reader", return_value=_CM()
        ), patch("cdk.sql.generic.SchemaContract") as sc:
            sc.return_value.cast_arrow_batch.side_effect = lambda b: b
            config = _endpoint_config(
                replication={"method": "incremental", "cursor_field": "updated_at"},
            )
            config["stream_source"]["selected_columns"] = ["*"]
            batches = await _drain(connector, runtime, config, _checkpoint())

        assert batches
        sql, _ = reader.calls[0]
        assert "SELECT *" in sql or "select *" in sql.lower()

    @pytest.mark.asyncio
    async def test_incremental_wildcard_with_cursor_outside_contract_raises(self):
        # SELECT * fetches everything, but the batch is cast through
        # SchemaContract, which keeps only contract columns — a cursor not
        # declared in the endpoint contract would never advance.
        connector = GenericSQLConnector()
        runtime = _FakeRuntime(is_adbc=True)
        with patch("cdk.sql.generic.materialize_runtime", new=AsyncMock()), patch(
            "cdk.sql.generic.SchemaContract"
        ):
            config = _endpoint_config(
                replication={"method": "incremental", "cursor_field": "modified_ts"},
            )
            config["stream_source"]["selected_columns"] = ["*"]
            with pytest.raises(ReadError, match="cursor_field 'modified_ts'"):
                await _drain(connector, runtime, config, _checkpoint())
        runtime.close.assert_awaited()


class TestReadConnectErrors:
    """read_batches connect-phase error classification mirrors connect()."""

    @pytest.mark.asyncio
    async def test_value_error_wrapped_in_read_error(self):
        # ValueError is not a deterministic config error — it must be wrapped
        # in ReadError so callers get user-facing context rather than a raw
        # internal exception.
        connector = GenericSQLConnector()
        runtime = _FakeRuntime(is_adbc=False)
        with patch(
            "cdk.sql.generic.materialize_runtime",
            new=AsyncMock(side_effect=ValueError("bad DSN")),
        ):
            with pytest.raises(ReadError, match="Database connection failed"):
                await _drain(connector, runtime, _endpoint_config(), _checkpoint())

    @pytest.mark.asyncio
    async def test_deterministic_error_propagates_unchanged(self):
        # Typed config errors (PlaceholderExpansionError, InvalidTypeMapError,
        # UnmappedTypeError) must still surface unchanged so the worker can
        # classify them without unwrapping.
        connector = GenericSQLConnector()
        runtime = _FakeRuntime(is_adbc=False)
        exc = PlaceholderExpansionError(
            placeholder="password", connection_id="db", detail="not found"
        )
        with patch(
            "cdk.sql.generic.materialize_runtime",
            new=AsyncMock(side_effect=exc),
        ):
            with pytest.raises(PlaceholderExpansionError):
                await _drain(connector, runtime, _endpoint_config(), _checkpoint())


class TestReadAdbcBranch:
    @pytest.mark.asyncio
    async def test_filters_and_cursor_compose_into_one_select(self):
        runtime = _FakeRuntime(is_adbc=True)
        checkpoint = _checkpoint(cursor={"cursor": "2024-01-02"})
        page = [pa.RecordBatch.from_pydict({"id": [3], "updated_at": ["2024-01-03"]})]
        reader = _RecordingReader([page, []])

        class _CM:
            async def __aenter__(self):
                return reader

            async def __aexit__(self, *exc):
                return False

        connector = GenericSQLConnector()
        with patch("cdk.sql.generic.materialize_runtime", new=AsyncMock()), patch(
            "cdk.sql.generic.open_adbc_reader", return_value=_CM()
        ), patch("cdk.sql.generic.SchemaContract") as sc:
            sc.return_value.cast_arrow_batch.side_effect = lambda b: b
            config = _endpoint_config(
                filters=[{"field": "status", "operator": "eq", "value": "active"}],
                replication={"method": "incremental", "cursor_field": "updated_at"},
            )
            await _drain(connector, runtime, config, checkpoint)

        sql, params = reader.calls[0]
        assert "?" in sql
        assert '"status" = ?' in sql
        # Inclusive resume bound (>=): the boundary row is re-read and deduped
        # by upsert, so a late row sharing the cursor value is not lost.
        assert '"updated_at" >= ?' in sql
        assert 'ORDER BY "updated_at"' in sql
        assert "LIMIT 2 OFFSET 0" in sql
        # Filter value first, cursor value second (WHERE build order).
        assert params == ["active", "2024-01-02"]
        # Runtime released after the read.
        runtime.close.assert_awaited()

    @pytest.mark.asyncio
    async def test_empty_columns_rejected(self):
        runtime = _FakeRuntime(is_adbc=True)
        connector = GenericSQLConnector()
        with patch("cdk.sql.generic.materialize_runtime", new=AsyncMock()), patch(
            "cdk.sql.generic.SchemaContract"
        ):
            config = _endpoint_config(columns=())
            config["endpoint_document"]["columns"] = []
            with pytest.raises(ReadError, match="non-empty column projection"):
                await _drain(connector, runtime, config, _checkpoint())
        # Even on the read error, the runtime is released.
        runtime.close.assert_awaited()

    @pytest.mark.asyncio
    async def test_saves_last_cursor_value_from_batch(self):
        runtime = _FakeRuntime(is_adbc=True)
        checkpoint = _checkpoint(cursor=None)
        page = [
            pa.RecordBatch.from_pydict(
                {"id": [1, 2], "updated_at": ["2024-01-02", "2024-01-09"]}
            )
        ]
        reader = _RecordingReader([page, []])

        class _CM:
            async def __aenter__(self):
                return reader

            async def __aexit__(self, *exc):
                return False

        connector = GenericSQLConnector()
        with patch("cdk.sql.generic.materialize_runtime", new=AsyncMock()), patch(
            "cdk.sql.generic.open_adbc_reader", return_value=_CM()
        ), patch("cdk.sql.generic.SchemaContract") as sc:
            sc.return_value.cast_arrow_batch.side_effect = lambda b: b
            config = _endpoint_config(
                replication={"method": "incremental", "cursor_field": "updated_at"},
            )
            await _drain(connector, runtime, config, checkpoint)

        saved = [c.args[2]["cursor"] for c in checkpoint.save_cursor.call_args_list]
        assert saved[-1] == "2024-01-09"


def _adbc_cm(reader: _RecordingReader):
    """Wrap a recording reader as the async CM ``open_adbc_reader`` returns."""

    class _CM:
        async def __aenter__(self):
            return reader

        async def __aexit__(self, *exc):
            return False

    return _CM()


class TestReadAdbcBranchPaging:
    """Filter-only, ORDER BY fallback, fixed-WHERE paging, and bind guards."""

    @pytest.mark.asyncio
    async def test_full_refresh_with_filters_only(self):
        runtime = _FakeRuntime(is_adbc=True)
        page = [pa.RecordBatch.from_pydict({"id": [1], "updated_at": ["2024-01-01"]})]
        reader = _RecordingReader([page, []])
        connector = GenericSQLConnector()
        with patch("cdk.sql.generic.materialize_runtime", new=AsyncMock()), patch(
            "cdk.sql.generic.open_adbc_reader", return_value=_adbc_cm(reader)
        ), patch("cdk.sql.generic.SchemaContract") as sc:
            sc.return_value.cast_arrow_batch.side_effect = lambda b: b
            config = _endpoint_config(
                filters=[{"field": "status", "operator": "eq", "value": "x"}],
            )
            await _drain(connector, runtime, config, _checkpoint())

        sql, params = reader.calls[0]
        assert '"status" = ?' in sql
        # No cursor -> no cursor predicate, ORDER BY falls back to first column.
        assert ">=" not in sql
        assert 'ORDER BY "id"' in sql
        assert params == ["x"]

    @pytest.mark.asyncio
    async def test_declared_order_by_field_used_without_fallback_warning(self, caplog):
        # source.database_pagination.order_by_field from the stream config
        # is the declared page ordering: no first-column fallback, no warning.
        from cdk.sql import generic as generic_mod

        generic_mod._order_by_fallback_logged.clear()
        runtime = _FakeRuntime(is_adbc=True)
        page = [pa.RecordBatch.from_pydict({"id": [1], "updated_at": ["2024-01-01"]})]
        reader = _RecordingReader([page, []])
        connector = GenericSQLConnector()
        with patch("cdk.sql.generic.materialize_runtime", new=AsyncMock()), patch(
            "cdk.sql.generic.open_adbc_reader", return_value=_adbc_cm(reader)
        ), patch("cdk.sql.generic.SchemaContract") as sc:
            sc.return_value.cast_arrow_batch.side_effect = lambda b: b
            config = _endpoint_config(
                database_pagination={"type": "offset", "order_by_field": "updated_at"},
            )
            with caplog.at_level("WARNING"):
                await _drain(connector, runtime, config, _checkpoint())

        sql, _ = reader.calls[0]
        assert 'ORDER BY "updated_at"' in sql
        assert not any("defaulting ORDER BY" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_order_by_field_conflicting_with_cursor_rejected(self):
        # Checkpoint advancement takes the cursor value of the page's last
        # row — the maximum only when pages are ordered by the cursor. A
        # declared ordering that diverges from the cursor would save
        # arbitrary cursor values and silently skip rows on later runs,
        # so the stream is rejected before any extraction work.
        runtime = _FakeRuntime(is_adbc=True)
        connector = GenericSQLConnector()
        with patch("cdk.sql.generic.materialize_runtime", new=AsyncMock()), patch(
            "cdk.sql.generic.SchemaContract"
        ):
            config = _endpoint_config(
                replication={"method": "incremental", "cursor_field": "updated_at"},
                database_pagination={"type": "offset", "order_by_field": "id"},
            )
            with pytest.raises(ReadError, match="conflicts with"):
                await _drain(connector, runtime, config, _checkpoint())
        runtime.close.assert_awaited()

    @pytest.mark.asyncio
    async def test_order_by_field_matching_cursor_allowed(self):
        # Declaring the cursor itself as the page ordering is redundant
        # but harmless — same order the cursor would impose.
        runtime = _FakeRuntime(is_adbc=True)
        checkpoint = _checkpoint(cursor={"cursor": "2024-01-02"})
        page = [pa.RecordBatch.from_pydict({"id": [3], "updated_at": ["2024-01-03"]})]
        reader = _RecordingReader([page, []])
        connector = GenericSQLConnector()
        with patch("cdk.sql.generic.materialize_runtime", new=AsyncMock()), patch(
            "cdk.sql.generic.open_adbc_reader", return_value=_adbc_cm(reader)
        ), patch("cdk.sql.generic.SchemaContract") as sc:
            sc.return_value.cast_arrow_batch.side_effect = lambda b: b
            config = _endpoint_config(
                replication={"method": "incremental", "cursor_field": "updated_at"},
                database_pagination={
                    "type": "offset",
                    "order_by_field": "updated_at",
                },
            )
            await _drain(connector, runtime, config, checkpoint)

        sql, params = reader.calls[0]
        assert 'ORDER BY "updated_at"' in sql
        assert '"updated_at" >= ?' in sql
        assert params == ["2024-01-02"]

    @pytest.mark.asyncio
    async def test_order_by_fallback_warns_once(self, caplog):
        from cdk.sql import generic as generic_mod

        generic_mod._order_by_fallback_logged.clear()
        runtime = _FakeRuntime(is_adbc=True)
        page = [pa.RecordBatch.from_pydict({"id": [1], "updated_at": ["2024-01-01"]})]
        reader = _RecordingReader([page, []])
        connector = GenericSQLConnector()
        with patch("cdk.sql.generic.materialize_runtime", new=AsyncMock()), patch(
            "cdk.sql.generic.open_adbc_reader", return_value=_adbc_cm(reader)
        ), patch("cdk.sql.generic.SchemaContract") as sc:
            sc.return_value.cast_arrow_batch.side_effect = lambda b: b
            with caplog.at_level("WARNING"):
                await _drain(connector, runtime, _endpoint_config(), _checkpoint())

        assert any("defaulting ORDER BY" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_order_by_fallback_dedupes_across_drains(self, caplog):
        from cdk.sql import generic as generic_mod

        generic_mod._order_by_fallback_logged.clear()
        connector = GenericSQLConnector()
        with patch("cdk.sql.generic.materialize_runtime", new=AsyncMock()), patch(
            "cdk.sql.generic.SchemaContract"
        ) as sc:
            sc.return_value.cast_arrow_batch.side_effect = lambda b: b
            with caplog.at_level("WARNING"):
                for _ in range(2):
                    runtime = _FakeRuntime(is_adbc=True)
                    page = [
                        pa.RecordBatch.from_pydict(
                            {"id": [1], "updated_at": ["2024-01-01"]}
                        )
                    ]
                    reader = _RecordingReader([page, []])
                    with patch(
                        "cdk.sql.generic.open_adbc_reader",
                        return_value=_adbc_cm(reader),
                    ):
                        await _drain(
                            connector, runtime, _endpoint_config(), _checkpoint()
                        )

        # Same (table, column) -> warned only on the first drain.
        warnings = [r for r in caplog.records if "defaulting ORDER BY" in r.message]
        assert len(warnings) == 1

    @pytest.mark.asyncio
    async def test_offset_advances_across_pages_with_fixed_where(self):
        # Two full pages then drain. The cursor is pinned at the read's initial
        # value; only OFFSET moves, so the WHERE params must be byte-for-byte
        # identical across pages.
        runtime = _FakeRuntime(is_adbc=True)
        checkpoint = _checkpoint(cursor={"cursor": "2024-01-01"})
        page1 = [
            pa.RecordBatch.from_pydict(
                {"id": [1, 2], "updated_at": ["2024-01-02", "2024-01-03"]}
            )
        ]
        page2 = [
            pa.RecordBatch.from_pydict(
                {"id": [3, 4], "updated_at": ["2024-01-04", "2024-01-05"]}
            )
        ]
        reader = _RecordingReader([page1, page2, []])
        connector = GenericSQLConnector()
        with patch("cdk.sql.generic.materialize_runtime", new=AsyncMock()), patch(
            "cdk.sql.generic.open_adbc_reader", return_value=_adbc_cm(reader)
        ), patch("cdk.sql.generic.SchemaContract") as sc:
            sc.return_value.cast_arrow_batch.side_effect = lambda b: b
            config = _endpoint_config(
                filters=[{"field": "status", "operator": "eq", "value": "active"}],
                replication={"method": "incremental", "cursor_field": "updated_at"},
            )
            await _drain(connector, runtime, config, checkpoint, batch_size=2)

        assert len(reader.calls) == 3
        sql0, params0 = reader.calls[0]
        sql1, params1 = reader.calls[1]
        assert "OFFSET 0" in sql0
        assert "OFFSET 2" in sql1
        # Same WHERE predicate and identical params on every page.
        assert params0 == params1 == ["active", "2024-01-01"]

    @pytest.mark.asyncio
    async def test_named_params_rejected_on_adbc_path(self):
        # If the dialect ignores the forced qmark paramstyle and yields a dict,
        # the ADBC execute path would bind parameter names instead of values.
        # The connector must fail loudly before that happens.
        runtime = _FakeRuntime(is_adbc=True)
        connector = GenericSQLConnector()
        with patch("cdk.sql.generic.materialize_runtime", new=AsyncMock()), patch(
            "cdk.sql.generic.open_adbc_reader",
            return_value=_adbc_cm(_RecordingReader([])),
        ), patch("cdk.sql.generic.SchemaContract") as sc, patch(
            "cdk.sql.generic.QueryBuilder"
        ) as qb:
            sc.return_value.cast_arrow_batch.side_effect = lambda b: b
            qb.return_value.build_select_query.return_value = (
                "SELECT 1",
                {"status_1": "active"},
            )
            with pytest.raises(ReadError, match="positional qmark parameters"):
                await _drain(connector, runtime, _endpoint_config(), _checkpoint())


class TestReadSqlAlchemyBranch:
    @pytest.mark.asyncio
    async def test_full_refresh_pages_via_acquire_connection(self):
        runtime = _FakeRuntime(is_adbc=False, engine=object())
        checkpoint = _checkpoint(cursor=None)

        class _Row:
            def __init__(self, mapping):
                self._mapping = mapping

        class _FakeConn:
            def __init__(self):
                self.calls = 0

            def exec_driver_sql(self, sql, params=None):
                self.calls += 1
                if self.calls == 1:
                    return [_Row({"id": 1, "updated_at": "2024-01-01"})]
                return []

            async def run_sync(self, fn, *args):
                # AsyncConnection.run_sync hands the sync Connection to fn;
                # this fake plays both roles.
                return fn(self, *args)

        conn = _FakeConn()

        class _AcquireCM:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *exc):
                return False

        connector = GenericSQLConnector()
        with patch("cdk.sql.generic.materialize_runtime", new=AsyncMock()), patch(
            "cdk.sql.generic.acquire_connection", return_value=_AcquireCM()
        ), patch("cdk.sql.generic.SchemaContract") as sc:
            sc.return_value.from_pylist.side_effect = lambda rows: rows
            out = await _drain(
                connector, runtime, _endpoint_config(), checkpoint, batch_size=2
            )

        # One page of one row -> one yielded batch; short page ends the loop.
        assert out == [[{"id": 1, "updated_at": "2024-01-01"}]]
        runtime.close.assert_awaited()

    @pytest.mark.asyncio
    async def test_declared_order_by_field_lands_in_paged_select(self):
        # source.database_pagination.order_by_field flows through
        # QueryConfig.order_by on the SQLAlchemy paging path.
        runtime = _FakeRuntime(is_adbc=False, engine=object())

        executed = []

        class _RecordingConn:
            def exec_driver_sql(self, sql, params=None):
                executed.append(sql)
                return []

            async def run_sync(self, fn, *args):
                return fn(self, *args)

        class _AcquireCM:
            async def __aenter__(self):
                return _RecordingConn()

            async def __aexit__(self, *exc):
                return False

        connector = GenericSQLConnector()
        with patch("cdk.sql.generic.materialize_runtime", new=AsyncMock()), patch(
            "cdk.sql.generic.acquire_connection", return_value=_AcquireCM()
        ), patch("cdk.sql.generic.SchemaContract") as sc:
            sc.return_value.from_pylist.side_effect = lambda rows: rows
            config = _endpoint_config(
                database_pagination={"type": "offset", "order_by_field": "updated_at"},
            )
            await _drain(connector, runtime, config, _checkpoint(cursor=None))

        assert "ORDER BY updated_at" in executed[0]

    @pytest.mark.asyncio
    async def test_incremental_cursor_uses_inclusive_bound(self):
        # The inclusive (>=) resume bound must hold on the SQLAlchemy paging
        # path too, not only ADBC: an exclusive > would filter out a late row
        # sharing the committed boundary value and lose it. The re-read is
        # deduped by upsert.
        runtime = _FakeRuntime(is_adbc=False, engine=object())

        executed = []

        class _RecordingConn:
            def exec_driver_sql(self, sql, params=None):
                executed.append(sql)
                return []

            async def run_sync(self, fn, *args):
                return fn(self, *args)

        class _AcquireCM:
            async def __aenter__(self):
                return _RecordingConn()

            async def __aexit__(self, *exc):
                return False

        connector = GenericSQLConnector()
        with patch("cdk.sql.generic.materialize_runtime", new=AsyncMock()), patch(
            "cdk.sql.generic.acquire_connection", return_value=_AcquireCM()
        ), patch("cdk.sql.generic.SchemaContract") as sc:
            sc.return_value.from_pylist.side_effect = lambda rows: rows
            config = _endpoint_config(
                replication={"method": "incremental", "cursor_field": "updated_at"},
            )
            await _drain(
                connector, runtime, config, _checkpoint(cursor={"cursor": "2024-01-02"})
            )

        assert "updated_at >=" in executed[0]

    @pytest.mark.asyncio
    async def test_query_builder_receives_dialect_paging_order_fallback(self):
        # The SQLAlchemy paging path hands the connector dialect's
        # paging_order_fallback hook to QueryBuilder, so per-system
        # OFFSET-without-ORDER-BY behavior comes from the connector
        # package, never from a dialect branch in shared code.
        runtime = _FakeRuntime(is_adbc=False, engine=object())

        class _EmptyConn:
            def exec_driver_sql(self, sql, params=None):
                return []

            async def run_sync(self, fn, *args):
                return fn(self, *args)

        class _AcquireCM:
            async def __aenter__(self):
                return _EmptyConn()

            async def __aexit__(self, *exc):
                return False

        connector = GenericSQLConnector()
        with patch("cdk.sql.generic.materialize_runtime", new=AsyncMock()), patch(
            "cdk.sql.generic.acquire_connection", return_value=_AcquireCM()
        ), patch("cdk.sql.generic.SchemaContract"), patch(
            "cdk.sql.generic.QueryBuilder"
        ) as qb:
            qb.return_value.build_select_query.return_value = ("SELECT 1", [])
            await _drain(
                connector, runtime, _endpoint_config(), _checkpoint(cursor=None)
            )

        assert (
            qb.call_args.kwargs["paging_order_fallback"]
            == connector.dialect.paging_order_fallback
        )


class TestControlPlaneDelegators:
    @pytest.mark.asyncio
    async def test_discovery_and_create_table_delegate(self):
        connector = GenericSQLConnector()
        runtime = _FakeRuntime(is_adbc=True)
        with patch(
            "cdk.sql.generic._sql_list_schemas", new=AsyncMock(return_value=["s"])
        ) as ls, patch(
            "cdk.sql.generic._sql_list_tables", new=AsyncMock(return_value=["t"])
        ) as lt, patch(
            "cdk.sql.generic._sql_list_columns", new=AsyncMock(return_value=([], []))
        ) as lc, patch(
            "cdk.sql.generic._sql_create_table", new=AsyncMock()
        ) as ct:
            assert await connector.list_schemas(runtime) == ["s"]
            assert await connector.list_tables(runtime, "public") == ["t"]
            assert await connector.list_columns(runtime, "public", "orders") == ([], [])
            await connector.create_table(runtime, "public", "orders", [], [])

        # Each delegator forwards the connector instance's own dialect object
        # as the required keyword (identity, not ANY) and the catalog scope
        # symmetrically (empty = session catalog).
        dialect = connector.dialect
        ls.assert_awaited_once_with(runtime, dialect=dialect, catalog="")
        lt.assert_awaited_once_with(runtime, "public", dialect=dialect, catalog="")
        lc.assert_awaited_once_with(
            runtime, "public", "orders", dialect=dialect, catalog=""
        )
        ct.assert_awaited_once_with(
            runtime, "public", "orders", [], [], dialect=dialect, catalog=""
        )
