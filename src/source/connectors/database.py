"""Contract-native database source connector.

Reads directly from the published ``database-endpoint`` contract:

* ``database_object.{catalog, schema, name}`` — target table.
* ``columns`` — column list (subset-able via ``stream_source.selected_columns``).
* ``primary_keys`` — surfaced for the destination but not used here.

Stream-level overrides come from the contract source block:

* ``filters[]`` — ``{field, operator, value}`` clauses.
* ``replication.{method, cursor_field, safety_window_seconds,
  tie_breaker_fields}`` — incremental cursor configuration.

The query builder consumes typed :class:`Filter` / :class:`QueryConfig`
inputs.
"""

from __future__ import annotations

import logging
from typing import Any, AsyncIterator, Dict, List, Optional

import pyarrow as pa

from .base import BaseConnector, ConnectionError, ReadError
from ..drivers.adbc_reader import (
    AdbcReadPlan,
    open_adbc_reader,
)
from ...destination.schema_contract import SchemaContract
from ...engine.type_map import InvalidTypeMapError, UnmappedTypeError
from ...secrets.exceptions import PlaceholderExpansionError
from ...shared.connection_runtime import ConnectionRuntime
from ...shared.database_utils import acquire_connection
from ...shared.query_builder import Filter, QueryBuilder, QueryConfig
from ...state.state_manager import StateManager

logger = logging.getLogger(__name__)


class DatabaseConnector(BaseConnector):
    """Source-side database connector consuming the contract endpoint."""

    def __init__(self, name: str = "DatabaseConnector"):
        super().__init__(name)
        self._runtime: ConnectionRuntime | None = None
        self._engine = None
        self._driver: str = ""
        self._initialized = False
        # ADBC-only mode mirrors the destination handler: set in
        # ``connect()`` from ``runtime.is_adbc``. When True, ``_engine``
        # stays None and reads go through ``open_adbc_reader``.
        self._adbc_only: bool = False

    async def connect(self, runtime: ConnectionRuntime):
        self._runtime = runtime
        runtime.acquire()
        try:
            await runtime.materialize(require_port=True)
        except (
            InvalidTypeMapError,
            UnmappedTypeError,
            PlaceholderExpansionError,
            ValueError,
        ):
            # Deterministic configuration / secret errors propagate with
            # their real type so callers distinguish "your type-map is
            # missing a rule" from "the DB is unreachable".
            raise
        except Exception as e:
            logger.error("Failed to connect to database: %s", e)
            raise ConnectionError(f"Database connection failed: {e}") from e
        self._driver = runtime.driver or ""
        # Reset prior-connection state so a reused connector instance
        # doesn't carry an old mode forward.
        self._adbc_only = False
        self._engine = None
        if runtime.is_adbc:
            self._adbc_only = True
        else:
            self._engine = runtime.engine
        self.is_connected = True
        self._initialized = True
        logger.info(
            "Connected to database via %s (%s)",
            self._driver,
            "ADBC" if self._adbc_only else "SQLAlchemy",
        )

    async def disconnect(self):
        if self._runtime:
            await self._runtime.close()
        self._engine = None
        self._adbc_only = False
        self.is_connected = False
        self._initialized = False
        logger.info("Database connection closed")

    async def read_batches(
        self,
        config: Dict[str, Any],
        *,
        state_manager: StateManager,
        stream_name: str,
        partition: Optional[Dict[str, Any]] = None,
        batch_size: int = 1000,
    ) -> AsyncIterator[pa.RecordBatch]:
        """Read upstream rows as Arrow batches typed via the endpoint contract."""
        if not self._initialized:
            raise ReadError(
                "DatabaseConnector.read_batches() called before connect()"
            )
        if not self._adbc_only and self._engine is None:
            raise ReadError(
                "DatabaseConnector.read_batches() called before connect()"
            )

        endpoint_doc = config.get("endpoint_document")
        if not endpoint_doc:
            raise ReadError(
                "DatabaseConnector: source config missing 'endpoint_document'"
            )
        database_object = endpoint_doc.get("database_object") or {}
        table_name = database_object.get("name")
        if not table_name:
            raise ReadError(
                "endpoint document missing database_object.name"
            )
        # No default schema: dialects without a schema concept
        # (sqlite, duckdb) would emit invalid ``public.<table>``
        # references if we forced one. When the endpoint omits
        # ``schema``, QueryBuilder emits an unqualified table name
        # and the driver uses the connection's current
        # schema/database -- which is what every dialect expects.
        schema_name = database_object.get("schema")

        if self._runtime is None:
            raise ReadError(
                "DatabaseConnector.read_batches() called before connect()"
            )
        stream_source = config.get("stream_source") or {}
        schema_contract = SchemaContract(endpoint_doc)
        column_names = self._select_columns(endpoint_doc, stream_source)
        filters = self._build_filters(stream_source.get("filters") or [])

        replication = stream_source.get("replication") or {}
        cursor_field = replication.get("cursor_field")
        if isinstance(cursor_field, list):
            cursor_field = cursor_field[0] if cursor_field else None
        replication_method = replication.get("method", "full_refresh")

        if (
            replication_method == "incremental"
            and cursor_field
            and cursor_field not in column_names
        ):
            # An incremental stream whose projection drops the cursor
            # column will silently revert to "full-scan + upsert" every
            # run: no cursor value is observable, so no state advances.
            # Loud and once.
            logger.warning(
                "stream %r: cursor_field %r not in selected columns %r; "
                "cursor will not advance",
                stream_name,
                cursor_field,
                column_names,
            )

        partition = partition or {}
        cursor_state = await state_manager.get_cursor(stream_name, partition)
        stored_cursor = cursor_state.get("cursor") if cursor_state else None
        cursor_value = stored_cursor if replication_method == "incremental" else None

        if self._adbc_only:
            # Stream-level filters require dialect-aware operator
            # rendering that the minimal ADBC SQL builder does not
            # implement today; reject loudly rather than silently
            # dropping the filter clauses.
            if filters:
                raise ReadError(
                    "ADBC-only source does not support stream-level filters yet; "
                    "remove filters[] or pick transport_type='sqlalchemy'"
                )
            async for batch in self._read_via_adbc_only(
                schema_contract=schema_contract,
                schema_name=schema_name,
                table_name=table_name,
                columns=column_names,
                cursor_field=(
                    cursor_field if replication_method == "incremental" else None
                ),
                cursor_value=cursor_value,
                batch_size=batch_size,
                state_manager=state_manager,
                stream_name=stream_name,
                partition=partition,
            ):
                yield batch
            logger.debug("Database read (ADBC-only) completed")
            return

        builder = QueryBuilder(self._driver)

        def page_query(offset: int):
            """Build the per-page SELECT. Limit / offset are pushed into
            ``QueryConfig`` so SQLAlchemy compiles dialect-correct paging
            (PostgreSQL/MySQL ``LIMIT ... OFFSET ...`` vs MSSQL ``OFFSET
            ... ROWS FETCH NEXT ... ROWS ONLY``). ``params`` is a list
            for positional dialects and a dict for named ones.
            """
            sql, params = builder.build_select_query(
                QueryConfig(
                    schema_name=schema_name,
                    table_name=table_name,
                    columns=column_names,
                    filters=filters,
                    cursor_field=(
                        cursor_field if replication_method == "incremental" else None
                    ),
                    cursor_value=cursor_value,
                    cursor_mode="inclusive",
                    limit=batch_size,
                    offset=offset,
                )
            )
            return sql, params

        last_cursor_value = cursor_value
        offset = 0

        async with acquire_connection(self._engine) as conn:
            while True:
                paged_query, paged_params = page_query(offset)
                if isinstance(paged_params, dict):
                    # Named-paramstyle dialects (Snowflake pyformat,
                    # BigQuery named): the driver binds by name and
                    # expects a dict, not a positional tuple.
                    if paged_params:
                        result = await conn.exec_driver_sql(paged_query, paged_params)
                    else:
                        result = await conn.exec_driver_sql(paged_query)
                elif paged_params:
                    result = await conn.exec_driver_sql(paged_query, tuple(paged_params))
                else:
                    result = await conn.exec_driver_sql(paged_query)

                rows = [dict(row._mapping) for row in result]
                if not rows:
                    break

                if cursor_field:
                    last_cursor_value = rows[-1].get(cursor_field, last_cursor_value)

                self.metrics["records_read"] += len(rows)
                self.metrics["batches_read"] += 1

                yield schema_contract.from_pylist(rows)

                if last_cursor_value is not None:
                    await state_manager.save_cursor(
                        stream_name,
                        partition,
                        {"cursor": last_cursor_value},
                    )

                offset += batch_size
                if len(rows) < batch_size:
                    break

        logger.debug("Database read completed with cursor: %s", last_cursor_value)

    async def _read_via_adbc_only(
        self,
        *,
        schema_contract: SchemaContract,
        schema_name: Optional[str],
        table_name: str,
        columns: List[str],
        cursor_field: Optional[str],
        cursor_value: Any,
        batch_size: int,
        state_manager: StateManager,
        stream_name: str,
        partition: Dict[str, Any],
    ) -> AsyncIterator[pa.RecordBatch]:
        """Stream Arrow batches via the ADBC-only path.

        Holds one DBAPI connection for the lifetime of the read; each
        page goes ``cursor.execute -> fetch_arrow_table -> cast``. The
        WHERE clause is fixed at the read's initial cursor_value
        (matching the SA path); paging
        advances via OFFSET only. Mixing cursor advancement with OFFSET
        would skip rows on every page after the first (the cursor moves
        right while OFFSET continues to skip rows the cursor would have
        included).
        """
        if self._runtime is None:
            raise ReadError(
                "DatabaseConnector._read_via_adbc_only requires a materialized runtime"
            )
        # Initial cursor value is fixed for the duration of the read;
        # last_cursor_value advances purely for checkpoint state.
        initial_cursor_value = cursor_value
        last_cursor_value: Any = cursor_value
        offset = 0
        cursor_missing_warned = False
        async with open_adbc_reader(self._driver, self._runtime) as reader:
            while True:
                plan = AdbcReadPlan(
                    table_name=table_name,
                    columns=tuple(columns),
                    schema_name=schema_name,
                    cursor_field=cursor_field,
                    cursor_value=initial_cursor_value if cursor_field else None,
                    cursor_mode="inclusive",
                    limit=batch_size,
                    offset=offset,
                )
                batches = await reader.fetch_page(plan)
                if not batches:
                    break

                page_rows = 0
                for batch in batches:
                    cast_batch = schema_contract.cast_arrow_batch(batch)
                    page_rows += cast_batch.num_rows
                    if cursor_field and cast_batch.num_rows > 0:
                        if cursor_field in cast_batch.schema.names:
                            last_cursor_value = cast_batch.column(cursor_field)[-1].as_py()
                        elif not cursor_missing_warned:
                            logger.warning(
                                "stream %r: cursor_field %r not present in "
                                "result batch; cursor will not advance",
                                stream_name,
                                cursor_field,
                            )
                            cursor_missing_warned = True
                    self.metrics["records_read"] += cast_batch.num_rows
                    self.metrics["batches_read"] += 1
                    yield cast_batch

                if last_cursor_value is not None:
                    await state_manager.save_cursor(
                        stream_name,
                        partition,
                        {"cursor": last_cursor_value},
                    )

                if page_rows < batch_size:
                    break
                offset += page_rows

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _select_columns(
        endpoint_doc: Dict[str, Any], stream_source: Dict[str, Any]
    ) -> List[str]:
        selected = stream_source.get("selected_columns")
        if selected:
            return list(selected)
        columns = endpoint_doc.get("columns") or []
        return [c["name"] for c in columns if c.get("name")]

    @staticmethod
    def _build_filters(stream_filters: List[Dict[str, Any]]) -> List[Filter]:
        out: List[Filter] = []
        for f in stream_filters:
            field = f.get("field")
            if not field:
                continue
            out.append(
                Filter(field=field, op=f.get("operator", "eq"), value=f.get("value"))
            )
        return out

    # ------------------------------------------------------------------
    # Base interface stubs
    # ------------------------------------------------------------------

    async def write_batch(self, batch: List[Dict[str, Any]], config: Dict[str, Any]):
        raise NotImplementedError("Source connector is read-only")

    def supports_incremental_read(self) -> bool:
        return True
