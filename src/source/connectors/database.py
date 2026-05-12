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
        self._engine = runtime.engine
        self._driver = runtime.driver or ""
        self.is_connected = True
        self._initialized = True
        logger.info("Connected to database via %s", self._driver)

    async def disconnect(self):
        if self._runtime:
            await self._runtime.close()
        self._engine = None
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
        if not self._initialized or self._engine is None:
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
        schema_name = database_object.get("schema") or "public"

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

        partition = partition or {}
        cursor_state = await state_manager.get_cursor(stream_name, partition)
        stored_cursor = cursor_state.get("cursor") if cursor_state else None
        cursor_value = stored_cursor if replication_method == "incremental" else None

        builder = QueryBuilder(self._driver)
        base_query, base_params = builder.build_select_query(
            QueryConfig(
                schema_name=schema_name,
                table_name=table_name,
                columns=column_names,
                filters=filters,
                cursor_field=cursor_field if replication_method == "incremental" else None,
                cursor_value=cursor_value,
                cursor_mode="inclusive",
            )
        )

        last_cursor_value = cursor_value
        offset = 0
        async with acquire_connection(self._engine) as conn:
            while True:
                paged_query = f"{base_query} LIMIT {batch_size} OFFSET {offset}"
                if base_params:
                    result = await conn.exec_driver_sql(paged_query, tuple(base_params))
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
