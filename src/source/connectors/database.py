"""Database source connector.

Reads from a SQLAlchemy-supported database driven by the typed
:class:`ResolvedSource`. The dialect-specific engine is built by
:class:`ConnectionRuntime` from the connector's transport spec; this
connector only consumes the resolved values.
"""

from __future__ import annotations

import logging
from typing import Any, AsyncIterator, Dict, List, Optional

from .base import BaseConnector, ConnectionError, ReadError
from ...engine.resolved import DatabaseReadEndpoint, ResolvedSource
from ...shared.connection_runtime import ConnectionRuntime
from ...shared.database_utils import acquire_connection, convert_record_from_db
from ...shared.query_builder import build_select_query
from ...state.state_manager import StateManager

logger = logging.getLogger(__name__)


class DatabaseConnector(BaseConnector):
    """Database source connector."""

    def __init__(self, name: str = "DatabaseConnector"):
        super().__init__(name)
        self._runtime: ConnectionRuntime | None = None
        self._engine = None
        self._driver: str = ""
        self._initialized = False

    async def connect(self, runtime: ConnectionRuntime) -> None:
        try:
            self._runtime = runtime
            runtime.acquire()
            await runtime.materialize(require_port=True)
            self._engine = runtime.engine
            self._driver = runtime.driver or ""
            self.is_connected = True
            self._initialized = True
            logger.info("Connected to database via %s", self._driver)
        except Exception as e:
            logger.error("Failed to connect to database: %s", e)
            raise ConnectionError(f"Database connection failed: {e}") from e

    async def disconnect(self) -> None:
        if self._runtime:
            await self._runtime.close()
        self._engine = None
        self.is_connected = False
        self._initialized = False
        logger.info("Database connection closed")

    async def read_batches(
        self,
        source: ResolvedSource,
        *,
        state_manager: StateManager,
        stream_id: str,
        partition: Optional[Dict[str, Any]] = None,
        batch_size: int = 1000,
    ) -> AsyncIterator[List[Dict[str, Any]]]:
        if not self._initialized:
            raise RuntimeError("Database connection not initialized. Call connect() first.")

        endpoint = source.endpoint
        if not isinstance(endpoint, DatabaseReadEndpoint):
            raise ReadError(
                f"DatabaseConnector requires a DatabaseReadEndpoint, got {type(endpoint).__name__}"
            )

        partition = partition or {}
        schema_name = endpoint.database_object.schema
        table_name = endpoint.database_object.name

        try:
            cursor_state = await state_manager.get_cursor(stream_id, partition)
            cursor_value = cursor_state.get("cursor") if cursor_state else None
            cursor_field = source.replication.cursor_field

            query_config: Dict[str, Any] = {
                "columns": [c.name for c in endpoint.columns] or ["*"],
                "filters": [
                    {"field": f.field, "op": f.op, "value": f.value}
                    for f in source.filters
                ],
                "cursor_field": cursor_field,
                "cursor_mode": "inclusive",
                "order_by": cursor_field,
                "order_direction": "asc",
            }

            query, params = build_select_query(
                dialect=self._driver,
                schema_name=schema_name,
                table_name=table_name,
                config=query_config,
                cursor_value=cursor_value,
            )

            async with acquire_connection(self._engine) as conn:
                offset = 0
                last_cursor_value = cursor_value

                while True:
                    batch_query = f"{query} LIMIT {batch_size} OFFSET {offset}"
                    if params:
                        result = await conn.exec_driver_sql(batch_query, tuple(params))
                    else:
                        result = await conn.exec_driver_sql(batch_query)

                    rows = [convert_record_from_db(dict(row._mapping)) for row in result]
                    if not rows:
                        break

                    if cursor_field and rows:
                        last_cursor_value = rows[-1].get(cursor_field)

                    self.metrics["records_read"] += len(rows)
                    self.metrics["batches_read"] += 1

                    yield rows

                    if last_cursor_value is not None:
                        await state_manager.save_cursor(
                            stream_id, partition, {"cursor": last_cursor_value}
                        )

                    offset += batch_size
                    if len(rows) < batch_size:
                        break

            logger.debug("Database read completed with final cursor: %s", last_cursor_value)

        except Exception as e:
            self.metrics["errors"] += 1
            logger.error("Database read failed: %s", e)
            raise ReadError(f"Database read failed: {e}") from e

    def supports_incremental_read(self) -> bool:
        return True
