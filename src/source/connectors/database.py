"""Database source connector using shared SQLAlchemy engine factory."""

import logging
from typing import Any, AsyncIterator, Dict, List, Optional

from .base import BaseConnector, ConnectionError, ReadError, WriteError
from ...shared.connection_runtime import ConnectionRuntime
from ...shared.database_utils import (
    acquire_connection,
    convert_record_from_db,
)
from ...shared.query_builder import build_select_query

logger = logging.getLogger(__name__)


class DatabaseConnector(BaseConnector):
    """
    Database source connector using shared SQLAlchemy engine.

    Features:
    - Shared engine factory with SSL-prefer fallback
    - Incremental reading with cursor support
    - Read-only: write operations are handled by the destination handler
    """

    def __init__(self, name: str = "DatabaseConnector"):
        super().__init__(name)
        self._runtime: ConnectionRuntime | None = None
        self._engine = None
        self._driver: str = ""
        self.table_info_cache = {}
        self._initialized = False

    def _parse_endpoint(self, endpoint: str, default_schema: str = "public") -> tuple[str, str]:
        """
        Parse endpoint string to extract schema and table name.

        Supports format: "schema/table" or just "table"

        Args:
            endpoint: Endpoint string (e.g., "public/wise_transfers" or "wise_transfers")
            default_schema: Schema to use if not specified in endpoint

        Returns:
            Tuple of (schema_name, table_name)
        """
        if endpoint and "/" in endpoint:
            parts = endpoint.split("/", 1)
            schema_name = parts[0] or default_schema
            table_name = parts[1]
        else:
            schema_name = default_schema
            table_name = endpoint
        return schema_name, table_name

    async def connect(self, runtime: ConnectionRuntime):
        """
        Establish connection to the database using ConnectionRuntime.

        Args:
            runtime: ConnectionRuntime with enriched config
        """
        try:
            self._runtime = runtime
            await runtime.materialize(require_port=True)
            self._engine = runtime.engine
            self._driver = runtime.driver or ""
            self.is_connected = True
            self._initialized = True
            logger.info("Connected to database via %s", self._driver)
        except Exception as e:
            logger.error("Failed to connect to database: %s", e)
            raise ConnectionError(f"Database connection failed: {e}")

    async def disconnect(self):
        """Close database connection."""
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
        state_manager: "StateManager",
        stream_name: str,
        partition: Optional[Dict[str, Any]] = None,
        batch_size: int = 1000
    ) -> AsyncIterator[List[Dict[str, Any]]]:
        """
        Read data in batches from database table with state management for incremental replication.

        Args:
            config: Read configuration
            state_manager: State manager for incremental replication
            stream_name: Name of the stream for state tracking
            partition: Optional partition identifier for sharded streams
            batch_size: Number of records per batch

        Yields:
            Batches of records as dictionaries
        """
        if not self._initialized:
            raise RuntimeError("Database connection not initialized. Call connect() first.")

        try:
            partition = partition or {}

            # Parse endpoint to extract schema and table name
            schema_name, table_name = self._parse_endpoint(
                config["endpoint"], config.get("schema", "public")
            )

            # Get current cursor from state manager for incremental reads
            cursor_state = await state_manager.get_cursor(stream_name, partition)
            cursor_value = cursor_state.get("cursor") if cursor_state else None

            logger.debug("Database read starting with cursor: %s", cursor_value)

            # Build query using shared query builder
            query, params = build_select_query(
                dialect=self._driver,
                schema_name=schema_name,
                table_name=table_name,
                config=config,
                cursor_value=cursor_value,
            )

            # Execute query with batching
            async with acquire_connection(self._engine) as conn:
                offset = 0
                last_cursor_value = cursor_value

                while True:
                    # Add pagination to query
                    batch_query = f"{query} LIMIT {batch_size} OFFSET {offset}"

                    # Execute through driver connection
                    if params:
                        result = await conn.exec_driver_sql(batch_query, tuple(params))
                    else:
                        result = await conn.exec_driver_sql(batch_query)

                    # Convert rows to dicts with type conversion
                    rows = [
                        convert_record_from_db(dict(row._mapping))
                        for row in result
                    ]

                    if not rows:
                        break

                    # Update cursor from the last record if replication_key exists
                    replication_key = config.get("replication_key")
                    if replication_key and rows:
                        last_cursor_value = rows[-1].get(replication_key)

                    self.metrics["records_read"] += len(rows)
                    self.metrics["batches_read"] += 1

                    yield rows

                    # Save cursor state after each batch
                    if last_cursor_value is not None:
                        await state_manager.save_cursor(
                            stream_name,
                            partition,
                            {"cursor": last_cursor_value}
                        )

                    offset += batch_size

                    # If we got less than batch_size, we're done
                    if len(rows) < batch_size:
                        break

            logger.debug("Database read completed with final cursor: %s", last_cursor_value)

        except Exception as e:
            self.metrics["errors"] += 1
            logger.error("Database read failed: %s", e)
            raise ReadError(f"Database read failed: {e}")

    async def write_batch(self, batch: List[Dict[str, Any]], config: Dict[str, Any]):
        """Source connector is read-only; writes are handled by the destination."""
        raise NotImplementedError("Source connector is read-only")

    def supports_incremental_read(self) -> bool:
        """Database supports incremental reading."""
        return True
