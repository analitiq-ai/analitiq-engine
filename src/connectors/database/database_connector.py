"""Generic database connector using driver delegation pattern."""

import logging
from typing import Any, AsyncIterator, Dict, List, Optional

from pydantic import BaseModel, Field, ConfigDict

from ..base import BaseConnector, ConnectionError, ReadError, WriteError
from .driver_factory import DriverFactory

logger = logging.getLogger(__name__)


class DatabaseConfig(BaseModel):
    """Pydantic model for database configuration validation."""
    model_config = ConfigDict(extra='allow')

    driver: str = Field(..., description="Database driver (postgresql, mysql, etc.)")
    host: str = Field(..., description="Database host")
    port: int = Field(..., description="Database port")
    database: str = Field(..., description="Database name")
    username: str = Field(..., description="Database username")
    password: str = Field(..., description="Database password")


class ConfigureConfig(BaseModel):
    """Pydantic model for database configuration settings."""
    model_config = ConfigDict(extra='forbid')
    
    auto_create_schema: bool = Field(False, description="Auto-create schema if not exists")
    auto_create_table: bool = Field(False, description="Auto-create table if not exists")
    auto_create_indexes: List[Dict[str, Any]] = Field(default_factory=list, description="Index definitions to create")


class EndpointConfig(BaseModel):
    """Pydantic model for database endpoint configuration validation."""
    model_config = ConfigDict(extra='allow')
    
    schema: str = Field("public", description="Database schema name")
    table: str = Field(..., description="Database table name")
    primary_key: List[str] = Field(default_factory=list, description="Primary key columns")
    unique_constraints: List[str] = Field(default_factory=list, description="Unique constraint columns")
    table_schema: Dict[str, Any] = Field(default_factory=dict, description="Table schema definition")
    write_mode: str = Field("insert", description="Write mode (insert, upsert)")
    conflict_resolution: Dict[str, Any] = Field(default_factory=dict, description="Conflict resolution config")
    configure: Optional[ConfigureConfig] = Field(None, description="Auto-configuration settings")


class DatabaseConnector(BaseConnector):
    """
    Generic database connector using driver delegation pattern.

    Features:
    - Driver-agnostic interface with database-specific implementations
    - Auto-creation of schemas, tables, and indexes
    - Pydantic V2 validation for configuration
    - Connection pooling through driver delegation
    - Incremental reading with cursor support
    - Upsert operations with conflict resolution
    """

    def __init__(self, name: str = "DatabaseConnector"):
        super().__init__(name)
        self.driver = None
        self.table_info_cache = {}
        self._initialized = False

    async def connect(self, config: Dict[str, Any]):
        """
        Establish connection to the database using driver delegation.

        Args:
            config: Connection configuration with driver and credentials
        """
        try:
            # Validate configuration with Pydantic
            db_config = DatabaseConfig(**config)
            
            # Create driver-specific instance
            self.driver = DriverFactory.create_driver(db_config.driver)
            
            # Create connection pool through driver
            await self.driver.create_connection_pool(config)
            
            self.is_connected = True
            self._initialized = True
            logger.info(f"Connected to database {db_config.database} on {db_config.host}.")

        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            raise ConnectionError(f"Database connection failed: {str(e)}")

    async def disconnect(self):
        """Close database connection."""
        if self.driver:
            await self.driver.close_connection_pool()
            self.is_connected = False
            self._initialized = False
            logger.info("Database connection closed")

    async def configure(self, config: Dict[str, Any]):
        """
        Configure database objects (schema, tables, indexes) based on endpoint configuration.
        
        Args:
            config: Endpoint configuration containing table schema and auto-creation flags
        """
        if not self._initialized:
            raise RuntimeError("Database connection not initialized. Call connect() first.")
            
        try:
            # Validate endpoint configuration with Pydantic
            endpoint_config = EndpointConfig(**config)
            
            # Skip configuration if configure section is not provided
            if not endpoint_config.configure:
                logger.info(f"No configure section found - assuming schema and table already exist for {endpoint_config.schema}.{endpoint_config.table}")
                return
            
            configure_config = endpoint_config.configure
            
            # Create schema if requested
            if configure_config.auto_create_schema and endpoint_config.schema:
                await self.driver.create_schema_if_not_exists(endpoint_config.schema)
            
            # Create table if requested
            if configure_config.auto_create_table:
                await self.driver.create_table_if_not_exists(
                    endpoint_config.schema,
                    endpoint_config.table,
                    endpoint_config.table_schema,
                    endpoint_config.primary_key,
                    endpoint_config.unique_constraints
                )
                
            # Create indexes if specified
            if configure_config.auto_create_indexes:
                await self.driver.create_indexes_if_not_exist(
                    endpoint_config.schema,
                    endpoint_config.table,
                    configure_config.auto_create_indexes
                )
            
            logger.info(f"Database configuration completed for {endpoint_config.schema}.{endpoint_config.table}")
            
        except Exception as e:
            logger.error(f"Database configuration failed: {str(e)}")
            raise ConnectionError(f"Database configuration failed: {str(e)}")

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
            endpoint_config = EndpointConfig(**config)
            partition = partition or {}

            # Get current cursor from state manager for incremental reads
            cursor_state = await state_manager.get_cursor(stream_name, partition)
            cursor_value = cursor_state.get("cursor") if cursor_state else None

            logger.debug(f"Database read starting with cursor: {cursor_value}")

            # Build incremental query through driver with cursor
            query, params = self.driver.build_incremental_query(
                endpoint_config.schema,
                endpoint_config.table,
                config,
                cursor_value=cursor_value
            )

            # Execute query with batching
            async with self.driver.connection_pool.acquire() as conn:
                offset = 0
                last_cursor_value = cursor_value

                while True:
                    # Add pagination to query
                    batch_query = f"{query} LIMIT {batch_size} OFFSET {offset}"

                    # Execute through driver
                    rows = await self.driver.execute_query(conn, batch_query, params)

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

            logger.debug(f"Database read completed with final cursor: {last_cursor_value}")

        except Exception as e:
            self.metrics["errors"] += 1
            logger.error(f"Database read failed: {str(e)}")
            raise ReadError(f"Database read failed: {str(e)}")

    async def write_batch(self, batch: List[Dict[str, Any]], config: Dict[str, Any]):
        """
        Write a batch of records to database table using driver delegation.

        Args:
            batch: List of records to write
            config: Write configuration
        """
        if not self._initialized:
            raise RuntimeError("Database connection not initialized. Call connect() first.")
            
        try:
            if not batch:
                return
                
            endpoint_config = EndpointConfig(**config)
            
            async with self.driver.connection_pool.acquire() as conn:
                if endpoint_config.write_mode == "upsert":
                    await self.driver.execute_upsert(
                        conn,
                        endpoint_config.schema,
                        endpoint_config.table,
                        batch,
                        endpoint_config.conflict_resolution
                    )
                else:  # insert
                    await self.driver.execute_insert(
                        conn,
                        endpoint_config.schema,
                        endpoint_config.table,
                        batch
                    )

            self.metrics["records_written"] += len(batch)
            self.metrics["batches_written"] += 1

        except Exception as e:
            self.metrics["errors"] += 1
            logger.error(f"Database write failed: {str(e)}")
            raise WriteError(f"Database write failed: {str(e)}")

    def supports_incremental_read(self) -> bool:
        """Database supports incremental reading."""
        return True

    def supports_upsert(self) -> bool:
        """Database supports upsert operations."""
        return True

    def supports_schema_evolution(self) -> bool:
        """Database supports schema evolution."""
        return True

    async def evolve_schema(self, changes: Dict[str, Any], config: Dict[str, Any]):
        """
        Evolve database schema based on detected changes using driver delegation.

        Args:
            changes: Schema changes from SchemaManager
            config: Evolution configuration
        """
        if not self._initialized:
            raise RuntimeError("Database connection not initialized. Call connect() first.")
            
        # This would delegate to driver-specific schema evolution logic
        # Implementation depends on the specific driver capabilities
        logger.info("Schema evolution requested - delegating to driver implementation")