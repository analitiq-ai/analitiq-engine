"""Abstract base driver for database-specific operations."""

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class BaseDatabaseDriver(ABC):
    """
    Abstract base class for database-specific drivers.

    Each driver implements database-specific operations like:
    - Connection management
    - Schema/table creation
    - Type mapping
    - Index management
    - Upsert operations
    """

    def __init__(self, name: str):
        self.name = name
        self.connection_pool = None

    @abstractmethod
    async def create_connection_pool(self, config: Dict[str, Any]):
        """Create database-specific connection pool."""
        pass

    @abstractmethod
    async def close_connection_pool(self):
        """Close the connection pool."""
        pass

    @abstractmethod
    async def create_schema_if_not_exists(self, schema_name: str):
        """Create schema if it doesn't exist."""
        pass

    @abstractmethod
    async def create_table_if_not_exists(
        self,
        schema_name: str,
        table_name: str,
        endpoint_schema: Dict[str, Any],
        primary_key: List[str],
        unique_constraints: List[str] = None
    ):
        """Create table if it doesn't exist with proper schema."""
        pass

    @abstractmethod
    async def create_indexes_if_not_exist(
        self,
        schema_name: str,
        table_name: str, 
        indexes: List[Dict[str, Any]]
    ):
        """Create indexes if they don't exist."""
        pass

    @abstractmethod
    def map_json_schema_to_sql_type(self, field_def: Dict[str, Any]) -> str:
        """Map JSON schema field definition to database-specific SQL type."""
        pass

    @abstractmethod
    async def execute_upsert(
        self,
        conn,
        schema_name: str,
        table_name: str,
        batch: List[Dict[str, Any]],
        conflict_config: Dict[str, Any]
    ):
        """Execute database-specific upsert operation."""
        pass

    @abstractmethod
    async def execute_insert(
        self,
        conn, 
        schema_name: str,
        table_name: str,
        batch: List[Dict[str, Any]]
    ):
        """Execute database-specific insert operation."""
        pass

    @abstractmethod
    async def execute_query(
        self,
        conn,
        query: str,
        params: List[Any] = None
    ) -> List[Dict[str, Any]]:
        """Execute query and return results as list of dictionaries."""
        pass

    def build_incremental_query(
        self,
        schema_name: str,
        table_name: str,
        config: Dict[str, Any],
        cursor_value: Optional[Any] = None
    ) -> Tuple[str, List[Any]]:
        """Build incremental read query with parameters and cursor support.

        Uses SQLAlchemy-based query builder for SQL injection protection.
        All identifiers are properly quoted and all values are parameterized.

        Subclasses can override this method for database-specific behavior,
        but the default implementation using the shared query builder should
        work for most SQLAlchemy-supported databases.

        Args:
            schema_name: Database schema name
            table_name: Table name
            config: Query configuration containing:
                - driver: Database driver/dialect (postgresql, mysql, etc.)
                - columns: List of column names or ["*"]
                - filters: List of structured filters [{field, op, value}]
                - cursor_field: Field for incremental cursor
                - cursor_mode: 'inclusive' (>=) or 'exclusive' (>)
                - order_by: Field to order by
                - limit: Max rows to return
            cursor_value: Current cursor value for incremental reads

        Returns:
            Tuple of (query_string, params_list)
        """
        from ...shared.query_builder import build_select_query

        # Get dialect from config, normalize to lowercase
        dialect = config.get("driver").lower()

        logger.debug(f"Building incremental query with dialect={dialect}, config: {config}")

        return build_select_query(
            dialect=dialect,
            schema_name=schema_name,
            table_name=table_name,
            config=config,
            cursor_value=cursor_value
        )

    @abstractmethod
    def get_connection_params(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract and validate connection parameters from config."""
        pass

    def get_full_table_name(self, schema_name: str, table_name: str) -> str:
        """Get fully qualified table name."""
        return f"{schema_name}.{table_name}" if schema_name else table_name

    def validate_identifier(self, identifier: str) -> bool:
        """Validate SQL identifier (table name, column name, etc.).

        Rejects hyphens because they require quoting in PostgreSQL.
        Only allows alphanumeric characters and underscores.
        """
        if not identifier:
            return False
        # Only allow alphanumeric and underscores (no hyphens - they require quoting)
        if not identifier.replace('_', '').isalnum():
            return False
        if identifier[0].isdigit():
            return False
        return True

    async def acquire_connection(self):
        """Acquire connection from pool."""
        if not self.connection_pool:
            raise RuntimeError("Connection pool not initialized")
        return await self.connection_pool.acquire()