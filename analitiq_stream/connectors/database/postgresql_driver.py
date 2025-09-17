"""PostgreSQL-specific database driver."""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from .base_driver import BaseDatabaseDriver
from .utils import extract_values_for_columns, convert_record_from_db

logger = logging.getLogger(__name__)


class PostgreSQLDriver(BaseDatabaseDriver):
    """
    PostgreSQL-specific driver implementing advanced features:
    - TIMESTAMPTZ, DECIMAL, JSONB types
    - ON CONFLICT upserts
    - Index management
    - Schema creation
    """

    def __init__(self):
        super().__init__("PostgreSQL")
        self.asyncpg = None
        self._column_types = {}  # Cache of column name -> database type mappings

    async def create_connection_pool(self, config: Dict[str, Any]):
        """Create PostgreSQL connection pool with advanced configuration."""
        try:
            import asyncpg
            self.asyncpg = asyncpg
        except ImportError:
            raise ImportError("asyncpg package required. Install with: pip install asyncpg")

        # Extract connection parameters
        connection_params = self.get_connection_params(config)
        
        # Add PostgreSQL-specific pool settings
        pool_config = config.get("connection_pool", {})
        connection_params.update({
            "min_size": pool_config.get("min_connections", 2),
            "max_size": pool_config.get("max_connections", 10),
            "max_queries": pool_config.get("max_overflow", 20),
            "max_inactive_connection_lifetime": pool_config.get("pool_recycle", 3600),
            "command_timeout": config.get("command_timeout", 300),
        })

        self.connection_pool = await asyncpg.create_pool(**connection_params)
        logger.info(f"PostgreSQL connection pool created with {connection_params['min_size']}-{connection_params['max_size']} connections")

    async def close_connection_pool(self):
        """Close PostgreSQL connection pool."""
        if self.connection_pool:
            await self.connection_pool.close()
            self.connection_pool = None
            logger.info("PostgreSQL connection pool closed")

    def get_connection_params(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract PostgreSQL connection parameters."""
        return {
            "host": config.get("host", "localhost"),
            "port": int(config.get("port", 5432)),
            "user": config.get("user", "postgres"),
            "password": config.get("password", ""),
            "database": config.get("database", "postgres"),
            "ssl": config.get("ssl_mode", "prefer"),
        }

    async def create_schema_if_not_exists(self, schema_name: str):
        """Create PostgreSQL schema if it doesn't exist."""
        if not self.validate_identifier(schema_name):
            raise ValueError(f"Invalid schema name: {schema_name}")
            
        query = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
        
        async with self.connection_pool.acquire() as conn:
            await conn.execute(query)
            logger.info(f"Schema '{schema_name}' ensured to exist")

    async def create_table_if_not_exists(
        self,
        schema_name: str,
        table_name: str,
        table_schema: Dict[str, Any],
        primary_key: List[str],
        unique_constraints: List[str] = None
    ):
        """Create PostgreSQL table with advanced schema definition."""
        if not self.validate_identifier(table_name):
            raise ValueError(f"Invalid table name: {table_name}")
            
        full_table_name = self.get_full_table_name(schema_name, table_name)
        
        # Build column definitions
        column_defs = []
        properties = table_schema.get("properties", {})
        
        for field_name, field_def in properties.items():
            if not self.validate_identifier(field_name):
                continue
                
            sql_type = self.map_json_schema_to_sql_type(field_def)
            nullable = "NOT NULL" if not field_def.get("nullable", True) else "NULL"
            default = self._get_default_clause(field_def)
            
            column_def = f"{field_name} {sql_type} {nullable}{default}"
            column_defs.append(column_def)
        
        # Add primary key constraint
        if primary_key:
            pk_columns = ", ".join(primary_key)
            column_defs.append(f"PRIMARY KEY ({pk_columns})")
        
        # Add unique constraints
        if unique_constraints:
            for constraint in unique_constraints:
                if isinstance(constraint, list):
                    constraint_columns = ", ".join(constraint)
                else:
                    constraint_columns = constraint
                column_defs.append(f"UNIQUE ({constraint_columns})")
        
        columns_sql = ",\n  ".join(column_defs)
        
        query = f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
          {columns_sql}
        )
        """
        
        async with self.connection_pool.acquire() as conn:
            await conn.execute(query)
            logger.info(f"Table '{full_table_name}' ensured to exist")
            
        # Build column type mapping for schema-aware conversions
        self._build_column_type_mapping(table_schema)

    def _get_default_clause(self, field_def: Dict[str, Any]) -> str:
        """Get DEFAULT clause for column definition."""
        default_value = field_def.get("default")
        if not default_value:
            return ""
            
        if default_value == "CURRENT_TIMESTAMP":
            return " DEFAULT CURRENT_TIMESTAMP"
        elif isinstance(default_value, str):
            return f" DEFAULT '{default_value}'"
        else:
            return f" DEFAULT {default_value}"

    async def create_indexes_if_not_exist(
        self,
        schema_name: str,
        table_name: str,
        indexes: List[Dict[str, Any]]
    ):
        """Create PostgreSQL indexes if they don't exist."""
        full_table_name = self.get_full_table_name(schema_name, table_name)
        
        async with self.connection_pool.acquire() as conn:
            for index_config in indexes:
                index_name = index_config.get("name")
                columns = index_config.get("columns", [])
                index_type = index_config.get("type", "btree").upper()
                
                if not index_name or not columns:
                    continue
                    
                columns_str = ", ".join(columns)
                
                query = f"""
                CREATE INDEX IF NOT EXISTS {index_name} 
                ON {full_table_name} 
                USING {index_type} ({columns_str})
                """
                
                try:
                    await conn.execute(query)
                    logger.info(f"Index '{index_name}' ensured to exist on {full_table_name}")
                except Exception as e:
                    logger.warning(f"Failed to create index '{index_name}': {e}")

    def map_json_schema_to_sql_type(self, field_def: Dict[str, Any]) -> str:
        """Map JSON schema to PostgreSQL-specific types."""
        # Check for explicit database_type first
        if "database_type" in field_def:
            return field_def["database_type"]
        
        field_type = field_def.get("type", "string")
        field_format = field_def.get("format")
        
        # Handle nullable types
        if isinstance(field_type, list) and "null" in field_type:
            field_type = [t for t in field_type if t != "null"][0]
        
        # Type mapping with PostgreSQL specifics
        if field_type == "string":
            if field_format == "date-time":
                return "TIMESTAMPTZ"
            elif field_format == "date":
                return "DATE"
            else:
                max_length = field_def.get("maxLength", 255)
                return f"VARCHAR({max_length})" if max_length <= 10485760 else "TEXT"
                
        elif field_type == "integer":
            return "BIGINT"
            
        elif field_type == "number":
            # Use precision and scale if available
            precision = field_def.get("precision", 15)
            scale = field_def.get("scale", 2)
            return f"DECIMAL({precision},{scale})"
            
        elif field_type == "boolean":
            return "BOOLEAN"
            
        elif field_type == "object":
            return "JSONB"
            
        elif field_type == "array":
            # Simple array support
            items_type = field_def.get("items", {}).get("type", "string")
            if items_type == "string":
                return "TEXT[]"
            elif items_type == "integer":
                return "BIGINT[]"
            else:
                return "JSONB"
        else:
            return "TEXT"
    
    def _build_column_type_mapping(self, table_schema: Dict[str, Any]):
        """Build mapping of column names to database types for schema-aware conversions."""
        properties = table_schema.get("properties", {})
        
        for field_name, field_def in properties.items():
            if not self.validate_identifier(field_name):
                continue
                
            sql_type = self.map_json_schema_to_sql_type(field_def)
            self._column_types[field_name] = sql_type
        
        logger.debug(f"Built column type mapping: {self._column_types}")

    async def execute_upsert(
        self,
        conn,
        schema_name: str,
        table_name: str,
        batch: List[Dict[str, Any]],
        conflict_config: Dict[str, Any]
    ):
        """Execute PostgreSQL ON CONFLICT upsert."""
        if not batch:
            return
            
        full_table_name = self.get_full_table_name(schema_name, table_name)
        
        # Get columns from first record
        columns = list(batch[0].keys())
        columns_str = ", ".join(columns)
        placeholders = ", ".join([f"${i+1}" for i in range(len(columns))])
        
        # Build conflict resolution
        conflict_column = conflict_config.get("on_conflict", "id")
        update_columns = conflict_config.get("update_columns", [])
        
        if not update_columns:
            # Update all non-conflict columns
            update_columns = [col for col in columns if col != conflict_column]
        
        update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])
        
        query = f"""
        INSERT INTO {full_table_name} ({columns_str}) 
        VALUES ({placeholders})
        ON CONFLICT ({conflict_column}) 
        DO UPDATE SET {update_clause}
        """
        
        # Prepare batch values using utilities for type conversion with schema awareness
        values = []
        for i, record in enumerate(batch):
            row_values = extract_values_for_columns(record, columns, self._column_types)
            values.append(row_values)
        
        try:
            await conn.executemany(query, values)
        except Exception as e:
            # Debug log the first problematic row for troubleshooting
            logger.error(f"Database upsert failed for table {full_table_name}")
            logger.error(f"Query: {query}")
            logger.error(f"Columns: {columns}")
            if batch:
                logger.error(f"First record data: {batch[0]}")
                logger.error(f"First record value types: {[(k, type(v).__name__, v) for k, v in batch[0].items()]}")
            raise

    async def execute_insert(
        self,
        conn,
        schema_name: str,
        table_name: str,
        batch: List[Dict[str, Any]]
    ):
        """Execute PostgreSQL INSERT."""
        if not batch:
            return
            
        full_table_name = self.get_full_table_name(schema_name, table_name)
        
        columns = list(batch[0].keys())
        columns_str = ", ".join(columns)
        placeholders = ", ".join([f"${i+1}" for i in range(len(columns))])
        
        query = f"INSERT INTO {full_table_name} ({columns_str}) VALUES ({placeholders})"
        
        # Prepare batch values using utilities for type conversion with schema awareness
        values = []
        for record in batch:
            row_values = extract_values_for_columns(record, columns, self._column_types)
            values.append(row_values)
        
        try:
            await conn.executemany(query, values)
        except Exception as e:
            # Debug log the first problematic row for troubleshooting
            logger.error(f"Database insert failed for table {full_table_name}")
            logger.error(f"Query: {query}")
            logger.error(f"Columns: {columns}")
            if batch:
                logger.error(f"First record data: {batch[0]}")
                logger.error(f"First record value types: {[(k, type(v).__name__, v) for k, v in batch[0].items()]}")
            raise

    async def execute_query(
        self,
        conn,
        query: str,
        params: List[Any] = None
    ) -> List[Dict[str, Any]]:
        """Execute PostgreSQL query and return results."""
        if params:
            rows = await conn.fetch(query, *params)
        else:
            rows = await conn.fetch(query)
        
        # Convert rows to dictionaries using utilities for consistent type conversion
        results = []
        for row in rows:
            record = dict(row)
            converted_record = convert_record_from_db(record)
            results.append(converted_record)
        
        return results

    def build_incremental_query(
        self,
        schema_name: str,
        table_name: str,
        config: Dict[str, Any],
        cursor_value: Optional[Any] = None
    ) -> Tuple[str, List[Any]]:
        """Build PostgreSQL incremental read query with cursor support."""
        full_table_name = self.get_full_table_name(schema_name, table_name)
        
        columns = config.get("columns", ["*"])
        columns_str = ", ".join(columns) if columns != ["*"] else "*"
        
        query = f"SELECT {columns_str} FROM {full_table_name}"
        params = []
        
        where_clauses = []
        
        # Add custom WHERE clause
        if config.get("where"):
            where_clauses.append(config["where"])
        
        # Add incremental filter using passed cursor_value parameter
        cursor_field = config.get("cursor_field", "updated_at")
        # Use the cursor_value parameter passed to this method, fallback to config
        if cursor_value is None:
            cursor_value = config.get("cursor_value")
        cursor_mode = config.get("cursor_mode", "exclusive")
        
        if cursor_value:
            operator = ">=" if cursor_mode == "inclusive" else ">"
            where_clauses.append(f"{cursor_field} {operator} ${len(params) + 1}")
            params.append(cursor_value)
        
        # Apply WHERE clauses
        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
        
        # Add ORDER BY
        if config.get("order_by"):
            query += f" ORDER BY {config['order_by']}"
        elif cursor_field:
            query += f" ORDER BY {cursor_field}"
        
        return query, params