"""SQLAlchemy-based database destination handler.

This handler provides a unified interface for all SQL databases supported by SQLAlchemy.
The specific database is determined by the `driver` field in the connection config.

Type casting is handled by the Arrow-based DestinationSchemaContract which provides
efficient columnar type conversion for batch operations.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    Integer,
    LargeBinary,
    MetaData,
    String,
    Table,
    Text,
    BigInteger,
    text,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncConnection

from ..base_handler import BaseDestinationHandler, BatchWriteResult
from ..schema_contract import DestinationSchemaContract
from ...grpc.generated.analitiq.v1 import (
    AckStatus,
    Cursor,
    SchemaMessage,
)
from ...shared.database_utils import create_database_engine


logger = logging.getLogger(__name__)


class DatabaseDestinationHandler(BaseDestinationHandler):
    """
    Unified database destination handler using SQLAlchemy.

    Supports any database that SQLAlchemy supports via the `driver` config field.
    The handler automatically:
    - Creates the target table if it doesn't exist
    - Creates the idempotency tracking table
    - Handles upsert operations for supported databases
    - Tracks batch commits for idempotency

    Configuration (connection config):
    - driver: Database driver (postgresql, mysql, sqlite, etc.)
    - host: Database host
    - port: Database port
    - database: Database name
    - username: Database username
    - password: Database password
    - ssl_mode: Optional SSL mode

    Configuration (endpoint config via SchemaMessage):
    - schema: Database schema name (default: public for PostgreSQL)
    - table: Target table name
    - primary_key: List of primary key columns
    - write_mode: insert, upsert, or truncate_insert
    """

    # Idempotency tracking table name
    BATCH_COMMITS_TABLE = "_batch_commits"

    def __init__(self) -> None:
        """Initialize the database handler."""
        self._engine: AsyncEngine | None = None
        self._metadata: MetaData = MetaData()
        self._config: Dict[str, Any] = {}
        self._connected: bool = False
        self._driver: str = ""

        # Schema configuration from SchemaMessage
        self._schema_name: str = "public"
        self._table_name: str = ""
        self._table: Table | None = None
        self._primary_keys: List[str] = []
        self._write_mode: str = "upsert"
        self._json_schema: Dict[str, Any] = {}

        # Batch commits table for idempotency
        self._batch_commits_table: Table | None = None

        # Arrow-based schema contract for vectorized type casting (built on schema configure)
        self._schema_contract: Optional[DestinationSchemaContract] = None

    @property
    def connector_type(self) -> str:
        """Return the connector type identifier."""
        return "database"

    @property
    def supports_transactions(self) -> bool:
        """All SQL databases support transactions."""
        return True

    @property
    def supports_upsert(self) -> bool:
        """PostgreSQL and MySQL support upsert."""
        return self._driver in ("postgresql", "postgres", "mysql", "mariadb")

    @property
    def supports_bulk_load(self) -> bool:
        """Bulk load support depends on database."""
        return False

    async def connect(self, connection_config: Dict[str, Any]) -> None:
        """
        Establish database connection using the shared engine factory.

        Args:
            connection_config: Connection configuration
        """
        self._config = connection_config
        self._engine, self._driver = await create_database_engine(
            connection_config, require_port=False
        )
        self._connected = True
        logger.info("DatabaseDestinationHandler connected to %s", self._driver)

    async def disconnect(self) -> None:
        """Close database connection."""
        if self._engine and self._connected:
            await self._engine.dispose()
            self._connected = False
            logger.info("DatabaseDestinationHandler disconnected")

    async def configure_schema(self, schema_msg: SchemaMessage) -> bool:
        """
        Configure database schema from SchemaMessage.

        Creates target table and idempotency tracking table if needed.

        Args:
            schema_msg: Schema configuration from engine

        Returns:
            True if configuration succeeded
        """
        if not self._engine:
            logger.error("Cannot configure schema: not connected")
            return False

        try:
            # Extract configuration from SchemaMessage
            db_config = schema_msg.destination_config.database
            self._schema_name = db_config.schema_name or "public"
            self._table_name = db_config.table_name

            if not self._table_name:
                logger.error("Table name is required in schema configuration")
                return False

            # Get write mode
            self._write_mode = self._get_write_mode(schema_msg.write_mode)

            # Parse JSON schema
            if schema_msg.json_schema:
                import json
                self._json_schema = json.loads(schema_msg.json_schema)

            # Get primary keys - prefer schema_msg, fallback to json_schema
            if schema_msg.primary_key:
                self._primary_keys = list(schema_msg.primary_key)
            elif self._json_schema.get("primary_keys"):
                # Database endpoint schema format uses 'primary_keys'
                self._primary_keys = list(self._json_schema["primary_keys"])
            else:
                self._primary_keys = []

            # Create tables
            await self._ensure_tables_exist()

            # Build schema contract for Arrow-based type casting
            if self._json_schema:
                self._schema_contract = DestinationSchemaContract(self._json_schema)
                logger.debug(
                    f"Built schema contract with {len(self._schema_contract.column_types)} columns"
                )

            logger.info(
                f"Schema configured: {self._schema_name}.{self._table_name}, "
                f"mode={self._write_mode}, pk={self._primary_keys}"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to configure schema: {e}")
            return False

    def _get_write_mode(self, proto_write_mode: int) -> str:
        """Convert protobuf write mode to string."""
        # WriteMode enum values from proto
        mode_map = {
            0: "unspecified",
            1: "insert",
            2: "upsert",
            3: "truncate_insert",
        }
        return mode_map.get(proto_write_mode, "upsert")

    async def _ensure_tables_exist(self) -> None:
        """Create target table and batch commits table if they don't exist."""
        if not self._engine:
            return

        # Create batch commits table for idempotency
        self._batch_commits_table = Table(
            self.BATCH_COMMITS_TABLE,
            self._metadata,
            Column("run_id", String(255), primary_key=True),
            Column("stream_id", String(255), primary_key=True),
            Column("batch_seq", BigInteger, primary_key=True),
            Column("committed_cursor", LargeBinary),
            Column("records_written", Integer),
            Column("committed_at", DateTime, default=datetime.utcnow),
            schema=self._schema_name,
            extend_existing=True,
        )

        # Create target table from JSON schema
        if self._json_schema:
            self._table = self._create_table_from_schema(
                self._table_name,
                self._json_schema,
                self._primary_keys,
            )
        else:
            # Minimal table structure if no schema provided
            logger.warning("No JSON schema provided, table must already exist")

        # Create tables in database
        async with self._engine.begin() as conn:
            # Create schema if needed (PostgreSQL)
            if self._driver in ("postgresql", "postgres") and self._schema_name != "public":
                await conn.execute(
                    text(f"CREATE SCHEMA IF NOT EXISTS {self._schema_name}")
                )

            # Create tables
            await conn.run_sync(self._metadata.create_all)

        logger.debug(f"Ensured tables exist in schema {self._schema_name}")

    def _create_table_from_schema(
        self,
        table_name: str,
        json_schema: Dict[str, Any],
        primary_keys: List[str],
    ) -> Table:
        """
        Create SQLAlchemy Table from schema definition.

        Supports two formats:
        1. JSON Schema format with 'properties' dict
        2. Database endpoint schema format with 'columns' array

        Args:
            table_name: Target table name
            json_schema: Schema definition (JSON Schema or endpoint schema)
            primary_keys: List of primary key column names

        Returns:
            SQLAlchemy Table object
        """
        columns = []

        # Check for database endpoint schema format (columns array)
        if "columns" in json_schema:
            for col_def in json_schema["columns"]:
                col_name = col_def.get("name")
                if not col_name:
                    continue
                sa_type = self._db_type_to_sqlalchemy(col_def.get("type", "TEXT"))
                is_pk = col_name in primary_keys
                nullable = col_def.get("nullable", True) and not is_pk

                columns.append(
                    Column(col_name, sa_type, primary_key=is_pk, nullable=nullable)
                )
        else:
            # JSON Schema format with properties
            properties = json_schema.get("properties", {})
            required = set(json_schema.get("required", []))

            for col_name, col_schema in properties.items():
                sa_type = self._json_type_to_sqlalchemy(col_schema)
                is_pk = col_name in primary_keys
                nullable = col_name not in required and not is_pk

                columns.append(
                    Column(col_name, sa_type, primary_key=is_pk, nullable=nullable)
                )

        return Table(
            table_name,
            self._metadata,
            *columns,
            schema=self._schema_name,
            extend_existing=True,
        )

    def _db_type_to_sqlalchemy(self, db_type: str) -> Any:
        """
        Convert database type string to SQLAlchemy type.

        Args:
            db_type: Database type string (e.g., 'BIGINT', 'VARCHAR(50)', 'TIMESTAMP')

        Returns:
            SQLAlchemy type
        """
        db_type_upper = db_type.upper()

        # Integer types
        if db_type_upper in ("BIGINT", "INT8"):
            return BigInteger()
        if db_type_upper in ("INTEGER", "INT", "INT4"):
            return Integer()

        # String types with length
        if db_type_upper.startswith("VARCHAR"):
            # Extract length from VARCHAR(n)
            import re
            match = re.search(r"\((\d+)\)", db_type_upper)
            if match:
                return String(int(match.group(1)))
            return String(255)
        if db_type_upper in ("TEXT", "CLOB"):
            return Text()

        # Numeric types
        if db_type_upper.startswith("NUMERIC") or db_type_upper.startswith("DECIMAL"):
            return Float()
        if db_type_upper in ("FLOAT", "DOUBLE", "REAL"):
            return Float()

        # Date/time types
        if db_type_upper in ("TIMESTAMP", "TIMESTAMPTZ", "DATETIME"):
            return DateTime(timezone=True)
        if db_type_upper == "DATE":
            return DateTime()

        # Boolean
        if db_type_upper in ("BOOLEAN", "BOOL"):
            return Boolean()

        # Binary
        if db_type_upper in ("BYTEA", "BLOB", "BINARY"):
            return LargeBinary()

        # Default to Text
        return Text()

    def _json_type_to_sqlalchemy(self, prop: Dict[str, Any]) -> Any:
        """
        Convert JSON Schema type to SQLAlchemy type.

        Args:
            prop: JSON Schema property definition

        Returns:
            SQLAlchemy type
        """
        json_type = prop.get("type", "string")
        json_format = prop.get("format")

        # Handle arrays and objects as Text (JSON)
        if json_type == "array" or json_type == "object":
            return Text()

        # String types
        if json_type == "string":
            max_length = prop.get("maxLength")
            if json_format == "date-time":
                return DateTime(timezone=True)
            if json_format == "date":
                return DateTime()
            if max_length and max_length <= 255:
                return String(max_length)
            return Text()

        # Numeric types
        if json_type == "integer":
            return BigInteger()

        if json_type == "number":
            return Float()

        # Boolean
        if json_type == "boolean":
            return Boolean()

        # Default to Text
        return Text()

    async def write_batch(
        self,
        run_id: str,
        stream_id: str,
        batch_seq: int,
        records: List[Dict[str, Any]],
        record_ids: List[str],
        cursor: Cursor,
    ) -> BatchWriteResult:
        """
        Write a batch of records to the database.

        Args:
            run_id: Pipeline run identifier
            stream_id: Stream identifier
            batch_seq: Batch sequence number
            records: Records to write
            record_ids: Record identifiers
            cursor: Cursor to return on success

        Returns:
            BatchWriteResult with status
        """
        if not self._engine or not self._connected:
            return BatchWriteResult(
                success=False,
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                failure_summary="Handler not connected",
            )

        if self._table is None or self._batch_commits_table is None:
            return BatchWriteResult(
                success=False,
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                failure_summary="Schema not configured",
            )

        # Check idempotency
        existing = await self._check_batch_committed(run_id, stream_id, batch_seq)
        if existing:
            logger.info(f"Batch already committed: {run_id}/{stream_id}/{batch_seq}")
            return BatchWriteResult(
                success=True,
                status=AckStatus.ACK_STATUS_ALREADY_COMMITTED,
                records_written=existing["records_written"],
                committed_cursor=Cursor(token=existing["committed_cursor"]),
            )

        if not records:
            # Empty batch - still record for idempotency
            await self._record_batch_commit(run_id, stream_id, batch_seq, cursor.token, 0)
            return BatchWriteResult(
                success=True,
                status=AckStatus.ACK_STATUS_SUCCESS,
                records_written=0,
                committed_cursor=cursor,
            )

        try:
            async with self._engine.begin() as conn:
                # Write records based on write mode
                if self._write_mode == "truncate_insert":
                    await self._truncate_and_insert(conn, records)
                elif self._write_mode == "upsert" and self._primary_keys:
                    await self._upsert_records(conn, records)
                else:
                    await self._insert_records(conn, records)

                # Record batch commit
                await self._record_batch_commit_in_txn(
                    conn, run_id, stream_id, batch_seq, cursor.token, len(records)
                )

            logger.info(f"Wrote batch {batch_seq}: {len(records)} records")
            return BatchWriteResult(
                success=True,
                status=AckStatus.ACK_STATUS_SUCCESS,
                records_written=len(records),
                committed_cursor=cursor,
            )

        except Exception as e:
            logger.error(f"Error writing batch: {e}")
            return BatchWriteResult(
                success=False,
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                failure_summary=str(e),
            )

    async def _check_batch_committed(
        self,
        run_id: str,
        stream_id: str,
        batch_seq: int,
    ) -> Optional[Dict[str, Any]]:
        """Check if batch was already committed."""
        if self._engine is None or self._batch_commits_table is None:
            return None

        async with self._engine.connect() as conn:
            result = await conn.execute(
                self._batch_commits_table.select().where(
                    (self._batch_commits_table.c.run_id == run_id) &
                    (self._batch_commits_table.c.stream_id == stream_id) &
                    (self._batch_commits_table.c.batch_seq == batch_seq)
                )
            )
            row = result.fetchone()
            if row:
                return {
                    "records_written": row.records_written,
                    "committed_cursor": row.committed_cursor,
                }
        return None

    async def _record_batch_commit(
        self,
        run_id: str,
        stream_id: str,
        batch_seq: int,
        cursor_bytes: bytes,
        records_written: int,
    ) -> None:
        """Record batch commit (outside transaction)."""
        if self._engine is None or self._batch_commits_table is None:
            return

        async with self._engine.begin() as conn:
            await conn.execute(
                self._batch_commits_table.insert().values(
                    run_id=run_id,
                    stream_id=stream_id,
                    batch_seq=batch_seq,
                    committed_cursor=cursor_bytes,
                    records_written=records_written,
                    committed_at=datetime.utcnow(),
                )
            )

    async def _record_batch_commit_in_txn(
        self,
        conn: AsyncConnection,
        run_id: str,
        stream_id: str,
        batch_seq: int,
        cursor_bytes: bytes,
        records_written: int,
    ) -> None:
        """Record batch commit within existing transaction."""
        if self._batch_commits_table is None:
            return

        await conn.execute(
            self._batch_commits_table.insert().values(
                run_id=run_id,
                stream_id=stream_id,
                batch_seq=batch_seq,
                committed_cursor=cursor_bytes,
                records_written=records_written,
                committed_at=datetime.utcnow(),
            )
        )

    async def _insert_records(
        self,
        conn: AsyncConnection,
        records: List[Dict[str, Any]],
    ) -> None:
        """Insert records (plain INSERT)."""
        if self._table is None:
            return

        # Prepare records - convert complex types to JSON strings
        prepared = self._prepare_records(records)
        await conn.execute(self._table.insert(), prepared)

    async def _upsert_records(
        self,
        conn: AsyncConnection,
        records: List[Dict[str, Any]],
    ) -> None:
        """Upsert records (INSERT ... ON CONFLICT)."""
        if self._table is None or not self._primary_keys:
            await self._insert_records(conn, records)
            return

        prepared = self._prepare_records(records)

        if self._driver in ("postgresql", "postgres"):
            # PostgreSQL upsert
            stmt = pg_insert(self._table).values(prepared)
            # Get columns actually present in the records
            record_columns = set(prepared[0].keys()) if prepared else set()
            update_cols = {
                c.name: c for c in stmt.excluded
                if c.name not in self._primary_keys and c.name in record_columns
            }
            stmt = stmt.on_conflict_do_update(
                index_elements=self._primary_keys,
                set_=update_cols,
            )
            await conn.execute(stmt)

        elif self._driver in ("mysql", "mariadb"):
            # MySQL upsert
            stmt = mysql_insert(self._table).values(prepared)
            # Get columns actually present in the records
            record_columns = set(prepared[0].keys()) if prepared else set()
            update_cols = {
                c.name: c for c in stmt.inserted
                if c.name not in self._primary_keys and c.name in record_columns
            }
            stmt = stmt.on_duplicate_key_update(**update_cols)
            await conn.execute(stmt)

        else:
            # Fallback to plain insert for other databases
            await self._insert_records(conn, records)

    async def _truncate_and_insert(
        self,
        conn: AsyncConnection,
        records: List[Dict[str, Any]],
    ) -> None:
        """Truncate table and insert records."""
        if self._table is None:
            return

        # Truncate
        await conn.execute(self._table.delete())

        # Insert
        await self._insert_records(conn, records)

    def _prepare_records(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Prepare records for insertion using Arrow-based vectorized casting.

        Uses the DestinationSchemaContract to efficiently cast all records
        to the proper types for this database in a single columnar operation.

        Args:
            records: Raw records from transformation

        Returns:
            Prepared records with proper types for database insertion
        """
        if not records:
            return records

        # Use Arrow-based schema contract for vectorized type casting
        if self._schema_contract:
            records = self._schema_contract.prepare_records(records)

        # Handle any remaining complex types (dicts/lists)
        # For non-PostgreSQL databases, complex types need to be JSON-serialized
        import json
        if self._driver not in ("postgresql", "postgres"):
            prepared = []
            for record in records:
                row = {}
                for key, value in record.items():
                    if isinstance(value, (dict, list)):
                        row[key] = json.dumps(value)
                    else:
                        row[key] = value
                prepared.append(row)
            return prepared

        return records

    async def health_check(self) -> bool:
        """Check database health."""
        if not self._engine or not self._connected:
            return False

        try:
            async with self._engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logger.warning(f"Health check failed: {e}")
            return False
