"""Arrow-based schema contract for destination type casting.

This module provides the DestinationSchemaContract class that handles:
1. Building Arrow schema from destination endpoint schema
2. Vectorized batch casting using PyArrow
3. Converting Arrow table back to dicts for database insertion

The schema contract is built once per destination table and reused for every batch,
providing efficient columnar type casting instead of row-by-row Python coercion.
"""

import logging
from datetime import datetime, date, time
from decimal import Decimal
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.compute as pc

logger = logging.getLogger(__name__)


class DestinationSchemaContract:
    """Schema mapping object per destination table.

    Built once from destination endpoint schema, reused for every batch.
    Maps destination native types to Arrow types for efficient columnar casting.

    Supports two schema formats:
    1. "columns" array: Database endpoint format with native types
       [{"name": "id", "type": "BIGINT", "nullable": true}, ...]
    2. JSON Schema "properties": API endpoint format
       {"properties": {"id": {"type": "integer"}, ...}}
    """

    def __init__(self, dest_endpoint_schema: Dict[str, Any]) -> None:
        """Build Arrow schema from destination endpoint schema.

        Args:
            dest_endpoint_schema: Schema definition (columns array or JSON Schema)
        """
        self._columns: List[Dict[str, Any]] = []
        self._arrow_schema: pa.Schema

        if "columns" in dest_endpoint_schema:
            # Columns array format (database endpoints) - native types
            self._columns = dest_endpoint_schema.get("columns", [])
            self._arrow_schema = self._build_arrow_schema()
        elif "properties" in dest_endpoint_schema:
            # JSON Schema format (API endpoints) - build Arrow schema directly
            self._arrow_schema = self._build_arrow_schema_from_json_schema(
                dest_endpoint_schema
            )
        else:
            self._arrow_schema = pa.schema([])

        self._column_types: Dict[str, str] = {
            f.name: str(f.type) for f in self._arrow_schema
        }

        logger.debug(f"Built schema contract with {len(self._arrow_schema)} fields")

    @property
    def arrow_schema(self) -> pa.Schema:
        """Get the Arrow schema."""
        return self._arrow_schema

    @property
    def column_types(self) -> Dict[str, str]:
        """Get mapping of column names to Arrow type strings."""
        return self._column_types

    def _build_arrow_schema_from_json_schema(
        self, json_schema: Dict[str, Any]
    ) -> pa.Schema:
        """Build Arrow schema directly from JSON Schema properties.

        Args:
            json_schema: JSON Schema with "properties" dict

        Returns:
            PyArrow Schema
        """
        fields = []
        required = set(json_schema.get("required", []))

        for name, prop in json_schema.get("properties", {}).items():
            arrow_type = self._json_schema_to_arrow(prop)
            fields.append(pa.field(name, arrow_type, nullable=name not in required))

        return pa.schema(fields)

    def _json_schema_to_arrow(self, prop: Dict[str, Any]) -> pa.DataType:
        """Map JSON Schema type directly to Arrow type.

        Args:
            prop: JSON Schema property definition

        Returns:
            PyArrow DataType
        """
        json_type = prop.get("type", "string")
        fmt = prop.get("format", "")

        if json_type == "integer":
            return pa.int64()
        elif json_type == "number":
            return pa.float64()
        elif json_type == "boolean":
            return pa.bool_()
        elif fmt == "date-time":
            return pa.timestamp("us")
        elif fmt == "date":
            return pa.date32()
        elif json_type in ("object", "array"):
            return pa.string()  # Serialize to JSON string
        else:
            return pa.string()

    def _build_arrow_schema(self) -> pa.Schema:
        """Build canonical Arrow schema from destination column types.

        Returns:
            PyArrow Schema built from columns array
        """
        fields = []

        for col in self._columns:
            col_name = col.get("name")
            if not col_name:
                continue

            col_type = col.get("type", "TEXT")
            nullable = col.get("nullable", True)

            arrow_type = self._native_to_arrow(col_type)
            fields.append(pa.field(col_name, arrow_type, nullable=nullable))

        return pa.schema(fields)

    def _native_to_arrow(self, native_type: str) -> pa.DataType:
        """Map destination native SQL type to Arrow type.

        Args:
            native_type: Native SQL type string (e.g., "BIGINT", "VARCHAR(50)")

        Returns:
            PyArrow DataType
        """
        # Get base type (strip parentheses for VARCHAR(n), DECIMAL(p,s), etc.)
        base = native_type.split("(")[0].upper().strip()

        # Parse precision/scale for DECIMAL/NUMERIC
        if base in ("DECIMAL", "NUMERIC") and "(" in native_type:
            try:
                params = native_type.split("(")[1].rstrip(")").split(",")
                precision = int(params[0].strip())
                scale = int(params[1].strip()) if len(params) > 1 else 0
                return pa.decimal128(precision, scale)
            except (ValueError, IndexError):
                return pa.decimal128(15, 2)  # Default precision

        # Type mapping
        type_map = {
            # Integer types
            "BIGINT": pa.int64(),
            "INT8": pa.int64(),
            "INT": pa.int32(),
            "INTEGER": pa.int32(),
            "INT4": pa.int32(),
            "SMALLINT": pa.int16(),
            "INT2": pa.int16(),
            "TINYINT": pa.int8(),
            # Float types
            "FLOAT": pa.float32(),
            "FLOAT4": pa.float32(),
            "DOUBLE": pa.float64(),
            "FLOAT8": pa.float64(),
            "REAL": pa.float64(),
            # String types
            "VARCHAR": pa.string(),
            "CHAR": pa.string(),
            "TEXT": pa.string(),
            "STRING": pa.string(),
            "CLOB": pa.string(),
            # Timestamp types
            "TIMESTAMP": pa.timestamp("us"),
            "TIMESTAMPTZ": pa.timestamp("us", tz="UTC"),
            "TIMESTAMP_TZ": pa.timestamp("us", tz="UTC"),
            "DATETIME": pa.timestamp("us"),
            # Date/Time types
            "DATE": pa.date32(),
            "TIME": pa.time64("us"),
            "TIMETZ": pa.time64("us"),
            # Boolean
            "BOOLEAN": pa.bool_(),
            "BOOL": pa.bool_(),
            # JSON types (stored as string)
            "JSON": pa.string(),
            "JSONB": pa.string(),
            # Binary
            "BYTEA": pa.binary(),
            "BLOB": pa.binary(),
            "BINARY": pa.binary(),
            "VARBINARY": pa.binary(),
        }

        return type_map.get(base, pa.string())

    def cast_batch(self, records: List[Dict[str, Any]]) -> pa.Table:
        """Vectorized cast of batch to canonical Arrow schema.

        Converts Python dicts to Arrow table with proper type casting.
        Handles missing columns by creating null arrays.

        Args:
            records: List of record dictionaries

        Returns:
            PyArrow Table with cast columns
        """
        if not records:
            return pa.table({}, schema=self._arrow_schema)

        # Convert records to Arrow table (infers types)
        try:
            table = pa.Table.from_pylist(records)
        except Exception as e:
            logger.warning(f"Failed to create Arrow table: {e}, returning empty")
            return pa.table({}, schema=self._arrow_schema)

        # Cast each column to target type (vectorized)
        arrays = []
        for field in self._arrow_schema:
            if field.name in table.column_names:
                col = table.column(field.name)
                if col.type != field.type:
                    try:
                        col = self._safe_cast_column(col, field.type, field.name)
                    except Exception as e:
                        logger.warning(
                            f"Failed to cast column {field.name} from {col.type} "
                            f"to {field.type}: {e}"
                        )
                        # Keep original column on failure
                arrays.append(col)
            else:
                # Missing column - create null array
                arrays.append(pa.nulls(len(table), type=field.type))

        return pa.Table.from_arrays(arrays, schema=self._arrow_schema)

    def _safe_cast_column(
        self, col: pa.ChunkedArray, target_type: pa.DataType, col_name: str
    ) -> pa.ChunkedArray:
        """Safely cast a column to target type with special handling.

        Handles special cases like:
        - String to timestamp conversion
        - String to numeric conversion
        - None/null handling

        Args:
            col: Source column
            target_type: Target Arrow type
            col_name: Column name for logging

        Returns:
            Cast column
        """
        source_type = col.type

        # Handle timestamp conversion from strings
        if pa.types.is_timestamp(target_type) and pa.types.is_string(source_type):
            # Parse ISO8601 timestamps
            try:
                return pc.strptime(col, format="%Y-%m-%dT%H:%M:%S", unit="us")
            except Exception:
                # Try with timezone
                try:
                    return pc.strptime(
                        col, format="%Y-%m-%dT%H:%M:%S%z", unit="us"
                    )
                except Exception:
                    # Fall back to cast
                    pass

        # Handle date conversion from strings
        if pa.types.is_date(target_type) and pa.types.is_string(source_type):
            try:
                return pc.strptime(col, format="%Y-%m-%d", unit="s").cast(target_type)
            except Exception:
                pass

        # Handle decimal from various types
        if pa.types.is_decimal(target_type):
            if pa.types.is_string(source_type):
                # Convert string to float first, then to decimal
                try:
                    float_col = pc.cast(col, pa.float64())
                    return pc.cast(float_col, target_type, safe=False)
                except Exception:
                    pass
            elif pa.types.is_floating(source_type) or pa.types.is_integer(source_type):
                return pc.cast(col, target_type, safe=False)

        # Default: use PyArrow cast (safe=False allows narrowing conversions)
        return pc.cast(col, target_type, safe=False)

    def to_dicts(self, table: pa.Table) -> List[Dict[str, Any]]:
        """Convert Arrow table to list of dicts for SQLAlchemy.

        Converts Arrow-native types back to Python types that SQLAlchemy
        and database drivers can handle.

        Args:
            table: PyArrow Table

        Returns:
            List of record dictionaries
        """
        return table.to_pylist()

    def prepare_records(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Prepare records for database insertion using Arrow casting.

        This is the main entry point for record preparation. It:
        1. Converts records to Arrow table with vectorized casting
        2. Converts back to dicts for SQLAlchemy

        Args:
            records: Raw records from transformation

        Returns:
            Prepared records with proper types
        """
        if not records:
            return records

        # Cast to Arrow schema (vectorized)
        arrow_table = self.cast_batch(records)

        # Convert back to dicts
        return self.to_dicts(arrow_table)
