"""Arrow-based schema contract for destination type casting.

This module provides the DestinationSchemaContract class that handles:
1. Building Arrow schema from destination endpoint schema
2. Vectorized batch casting using PyArrow
3. Converting Arrow table back to dicts for database insertion

The schema contract is built once per destination table and reused for every batch,
providing efficient columnar type casting instead of row-by-row Python coercion.

Two schema shapes are supported, handled on distinct paths:

- ``"columns"`` (database endpoints, native SQL types): native→Arrow
  translation is delegated to the connector's ``type-map.json`` via
  :class:`TypeMapper`.
- ``"properties"`` (JSON-Schema endpoints, API-style types): translated
  directly by :meth:`_json_schema_to_arrow`; the type-map is not consulted
  because JSON-Schema types are self-describing.

Either path raises on unknown types — no silent fallback to ``Utf8``.
"""

import logging
from datetime import datetime, date, time
from decimal import Decimal
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.compute as pc

from src.engine.type_map import TypeMapper, canonical_to_arrow

logger = logging.getLogger(__name__)


class DestinationSchemaContract:
    """Schema mapping object per destination table.

    Built once from destination endpoint schema, reused for every batch.
    Column ``type`` strings are interpreted by the destination connector's
    :class:`TypeMapper` — native → canonical Arrow → ``pa.DataType`` — so
    there is no hardcoded dialect knowledge here.

    Supports two schema formats:
    1. ``columns`` array: Database endpoint format with native types
       ``[{"name": "id", "type": "BIGINT", "nullable": true}, ...]``
    2. JSON Schema ``properties``: API endpoint format
       ``{"properties": {"id": {"type": "integer"}, ...}}``
    """

    def __init__(
        self,
        dest_endpoint_schema: Dict[str, Any],
        *,
        type_mapper: Optional[TypeMapper] = None,
    ) -> None:
        """Build Arrow schema from destination endpoint schema.

        Args:
            dest_endpoint_schema: Schema definition (columns array or JSON Schema).
            type_mapper: Destination connector's ``TypeMapper``. Required
                when the schema uses the ``columns`` array format (native
                SQL types). JSON Schema payloads do not use it.
        """
        self._columns: List[Dict[str, Any]] = []
        self._type_mapper = type_mapper
        self._arrow_schema: pa.Schema

        if "columns" in dest_endpoint_schema:
            if type_mapper is None:
                raise ValueError(
                    "DestinationSchemaContract: type_mapper is required for "
                    "'columns' schema payloads (native SQL types cannot be "
                    "interpreted without the connector's type-map)"
                )
            self._columns = dest_endpoint_schema.get("columns", [])
            self._arrow_schema = self._build_arrow_schema()
        elif "properties" in dest_endpoint_schema:
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
        """Map a JSON Schema property to an Arrow type.

        The JSON-Schema branch does not use the connector's type-map (API
        endpoints are self-describing); this method is the authoritative
        mapping. Unknown ``type``/``format`` combinations raise rather than
        silently becoming ``Utf8`` — the same contract the type-map matcher
        enforces on the SQL side.
        """
        json_type = prop.get("type")
        fmt = prop.get("format", "")

        if fmt == "date-time":
            return pa.timestamp("us")
        if fmt == "date":
            return pa.date32()
        if json_type == "string":
            return pa.string()
        if json_type == "integer":
            return pa.int64()
        if json_type == "number":
            return pa.float64()
        if json_type == "boolean":
            return pa.bool_()
        if json_type in ("object", "array"):
            # Serialized to JSON string at the storage boundary.
            return pa.string()
        raise ValueError(
            f"JSON Schema property has unsupported type/format "
            f"(type={json_type!r}, format={fmt!r})"
        )

    def _build_arrow_schema(self) -> pa.Schema:
        """Build canonical Arrow schema from destination column types.

        Each column's ``type`` is run through the connector's type-map
        (native → canonical) then parsed into a ``pa.DataType``. Unmapped
        natives raise from :class:`TypeMapper` — there is no silent default.
        """
        assert self._type_mapper is not None  # guarded in __init__
        fields = []

        for index, col in enumerate(self._columns):
            col_name = col.get("name")
            if not col_name:
                raise ValueError(
                    f"destination schema column at index {index} has no 'name' "
                    f"field; unnamed columns indicate a malformed endpoint payload"
                )

            col_type = col.get("type")
            if not col_type:
                raise ValueError(
                    f"column {col_name!r} has no 'type' field in destination schema"
                )
            nullable = col.get("nullable", True)

            canonical = self._type_mapper.to_canonical(col_type)
            arrow_type = canonical_to_arrow(canonical)
            fields.append(pa.field(col_name, arrow_type, nullable=nullable))

        return pa.schema(fields)

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

        # Convert records to Arrow table (infers types). A failure here
        # means the batch shape is incompatible with Arrow's type inference
        # — losing the records would be a silent data-loss bug, so we raise
        # with enough context for the caller to classify the failure.
        try:
            table = pa.Table.from_pylist(records)
        except Exception as e:
            raise ValueError(
                f"could not build Arrow table from {len(records)} records: {e}"
            ) from e

        # Cast each column to target type (vectorized).
        arrays = []
        for field in self._arrow_schema:
            if field.name in table.column_names:
                col = table.column(field.name)
                if col.type != field.type:
                    try:
                        col = self._safe_cast_column(col, field.type, field.name)
                    except Exception as e:
                        raise ValueError(
                            f"column {field.name!r}: cannot cast "
                            f"{col.type} → {field.type}: {e}"
                        ) from e
                arrays.append(col)
            else:
                # Missing column in the source batch. Null-filling is
                # semantically valid (nullable destination fields default
                # to NULL) but surprising — emit a debug record so it can
                # be correlated during investigations.
                logger.debug(
                    "column %r absent from batch; filling with nulls",
                    field.name,
                )
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
