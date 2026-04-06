"""Parquet formatter implementation.

Parquet is a columnar storage format optimized for analytics workloads.
Requires pyarrow: poetry install -E analytics
"""

from typing import Any, BinaryIO, Dict, List, Optional

from .base import BaseFormatter


class ParquetFormatter(BaseFormatter):
    """
    Formatter for Apache Parquet format.

    Parquet is a columnar format that provides:
    - Efficient compression
    - Column pruning for selective reads
    - Predicate pushdown for filtering
    - Schema preservation

    Configuration options:
    - compression: Compression codec (snappy, gzip, zstd, none). Default: snappy
    - row_group_size: Number of rows per row group. Default: 10000
    - version: Parquet version ('1.0', '2.4', '2.6'). Default: '2.6'

    Requires: pyarrow (install with: poetry install -E analytics)
    """

    @property
    def format_name(self) -> str:
        """Return the format identifier."""
        return "parquet"

    @property
    def file_extension(self) -> str:
        """Return the file extension."""
        return ".parquet"

    @property
    def is_binary(self) -> bool:
        """Parquet is a binary format."""
        return True

    @property
    def content_type(self) -> str:
        """Return the MIME content type."""
        return "application/vnd.apache.parquet"

    def _ensure_pyarrow(self) -> None:
        """Ensure pyarrow is available."""
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            raise ImportError(
                "ParquetFormatter requires pyarrow. "
                "Install with: poetry install -E analytics"
            )

    def _infer_schema(
        self,
        records: List[Dict[str, Any]],
        schema: Optional[Dict[str, Any]] = None,
    ):
        """
        Infer PyArrow schema from records or JSON Schema.

        Args:
            records: List of records
            schema: Optional JSON Schema

        Returns:
            PyArrow schema
        """
        import pyarrow as pa

        if schema and "properties" in schema:
            # Build schema from JSON Schema
            fields = []
            for name, prop in schema["properties"].items():
                pa_type = self._json_type_to_arrow(prop)
                nullable = name not in schema.get("required", [])
                fields.append(pa.field(name, pa_type, nullable=nullable))
            return pa.schema(fields)

        # Infer from data - let PyArrow handle it
        return None

    def _json_type_to_arrow(self, prop: Dict[str, Any]):
        """
        Convert JSON Schema type to PyArrow type.

        Args:
            prop: JSON Schema property definition

        Returns:
            PyArrow type
        """
        import pyarrow as pa

        json_type = prop.get("type", "string")
        json_format = prop.get("format")

        # Handle arrays
        if json_type == "array":
            items = prop.get("items", {})
            item_type = self._json_type_to_arrow(items)
            return pa.list_(item_type)

        # Handle objects as JSON strings
        if json_type == "object":
            return pa.string()

        # Handle primitives
        type_mapping = {
            "string": pa.string(),
            "integer": pa.int64(),
            "number": pa.float64(),
            "boolean": pa.bool_(),
            "null": pa.null(),
        }

        # Handle format specifiers
        if json_type == "string":
            if json_format == "date-time":
                return pa.timestamp("us", tz="UTC")
            if json_format == "date":
                return pa.date32()
            if json_format == "time":
                return pa.time64("us")

        if json_type == "integer":
            # Check for specific integer types
            if prop.get("format") == "int32":
                return pa.int32()

        return type_mapping.get(json_type, pa.string())

    def serialize_batch(
        self,
        records: List[Dict[str, Any]],
        schema: Optional[Dict[str, Any]] = None,
    ) -> bytes:
        """
        Serialize a batch of records to Parquet format.

        Args:
            records: List of record dictionaries
            schema: Optional JSON Schema

        Returns:
            Parquet file bytes
        """
        self._ensure_pyarrow()
        import pyarrow as pa
        import pyarrow.parquet as pq

        if not records:
            return b""

        # Convert records to PyArrow Table
        pa_schema = self._infer_schema(records, schema)

        # Prepare records for conversion
        prepared_records = self._prepare_records(records)

        if pa_schema:
            table = pa.Table.from_pylist(prepared_records, schema=pa_schema)
        else:
            table = pa.Table.from_pylist(prepared_records)

        # Get configuration options
        compression = self._config.get("compression", "snappy")
        if compression == "none":
            compression = None

        row_group_size = self._config.get("row_group_size", 10000)
        version = self._config.get("version", "2.6")

        # Write to buffer
        buffer = pa.BufferOutputStream()
        pq.write_table(
            table,
            buffer,
            compression=compression,
            row_group_size=row_group_size,
            version=version,
        )

        return buffer.getvalue().to_pybytes()

    def write_batch_to_stream(
        self,
        records: List[Dict[str, Any]],
        stream: BinaryIO,
        schema: Optional[Dict[str, Any]] = None,
        append: bool = True,
    ) -> int:
        """
        Write a batch of records directly to a stream.

        Note: Parquet doesn't support true append mode. Each call creates
        a complete Parquet file. For appending, use separate files and
        combine them later.

        Args:
            records: List of record dictionaries
            stream: Binary stream to write to
            schema: Optional JSON Schema
            append: Ignored for Parquet (always writes complete file)

        Returns:
            Number of bytes written
        """
        data = self.serialize_batch(records, schema)
        stream.write(data)
        return len(data)

    def _prepare_records(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Prepare records for Parquet conversion.

        Converts complex types to JSON strings where needed.

        Args:
            records: Raw records

        Returns:
            Prepared records
        """
        import json

        prepared = []
        for record in records:
            row = {}
            for key, value in record.items():
                # Convert complex objects to JSON strings
                if isinstance(value, dict):
                    row[key] = json.dumps(value)
                else:
                    row[key] = value
            prepared.append(row)

        return prepared
