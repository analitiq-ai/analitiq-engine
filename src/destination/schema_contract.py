"""Arrow-based schema contract for connector endpoints.

The contract is the single seam where an endpoint's declared types
(database ``columns`` or JSON-Schema ``properties``) are turned into a
``pa.Schema``. It is used on both ends of the pipeline:

- **Source side** — connectors call :meth:`from_pylist` to materialize
  Arrow once, at the source boundary, when the underlying driver hands
  back dicts (JSON APIs, SQLAlchemy row mappings).
- **Destination side** — handlers call :meth:`cast_arrow_batch` to align
  an incoming Arrow batch with the destination's column types, then
  :meth:`to_dicts` once at the SQLAlchemy boundary.

Two endpoint schema shapes are supported:

- ``"columns"`` (database endpoints, native SQL types): native→Arrow
  translation is delegated to the connector's ``type-map.json`` via
  :class:`TypeMapper`.
- ``"properties"`` (JSON-Schema endpoints, API-style types): translated
  directly by :meth:`_json_schema_to_arrow`.

Either path raises on unknown types — no silent fallback to ``Utf8``.
"""

import logging
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.compute as pc

from src.engine.type_map import TypeMapper, canonical_to_arrow

logger = logging.getLogger(__name__)


class SchemaContract:
    """Arrow schema mapping for a connector endpoint.

    Built once from an endpoint schema and reused for every batch. The
    contract owns the canonical ``pa.Schema`` for that endpoint and the
    type coercions required to align other batches to it.

    Supports two endpoint schema formats:

    1. ``columns`` array (database endpoints, native SQL types):
       ``[{"name": "id", "native_type": "BIGINT", "nullable": true}, ...]``
    2. JSON Schema ``properties`` (API endpoints):
       ``{"properties": {"id": {"type": "integer"}, ...}}``
    """

    def __init__(
        self,
        endpoint_schema: Dict[str, Any],
        *,
        type_mapper: Optional[TypeMapper] = None,
    ) -> None:
        """Build the Arrow schema from an endpoint definition.

        Args:
            endpoint_schema: Endpoint definition (columns array or JSON Schema).
            type_mapper: Connector's ``TypeMapper``. Required when the
                schema uses the ``columns`` array format (native SQL
                types). JSON Schema payloads do not use it.
        """
        self._columns: List[Dict[str, Any]] = []
        self._type_mapper = type_mapper

        if "columns" in endpoint_schema:
            if type_mapper is None:
                raise ValueError(
                    "SchemaContract: type_mapper is required for "
                    "'columns' schema payloads (native SQL types cannot be "
                    "interpreted without the connector's type-map)"
                )
            self._columns = endpoint_schema.get("columns", [])
            self._arrow_schema = self._build_arrow_schema()
        elif "properties" in endpoint_schema:
            self._arrow_schema = self._build_arrow_schema_from_json_schema(
                endpoint_schema
            )
        else:
            self._arrow_schema = pa.schema([])

        self._column_types: Dict[str, str] = {
            f.name: str(f.type) for f in self._arrow_schema
        }

        logger.debug("Built schema contract with %d fields", len(self._arrow_schema))

    @property
    def arrow_schema(self) -> pa.Schema:
        """The canonical Arrow schema for this endpoint."""
        return self._arrow_schema

    @property
    def column_types(self) -> Dict[str, str]:
        """Mapping of column names to Arrow type strings."""
        return self._column_types

    def from_pylist(self, records: List[Dict[str, Any]]) -> pa.RecordBatch:
        """Materialize a record batch from dicts using this endpoint's schema.

        Source-side entry point: connectors that receive dict-shaped rows
        (JSON parse output, SQLAlchemy row mappings) call this once at
        the connector boundary so the rest of the pipeline carries Arrow.
        """
        return pa.RecordBatch.from_pylist(records, schema=self._arrow_schema)

    def cast_arrow_batch(self, record_batch: pa.RecordBatch) -> pa.RecordBatch:
        """Cast an incoming Arrow batch to this endpoint's schema.

        Destination-side entry point: takes a batch shaped by the source
        and aligns it column-by-column to the destination's types. Missing
        columns are filled with nulls; extra columns are dropped.
        """
        if record_batch.num_rows == 0:
            return pa.RecordBatch.from_pylist([], schema=self._arrow_schema)

        existing = {
            name: record_batch.column(i)
            for i, name in enumerate(record_batch.schema.names)
        }
        arrays: List[pa.Array] = []
        for field in self._arrow_schema:
            col = existing.get(field.name)
            if col is None:
                logger.debug(
                    "column %r absent from batch; filling with nulls", field.name
                )
                arrays.append(pa.nulls(record_batch.num_rows, type=field.type))
                continue
            if col.type == field.type:
                arrays.append(col)
                continue
            try:
                arrays.append(self._safe_cast_array(col, field.type, field.name))
            except Exception as e:
                raise ValueError(
                    f"column {field.name!r}: cannot cast "
                    f"{col.type} → {field.type}: {e}"
                ) from e
        return pa.RecordBatch.from_arrays(arrays, schema=self._arrow_schema)

    @staticmethod
    def to_dicts(batch: Any) -> List[Dict[str, Any]]:
        """Materialize an Arrow ``Table`` or ``RecordBatch`` to dicts.

        This is the SQLAlchemy boundary — call once, immediately before
        ``insert().values(...)``. Anywhere else, prefer to keep the data
        in Arrow.
        """
        return batch.to_pylist()

    # ------------------------------------------------------------------
    # Schema construction
    # ------------------------------------------------------------------

    def _build_arrow_schema_from_json_schema(
        self, json_schema: Dict[str, Any]
    ) -> pa.Schema:
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
            return pa.string()
        raise ValueError(
            f"JSON Schema property has unsupported type/format "
            f"(type={json_type!r}, format={fmt!r})"
        )

    def _build_arrow_schema(self) -> pa.Schema:
        assert self._type_mapper is not None  # guarded in __init__
        fields = []
        for index, col in enumerate(self._columns):
            col_name = col.get("name")
            if not col_name:
                raise ValueError(
                    f"schema column at index {index} has no 'name' field; "
                    f"unnamed columns indicate a malformed endpoint payload"
                )
            col_type = col.get("native_type") or col.get("type")
            if not col_type:
                raise ValueError(
                    f"column {col_name!r} has no 'native_type' field"
                )
            nullable = col.get("nullable", True)
            canonical = self._type_mapper.to_canonical(col_type)
            arrow_type = canonical_to_arrow(canonical)
            fields.append(pa.field(col_name, arrow_type, nullable=nullable))
        return pa.schema(fields)

    # ------------------------------------------------------------------
    # Casting helpers
    # ------------------------------------------------------------------

    def _safe_cast_array(
        self, col: pa.Array, target_type: pa.DataType, col_name: str
    ) -> pa.Array:
        """Type-aware cast for a single Arrow ``Array``.

        Handles the messy real-world conversions: string→timestamp with
        multiple format fallbacks, naive→tz-aware promotion (e.g. Wise's
        ``"2026-03-23 10:18:24"`` shipped to a tz-aware timestamp column),
        and string/numeric→decimal via float.
        """
        source_type = col.type

        if pa.types.is_timestamp(target_type) and pa.types.is_string(source_type):
            target_tz = getattr(target_type, "tz", None)
            target_unit = getattr(target_type, "unit", "us") or "us"
            for fmt in (
                "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d",
            ):
                try:
                    parsed = pc.strptime(col, format=fmt, unit=target_unit)
                except Exception:
                    continue
                parsed_tz = getattr(parsed.type, "tz", None)
                if target_tz and not parsed_tz:
                    parsed = pc.assume_timezone(parsed, target_tz)
                if parsed.type != target_type:
                    parsed = pc.cast(parsed, target_type, safe=False)
                return parsed

        if pa.types.is_date(target_type) and pa.types.is_string(source_type):
            try:
                return pc.strptime(col, format="%Y-%m-%d", unit="s").cast(target_type)
            except Exception:
                pass

        if pa.types.is_decimal(target_type):
            if pa.types.is_string(source_type):
                try:
                    float_col = pc.cast(col, pa.float64())
                    return pc.cast(float_col, target_type, safe=False)
                except Exception:
                    pass
            elif pa.types.is_floating(source_type) or pa.types.is_integer(source_type):
                return pc.cast(col, target_type, safe=False)

        return pc.cast(col, target_type, safe=False)
