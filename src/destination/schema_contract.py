"""Arrow-based schema contract for connector endpoints.

The contract is the single place where an endpoint's declared types
are turned into a ``pa.Schema``. Every column/property in every
endpoint document MUST declare a fully-qualified canonical Arrow type
via the ``arrow_type`` field. No inference, no JSON-Schema
``type``/``format`` heuristics, no native-SQL fallbacks.

Two endpoint shapes are supported:

* ``"columns"`` — database endpoints. Each column declares
  ``arrow_type`` (e.g. ``"Int64"``, ``"Decimal128(38, 9)"``,
  ``"Timestamp(MICROSECOND, UTC)"``).
* ``"properties"`` — JSON-Schema endpoints (APIs). Same vocabulary.

Both resolve through :func:`canonical_to_arrow`. Anything missing or
unparseable raises.

The contract has two entry points:

* Source side — :meth:`from_pylist` builds one Arrow column per field
  directly from the input values, using the declared ``arrow_type``
  and (for date/timestamp fields) the declared ``source_format``
  strptime pattern. No Python ``datetime`` intermediate. No
  fallbacks.
* Destination side — :meth:`cast_arrow_batch` aligns an incoming
  batch to its declared schema. Missing columns become typed nulls
  (when nullable); type mismatches go through a single ``pc.cast``.
"""

import logging
from decimal import Decimal
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.compute as pc

from src.engine.type_map import canonical_to_arrow

logger = logging.getLogger(__name__)


class SchemaContract:
    """Arrow schema mapping for a connector endpoint.

    Built once from an endpoint schema and reused for every batch.
    """

    def __init__(self, endpoint_schema: Dict[str, Any]) -> None:
        """Build the Arrow schema from an endpoint definition.

        Args:
            endpoint_schema: Endpoint definition (``columns`` array or
                JSON-Schema ``properties`` object). Every declared
                column/property must carry an ``arrow_type``.
        """
        if "columns" in endpoint_schema:
            field_defs = endpoint_schema.get("columns") or []
            if not field_defs:
                raise ValueError(
                    "SchemaContract: 'columns' is present but empty; the "
                    "contract must declare every column"
                )
            self._arrow_schema, self._field_defs = self._schema_from_columns(
                field_defs
            )
        elif "properties" in endpoint_schema:
            properties = endpoint_schema.get("properties") or {}
            if not properties:
                raise ValueError(
                    "SchemaContract: 'properties' is present but empty; "
                    "the contract must declare every field"
                )
            required = set(endpoint_schema.get("required", []))
            self._arrow_schema, self._field_defs = self._schema_from_properties(
                properties, required
            )
        else:
            raise ValueError(
                "SchemaContract: endpoint schema must declare either "
                "'columns' (database endpoint) or 'properties' (JSON-Schema "
                f"endpoint); got keys {sorted(endpoint_schema.keys())!r}"
            )

        self._column_types: Dict[str, str] = {
            f.name: str(f.type) for f in self._arrow_schema
        }
        logger.debug(
            "Built schema contract with %d fields", len(self._arrow_schema),
        )

    @property
    def arrow_schema(self) -> pa.Schema:
        """The canonical Arrow schema for this endpoint."""
        return self._arrow_schema

    @property
    def column_types(self) -> Dict[str, str]:
        """Mapping of column names to Arrow type strings."""
        return self._column_types

    def from_pylist(self, records: List[Dict[str, Any]]) -> pa.RecordBatch:
        """Build a record batch from dict rows using this endpoint's schema.

        For each declared field, one Arrow column is built directly:

        * If the field declares a ``source_format`` (strptime pattern)
          and the target is Timestamp or Date, the column is built as
          a string array and parsed in one ``pc.strptime`` call. No
          Python ``datetime`` intermediate.
        * Otherwise the column is built via
          ``pa.array(values, type=target)``. PyArrow accepts driver-
          native Python types (``int`` for Int64, ``Decimal``/``str``
          for Decimal128, ``datetime`` for Timestamp, ``str`` for
          Utf8, etc.).

        Any failure raises immediately — there is no fallback chain.
        """
        if not records:
            return pa.RecordBatch.from_pylist([], schema=self._arrow_schema)

        arrays: List[pa.Array] = []
        for field in self._arrow_schema:
            values = [r.get(field.name) for r in records]
            field_def = self._field_defs.get(field.name) or {}
            source_format = field_def.get("source_format")
            try:
                arrays.append(self._build_column(field, values, source_format))
            except (pa.ArrowTypeError, pa.ArrowInvalid, ValueError) as e:
                raise ValueError(
                    f"column {field.name!r}: cannot build "
                    f"{field.type} from source values: {e}"
                ) from e
        return pa.RecordBatch.from_arrays(arrays, schema=self._arrow_schema)

    def cast_arrow_batch(self, record_batch: pa.RecordBatch) -> pa.RecordBatch:
        """Cast an incoming Arrow batch to this endpoint's schema.

        Each column either matches the target type (no-op), is absent
        and nullable (typed nulls), or is converted through a single
        ``pc.cast``. No per-type heuristics — the source contract is
        responsible for producing values that cast cleanly.
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
                if not field.nullable:
                    raise ValueError(
                        f"column {field.name!r} is required by the destination "
                        f"schema but absent from the incoming batch"
                    )
                arrays.append(pa.nulls(record_batch.num_rows, type=field.type))
                continue
            if col.type == field.type:
                arrays.append(col)
                continue
            try:
                arrays.append(pc.cast(col, field.type, safe=False))
            except Exception as e:
                raise ValueError(
                    f"column {field.name!r}: cannot cast "
                    f"{col.type} → {field.type}: {e}"
                ) from e
        return pa.RecordBatch.from_arrays(arrays, schema=self._arrow_schema)

    @staticmethod
    def to_dicts(batch: Any) -> List[Dict[str, Any]]:
        """Convert an Arrow ``Table`` or ``RecordBatch`` to dicts.

        Call once, at the SQLAlchemy boundary, immediately before
        ``insert().values(...)``. Anywhere else, keep the data in
        Arrow.
        """
        return batch.to_pylist()

    # ------------------------------------------------------------------
    # Column construction
    # ------------------------------------------------------------------

    @staticmethod
    def _build_column(
        field: pa.Field,
        values: List[Any],
        source_format: Optional[str],
    ) -> pa.Array:
        """Build one Arrow column directly from the input values.

        Routes through ``pc.strptime`` for Timestamp / Date fields
        with a declared ``source_format``. Otherwise uses
        ``pa.array(values, type=target)``. All-null columns get
        typed nulls so the column carries the declared type rather
        than Arrow's bare ``null``.
        """
        if all(v is None for v in values):
            return pa.nulls(len(values), type=field.type)

        if source_format and (
            pa.types.is_timestamp(field.type) or pa.types.is_date(field.type)
        ):
            string_col = pa.array(values, type=pa.string())
            unit = (
                getattr(field.type, "unit", None)
                or ("us" if pa.types.is_timestamp(field.type) else "s")
            )
            parsed = pc.strptime(string_col, format=source_format, unit=unit)
            if parsed.type == field.type:
                return parsed
            tz = getattr(field.type, "tz", None)
            if tz and not getattr(parsed.type, "tz", None):
                parsed = pc.assume_timezone(parsed, tz)
            return pc.cast(parsed, field.type, safe=False)

        if pa.types.is_decimal(field.type):
            converted = [
                None if v is None else Decimal(str(v)) for v in values
            ]
            return pa.array(converted, type=field.type)

        return pa.array(values, type=field.type)

    # ------------------------------------------------------------------
    # Schema construction
    # ------------------------------------------------------------------

    @staticmethod
    def _schema_from_columns(
        columns: List[Dict[str, Any]],
    ) -> tuple[pa.Schema, Dict[str, Dict[str, Any]]]:
        fields = []
        defs: Dict[str, Dict[str, Any]] = {}
        for index, col in enumerate(columns):
            name = col.get("name")
            if not name:
                raise ValueError(
                    f"column at index {index} has no 'name' field; "
                    f"unnamed columns indicate a malformed endpoint payload"
                )
            arrow_type = SchemaContract._require_arrow_type(col, name)
            nullable = bool(col.get("nullable", True))
            fields.append(pa.field(name, arrow_type, nullable=nullable))
            defs[name] = col
        return pa.schema(fields), defs

    @staticmethod
    def _schema_from_properties(
        properties: Dict[str, Any], required: set,
    ) -> tuple[pa.Schema, Dict[str, Dict[str, Any]]]:
        fields = []
        defs: Dict[str, Dict[str, Any]] = {}
        for name, prop in properties.items():
            arrow_type = SchemaContract._require_arrow_type(prop, name)
            fields.append(
                pa.field(name, arrow_type, nullable=name not in required)
            )
            defs[name] = prop
        return pa.schema(fields), defs

    @staticmethod
    def _require_arrow_type(field_def: Dict[str, Any], name: str) -> pa.DataType:
        """Resolve the declared canonical ``arrow_type`` to a ``pa.DataType``.

        Every endpoint field must declare a fully-qualified canonical
        Arrow type. Missing, empty, or unparseable declarations raise.
        """
        canonical = field_def.get("arrow_type")
        if not canonical:
            raise ValueError(
                f"field {name!r} has no 'arrow_type' declaration; "
                f"endpoint contracts must declare a fully-qualified "
                f"canonical Arrow type for every field"
            )
        try:
            return canonical_to_arrow(canonical)
        except Exception as e:
            raise ValueError(
                f"field {name!r}: cannot parse arrow_type "
                f"{canonical!r}: {e}"
            ) from e
