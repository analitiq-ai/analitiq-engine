"""Build pa.Schema from endpoint arrow_type declarations."""

import json
import logging
import math
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.compute as pc

from .json_utils import decode_json_fields
from .type_map import resolve_arrow_type
from .type_map.exceptions import InvalidTypeMapError

logger = logging.getLogger(__name__)


# Opaque-blob marker. The Arrow wire shape is ``pa.large_string``;
# encode/decode happens at the source and handler boundaries.
_JSON_ARROW_TYPE: str = "Json"

# Largest finite magnitude per narrow float width. A finite Python float
# (float64) above these silently overflows to +/-inf when stored in the
# narrower Arrow array, so it must be rejected per-row instead. float64
# needs no entry: math.isfinite already covers it.
_FLOAT_MAX_BY_BIT_WIDTH: Dict[int, float] = {
    16: 65504.0,
    32: math.ldexp(2 - 2**-23, 127),  # 3.4028234663852886e+38
}


def _is_json_field(field_def: Dict[str, Any]) -> bool:
    return field_def.get("arrow_type") == _JSON_ARROW_TYPE


class SchemaContract:
    """Arrow schema mapping for a connector endpoint."""

    def __init__(self, endpoint_schema: Dict[str, Any]) -> None:
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
        return self._arrow_schema

    @property
    def column_types(self) -> Dict[str, str]:
        return self._column_types

    @property
    def json_columns(self) -> set:
        return {n for n, defn in self._field_defs.items() if _is_json_field(defn)}

    def to_db_records(
        self, record_batch: pa.RecordBatch
    ) -> List[Dict[str, Any]]:
        """Materialise a batch for a SQL destination.

        JSON columns stay as wire-format strings (their Arrow shape is
        ``pa.large_string``) — they bind directly into TEXT / JSONB
        columns without per-row coercion. ``datetime`` / ``Decimal`` /
        ``date`` pass through as Python objects; SA's column adapters
        handle them uniformly across dialects.
        """
        record_batch = self.cast_arrow_batch(record_batch)
        return record_batch.to_pylist()

    def decode_json_columns(
        self, records: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Parse JSON-encoded string values into Python dict/list.

        Available for callers that need decoded objects (e.g. an API
        destination handing values to ``orjson``). The SQL destination
        path keeps strings — JSON columns bind directly to TEXT/JSONB
        without coercion.

        Idempotent on ``None`` and already-parsed dict/list values.
        Malformed strings raise ``ValueError`` carrying the column
        name and row index.
        """
        return decode_json_fields(records, self.json_columns)

    def from_pylist(self, records: List[Dict[str, Any]]) -> pa.RecordBatch:
        """Build a record batch from dict rows using this endpoint's schema."""
        if not records:
            return pa.RecordBatch.from_pylist([], schema=self._arrow_schema)

        arrays: List[pa.Array] = []
        for field in self._arrow_schema:
            values = [r.get(field.name) for r in records]
            field_def = self._field_defs.get(field.name) or {}
            try:
                arrays.append(self._build_column(field, values, field_def))
            except ValueError:
                # _build_column already names the offending row; passing
                # it through preserves that precision instead of pinning
                # the blame on the first-non-null heuristic below.
                raise
            except (pa.ArrowTypeError, pa.ArrowInvalid) as e:
                bad_index = next(
                    (i for i, v in enumerate(values) if v is not None), None
                )
                raise ValueError(
                    f"column {field.name!r}: cannot build "
                    f"{field.type} from source values "
                    f"(first non-null at row {bad_index}): {e}"
                ) from e
        return pa.RecordBatch.from_arrays(arrays, schema=self._arrow_schema)

    def cast_arrow_batch(self, record_batch: pa.RecordBatch) -> pa.RecordBatch:
        """Cast an incoming Arrow batch to this endpoint's schema."""
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
                logger.warning(
                    "column %r absent from incoming batch; filling with typed nulls",
                    field.name,
                )
                arrays.append(pa.nulls(record_batch.num_rows, type=field.type))
                continue
            if col.type == field.type:
                arrays.append(col)
                continue
            try:
                arrays.append(pc.cast(col, field.type, safe=False))
            except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as e:
                raise ValueError(
                    f"column {field.name!r}: cannot cast "
                    f"{col.type} → {field.type}: {e}"
                ) from e
        return pa.RecordBatch.from_arrays(arrays, schema=self._arrow_schema)

    @staticmethod
    def to_dicts(batch: Any) -> List[Dict[str, Any]]:
        """Convert an Arrow ``Table`` or ``RecordBatch`` to dicts."""
        return batch.to_pylist()

    @staticmethod
    def _build_column(
        field: pa.Field,
        values: List[Any],
        field_def: Dict[str, Any],
    ) -> pa.Array:
        source_format = field_def.get("source_format")
        if all(v is None for v in values):
            if not field.nullable:
                raise ValueError(
                    f"column {field.name!r} is non-nullable but every "
                    f"source value is None"
                )
            return pa.nulls(len(values), type=field.type)

        if _is_json_field(field_def):
            # Opaque JSON blob: a Json column may carry only None, a dict,
            # or a list. Anything else (int, datetime, raw string) is an
            # author mistake — fail loud with the offending row index so
            # the source author can locate the bad value, rather than
            # letting a string round-trip through the decoder where
            # ``json.loads`` would raise far from the source.
            serialized: List[Any] = []
            for row, v in enumerate(values):
                if v is None:
                    serialized.append(None)
                elif isinstance(v, (dict, list)):
                    serialized.append(json.dumps(v))
                else:
                    raise ValueError(
                        f"column {field.name!r} declared arrow_type='Json' "
                        f"but row {row} carries {type(v).__name__}; only "
                        f"dict, list, or None are accepted"
                    )
            return pa.array(serialized, type=field.type)

        if source_format and (
            pa.types.is_timestamp(field.type) or pa.types.is_date(field.type)
        ):
            for v in values:
                if v is not None and not isinstance(v, str):
                    raise TypeError(
                        f"column {field.name!r} declares source_format but a "
                        f"non-string value of type {type(v).__name__} was found; "
                        f"source_format only applies to string inputs"
                    )
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

        if (
            pa.types.is_timestamp(field.type)
            or pa.types.is_date(field.type)
            or pa.types.is_time(field.type)
        ) and any(isinstance(v, str) for v in values if v is not None):
            return SchemaContract._build_temporal_from_strings(field, values)

        if (
            pa.types.is_integer(field.type) or pa.types.is_floating(field.type)
        ) and any(isinstance(v, str) for v in values if v is not None):
            return SchemaContract._build_numeric_from_strings(field, values)

        return pa.array(values, type=field.type)

    @staticmethod
    def _build_temporal_from_strings(
        field: pa.Field, values: List[Any]
    ) -> pa.Array:
        """Parse ISO-8601 strings into a timestamp / date / time column.

        Triggered for JSON-Schema ``format: date-time | date | time`` fields
        whose endpoint declares an Arrow temporal ``arrow_type`` but does not
        pin a ``source_format``. PyArrow refuses to coerce strings into a
        timestamp/date/time array directly, so we parse with the stdlib
        first and hand typed Python objects to ``pa.array``.
        """
        is_ts = pa.types.is_timestamp(field.type)
        is_date = pa.types.is_date(field.type)
        tz = getattr(field.type, "tz", None) if is_ts else None

        parsed: List[Any] = []
        for row, v in enumerate(values):
            if v is None:
                parsed.append(None)
                continue
            if not isinstance(v, str):
                parsed.append(v)
                continue
            try:
                if is_ts:
                    dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
                    if tz and dt.tzinfo is None:
                        raise ValueError(
                            f"value {v!r} is naive but column declares tz={tz!r}"
                        )
                    if not tz and dt.tzinfo is not None:
                        dt = dt.replace(tzinfo=None)
                    parsed.append(dt)
                elif is_date:
                    parsed.append(date.fromisoformat(v[:10]))
                else:
                    parsed.append(time.fromisoformat(v))
            except ValueError as exc:
                raise ValueError(
                    f"column {field.name!r} at row {row}: cannot parse "
                    f"{v!r} as {field.type}: {exc}"
                ) from exc
        return pa.array(parsed, type=field.type)

    @staticmethod
    def _build_numeric_from_strings(
        field: pa.Field, values: List[Any]
    ) -> pa.Array:
        """Coerce string representations of numbers into an integer or float column.

        Triggered when the declared Arrow type is an integer or float family
        and at least one non-null source value is a string (e.g. JSON APIs
        that encode numbers as ``"0"``, ``"14.5"``). None values become typed
        nulls.

        Every non-null value — parsed string or native numeric — passes the
        same per-row checks, so identical author intent behaves identically
        regardless of input shape:

        - integer columns accept only ``int`` (or an integer string) within
          the declared width; a native float would silently truncate inside
          ``pa.array`` (1.5 -> 1) and is rejected like the string ``"1.5"``
        - float columns reject non-finite values and values whose magnitude
          exceeds the declared width's largest finite float, which would
          silently overflow to +/-inf inside the narrower Arrow array
          (e.g. ``"1e40"`` in a Float32 column)
        - ``bool`` is rejected — it is a Python subclass of ``int`` and
          would silently coerce to 0/1 otherwise

        Raises ``ValueError`` naming the column and row on every rejection.
        """
        is_int = pa.types.is_integer(field.type)
        if is_int:
            bits = field.type.bit_width
            if pa.types.is_signed_integer(field.type):
                lo, hi = -(1 << (bits - 1)), (1 << (bits - 1)) - 1
            else:
                lo, hi = 0, (1 << bits) - 1
        else:
            float_max = _FLOAT_MAX_BY_BIT_WIDTH.get(field.type.bit_width)

        converted: List[Any] = []
        for row, v in enumerate(values):
            if v is None:
                converted.append(None)
                continue
            if isinstance(v, bool):
                raise ValueError(
                    f"column {field.name!r} at row {row}: expected numeric or "
                    f"numeric string, got bool {v!r}; declare arrow_type='Boolean' "
                    f"or fix the source mapping"
                )
            if isinstance(v, str):
                try:
                    parsed = int(v) if is_int else float(v)
                except (ValueError, OverflowError) as exc:
                    raise ValueError(
                        f"column {field.name!r} at row {row}: cannot parse "
                        f"{v!r} as {field.type}: {exc}"
                    ) from exc
            elif isinstance(v, int) or (not is_int and isinstance(v, float)):
                parsed = v
            else:
                raise ValueError(
                    f"column {field.name!r} at row {row}: expected numeric or "
                    f"numeric string for {field.type}, got "
                    f"{type(v).__name__} {v!r}"
                )
            if is_int:
                if not lo <= parsed <= hi:
                    raise ValueError(
                        f"column {field.name!r} at row {row}: value {v!r} "
                        f"out of range for {field.type}"
                    )
            else:
                try:
                    parsed = float(parsed)
                except OverflowError as exc:
                    raise ValueError(
                        f"column {field.name!r} at row {row}: value {v!r} "
                        f"out of range for {field.type}: {exc}"
                    ) from exc
                if not math.isfinite(parsed):
                    raise ValueError(
                        f"column {field.name!r} at row {row}: value {v!r} "
                        f"is non-finite; use None for missing values"
                    )
                if float_max is not None and abs(parsed) > float_max:
                    raise ValueError(
                        f"column {field.name!r} at row {row}: value {v!r} "
                        f"overflows {field.type} (largest finite magnitude "
                        f"{float_max!r})"
                    )
            converted.append(parsed)
        return pa.array(converted, type=field.type)

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
        if not field_def.get("arrow_type"):
            raise ValueError(
                f"field {name!r} has no 'arrow_type' declaration; "
                f"endpoint contracts must declare an Arrow type for every field"
            )
        try:
            return resolve_arrow_type(field_def, where=f"field {name!r}")
        except InvalidTypeMapError as e:
            raise ValueError(
                f"field {name!r}: cannot parse arrow_type "
                f"{field_def.get('arrow_type')!r}: {e}"
            ) from e
