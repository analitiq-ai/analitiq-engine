"""Build pa.Schema from endpoint arrow_type declarations."""

import json
import logging
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.compute as pc

from src.engine.type_map import resolve_arrow_type
from src.engine.type_map.exceptions import InvalidTypeMapError

logger = logging.getLogger(__name__)


# Opaque-blob marker. The Arrow wire shape is ``pa.large_string``;
# encode/decode happens at the source and handler boundaries.
_JSON_ARROW_TYPE: str = "Json"


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

        Arrow-space schema alignment, then ``to_pylist``, then Json
        column normalisation. JSON columns are guaranteed to be either
        ``None`` or a serialised JSON string on the way out -- not a
        Python dict / list.

        Why pre-serialise instead of relying on SQLAlchemy's JSON /
        JSONB ``bind_processor``? SA + asyncpg's batched-INSERT path
        (executemany ``values_plus_batch``) does not always invoke the
        column-level bind_processor for every row, so a Python dict
        can reach asyncpg unserialised and trip ``DataError: expected
        str, got dict`` against the JSONB binary protocol. Producing
        strings here is dialect-agnostic and side-steps the bind-time
        ambiguity entirely. PG accepts the string as JSONB text input;
        other dialects pass it through their TEXT/JSON column types.

        ``datetime`` / ``Decimal`` / ``date`` still pass through as
        Python objects -- SA's adapters handle those uniformly across
        dialects and the issue above is specific to dict/list bindings
        against the JSON column family.
        """
        record_batch = self.cast_arrow_batch(record_batch)
        records = record_batch.to_pylist()
        records = self.decode_json_columns(records)
        return self.encode_json_columns(records)

    def encode_json_columns(
        self, records: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Serialise dict / list values in JSON columns to JSON strings.

        Inverse of :meth:`decode_json_columns`. Idempotent: ``None``
        and already-encoded string values pass through untouched.
        Mutates the input records in place (consistent with
        ``decode_json_columns``) and returns them for chaining.
        """
        json_cols = self.json_columns
        if not json_cols or not records:
            return records
        for record in records:
            for col in json_cols:
                value = record.get(col)
                if value is None or isinstance(value, str):
                    continue
                # dict / list / tuple — anything else would have been
                # rejected at ``cast_arrow_batch`` time when the JSON
                # column was built from the Arrow batch.
                record[col] = json.dumps(value)
        return records

    def decode_json_columns(
        self, records: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Parse JSON-encoded string values back into Python dict/list.

        Decode is idempotent: ``None`` and already-parsed dict/list values
        pass through untouched so the caller can apply this more than
        once. Malformed strings raise ``ValueError`` carrying the column
        name and row index — the destination handler classifies that as
        a data-shape failure rather than letting a bare
        ``JSONDecodeError`` escape from deep in the stack.
        """
        json_cols = self.json_columns
        if not json_cols or not records:
            return records
        for row, record in enumerate(records):
            for col in json_cols:
                value = record.get(col)
                if not isinstance(value, str):
                    continue
                try:
                    record[col] = json.loads(value)
                except json.JSONDecodeError as exc:
                    raise ValueError(
                        f"Json column {col!r} at row {row}: value is not "
                        f"valid JSON ({exc})"
                    ) from exc
        return records

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
