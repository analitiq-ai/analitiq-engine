"""Build pa.Schema from endpoint arrow_type declarations."""

import base64
import json
import logging
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


# ---------------------------------------------------------------------------
# Arrow → JSON-shape cast (used by JSON-emitting destinations)
# ---------------------------------------------------------------------------


def arrow_to_json_shape(batch: pa.RecordBatch) -> pa.RecordBatch:
    """Cast Arrow columns into JSON-native shape.

    Arrow's native ``to_pylist`` materialises ``pa.timestamp`` as
    ``datetime``, ``pa.decimal128`` as ``Decimal``, ``pa.binary`` as
    ``bytes``, ``pa.time`` as ``time`` — none of which ``json.dumps``
    serialises. This helper does the conversion in Arrow space (vectorised
    via ``pc.strftime`` / ``pc.cast``) so the materialised dict carries only
    JSON-native primitives and the destination handler stops needing a
    custom JSON encoder.

    Recurses into struct and list types so a nested timestamp inside an
    Object marker or List(Object) gets converted too. Null masks on
    parent struct rows are preserved.
    """
    new_fields: List[pa.Field] = []
    new_arrays: List[pa.Array] = []
    for index, field in enumerate(batch.schema):
        f, arr = _column_to_json_shape(field, batch.column(index))
        new_fields.append(f)
        new_arrays.append(arr)
    return pa.RecordBatch.from_arrays(new_arrays, schema=pa.schema(new_fields))


def _retype(field: pa.Field, new_type: pa.DataType) -> pa.Field:
    return pa.field(field.name, new_type, nullable=field.nullable)


def _column_to_json_shape(
    field: pa.Field, column: pa.Array
) -> tuple[pa.Field, pa.Array]:
    t = field.type
    if pa.types.is_timestamp(t):
        # %S in PyArrow strftime includes the fractional second for
        # sub-second-unit timestamps; %z renders the offset as ``+HHMM``
        # (no colon). Both are RFC 3339-acceptable.
        fmt = "%Y-%m-%dT%H:%M:%S%z" if t.tz else "%Y-%m-%dT%H:%M:%S"
        return _retype(field, pa.string()), pc.strftime(column, format=fmt)
    if pa.types.is_date(t):
        return _retype(field, pa.string()), pc.strftime(column, format="%Y-%m-%d")
    if pa.types.is_time(t):
        # pc.strftime doesn't operate on Time types; cast is the only
        # vectorised path. Output is ``HH:MM:SS[.ffffff]``.
        return _retype(field, pa.string()), pc.cast(column, pa.string())
    if pa.types.is_decimal(t):
        return _retype(field, pa.string()), pc.cast(column, pa.string())
    if (
        pa.types.is_binary(t)
        or pa.types.is_large_binary(t)
        or pa.types.is_fixed_size_binary(t)
    ):
        # PyArrow has no vectorised base64. Per-element encode is
        # acceptable because binary columns are rare in API payloads.
        encoded = [
            None if v is None else base64.b64encode(v).decode("ascii")
            for v in column.to_pylist()
        ]
        return _retype(field, pa.string()), pa.array(encoded, type=pa.string())
    if pa.types.is_struct(t):
        sub_fields: List[pa.Field] = []
        sub_arrays: List[pa.Array] = []
        for j, sub_field in enumerate(t):
            sf, sa = _column_to_json_shape(sub_field, column.field(j))
            sub_fields.append(sf)
            sub_arrays.append(sa)
        # Preserve the parent's null mask — pa.StructArray.from_arrays
        # otherwise treats every row as non-null even if the original
        # struct had nulls (its children carry placeholder values for
        # those slots).
        mask = column.is_null() if column.null_count else None
        new_array = pa.StructArray.from_arrays(
            sub_arrays, fields=sub_fields, mask=mask
        )
        return _retype(field, pa.struct(sub_fields)), new_array
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        element_field = pa.field("item", t.value_type)
        ef, ea = _column_to_json_shape(element_field, column.values)
        if pa.types.is_list(t):
            new_array = pa.ListArray.from_arrays(column.offsets, ea)
            new_type = pa.list_(ef.type)
        else:
            new_array = pa.LargeListArray.from_arrays(column.offsets, ea)
            new_type = pa.large_list(ef.type)
        return _retype(field, new_type), new_array
    # JSON-native already: bool, integer, float, string, null.
    return field, column


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

        Arrow-space schema alignment, then ``to_pylist``, then Json wire-
        strings → dicts. SQLAlchemy is the wire-format-aware receiver
        beyond this point — it serialises ``datetime`` / ``Decimal`` /
        ``dict`` natively into their column types.
        """
        record_batch = self.cast_arrow_batch(record_batch)
        records = record_batch.to_pylist()
        return self.decode_json_columns(records)

    def to_json_records(
        self, record_batch: pa.RecordBatch
    ) -> List[Dict[str, Any]]:
        """Materialise a batch for a JSON-emitting destination.

        Same pipeline as :meth:`to_db_records` plus one extra Arrow-space
        cast (:func:`arrow_to_json_shape`) so timestamp / date / time /
        decimal / binary columns become canonical JSON strings before
        ``to_pylist``. The result is safe to pass to ``json.dumps`` with
        no custom encoder.
        """
        record_batch = self.cast_arrow_batch(record_batch)
        record_batch = arrow_to_json_shape(record_batch)
        records = record_batch.to_pylist()
        return self.decode_json_columns(records)

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

        return pa.array(values, type=field.type)

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
