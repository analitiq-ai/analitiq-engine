"""Build pa.Schema from endpoint arrow_type declarations."""

import logging
from decimal import Decimal
from typing import Any, Dict, List, Optional

import pyarrow as pa
import pyarrow.compute as pc

from src.engine.type_map import resolve_arrow_type
from src.engine.type_map.exceptions import InvalidTypeMapError

logger = logging.getLogger(__name__)


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

    def from_pylist(self, records: List[Dict[str, Any]]) -> pa.RecordBatch:
        """Build a record batch from dict rows using this endpoint's schema."""
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
        source_format: Optional[str],
    ) -> pa.Array:
        if all(v is None for v in values):
            if not field.nullable:
                raise ValueError(
                    f"column {field.name!r} is non-nullable but every "
                    f"source value is None"
                )
            return pa.nulls(len(values), type=field.type)

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
