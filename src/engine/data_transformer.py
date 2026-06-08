"""Data transformation utilities for the streaming engine.

Supports assignment-based mapping (per MAPPING_AND_TRANSFORMATIONS.md).
"""

import asyncio
import json
import logging
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa

from .exceptions import TransformationError
from ..shared.dict_path import walk_path
from cdk.type_map.arrow import resolve_arrow_type
from cdk.type_map.exceptions import InvalidTypeMapError

logger = logging.getLogger(__name__)


class AssignmentTransformer:
    """
    Handles assignment-based mapping as per MAPPING_AND_TRANSFORMATIONS.md spec.

    Each assignment rule specifies:
    - target: {path: [...], type: str, nullable: bool}
    - value: {kind: "const"|"expr", const: {...}, expr: {...}}
    - validate: {rules: [...], on_error: str}
    """

    # Function catalog for built-in transforms
    FUNCTION_CATALOG = {
        "iso_to_date": {"version": 1, "fn": "_fn_iso_to_date"},
        "iso_to_datetime": {"version": 1, "fn": "_fn_iso_to_datetime"},
        "iso_to_timestamp": {"version": 1, "fn": "_fn_iso_to_timestamp"},
        "trim": {"version": 1, "fn": "_fn_trim"},
        "lower": {"version": 1, "fn": "_fn_lower"},
        "upper": {"version": 1, "fn": "_fn_upper"},
        "to_int": {"version": 1, "fn": "_fn_to_int"},
        "to_float": {"version": 1, "fn": "_fn_to_float"},
        "to_string": {"version": 1, "fn": "_fn_to_string"},
        "abs": {"version": 1, "fn": "_fn_abs"},
        "now": {"version": 1, "fn": "_fn_now"},
        "default": {"version": 1, "fn": "_fn_default"},
        "coalesce": {"version": 1, "fn": "_fn_coalesce"},
    }

    def __init__(self):
        pass

    async def transform_record(
        self,
        record: Dict[str, Any],
        assignments: List[Dict[str, Any]],
        default_on_error: str = "dlq"
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Transform a single record using assignment rules.

        Args:
            record: Source record
            assignments: List of assignment rules
            default_on_error: Default action on error (dlq, skip_record, etc.)

        Returns:
            Tuple of (transformed_record, errors)
        """
        result = {}
        errors = []

        for assignment in assignments:
            try:
                target = assignment.get("target", {})
                value_spec = assignment.get("value", {})
                validate = assignment.get("validate")

                target_path = target.get("path", [])
                nullable = target.get("nullable", True)

                # Evaluate the value
                value = await self._evaluate_value(record, result, value_spec)

                # Validate if rules specified
                if validate:
                    validation_error = self._validate_value(value, validate.get("rules", []), target_path)
                    if validation_error:
                        on_error = validate.get("on_error", default_on_error)
                        errors.append({
                            "field": ".".join(target_path),
                            "error": validation_error,
                            "action": on_error,
                            "value": value,
                        })
                        if on_error == "skip_record":
                            return None, errors
                        elif on_error == "default_value":
                            value = validate.get("default")
                        elif on_error in ("dlq", "quarantine"):
                            continue  # Skip this field
                        # else continue with the value

                # Check nullability
                if value is None and not nullable:
                    errors.append({
                        "field": ".".join(target_path),
                        "error": "Value is null but field is not nullable",
                        "action": default_on_error,
                    })
                    continue

                self._set_nested_value(result, target_path, value)

            except TransformationError as e:
                field_path = ".".join(assignment.get("target", {}).get("path", ["unknown"]))
                errors.append({
                    "field": field_path,
                    "error": str(e),
                    "action": default_on_error,
                })

        return result, errors

    async def _evaluate_value(
        self,
        record: Dict[str, Any],
        partial_result: Dict[str, Any],
        value_spec: Dict[str, Any]
    ) -> Any:
        """Evaluate a value specification (const or expr)."""
        kind = value_spec.get("kind", "expr")

        if kind == "const":
            const = value_spec.get("const", {})
            return const.get("value")

        elif kind == "expr":
            expr = value_spec.get("expr", {})
            return await self._evaluate_expression(record, partial_result, expr)

        raise TransformationError(f"Unknown value kind: {kind!r}")

    async def _evaluate_expression(
        self,
        record: Dict[str, Any],
        partial_result: Dict[str, Any],
        expr: Dict[str, Any]
    ) -> Any:
        """Evaluate an expression AST node."""
        op = expr.get("op")

        match op:
            case "get":
                path = expr.get("path", [])
                return self._get_nested_value(record, path)

            case "const":
                return expr.get("value")

            case "pipe":
                args = expr.get("args", [])
                if not args:
                    raise TransformationError(f"pipe expression requires at least 1 arg, got {len(args)}")
                # First arg is the initial value, rest are functions to apply
                value = await self._evaluate_expression(record, partial_result, args[0])
                for fn_expr in args[1:]:
                    value = await self._apply_function_expression(value, fn_expr)
                return value

            case "fn":
                # Function call with input from previous pipe stage
                return await self._apply_function(
                    None,  # No input value directly
                    expr.get("name"),
                    expr.get("version", 1),
                    expr.get("args", [])
                )

            case "if":
                args = expr.get("args", [])
                if len(args) != 3:
                    raise TransformationError(
                        f"if expression requires 3 args, got {len(args)}"
                    )
                condition = await self._evaluate_expression(record, partial_result, args[0])
                if condition:
                    return await self._evaluate_expression(record, partial_result, args[1])
                else:
                    return await self._evaluate_expression(record, partial_result, args[2])

            case "eq":
                args = expr.get("args", [])
                if len(args) != 2:
                    raise TransformationError(f"eq expression requires 2 args, got {len(args)}")
                left = await self._evaluate_expression(record, partial_result, args[0])
                right = await self._evaluate_expression(record, partial_result, args[1])
                return left == right

            case "neq":
                args = expr.get("args", [])
                if len(args) != 2:
                    raise TransformationError(f"neq expression requires 2 args, got {len(args)}")
                left = await self._evaluate_expression(record, partial_result, args[0])
                right = await self._evaluate_expression(record, partial_result, args[1])
                return left != right

            case "gt" | "gte" | "lt" | "lte":
                args = expr.get("args", [])
                if len(args) != 2:
                    raise TransformationError(f"{op} expression requires 2 args, got {len(args)}")
                left = await self._evaluate_expression(record, partial_result, args[0])
                right = await self._evaluate_expression(record, partial_result, args[1])
                # Comparing incompatible operand types (e.g. a null field
                # against a number) is bad record data, not an engine defect:
                # route it through the per-record error path, not the fatal one.
                try:
                    match op:
                        case "gt":
                            return left > right
                        case "gte":
                            return left >= right
                        case "lt":
                            return left < right
                        case "lte":
                            return left <= right
                except TypeError as exc:
                    raise TransformationError(
                        f"{op} expression cannot compare {left!r} and {right!r}: {exc}"
                    ) from exc

            case "and":
                args = expr.get("args", [])
                for arg in args:
                    if not await self._evaluate_expression(record, partial_result, arg):
                        return False
                return True

            case "or":
                args = expr.get("args", [])
                for arg in args:
                    if await self._evaluate_expression(record, partial_result, arg):
                        return True
                return False

            case "not":
                args = expr.get("args", [])
                if len(args) != 1:
                    raise TransformationError(
                        f"not expression requires 1 arg, got {len(args)}"
                    )
                return not await self._evaluate_expression(record, partial_result, args[0])

            case "concat":
                args = expr.get("args", [])
                parts = []
                for arg in args:
                    val = await self._evaluate_expression(record, partial_result, arg)
                    if val is not None:
                        parts.append(str(val))
                return "".join(parts)

            case "coalesce":
                args = expr.get("args", [])
                for arg in args:
                    val = await self._evaluate_expression(record, partial_result, arg)
                    if val is not None:
                        return val
                return None

            case _:
                raise TransformationError(f"Unknown expression op: {op!r}")

    async def _apply_function_expression(self, value: Any, fn_expr: Dict[str, Any]) -> Any:
        """Apply a function expression to a value (used in pipe)."""
        op = fn_expr.get("op")

        if op == "fn":
            return await self._apply_function(
                value,
                fn_expr.get("name"),
                fn_expr.get("version", 1),
                fn_expr.get("args", [])
            )
        else:
            raise TransformationError(f"Expected fn op in pipe stage, got: {op!r}")

    async def _apply_function(
        self,
        value: Any,
        name: str,
        version: int,
        args: List[Any]
    ) -> Any:
        """Apply a catalog function to a value."""
        catalog_entry = self.FUNCTION_CATALOG.get(name)
        if not catalog_entry:
            raise TransformationError(f"Unknown function: {name!r}")

        fn_name = catalog_entry["fn"]
        method = getattr(self, fn_name, None)
        if method is None:
            raise TransformationError(
                f"FUNCTION_CATALOG entry for {name!r} references missing method {fn_name!r}"
            )
        # A wrong argument count for the function (an authoring error in the
        # mapping) surfaces as TypeError from the call; route it through the
        # per-record error path rather than aborting the batch.
        try:
            return await method(value, *args)
        except TypeError as exc:
            raise TransformationError(
                f"function {name!r} called with wrong arguments: {exc}"
            ) from exc

    # Function implementations
    async def _fn_iso_to_date(self, value: Any) -> str:
        """Convert ISO string to date string (YYYY-MM-DD). Raises on
        unparseable input — previously returned the raw string unchanged,
        which passed a non-date value into typed date columns and surfaced
        as a destination connector write error rather than a transformation
        error."""
        if value is None:
            return None
        try:
            dt = datetime.fromisoformat(str(value))
            return dt.strftime('%Y-%m-%d')
        except (ValueError, TypeError) as e:
            raise TransformationError(
                f"iso_to_date failed for {value!r}: {e}"
            ) from e

    async def _fn_iso_to_datetime(self, value: Any) -> datetime:
        """Convert ISO string to datetime object. Raises on unparseable
        input — a ``datetime.now()`` fallback would silently fabricate
        timestamps and corrupt time-based queries and incremental sync."""
        if value is None:
            return None
        try:
            return datetime.fromisoformat(str(value))
        except (ValueError, TypeError) as e:
            raise TransformationError(
                f"iso_to_datetime failed for {value!r}: {e}"
            ) from e

    async def _fn_iso_to_timestamp(self, value: Any) -> datetime:
        """Alias for iso_to_datetime."""
        return await self._fn_iso_to_datetime(value)

    async def _fn_trim(self, value: Any) -> str:
        """Trim whitespace from string."""
        if value is None:
            return None
        return str(value).strip()

    async def _fn_lower(self, value: Any) -> str:
        """Convert to lowercase."""
        if value is None:
            return None
        return str(value).lower()

    async def _fn_upper(self, value: Any) -> str:
        """Convert to uppercase."""
        if value is None:
            return None
        return str(value).upper()

    async def _fn_to_int(self, value: Any) -> Optional[int]:
        """Convert to integer via int(float(value)). None passes through.

        Decimal strings and floats are truncated toward zero ("3.9" → 3).
        Raises TransformationError on unparseable input — silently returning
        None would mask mis-configured pipelines with no DLQ entry."""
        if value is None:
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError, OverflowError) as e:
            raise TransformationError(
                f"to_int: cannot convert {value!r} ({type(value).__name__}) to int: {e}"
            ) from e

    async def _fn_to_float(self, value: Any) -> Optional[float]:
        """Convert to float. None passes through.

        Raises TransformationError on unparseable input — same rationale as
        _fn_to_int; silently returning None masks mis-configured pipelines."""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError, OverflowError) as e:
            raise TransformationError(
                f"to_float: cannot convert {value!r} ({type(value).__name__}) to float: {e}"
            ) from e

    async def _fn_to_string(self, value: Any) -> str:
        """Convert to string."""
        if value is None:
            return ""
        return str(value)

    async def _fn_abs(self, value: Any) -> Any:
        """Absolute value. None passes through.

        Raises TransformationError for non-numeric input — silently returning
        the value unchanged would mask mis-configured pipelines with no DLQ entry."""
        if value is None:
            return None
        if isinstance(value, (int, float, Decimal)):
            return abs(value)
        raise TransformationError(
            f"abs: cannot apply to {value!r} ({type(value).__name__}); expected int, float, or Decimal"
        )

    async def _fn_now(self, value: Any = None) -> datetime:
        """Return current datetime."""
        return datetime.now()

    async def _fn_default(self, value: Any, default_value: Any = None) -> Any:
        """Return default if value is None."""
        return default_value if value is None else value

    async def _fn_coalesce(self, value: Any, *alternatives: Any) -> Any:
        """Return first non-None value."""
        if value is not None:
            return value
        for alt in alternatives:
            if alt is not None:
                return alt
        return None

    def _get_nested_value(self, record: Dict[str, Any], path: List[str]) -> Any:
        return walk_path(record, path)

    def _set_nested_value(self, result: Dict[str, Any], path: List[str], value: Any) -> None:
        """Set value at nested path."""
        if not path:
            return

        current = result
        for key in path[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]
        current[path[-1]] = value

    def _validate_value(
        self,
        value: Any,
        rules: List[Dict[str, Any]],
        field_path: List[str]
    ) -> Optional[str]:
        """Validate value against rules. Returns error message or None."""
        for rule in rules:
            rule_type = rule.get("type")

            match rule_type:
                case "not_null" | "required":
                    if value is None:
                        return rule.get("message", "Value cannot be null")

                case "min_length":
                    min_len = rule.get("value", 0)
                    if value is not None and len(str(value)) < min_len:
                        return rule.get("message", f"Value must be at least {min_len} characters")

                case "max_length":
                    max_len = rule.get("value", 0)
                    if value is not None and len(str(value)) > max_len:
                        return rule.get("message", f"Value must be at most {max_len} characters")

                case "pattern":
                    import re
                    pattern = rule.get("value", "")
                    if value is not None:
                        # A malformed regex in the stream config is an authoring
                        # error: surface it as a transform error, not a fatal crash.
                        try:
                            is_match = re.match(pattern, str(value))
                        except re.error as exc:
                            raise TransformationError(
                                f"pattern validation rule has invalid regex {pattern!r}: {exc}"
                            ) from exc
                        if not is_match:
                            return rule.get("message", "Value does not match pattern")

                case "range":
                    min_val = rule.get("min")
                    max_val = rule.get("max")
                    if value is not None:
                        # Comparing a value against bounds of an incompatible
                        # type is bad record data: route it per-record.
                        try:
                            if min_val is not None and value < min_val:
                                return rule.get("message", f"Value must be >= {min_val}")
                            if max_val is not None and value > max_val:
                                return rule.get("message", f"Value must be <= {max_val}")
                        except TypeError as exc:
                            raise TransformationError(
                                f"range validation cannot compare {value!r} with bounds "
                                f"min={min_val!r} max={max_val!r}: {exc}"
                            ) from exc

                case "in_list":
                    allowed = rule.get("value", [])
                    if value is not None and value not in allowed:
                        return rule.get("message", f"Value must be one of: {allowed}")

        return None

class DataTransformer:
    """Apply contract mapping assignments to a batch of records."""

    def __init__(self):
        self.assignment_transformer = AssignmentTransformer()

    async def apply_transformations(
        self,
        batch: List[Dict[str, Any]],
        config: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Apply assignment-based transformations to batch.

        Args:
            batch: List of records to transform
            config: Stream configuration with mapping rules

        Returns:
            Transformed batch

        Raises:
            TransformationError: If transformation fails
        """
        mapping = config.get("mapping", {})
        # Warn before the assignments early-return so stray legacy keys are
        # flagged even when valid assignments are present alongside them.
        legacy_keys = {"field_mappings", "computed_fields"} & mapping.keys()
        if legacy_keys:
            logger.warning(
                "mapping contains legacy keys %s which are no longer supported "
                "and are ignored; migrate to 'assignments'.",
                sorted(legacy_keys),
            )
        assignments = mapping.get("assignments", [])
        if assignments:
            return await self._apply_assignment_transformations(batch, assignments)
        return batch

    async def _apply_assignment_transformations(
        self,
        batch: List[Dict[str, Any]],
        assignments: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Apply assignment-based transformations.

        Any per-record transform error fails the whole batch. Silently
        dropping records would deliver a shorter batch than the source
        emitted with no count reconciliation, no DLQ routing, and no
        FATAL signal — the engine's error_strategy is the right place
        to decide retry vs DLQ, not this transformer.

        Fields whose target ``arrow_type`` is ``"Json"`` are JSON-encoded
        here so the engine's Arrow batch (``pa.large_string`` column) can
        accept them. Destination handlers reverse this at the write
        boundary.
        """
        kept: List[tuple[int, Dict[str, Any]]] = []
        all_errors: List[Dict[str, Any]] = []

        for source_row, record in enumerate(batch):
            await asyncio.sleep(0)  # Yield for async safety
            result, errors = await self.assignment_transformer.transform_record(
                record, assignments
            )
            if errors:
                for err in errors:
                    err.setdefault("row", source_row)
                all_errors.extend(errors)
            if result is not None:
                kept.append((source_row, result))

        # Skip the Json-encoding pass when prior errors exist — those
        # records will be discarded by the raise below, and mutating
        # their Json columns in-place beforehand would leak a partially
        # transformed view to anyone introspecting them.
        if not all_errors:
            json_cols = _json_target_names(assignments)
            if json_cols:
                for source_row, record in kept:
                    for col in json_cols:
                        value = record.get(col)
                        if value is None or isinstance(value, str):
                            # Strings pass through to pa.large_string
                            # unchanged — the destination decoder will
                            # json.loads them. Useful when the assignment
                            # is ``get`` from a Json source column.
                            continue
                        if not isinstance(value, (dict, list)):
                            all_errors.append({
                                "field": col,
                                "row": source_row,
                                "error": (
                                    f"Json target requires dict/list/str/None, "
                                    f"got {type(value).__name__}"
                                ),
                                "action": "dlq",
                            })
                            continue
                        try:
                            record[col] = json.dumps(value)
                        except TypeError as exc:
                            # Non-JSON-serializable values (datetime,
                            # Decimal, UUID, …) join the per-record error
                            # stream so they follow the same DLQ / retry
                            # policy as every other transform failure.
                            all_errors.append({
                                "field": col,
                                "row": source_row,
                                "error": f"Json target value is not JSON-serializable: {exc}",
                                "action": "dlq",
                            })

        if all_errors:
            summary = "; ".join(
                f"{err.get('field', '?')} (row {err.get('row', '?')}): "
                f"{err.get('error', err)}"
                for err in all_errors[:5]
            )
            suffix = f" (+{len(all_errors) - 5} more)" if len(all_errors) > 5 else ""
            raise TransformationError(
                f"Assignment transformations produced {len(all_errors)} error(s): "
                f"{summary}{suffix}"
            )

        return [record for _, record in kept]


def _json_target_names(assignments: List[Dict[str, Any]]) -> set:
    """Target column names whose ``target.arrow_type`` is ``"Json"``."""
    names: set = set()
    for a in assignments:
        target = a.get("target") or {}
        if target.get("arrow_type") == "Json":
            names.add(_normalize_path(target.get("path")))
    return names


def _normalize_path(path: Any) -> str:
    if isinstance(path, str):
        return path
    if isinstance(path, list):
        if len(path) != 1 or not isinstance(path[0], str):
            raise TransformationError(
                f"assignment path must be a single column name; got {path!r}"
            )
        return path[0]
    raise TransformationError(
        f"assignment path must be str or [str]; got {type(path).__name__}"
    )


def build_output_schema(
    assignments: List[Dict[str, Any]],
) -> pa.Schema:
    """Build the post-transform Arrow schema from a stream's assignments.

    Object/List targets declare ``arrow_type: "Object"`` with a
    ``target.properties`` map, or ``arrow_type: "List"`` with
    ``target.items`` — :func:`resolve_arrow_type` handles the recursion.
    """
    fields: List[pa.Field] = []
    for index, assignment in enumerate(assignments):
        target = assignment.get("target") or {}
        target_name = _normalize_path(target.get("path"))
        nullable = bool(target.get("nullable", True))

        if not target.get("arrow_type"):
            raise TransformationError(
                f"assignment[{index}] target={target_name!r}: missing "
                f"target.arrow_type; every assignment must declare an "
                f"Arrow type"
            )
        try:
            arrow_type = resolve_arrow_type(
                target, where=f"assignment[{index}] target={target_name!r}"
            )
        except InvalidTypeMapError as e:
            raise TransformationError(
                f"assignment[{index}] target={target_name!r}: cannot "
                f"parse target.arrow_type={target.get('arrow_type')!r}: {e}"
            ) from e

        fields.append(pa.field(target_name, arrow_type, nullable=nullable))
    return pa.schema(fields)
