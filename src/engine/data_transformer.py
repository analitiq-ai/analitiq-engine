"""Data transformation utilities for the streaming engine.

Supports both:
- New assignment-based mapping (MAPPING_AND_TRANSFORMATIONS.md spec)
- Legacy field_mappings + computed_fields format (for backwards compatibility)
"""

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from .exceptions import TransformationError
from .expression_evaluator import SecureExpressionEvaluator

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
                target_type = target.get("type", "string")
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

                # Type coercion
                value = self._coerce_type(value, target_type)

                # Set value in result (supports nested paths)
                self._set_nested_value(result, target_path, value)

            except Exception as e:
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

        return None

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
                    return None
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
                    raise ValueError("if expression requires 3 args: [condition, then, else]")
                condition = await self._evaluate_expression(record, partial_result, args[0])
                if condition:
                    return await self._evaluate_expression(record, partial_result, args[1])
                else:
                    return await self._evaluate_expression(record, partial_result, args[2])

            case "eq":
                args = expr.get("args", [])
                if len(args) != 2:
                    return False
                left = await self._evaluate_expression(record, partial_result, args[0])
                right = await self._evaluate_expression(record, partial_result, args[1])
                return left == right

            case "neq":
                args = expr.get("args", [])
                if len(args) != 2:
                    return False
                left = await self._evaluate_expression(record, partial_result, args[0])
                right = await self._evaluate_expression(record, partial_result, args[1])
                return left != right

            case "gt" | "gte" | "lt" | "lte":
                args = expr.get("args", [])
                if len(args) != 2:
                    return False
                left = await self._evaluate_expression(record, partial_result, args[0])
                right = await self._evaluate_expression(record, partial_result, args[1])
                match op:
                    case "gt":
                        return left > right
                    case "gte":
                        return left >= right
                    case "lt":
                        return left < right
                    case "lte":
                        return left <= right

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
                if not args:
                    return True
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
                logger.warning(f"Unknown expression op: {op}")
                return None

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
            logger.warning(f"Expected fn op in pipe, got: {op}")
            return value

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
            logger.warning(f"Unknown function: {name}")
            return value

        fn_name = catalog_entry["fn"]
        method = getattr(self, fn_name, None)
        if method:
            return await method(value, *args)
        return value

    # Function implementations
    async def _fn_iso_to_date(self, value: Any) -> str:
        """Convert ISO datetime to date string."""
        if value is None:
            return None
        try:
            iso_str = str(value)
            if iso_str.endswith('Z'):
                iso_str = iso_str.replace('Z', '+00:00')
            dt = datetime.fromisoformat(iso_str)
            return dt.strftime('%Y-%m-%d')
        except (ValueError, TypeError) as e:
            logger.warning(f"iso_to_date failed for '{value}': {e}")
            return str(value)

    async def _fn_iso_to_datetime(self, value: Any) -> datetime:
        """Convert ISO string to datetime object."""
        if value is None:
            return None
        try:
            iso_str = str(value)
            if iso_str.endswith('Z'):
                iso_str = iso_str.replace('Z', '+00:00')
            return datetime.fromisoformat(iso_str)
        except (ValueError, TypeError) as e:
            logger.warning(f"iso_to_datetime failed for '{value}': {e}")
            return datetime.now()

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

    async def _fn_to_int(self, value: Any) -> int:
        """Convert to integer."""
        if value is None:
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    async def _fn_to_float(self, value: Any) -> float:
        """Convert to float."""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    async def _fn_to_string(self, value: Any) -> str:
        """Convert to string."""
        if value is None:
            return ""
        return str(value)

    async def _fn_abs(self, value: Any) -> Any:
        """Absolute value."""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return abs(value)
        return value

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
        """Get value from nested path."""
        current = record
        for key in path:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return None
        return current

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
                    if value is not None and not re.match(pattern, str(value)):
                        return rule.get("message", f"Value does not match pattern")

                case "range":
                    min_val = rule.get("min")
                    max_val = rule.get("max")
                    if value is not None:
                        if min_val is not None and value < min_val:
                            return rule.get("message", f"Value must be >= {min_val}")
                        if max_val is not None and value > max_val:
                            return rule.get("message", f"Value must be <= {max_val}")

                case "in_list":
                    allowed = rule.get("value", [])
                    if value is not None and value not in allowed:
                        return rule.get("message", f"Value must be one of: {allowed}")

        return None

    def _coerce_type(self, value: Any, target_type: str) -> Any:
        """
        Coerce value to JSON-compatible type for transmission.

        NOTE: Datetime/date/time coercion is intentionally NOT done here.
        Type coercion to destination-specific types (e.g., Python datetime
        objects for PostgreSQL) is handled by the destination-side type
        coercer based on the JSON schema. This avoids losing type information
        during JSON serialization over gRPC.

        This method only handles basic JSON-native type coercion:
        - string, integer, number, boolean
        """
        if value is None:
            return None

        match target_type:
            case "string":
                return str(value) if value is not None else None
            case "integer":
                try:
                    return int(float(value))
                except (ValueError, TypeError):
                    return value
            case "decimal" | "number":
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return value
            case "boolean":
                if isinstance(value, bool):
                    return value
                if isinstance(value, str):
                    return value.lower() in ("true", "1", "yes")
                return bool(value)
            case "date" | "datetime" | "time":
                # Pass through as-is - destination handles type coercion
                # based on JSON schema and database requirements
                return value
            case "object" | "array":
                return value
            case _:
                return value


class DataTransformer:
    """
    Handles field mappings, transformations, and computed fields.

    Supports both:
    - New assignment-based mapping (assignments array)
    - Legacy field_mappings + computed_fields format
    """

    def __init__(self):
        self.expression_evaluator = SecureExpressionEvaluator()
        self.assignment_transformer = AssignmentTransformer()

    async def apply_transformations(
        self,
        batch: List[Dict[str, Any]],
        config: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Apply field mappings and transformations to batch.

        Args:
            batch: List of records to transform
            config: Stream configuration with mapping rules

        Returns:
            Transformed batch

        Raises:
            TransformationError: If transformation fails
        """
        mapping = config.get("mapping", {})

        # Check for new assignment-based format
        assignments = mapping.get("assignments", [])
        if assignments:
            return await self._apply_assignment_transformations(batch, assignments)

        # Fall back to legacy format
        field_mappings = mapping.get("field_mappings", {})
        computed_fields = mapping.get("computed_fields", {})

        if not field_mappings and not computed_fields:
            return batch  # No transformations to apply

        return await self._apply_legacy_transformations(
            batch, field_mappings, computed_fields
        )

    async def _apply_assignment_transformations(
        self,
        batch: List[Dict[str, Any]],
        assignments: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Apply assignment-based transformations."""
        transformed_batch = []
        all_errors = []

        for record in batch:
            await asyncio.sleep(0)  # Yield for async safety
            result, errors = await self.assignment_transformer.transform_record(
                record, assignments
            )
            if result is not None:
                transformed_batch.append(result)
            if errors:
                all_errors.extend(errors)

        if all_errors:
            logger.warning(f"Transformation completed with {len(all_errors)} errors")
            for error in all_errors[:5]:  # Log first 5 errors
                logger.warning(f"  Field '{error['field']}': {error['error']}")

        return transformed_batch

    async def _apply_legacy_transformations(
        self,
        batch: List[Dict[str, Any]],
        field_mappings: Dict[str, Any],
        computed_fields: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Apply legacy field_mappings and computed_fields."""
        transformed_batch = []

        try:
            for record in batch:
                transformed_record = {}

                # Apply field mappings
                for source_field, mapping_config in field_mappings.items():
                    if isinstance(mapping_config, dict):
                        target_field = mapping_config.get("target", source_field)
                        transformations = mapping_config.get("transformations", [])
                    else:
                        target_field = mapping_config
                        transformations = []

                    source_value = self._get_nested_value(record, source_field)
                    transformed_value = await self._apply_field_transformations(
                        source_value, transformations
                    )
                    transformed_record[target_field] = transformed_value

                # Apply computed fields
                for field_name, field_config in computed_fields.items():
                    if isinstance(field_config, dict):
                        expression = field_config.get("expression", "")
                    else:
                        expression = field_config

                    computed_value = await self.expression_evaluator.evaluate(
                        expression, record, transformed_record
                    )
                    transformed_record[field_name] = computed_value

                transformed_batch.append(transformed_record)

        except Exception as e:
            logger.error(f"Transformation failed: {e}")
            raise TransformationError(f"Data transformation failed: {e}") from e

        return transformed_batch

    def _get_nested_value(self, record: Dict[str, Any], field_path: str) -> Any:
        """Get value from nested field path like 'details.merchant.name'."""
        if "." not in field_path:
            return record.get(field_path)

        current = record
        for field in field_path.split("."):
            if isinstance(current, dict) and field in current:
                current = current[field]
            else:
                return None
        return current

    async def _apply_field_transformations(
        self,
        value: Any,
        transformations: List[str]
    ) -> Any:
        """Apply transformations to a field value."""
        if not transformations or value is None:
            return value

        for transformation in transformations:
            await asyncio.sleep(0)  # Yield for async safety

            match transformation:
                case "abs" if isinstance(value, (int, float)):
                    value = abs(value)
                case "strip" | "trim" if isinstance(value, str):
                    value = value.strip()
                case "lowercase" | "lower" if isinstance(value, str):
                    value = value.lower()
                case "uppercase" | "upper" if isinstance(value, str):
                    value = value.upper()
                case "iso_to_date" if isinstance(value, str):
                    value = await self._parse_iso_date(value)
                case "iso_to_timestamp" if isinstance(value, str):
                    value = await self._parse_iso_timestamp(value)
                case "iso_string_to_datetime":
                    value = await self._parse_iso_to_datetime_object(value)
                case "to_int" if isinstance(value, (str, float)):
                    try:
                        value = int(float(value))
                    except (ValueError, TypeError):
                        logger.warning(f"Failed to convert {value} to int")
                case "to_float" if isinstance(value, (str, int)):
                    try:
                        value = float(value)
                    except (ValueError, TypeError):
                        logger.warning(f"Failed to convert {value} to float")
                case "to_str":
                    value = str(value) if value is not None else ""
                case _:
                    logger.warning(f"Unknown transformation: {transformation}")

        return value

    async def _parse_iso_date(self, value: str) -> str:
        """Parse ISO datetime string to date format."""
        try:
            if value.endswith('Z'):
                value = value.replace('Z', '+00:00')
            dt = datetime.fromisoformat(value)
            return dt.strftime('%Y-%m-%d')
        except ValueError as e:
            logger.warning(f"Failed to parse ISO date '{value}': {e}")
            return value

    async def _parse_iso_timestamp(self, value: str) -> datetime:
        """Parse ISO datetime string to datetime object."""
        try:
            if value.endswith('Z'):
                value = value.replace('Z', '+00:00')
            return datetime.fromisoformat(value)
        except ValueError as e:
            logger.warning(f"Failed to parse ISO timestamp '{value}': {e}")
            return datetime.now()

    async def _parse_iso_to_datetime_object(self, value: Any) -> datetime:
        """Convert ISO timestamp string or datetime to datetime object."""
        try:
            if value is None:
                raise ValueError("Cannot convert None to datetime")

            if isinstance(value, datetime):
                return value

            iso_string = str(value)
            if iso_string.endswith('Z'):
                iso_string = iso_string.replace('Z', '+00:00')

            return datetime.fromisoformat(iso_string)
        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to parse value '{value}' to datetime: {e}")
            return datetime.now()
