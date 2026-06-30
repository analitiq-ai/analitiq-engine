"""Vectorized, Arrow-native record transformation for the streaming engine.

A stream's ``mapping.assignments`` (per ``mapping-and-transformations.md``) are
compiled once into a :class:`CompiledTransform` and then applied to each
``pa.RecordBatch`` with ``pyarrow.compute`` -- the batch never leaves Arrow.
There is a single transform path: every assignment, every expression op, and
every function in the catalog is a vectorized column operation.

Type conversion has one authority. When an assignment's evaluated value lands in
a column of a different Arrow type than the target declares, the conversion is
gated by the **conversion matrix** (:mod:`cdk.type_map.conversions`) and executed
by the same ``pc.cast(safe=True)`` the destination uses -- so the transform and
the destination cast of one column always agree (both parse ``"1" -> Int64``,
both reject a lossy ``Float64 -> Int64``). A ``const`` literal is a Python value
declared in the mapping, not a typed Arrow column, so it is materialised at the
target type directly (``pa.array``) -- there is no source arrow_type to classify.
Nested (``Object``/``List``) and ``Json`` targets are assembled structurally, not
through the scalar matrix.

Failures are loud and batch-wide. A row that fails a validation rule, an
expression that cannot be evaluated, an unparseable cast, or a null in a
non-nullable column fails the whole batch with a :class:`TransformationError`.
The engine's ``error_strategy`` -- not this module -- decides retry vs DLQ.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from cdk.type_map.arrow import classify_arrow_conversion, resolve_arrow_type
from cdk.type_map.exceptions import InvalidTypeMapError

from .exceptions import TransformationError

# A compiled expression: given the source batch, return one value column.
_ExprFn = Callable[[pa.RecordBatch], pa.Array]


def build_output_schema(assignments: list[dict[str, Any]]) -> pa.Schema:
    """Build the post-transform Arrow schema from a stream's assignments.

    Object/List targets declare ``arrow_type: "Object"`` with a
    ``target.properties`` map, or ``arrow_type: "List"`` with ``target.items`` --
    :func:`resolve_arrow_type` handles the recursion.
    """
    fields: list[pa.Field] = []
    for index, assignment in enumerate(assignments):
        target = assignment.get("target") or {}
        target_name = _normalize_path(target.get("path"))
        nullable = bool(target.get("nullable", True))

        if not target.get("arrow_type"):
            raise TransformationError(
                f"assignment[{index}] target={target_name!r}: missing "
                f"target.arrow_type; every assignment must declare an Arrow type"
            )
        try:
            arrow_type = resolve_arrow_type(
                target, where=f"assignment[{index}] target={target_name!r}"
            )
        except InvalidTypeMapError as e:
            raise TransformationError(
                f"assignment[{index}] target={target_name!r}: cannot parse "
                f"target.arrow_type={target.get('arrow_type')!r}: {e}"
            ) from e

        fields.append(pa.field(target_name, arrow_type, nullable=nullable))
    return pa.schema(fields)


@dataclass(frozen=True, slots=True)
class _Step:
    """One compiled assignment: how to build, type, and validate a column."""

    field: pa.Field
    build: _ExprFn
    is_const: bool
    is_json: bool
    validate: dict[str, Any] | None


class CompiledTransform:
    """A stream's assignments compiled to vectorized column operations.

    Built once per stream by :func:`compile_transform`; :meth:`run` applies it to
    each batch with no per-record Python and no ``to_pylist``/``from_pylist``
    round-trip.
    """

    def __init__(self, output_schema: pa.Schema, steps: list[_Step]) -> None:
        self.output_schema = output_schema
        self._steps = steps

    def run(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        """Apply the transform to *batch*, returning the output batch.

        Raises :class:`TransformationError` if any column fails to build, any
        validation rule fails on any row, any conversion is rejected, or a
        non-nullable column ends up with nulls. The error names the column and
        (for validation) the offending rows.
        """
        arrays: list[pa.Array] = []
        errors: list[str] = []

        for step in self._steps:
            value = step.build(batch)

            if step.validate is not None:
                errors.extend(_run_validation(value, step.validate, step.field))

            array = self._coerce(value, step)

            if not step.field.nullable and array.null_count > 0:
                errors.append(
                    f"column {step.field.name!r}: {array.null_count} null "
                    f"value(s) but field is not nullable"
                )
            arrays.append(array)

        if errors:
            shown = "; ".join(errors[:5])
            suffix = f" (+{len(errors) - 5} more)" if len(errors) > 5 else ""
            raise TransformationError(
                f"transform produced {len(errors)} error(s): {shown}{suffix}"
            )

        return pa.RecordBatch.from_arrays(arrays, schema=self.output_schema)

    @staticmethod
    def _coerce(value: pa.Array, step: _Step) -> pa.Array:
        """Convert an evaluated value column to the target field type.

        - A ``const`` is already built at the target type (or JSON-encoded for a
          ``Json`` target); it passes through.
        - A ``Json`` target is encoded structurally (string passthrough, or
          ``json.dumps`` of a struct/list column).
        - A nested (``struct``/``list``) target is assembled with a structural
          cast.
        - A scalar target whose type already matches passes through; otherwise the
          conversion is gated by the matrix and executed by ``_retype_column``.
        """
        field = step.field
        if step.is_const:
            return value
        if step.is_json:
            return _encode_json_column(value, field)
        if value.type == field.type:
            return value
        if pa.types.is_nested(field.type):
            return _cast_structural(value, field)
        return _retype_column(value, field)


def compile_transform(assignments: list[dict[str, Any]]) -> CompiledTransform:
    """Compile a stream's assignments into a :class:`CompiledTransform`.

    Static work (schema building, expression compilation, validation setup)
    happens here once; the returned object is applied per batch. Raises
    :class:`TransformationError` for a malformed assignment.
    """
    output_schema = build_output_schema(assignments)
    steps: list[_Step] = []
    for index, (assignment, field) in enumerate(zip(assignments, output_schema)):
        target = assignment.get("target") or {}
        is_json = target.get("arrow_type") == "Json"
        value_spec = assignment.get("value") or {}
        build, is_const = _compile_value(value_spec, field, is_json, index)
        validate = assignment.get("validate")
        steps.append(
            _Step(
                field=field,
                build=build,
                is_const=is_const,
                is_json=is_json,
                validate=validate,
            )
        )
    return CompiledTransform(output_schema, steps)


def _compile_value(
    value_spec: dict[str, Any], field: pa.Field, is_json: bool, index: int
) -> tuple[_ExprFn, bool]:
    """Compile an assignment's ``value`` block into a column builder.

    Returns ``(build_fn, is_const)``. A ``const`` builds a broadcast column at the
    target type (JSON-encoded for a ``Json`` target); an ``expr`` compiles its AST
    to vectorized compute that produces a column at its natural type.
    """
    kind = value_spec.get("kind", "expr")
    if kind == "const":
        const_value = (value_spec.get("const") or {}).get("value")
        build = _compile_const(const_value, field, is_json)
        return build, True
    if kind == "expr":
        return _compile_expr(value_spec.get("expr") or {}), False
    raise TransformationError(
        f"assignment[{index}] target={field.name!r}: unknown value kind {kind!r}"
    )


def _compile_const(const_value: Any, field: pa.Field, is_json: bool) -> _ExprFn:
    """Build a closure that broadcasts a constant to a column at the target type."""
    if is_json:
        encoded = _json_encode_scalar(const_value, field.name)

        def build_json(batch: pa.RecordBatch) -> pa.Array:
            return _build_const_array([encoded] * batch.num_rows, field)

        return build_json

    def build(batch: pa.RecordBatch) -> pa.Array:
        return _build_const_array([const_value] * batch.num_rows, field)

    return build


# Expression AST -> vectorized compute. Each compiler returns a closure over the
# batch so the (static) AST walk happens once at compile time, not per batch.


def _compile_expr(expr: dict[str, Any]) -> _ExprFn:
    """Compile one expression AST node into a vectorized column builder."""
    op = expr.get("op")

    match op:
        case "get":
            path = expr.get("path") or []
            return lambda batch: _get_path(batch, path)

        case "const":
            value = expr.get("value")
            return lambda batch: pa.array([value] * batch.num_rows)

        case "pipe":
            args = expr.get("args") or []
            if not args:
                raise TransformationError(
                    f"pipe expression requires at least 1 arg, got {len(args)}"
                )
            seed = _compile_expr(args[0])
            fns = [_compile_fn(node) for node in args[1:]]

            def run_pipe(batch: pa.RecordBatch) -> pa.Array:
                value = seed(batch)
                for fn in fns:
                    value = fn(value)
                return value

            return run_pipe

        case "if":
            args = _expect_args(expr, op, 3)
            cond, then_, else_ = (_compile_expr(a) for a in args)
            return lambda batch: _if_else(cond(batch), then_(batch), else_(batch))

        case "eq":
            args = _expect_args(expr, op, 2)
            left, right = (_compile_expr(a) for a in args)
            return lambda batch: _compare(pc.equal, left(batch), right(batch), op)

        case "neq":
            args = _expect_args(expr, op, 2)
            left, right = (_compile_expr(a) for a in args)
            return lambda batch: _compare(pc.not_equal, left(batch), right(batch), op)

        case "gt" | "gte" | "lt" | "lte":
            args = _expect_args(expr, op, 2)
            left, right = (_compile_expr(a) for a in args)
            kernel = {
                "gt": pc.greater,
                "gte": pc.greater_equal,
                "lt": pc.less,
                "lte": pc.less_equal,
            }[op]
            return lambda batch: _compare(kernel, left(batch), right(batch), op)

        case "and" | "or":
            args = expr.get("args") or []
            if not args:
                raise TransformationError(
                    f"{op} expression requires at least 1 arg, got 0"
                )
            operands = [_compile_expr(a) for a in args]
            reduce = pc.and_ if op == "and" else pc.or_
            return lambda batch: _bool_reduce(reduce, [o(batch) for o in operands])

        case "not":
            args = _expect_args(expr, op, 1)
            inner = _compile_expr(args[0])
            return lambda batch: pc.invert(_truthy(inner(batch)))

        case "concat":
            args = expr.get("args") or []
            if not args:
                raise TransformationError(
                    "concat expression requires at least 1 arg, got 0"
                )
            parts = [_compile_expr(a) for a in args]
            return lambda batch: _concat([p(batch) for p in parts])

        case "coalesce":
            args = expr.get("args") or []
            if not args:
                raise TransformationError(
                    "coalesce expression requires at least 1 arg, got 0"
                )
            parts = [_compile_expr(a) for a in args]
            return lambda batch: _coalesce([p(batch) for p in parts])

        case _:
            raise TransformationError(f"Unknown expression op: {op!r}")


def _compile_fn(node: dict[str, Any]) -> Callable[[pa.Array], pa.Array]:
    """Compile a ``fn`` AST node (a pipe stage) into a vectorized column function."""
    if node.get("op") != "fn":
        raise TransformationError(
            f"Expected fn op in pipe stage, got: {node.get('op')!r}"
        )
    name = node.get("name")
    version = node.get("version", 1)
    args = node.get("args") or []
    if not name:
        raise TransformationError(
            f"fn expression is missing a 'name' field (got {name!r}); "
            "check the pipeline mapping config"
        )
    versions = _FUNCTION_CATALOG.get(name)
    if versions is None:
        raise TransformationError(f"Unknown function: {name!r}")
    kernel = versions.get(version)
    if kernel is None:
        raise TransformationError(
            f"function {name!r} has no handler for version {version}; "
            f"registered versions: {sorted(versions)}"
        )
    return lambda column: kernel(column, *args)


# ---------------------------------------------------------------------------
# Vectorized expression helpers
# ---------------------------------------------------------------------------


def _get_path(batch: pa.RecordBatch, path: list[str]) -> pa.Array:
    """Read a source column at *path*; a missing column/segment yields all-nulls.

    Mirrors the per-record ``walk_path``: an absent top-level field or a missing
    nested segment resolves to ``None`` for every row.
    """
    if not path or path[0] not in batch.schema.names:
        return pa.nulls(batch.num_rows)
    column = batch.column(path[0])
    if len(path) == 1:
        return column
    try:
        return pc.struct_field(column, path[1:])
    except (pa.ArrowInvalid, pa.ArrowTypeError, KeyError):
        return pa.nulls(batch.num_rows)


def _if_else(cond: pa.Array, then_: pa.Array, else_: pa.Array) -> pa.Array:
    """Evaluate a vectorized ternary; ``cond`` is coerced to boolean truthiness."""
    then_, else_ = _unify_null_typed([then_, else_])
    try:
        return pc.if_else(_truthy(cond), then_, else_)
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as e:
        raise TransformationError(f"if expression failed: {e}") from e


def _coalesce(arrays: list[pa.Array]) -> pa.Array:
    """Return the first non-null across columns.

    An all-null-typed operand adopts the others' type, so a column that is
    entirely null still coalesces instead of failing on a missing kernel.
    """
    unified = _unify_null_typed(arrays)
    try:
        return pc.coalesce(*unified)
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as e:
        raise TransformationError(f"coalesce expression failed: {e}") from e


def _unify_null_typed(arrays: list[pa.Array]) -> list[pa.Array]:
    """Cast any null-typed column to the single concrete type among *arrays*.

    A column whose values are all null infers Arrow ``null`` type, which shares no
    kernel with a typed column. When exactly one concrete type is present, the
    null-typed columns adopt it; otherwise the arrays are returned unchanged and
    the kernel surfaces a genuine type mismatch loudly.
    """
    concrete = {a.type for a in arrays if not pa.types.is_null(a.type)}
    if len(concrete) != 1:
        return arrays
    target = concrete.pop()
    return [a if a.type == target else pc.cast(a, target) for a in arrays]


def _compare(
    kernel: Callable[[Any, Any], pa.Array], left: pa.Array, right: pa.Array, op: str
) -> pa.Array:
    """Apply a comparison kernel, surfacing incompatible operand types loudly."""
    try:
        return kernel(left, right)
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as e:
        raise TransformationError(
            f"{op} expression cannot compare {left.type} and {right.type}: {e}"
        ) from e


def _bool_reduce(
    kernel: Callable[[Any, Any], pa.Array], operands: list[pa.Array]
) -> pa.Array:
    """Reduce boolean operands with *kernel*. None/0/'' count as False (truthiness)."""
    result = _truthy(operands[0])
    for operand in operands[1:]:
        result = kernel(result, _truthy(operand))
    return result


def _truthy(array: pa.Array) -> pa.Array:
    """Coerce a column to boolean truthiness: null counts as False."""
    if pa.types.is_boolean(array.type):
        return pc.fill_null(array, False)
    try:
        return pc.fill_null(pc.cast(array, pa.bool_()), False)
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as e:
        raise TransformationError(
            f"value of type {array.type} has no boolean truth value: {e}"
        ) from e


def _concat(parts: list[pa.Array]) -> pa.Array:
    """Concatenate columns as strings, dropping nulls (mirrors per-record concat)."""
    try:
        strings = [pc.cast(part, pa.string()) for part in parts]
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as e:
        raise TransformationError(f"concat expression cannot stringify: {e}") from e
    return pc.binary_join_element_wise(*strings, pa.scalar(""), null_handling="skip")


# ---------------------------------------------------------------------------
# Function catalog -- vectorized kernels
# ---------------------------------------------------------------------------


def _fn_trim(column: pa.Array) -> pa.Array:
    return pc.utf8_trim_whitespace(pc.cast(column, pa.string()))


def _fn_lower(column: pa.Array) -> pa.Array:
    return pc.utf8_lower(pc.cast(column, pa.string()))


def _fn_upper(column: pa.Array) -> pa.Array:
    return pc.utf8_upper(pc.cast(column, pa.string()))


def _fn_to_int(column: pa.Array) -> pa.Array:
    """Parse to int, truncating toward zero ("3.9" -> 3). Loud on unparseable."""
    try:
        as_float = pc.cast(column, pa.float64())
        return pc.cast(pc.trunc(as_float), pa.int64())
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as e:
        raise TransformationError(f"to_int: cannot convert {column.type}: {e}") from e


def _fn_to_float(column: pa.Array) -> pa.Array:
    try:
        return pc.cast(column, pa.float64())
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as e:
        raise TransformationError(f"to_float: cannot convert {column.type}: {e}") from e


def _fn_to_string(column: pa.Array) -> pa.Array:
    """Format as string -- the explicit conversion the matrix points authors to."""
    try:
        return pc.cast(column, pa.string())
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as e:
        raise TransformationError(
            f"to_string: cannot convert {column.type}: {e}"
        ) from e


def _fn_abs(column: pa.Array) -> pa.Array:
    try:
        return pc.abs(column)
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as e:
        raise TransformationError(
            f"abs: cannot apply to {column.type}; expected a numeric column: {e}"
        ) from e


def _fn_now(column: pa.Array) -> pa.Array:
    """Broadcast the current UTC datetime to every row."""
    return pa.array([datetime.now(timezone.utc)] * len(column))


def _fn_default(column: pa.Array, default_value: Any = None) -> pa.Array:
    """Substitute *default_value* wherever the column is null."""
    try:
        return pc.fill_null(column, pa.scalar(default_value, column.type))
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as e:
        raise TransformationError(
            f"default: cannot fill {column.type} with {default_value!r}: {e}"
        ) from e


def _fn_coalesce(column: pa.Array, *alternatives: Any) -> pa.Array:
    """Return the first non-null among the column and the literal alternatives."""
    scalars = [pa.scalar(alt, column.type) for alt in alternatives]
    return pc.coalesce(column, *scalars)


def _fn_iso_to_datetime(column: pa.Array) -> pa.Array:
    """Parse ISO-8601 strings into timezone-aware microsecond timestamps."""
    try:
        return pc.cast(pc.cast(column, pa.string()), pa.timestamp("us", tz="UTC"))
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as e:
        raise TransformationError(f"iso_to_datetime failed: {e}") from e


def _fn_iso_to_date(column: pa.Array) -> pa.Array:
    """Render the date part of an ISO-8601 value as a ``YYYY-MM-DD`` string.

    Every ISO-8601 form begins with the calendar date, so the leading 10
    characters are sliced and validated by parsing them as a naive timestamp.
    This accepts the full range the per-record path did -- a bare date, a naive
    datetime, and a tz-suffixed timestamp (``Z`` or an offset) alike -- keeping
    the original wall-clock date, while still failing loud on a value that is
    not a date. (A direct string -> date32 cast is unavailable on pyarrow 12, so
    the date is round-tripped through a timestamp.)
    """
    try:
        date_part = pc.utf8_slice_codeunits(pc.cast(column, pa.string()), 0, 10)
        return pc.strftime(pc.cast(date_part, pa.timestamp("s")), format="%Y-%m-%d")
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as e:
        raise TransformationError(f"iso_to_date failed: {e}") from e


# name -> {version -> kernel}. New behaviour ships under a new version int; never
# rewrite an existing version in place (mappings pin ``version``).
_FUNCTION_CATALOG: dict[str, dict[int, Callable[..., pa.Array]]] = {
    "iso_to_date": {1: _fn_iso_to_date},
    "iso_to_datetime": {1: _fn_iso_to_datetime},
    "iso_to_timestamp": {1: _fn_iso_to_datetime},
    "trim": {1: _fn_trim},
    "lower": {1: _fn_lower},
    "upper": {1: _fn_upper},
    "to_int": {1: _fn_to_int},
    "to_float": {1: _fn_to_float},
    "to_string": {1: _fn_to_string},
    "abs": {1: _fn_abs},
    "now": {1: _fn_now},
    "default": {1: _fn_default},
    "coalesce": {1: _fn_coalesce},
}


# ---------------------------------------------------------------------------
# Validation -- vectorized, batch-wide, fail-loud
# ---------------------------------------------------------------------------


def _run_validation(
    value: pa.Array, validate: dict[str, Any], field: pa.Field
) -> list[str]:
    """Return one error string per failing rule, or ``[]`` if every row passes.

    Each rule becomes a boolean failure mask over the batch; a null value is
    exempt from every rule except ``not_null`` (mirroring the per-record
    ``if value is not None`` guard). A malformed rule (bad regex, type mismatch)
    fails loud with a :class:`TransformationError`.
    """
    errors: list[str] = []
    rules = validate.get("rules") or []
    present = pc.is_valid(value)

    for rule in rules:
        rule_type = rule.get("type")
        mask = _rule_failure_mask(value, present, rule, rule_type, field)
        if mask is None:
            continue
        if pc.any(mask, min_count=0).as_py():
            rows = [i for i, failed in enumerate(mask.to_pylist()) if failed]
            message = rule.get("message")
            detail = f": {message}" if message else ""
            errors.append(
                f"column {field.name!r}: {len(rows)} row(s) fail rule "
                f"{rule_type!r}{detail} (rows {rows[:5]})"
            )
    return errors


def _rule_failure_mask(
    value: pa.Array,
    present: pa.Array,
    rule: dict[str, Any],
    rule_type: str | None,
    field: pa.Field,
) -> pa.Array | None:
    """Compute the boolean failure mask for one validation rule.

    Returns ``None`` for an unrecognised rule type (ignored, as the per-record
    validator did). Failures are ``present AND predicate`` so nulls never trip a
    value rule.
    """

    def failing(predicate: pa.Array) -> pa.Array:
        return pc.and_(present, pc.fill_null(predicate, False))

    try:
        match rule_type:
            case "not_null" | "required":
                return pc.is_null(value)
            case "min_length":
                length = pc.utf8_length(pc.cast(value, pa.string()))
                return failing(pc.less(length, rule.get("value", 0)))
            case "max_length":
                length = pc.utf8_length(pc.cast(value, pa.string()))
                return failing(pc.greater(length, rule.get("value", 0)))
            case "pattern":
                pattern = rule.get("value", "")
                matched = pc.match_substring_regex(
                    pc.cast(value, pa.string()), pattern=f"^(?:{pattern})"
                )
                return failing(pc.invert(matched))
            case "range":
                return _range_failure_mask(value, present, rule)
            case "in_list":
                allowed = rule.get("value", [])
                return failing(pc.invert(pc.is_in(value, value_set=pa.array(allowed))))
            case _:
                return None
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as e:
        raise TransformationError(
            f"column {field.name!r}: validation rule {rule_type!r} is "
            f"invalid for a {value.type} column: {e}"
        ) from e


def _range_failure_mask(
    value: pa.Array, present: pa.Array, rule: dict[str, Any]
) -> pa.Array:
    min_val = rule.get("min")
    max_val = rule.get("max")
    fail = pa.array([False] * len(value))
    if min_val is not None:
        fail = pc.or_(fail, pc.fill_null(pc.less(value, min_val), False))
    if max_val is not None:
        fail = pc.or_(fail, pc.fill_null(pc.greater(value, max_val), False))
    return pc.and_(present, fail)


# ---------------------------------------------------------------------------
# Type materialisation
# ---------------------------------------------------------------------------


def _retype_column(column: pa.Array, field: pa.Field) -> pa.Array:
    """Convert a source column to its target scalar type, gated by the matrix.

    The conversion matrix (:mod:`cdk.type_map.conversions`) -- the same policy the
    destination cast consults -- decides whether the conversion is permitted; a
    ``forbidden`` or ``explicit`` pair fails loud, naming the function an
    ``explicit`` conversion must declare (rather than a cryptic ``ArrowTypeError``
    or a silent stringification). A permitted pair runs through the same
    ``pc.cast(safe=True)`` as ``SchemaContract.cast_arrow_batch``, so the transform
    and the destination execute an identical conversion: both parse ``"1" ->
    Int64``, both reject a lossy ``Float64 -> Int64`` or an out-of-range narrowing.
    """
    conversion = classify_arrow_conversion(column.type, field.type)
    if conversion.mode == "forbidden":
        raise TransformationError(
            f"column {field.name!r}: converting {column.type} -> {field.type} "
            f"is not a permitted conversion"
        )
    if conversion.mode == "explicit":
        raise TransformationError(
            f"column {field.name!r}: converting {column.type} -> {field.type} "
            f"requires an explicit '{conversion.fn}' conversion declared in the "
            f"mapping"
        )
    try:
        return pc.cast(column, field.type, safe=True)
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as e:
        raise TransformationError(
            f"column {field.name!r}: cannot convert {column.type} -> "
            f"{field.type}: {e}"
        ) from e


def _cast_structural(column: pa.Array, field: pa.Field) -> pa.Array:
    """Assemble a nested (``struct``/``list``) target column.

    Nested shapes are outside the scalar conversion matrix; the cast is the
    structural materialisation of the declared shape. Wraps pyarrow's errors so a
    shape mismatch fails the batch with the column named.
    """
    try:
        return pc.cast(column, field.type, safe=True)
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as e:
        raise TransformationError(
            f"column {field.name!r}: cannot assemble {field.type} from "
            f"{column.type}: {e}"
        ) from e


def _build_const_array(values: list[Any], field: pa.Field) -> pa.Array:
    """Materialise a ``const`` literal's Python *values* at the target type.

    A const is a Python value declared in the mapping (not a typed source column),
    so there is no source arrow_type to classify and ``pa.array`` constructs it at
    the target type directly. Wraps pyarrow's conversion errors with the column
    name so a bad constant fails the batch with a clear message.
    """
    try:
        return pa.array(values, type=field.type)
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as e:
        raise TransformationError(
            f"column {field.name!r}: cannot build {field.type} column: {e}"
        ) from e


def _encode_json_column(value: pa.Array, field: pa.Field) -> pa.Array:
    """Encode a value column for a ``Json`` target (carried as a string column).

    A string column passes through (it is already a JSON-encoded value an API
    source shipped, or a string const). A struct/list column is ``json.dumps``-ed
    per row -- a column-level encode, not a per-record round-trip. The destination
    decoder reverses this at the write boundary.
    """
    if pa.types.is_string(value.type) or pa.types.is_large_string(value.type):
        return pc.cast(value, field.type)
    if pa.types.is_null(value.type):
        return pa.nulls(len(value), type=field.type)
    encoded = [_json_encode_scalar(item, field.name) for item in value.to_pylist()]
    return pa.array(encoded, type=field.type)


def _json_encode_scalar(item: Any, field_name: str) -> str | None:
    """JSON-encode one value for a ``Json`` column; pass strings/None through."""
    if item is None or isinstance(item, str):
        return item
    if not isinstance(item, (dict, list)):
        raise TransformationError(
            f"column {field_name!r}: Json target requires dict/list/str/None, "
            f"got {type(item).__name__}"
        )
    try:
        return json.dumps(item)
    except TypeError as e:
        raise TransformationError(
            f"column {field_name!r}: Json target value is not JSON-serializable: {e}"
        ) from e


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _expect_args(expr: dict[str, Any], op: str, count: int) -> list[dict[str, Any]]:
    args = expr.get("args") or []
    if len(args) != count:
        raise TransformationError(
            f"{op} expression requires {count} args, got {len(args)}"
        )
    return args


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
