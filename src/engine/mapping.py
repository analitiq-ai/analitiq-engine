"""The stream mapping: one typed document, compiled to Arrow compute.

This module owns the whole mapping vocabulary -- the path grammar, the
expression AST, the function catalog, the validation rules and the output
schema. A stream's mapping is the contract's :class:`StreamMapping`, read as the
validated document, compiled once by
:func:`compile_mapping` into a :class:`CompiledTransform`, and then applied to
each ``pa.RecordBatch`` with ``pyarrow.compute`` -- the batch never leaves
Arrow. There is a single transform path: every assignment, every expression
op, and every function in the catalog is a vectorized column operation.

A source path is an ordered token array (``["a", "b"]``) from the contract
document all the way to ``pc.struct_field``. Nothing splits a string on a dot
anywhere on that route: a dotted string is a path plus an unstated splitting
convention, and when one module split it and another expected tokens, a nested
read silently produced an all-null column instead of failing.

The compilers cover exactly the contract's expression forms and conversion
function names, and that is checked at import: a contract release that adds a
form or a function fails the engine at startup, not on the first batch that
carries it.

Each validation rule is compiled with its effective error strategy: the
assignment's ``validate.error_handling.strategy`` when declared, else the
pipeline's ``runtime.error_handling.strategy`` the transform was compiled
with. A rule failure raises :class:`ValidationFailure`, which carries the
strictest strategy among the rules that failed on the batch, and the stream
disposes of the batch through :meth:`BatchPolicy.reject` -- fail, dead-letter
or skip. The override's ``max_retries`` / ``retry_delay_seconds`` are not
read: a rule is a pure function of the batch, so a retry would fail the same
rows the same way.

Type conversion has one authority. When an assignment's evaluated value lands in
a column of a different Arrow type than the target declares, the conversion is
gated by the **conversion matrix** (:mod:`cdk.type_map.conversions`) and executed
by the same ``pc.cast(safe=True)`` the destination uses -- so the transform and
the destination cast of one column always agree (both parse ``"1" -> Int64``,
both reject a lossy ``Float64 -> Int64``). A ``constant`` is a Python value
declared in the mapping, not a typed Arrow column, so it is materialised at the
target type directly (``pa.array``) -- there is no source arrow_type to classify.
Nested (``Object``/``List``) and ``Json`` targets are assembled structurally, not
through the scalar matrix.

Failures are loud and batch-wide. An expression that cannot be evaluated, an
unparseable cast, or a null in a non-nullable column fails the whole batch
with a :class:`TransformationError`, a mapping defect no strategy relaxes. A
row that fails a validation rule fails the whole batch with a
:class:`ValidationFailure`; the strategy it carries decides what the stream
does with the batch.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Final, cast, get_args

import pyarrow as pa
import pyarrow.compute as pc
from analitiq.contracts.stream import (
    Assignment,
    AssignmentValue,
    ConstantAssignmentValue,
    ExpressionAssignmentValue,
    FnExpression,
    GetExpression,
    PipeExpression,
    StreamMapping,
    Validation,
    ValidationRule,
)

from cdk.type_map.arrow import (
    classify_arrow_conversion,
    first_blocked_nested_leaf,
    resolve_arrow_type,
)
from cdk.type_map.exceptions import InvalidTypeMapError
from src.config.utils import author_set

from .batch_policy import ErrorStrategy
from .exceptions import TransformationError, ValidationFailure

# A compiled expression: given the source batch, return one value column.
_ExprFn = Callable[[pa.RecordBatch], pa.Array]

# The rules a null answers rather than skips: every other rule exempts a null
# value (mirroring the per-record ``if value is not None`` guard), and these
# are the ones a null LIST ancestor must fail too. Named once, so a rule type
# added to the mask without the ancestor fold cannot silently exempt itself.
_NULL_SENSITIVE_RULES: Final[frozenset[str]] = frozenset({"not_null", "required"})


def build_output_schema(assignments: list[Assignment]) -> pa.Schema:
    """Build the post-transform Arrow schema from a stream's assignments.

    A target names exactly one field on the destination record root. Nesting is
    declared by ``arrow_type: "Object"`` with a ``target.properties`` map, or
    ``arrow_type: "List"`` with ``target.items`` -- :func:`resolve_arrow_type`
    handles the recursion.
    """
    fields: list[pa.Field] = []
    for index, assignment in enumerate(assignments):
        target = assignment.target
        where = f"assignment[{index}] target={target.path!r}"
        try:
            arrow_type = resolve_arrow_type(
                target.model_dump(exclude_none=True), where=where
            )
        except InvalidTypeMapError as e:
            # The contract's published type pattern and the CDK's type parser are
            # maintained separately; where they disagree the mapping must name
            # the assignment rather than let a CDK error escape untraced.
            raise TransformationError(
                f"{where}: cannot parse target.arrow_type={target.arrow_type!r}: {e}"
            ) from e

        fields.append(pa.field(target.path, arrow_type, nullable=target.nullable))
    return pa.schema(fields)


@dataclass(frozen=True, slots=True)
class _Step:
    """One compiled assignment: how to build and type a column."""

    field: pa.Field
    build: _ExprFn
    is_const: bool
    is_json: bool


@dataclass(frozen=True)
class _BoundRule:
    """A validation rule with the strategy its failure is handled under."""

    rule: ValidationRule
    strategy: ErrorStrategy


class CompiledTransform:
    """A stream's assignments compiled to vectorized column operations.

    Built once per stream by :func:`compile_mapping`; :meth:`run` applies it to
    each batch with no per-record Python and no ``to_pylist``/``from_pylist``
    round-trip.
    """

    def __init__(
        self,
        output_schema: pa.Schema,
        steps: list[_Step],
        rules: list[_BoundRule],
    ) -> None:
        self.output_schema = output_schema
        self._steps = steps
        self._rules = rules

    def run(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        """Apply the transform to *batch*, returning the output batch.

        Raises :class:`TransformationError` if any column fails to build, any
        conversion is rejected, or a non-nullable column ends up with nulls;
        that is a mapping defect whatever the rules say. A build or
        conversion failure raises on the spot, before the rules have been
        reported; a non-nullable null is collected, and the rule failures on
        the same batch ride along in that message. Otherwise raises
        :class:`ValidationFailure` if any rule fails on any row, naming the
        column and the offending rows and carrying the strictest strategy
        among the failed rules.

        Every column is built before any rule runs: a rule's ``field`` may
        address any declared target, not just its own assignment's, so
        validation needs the whole record the assignments build together.
        Rules run against the built (pre-conversion) values, exactly as they
        did when each rule was bound to its own column.
        """
        errors: list[str] = []
        built = {step.field.name: step.build(batch) for step in self._steps}

        rule_errors: list[str] = []
        failed_under: list[ErrorStrategy] = []
        for bound in self._rules:
            messages = _rule_errors(built, bound.rule)
            rule_errors.extend(messages)
            failed_under.extend(bound.strategy for _ in messages)

        arrays: list[pa.Array] = []
        for step in self._steps:
            array = self._coerce(built[step.field.name], step)
            if not step.field.nullable and array.null_count > 0:
                errors.append(
                    f"column {step.field.name!r}: {array.null_count} null "
                    f"value(s) but field is not nullable"
                )
            arrays.append(array)

        if errors:
            raise TransformationError(_summarise(errors + rule_errors))
        if rule_errors:
            raise ValidationFailure(
                _summarise(rule_errors),
                strategy=ErrorStrategy.strictest(failed_under),
            )

        return pa.RecordBatch.from_arrays(arrays, schema=self.output_schema)

    @staticmethod
    def _coerce(value: pa.Array, step: _Step) -> pa.Array:
        """Convert an evaluated value column to the target field type.

        - A ``constant`` is already built at the target type (or JSON-encoded
          for a ``Json`` target); it passes through.
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


def _summarise(errors: list[str]) -> str:
    """Build the batch-level message: the first few errors, plus a count of the rest."""
    shown = "; ".join(errors[:5])
    suffix = f" (+{len(errors) - 5} more)" if len(errors) > 5 else ""
    return f"transform produced {len(errors)} error(s): {shown}{suffix}"


def compile_mapping(
    document: StreamMapping, *, default_strategy: ErrorStrategy
) -> CompiledTransform:
    """Compile a stream's mapping into a :class:`CompiledTransform`.

    Static work (schema building, expression compilation, validation setup)
    happens here once; the returned object is applied per batch.
    ``default_strategy`` is the pipeline's ``runtime.error_handling.strategy``,
    which a validation rule takes unless its assignment overrides it. Raises
    :class:`TransformationError` for a target ``arrow_type`` the engine's type
    grammar rejects.
    """
    assignments = document.assignments
    output_schema = build_output_schema(assignments)
    steps: list[_Step] = []
    for assignment, field in zip(assignments, output_schema):
        is_json = assignment.target.arrow_type == "Json"
        build, is_const = _compile_value(assignment.value, field, is_json)
        steps.append(
            _Step(
                field=field,
                build=build,
                is_const=is_const,
                is_json=is_json,
            )
        )
    # Rules are held per transform rather than per step: a rule's `field`
    # addresses any declared target (checked by the contract), so it is
    # resolved against the built record, not its own assignment's column.
    rules = [
        _BoundRule(rule, _rule_strategy(assignment.validation, default_strategy))
        for assignment in assignments
        if assignment.validation is not None
        for rule in assignment.validation.rules
    ]
    return CompiledTransform(output_schema, steps, rules)


def _rule_strategy(
    validation: Validation, default_strategy: ErrorStrategy
) -> ErrorStrategy:
    """Resolve the strategy an assignment's rules fail under.

    The contract's ``error_handling`` is documented as an override of the
    pipeline ``runtime.error_handling`` default, so only a ``strategy`` the
    author wrote overrides it. An absent block, or a block that sets only the
    retry fields, means the pipeline default -- never the contract model's
    own default value. Author intent is decided by the same
    :func:`author_set` the pipeline block goes through, so the two blocks
    cannot drift. Only ``strategy`` is read: the block's retry fields
    describe a retry loop a deterministic rule has no use for.
    """
    override = validation.error_handling
    if override is None:
        return default_strategy
    chosen = author_set(override, strategy=override.strategy)
    return ErrorStrategy(chosen["strategy"]) if chosen else default_strategy


def _compile_value(
    value: AssignmentValue, field: pa.Field, is_json: bool
) -> tuple[_ExprFn, bool]:
    """Compile an assignment's ``value`` block into a column builder.

    Returns ``(build_fn, is_const)``. A constant builds a broadcast column at
    the target type (JSON-encoded for a ``Json`` target); an expression
    compiles its AST to vectorized compute that produces a column at its
    natural type.
    """
    if isinstance(value, ConstantAssignmentValue):
        # The constant's own arrow_type declares the literal's JSON kind for
        # authoring tools; the column is built at the TARGET type, which is the
        # type the destination receives.
        return _compile_const(value.constant.value, field, is_json), True
    return _compile_expr(value.expression), False


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


# Expression -> vectorized compute. Each compiler returns a closure over the
# batch so the (static) expression walk happens once at compile time, not per
# batch.


def _compile_get(expr: GetExpression) -> _ExprFn:
    path = list(expr.path)
    return lambda batch: _get_path(batch, path)


def _compile_pipe(expr: PipeExpression) -> _ExprFn:
    # The contract fixes the positions: a `get` seed, then `fn` stages.
    seed = _compile_get(cast(GetExpression, expr.args[0]))
    stages = [_FUNCTIONS[cast(FnExpression, stage).name] for stage in expr.args[1:]]

    def run_pipe(batch: pa.RecordBatch) -> pa.Array:
        value = seed(batch)
        for stage in stages:
            value = stage(value)
        return value

    return run_pipe


_EXPR_COMPILERS: Final[dict[type, Callable[[Any], _ExprFn]]] = {
    GetExpression: _compile_get,
    PipeExpression: _compile_pipe,
}

_EXPRESSION_FORMS = get_args(
    ExpressionAssignmentValue.model_fields["expression"].annotation
)
if set(_EXPR_COMPILERS) != set(_EXPRESSION_FORMS):
    raise TypeError(
        "the contract's expression forms and the engine's compilers differ: "
        f"{sorted(f.__name__ for f in set(_EXPR_COMPILERS) ^ set(_EXPRESSION_FORMS))}"
    )


def _compile_expr(expr: GetExpression | PipeExpression) -> _ExprFn:
    """Compile one expression node into a vectorized column builder."""
    return _EXPR_COMPILERS[type(expr)](expr)


# ---------------------------------------------------------------------------
# Vectorized expression helpers
# ---------------------------------------------------------------------------


def _get_path(batch: pa.RecordBatch, path: list[str]) -> pa.Array:
    """Read a source column at *path*; a missing column/segment yields all-nulls.

    Mirrors the per-record ``walk_path``: an absent top-level field or a missing
    nested segment resolves to ``None`` for every row.
    """
    if path[0] not in batch.schema.names:
        return pa.nulls(batch.num_rows)
    column = batch.column(path[0])
    if len(path) == 1:
        return column
    try:
        return pc.struct_field(column, path[1:])
    except (pa.ArrowInvalid, pa.ArrowTypeError, KeyError):
        return pa.nulls(batch.num_rows)


def _string_form(column: pa.Array) -> pa.Array:
    """Render a column as strings the way the per-record ``str()`` did.

    Booleans become ``"True"``/``"False"`` rather than Arrow's lowercase
    ``"true"``/``"false"``; everything else uses Arrow's string cast. Shared by
    ``to_string`` and the string-length and pattern rules so they never
    diverge on booleans.
    """
    if pa.types.is_boolean(column.type):
        return pc.if_else(column, pa.scalar("True"), pa.scalar("False"))
    return pc.cast(column, pa.string())


# ---------------------------------------------------------------------------
# Function catalog -- vectorized kernels
# ---------------------------------------------------------------------------


def _fn_to_string(column: pa.Array) -> pa.Array:
    """Format as string -- the explicit conversion the matrix points authors to.

    Booleans render as ``"True"``/``"False"`` to match the per-record ``str()``
    the catalog v1 used (via :func:`_string_form`); Arrow's cast would emit
    lowercase ``"true"``/``"false"`` and silently change every existing
    boolean-to-string mapping.
    """
    try:
        return _string_form(column)
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as e:
        raise TransformationError(
            f"to_string: cannot convert {column.type}: {e}"
        ) from e


#: One kernel per conversion function the contract lets a `pipe` stage name.
_FUNCTIONS: Final[dict[str, Callable[[pa.Array], pa.Array]]] = {
    "to_string": _fn_to_string,
}

_FUNCTION_NAMES = get_args(FnExpression.model_fields["name"].annotation)
if set(_FUNCTIONS) != set(_FUNCTION_NAMES):
    raise TypeError(
        "the contract's conversion functions and the engine's kernels differ: "
        f"{sorted(set(_FUNCTIONS) ^ set(_FUNCTION_NAMES))}"
    )


# ---------------------------------------------------------------------------
# Validation -- vectorized, batch-wide, fail-loud
# ---------------------------------------------------------------------------


def _rule_label(tokens: list[str]) -> str:
    """How an error names the addressed field: the token array's spelling.

    A single token reads as the column name it is; a nested address is
    reported as the token list, never joined by a dot -- this contract spells
    nesting one token at a time, and an error message is a place authors
    copy from.
    """
    return repr(tokens[0]) if len(tokens) == 1 else repr(list(tokens))


def _addressed_values(
    built: Mapping[str, pa.Array], tokens: list[str]
) -> tuple[pa.Array, pa.Array | None, list[int]]:
    """Return a rule's addressed values, their source rows, and null ancestors.

    The first token selects a built column; each later token descends into
    it -- ``struct_field`` for an ``Object`` level, and a ``List`` level is
    flattened first, with ``list_parent_indices`` composing the element ->
    batch-row map. The second element is that map, or ``None`` when no list
    was crossed (value *i* is row *i*).

    The map stays an Arrow array the whole way down. It is read only to
    name rows in an error, so materialising it costs a Python int per
    element per rule on every batch that PASSES -- the common case, and
    exactly the per-record Python :class:`CompiledTransform` promises not
    to do. ``pc.take`` composes one level onto the next inside Arrow, and
    the caller takes the few indexes it actually reports.

    The third element is every batch row a null LIST removed from the walk:
    ``list_flatten`` drops a null list's (nonexistent) elements, where a
    null struct simply propagates null children -- two spellings of one
    fact ("an ancestor of the addressed field is null") that must reach the
    rules as one answer. The caller folds these rows into ``not_null``
    failures and exempts them from value rules, exactly as the null
    children the struct path yields are treated by the mask. An EMPTY list
    is not in it: zero elements is data, and grades as such.
    """
    value = built[tokens[0]]
    row_map: pa.Array | None = None
    null_ancestors: set[int] = set()
    for token in tokens[1:]:
        while pa.types.is_list(value.type) or pa.types.is_large_list(value.type):
            # Same rule as the failure mask below: the scan stays out of
            # Python unless there is something to find. A batch whose lists
            # are all present -- the common case -- pays nothing here, and
            # one with nulls pays only for the nulls.
            if value.null_count:
                absent = pc.indices_nonzero(pc.is_null(value))
                null_ancestors.update(
                    absent.to_pylist()
                    if row_map is None
                    else _row_numbers(row_map, absent)
                )
            parents = pc.list_parent_indices(value)
            row_map = parents if row_map is None else pc.take(row_map, parents)
            value = pc.list_flatten(value)
        if pa.types.is_null(value.type):
            # A column that carried no value anywhere infers as Arrow's `null`
            # type, which no `struct_field` kernel accepts. Every deeper token
            # addresses only nulls, so the null values stand in for the field.
            break
        value = pc.struct_field(value, token)
    return value, row_map, sorted(null_ancestors)


def _row_numbers(row_map: pa.Array, indices: pa.Array) -> list[int]:
    """Name the batch rows *indices* address, through *row_map*.

    The one place an index leaves Arrow for Python, called with the handful
    of indexes an error actually names rather than with one per row.
    """
    rows: list[int] = pc.take(row_map, indices).to_pylist()
    return rows


def _rule_errors(built: Mapping[str, pa.Array], rule: ValidationRule) -> list[str]:
    """Return the error for *rule* over the built record, or ``[]`` on pass.

    The rule becomes a boolean failure mask over the addressed values; a null
    value is exempt from every rule except ``not_null`` (mirroring the
    per-record ``if value is not None`` guard), and a null LIST ancestor is
    the same null one level up -- it fails ``not_null`` on the addressed
    field exactly as a null struct parent's propagated null does, and is
    exempt from value rules the same way. A malformed rule (bad regex, type
    mismatch, a path into a value that carries no such structure) fails
    loud with a :class:`TransformationError`. When the address crossed a
    ``List``, a batch row fails if any of its elements does.
    """
    tokens = list(rule.field)
    label = _rule_label(tokens)
    try:
        value, row_map, null_ancestors = _addressed_values(built, tokens)
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as e:
        raise TransformationError(
            f"column {label}: validation rule {rule.type!r} addresses a "
            f"declared field the built value does not carry: {e}"
        ) from e
    present = pc.is_valid(value)
    mask = _rule_failure_mask(value, present, rule, label)
    # The passing path stays inside Arrow: `pc.any` answers from the mask's
    # own buffers, where `to_pylist` would allocate a Python object per row
    # per rule on every batch that passes -- the common case, and the hot
    # one. Row indexes are materialised only for a rule that actually
    # failed, or for the null ancestors a null-sensitive rule must add.
    ancestors = null_ancestors if rule.type in _NULL_SENSITIVE_RULES else []
    if not pc.any(mask, min_count=0).as_py():
        if not ancestors:
            return []
        rows = list(ancestors)
    else:
        failing = pc.indices_nonzero(mask)
        rows = sorted(
            set(
                failing.to_pylist()
                if row_map is None
                else _row_numbers(row_map, failing)
            )
            | set(ancestors)
        )
    detail = f": {rule.message}" if rule.message else ""
    return [
        f"column {label}: {len(rows)} row(s) fail rule "
        f"{rule.type!r}{detail} (rows {rows[:5]})"
    ]


def _rule_failure_mask(
    value: pa.Array,
    present: pa.Array,
    rule: ValidationRule,
    label: str,
) -> pa.Array:
    """Compute the boolean failure mask for one validation rule.

    Failures are ``present AND predicate`` so nulls never trip a value rule.
    """

    def failing(predicate: pa.Array) -> pa.Array:
        return pc.and_(present, pc.fill_null(predicate, False))

    try:
        match rule.type:
            case rule_type if rule_type in _NULL_SENSITIVE_RULES:
                return pc.is_null(value)
            case "min_length":
                length = pc.utf8_length(_string_form(value))
                return failing(pc.less(length, rule.value))
            case "max_length":
                length = pc.utf8_length(_string_form(value))
                return failing(pc.greater(length, rule.value))
            case "pattern":
                matched = pc.match_substring_regex(
                    _string_form(value), pattern=f"^(?:{rule.value})"
                )
                return failing(pc.invert(matched))
            case "range":
                return _range_failure_mask(value, present, rule)
            case "in_list":
                return failing(
                    pc.invert(pc.is_in(value, value_set=pa.array(rule.value)))
                )
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as e:
        raise TransformationError(
            f"column {label}: validation rule {rule.type!r} is "
            f"invalid for a {value.type} column: {e}"
        ) from e
    # Reached only if the contract's rule-type vocabulary grows and this match
    # does not: a rule the engine cannot enforce must fail, never pass silently.
    raise TransformationError(
        f"column {label}: validation rule type {rule.type!r} has no "
        f"engine implementation"
    )


def _range_failure_mask(
    value: pa.Array, present: pa.Array, rule: ValidationRule
) -> pa.Array:
    """Fail rows outside the bounds carried in the rule's ``value`` object."""
    bounds = rule.value
    fail = pa.array([False] * len(value))
    minimum = bounds.get("min")
    maximum = bounds.get("max")
    if minimum is not None:
        fail = pc.or_(fail, pc.fill_null(pc.less(value, minimum), False))
    if maximum is not None:
        fail = pc.or_(fail, pc.fill_null(pc.greater(value, maximum), False))
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
    """Assemble a nested (``struct``/``list``) target column, gating each leaf.

    The shape is materialised structurally, but every scalar leaf inside it is a
    real ``source -> target`` conversion and clears the same matrix a top-level
    scalar retype does: an ``Int64 -> Utf8`` leaf is ``explicit`` and an
    ``Object -> Int64`` leaf is ``forbidden`` whether it sits at the top level or
    three fields deep. Without this gate ``pc.cast`` would silently stringify a
    numeric struct leaf -- the same author intent a scalar retype rejects. A
    blocked leaf fails the batch naming the leaf's path; the mapping grammar has
    no per-leaf function slot, so an ``explicit`` leaf is resolved by
    restructuring the mapping to supply that leaf already typed. Wraps pyarrow's
    errors so a shape mismatch fails the batch with the column named.
    """
    blocked = first_blocked_nested_leaf(column.type, field.type, field.name)
    if blocked is not None:
        conversion = blocked.conversion
        detail = (
            f"requires an explicit '{conversion.fn}' conversion, which the mapping "
            f"grammar cannot express per leaf -- restructure the mapping to supply "
            f"this leaf already typed"
            if conversion.mode == "explicit"
            else "is not a permitted conversion"
        )
        raise TransformationError(
            f"column {field.name!r}: nested leaf {blocked.path!r} converting "
            f"{blocked.source} -> {blocked.target} {detail}"
        )
    try:
        return pc.cast(column, field.type, safe=True)
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as e:
        raise TransformationError(
            f"column {field.name!r}: cannot assemble {field.type} from "
            f"{column.type}: {e}"
        ) from e


def _build_const_array(values: list[Any], field: pa.Field) -> pa.Array:
    """Materialise a ``constant``'s Python *values* at the target type.

    A constant is a Python value declared in the mapping (not a typed source
    column), so there is no source arrow_type to classify and ``pa.array``
    constructs it at the target type directly. Wraps pyarrow's conversion errors
    with the column name so a bad constant fails the batch with a clear message.
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
    source shipped, or a string constant). A struct/list column is
    ``json.dumps``-ed per row -- a column-level encode, not a per-record
    round-trip. The destination decoder reverses this at the write boundary.
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
