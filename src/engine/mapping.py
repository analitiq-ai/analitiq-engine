"""The stream mapping: one typed document, compiled to Arrow compute.

This module owns the whole mapping vocabulary -- the path grammar, the
expression AST, the function catalog, the validation rules and the output
schema. A stream's mapping document (per ``mapping-and-transformations.md``)
is parsed once into a :class:`MappingDocument`, compiled once by
:func:`compile_mapping` into a :class:`CompiledTransform`, and then applied to
each ``pa.RecordBatch`` with ``pyarrow.compute`` -- the batch never leaves
Arrow. There is a single transform path: every assignment, every expression
op, and every function in the catalog is a vectorized column operation.

A source path is an ordered token array (``["a", "b"]``) from the contract
document all the way to ``pc.struct_field``. Nothing splits a string on a dot
anywhere on that route: a dotted string is a path plus an unstated splitting
convention, and when one module split it and another expected tokens, a nested
read silently produced an all-null column instead of failing.

:class:`MappingDocument` is closed (``extra="forbid"``) at every level, and the
expression AST -- the one sub-tree that is not a contract model, because the
engine compiles a wider op set than the contract publishes -- is closed op by
op at compile time (:data:`_EXPR_NODE_KEYS`). So a field the contract carries
cannot go missing between the document and the transform: it is either compiled
or it is rejected by name. This is a deliberate departure from a
forward-compatible protocol that tolerates unknown keys -- affordable only
because the engine and the contract models are pinned in lockstep, and worth it
because a dropped field is silent while a named rejection is readable.

One contract field is deliberately not acted on: ``validate.error_handling``.
Failure handling is a pipeline-runtime setting (strategy, retries, delay) the
engine applies to a whole batch, and a validation failure fails the whole batch
by design, so an assignment-scoped override has no grain to act at. It is
carried for the control plane and named as inert in
``mapping-and-transformations.md`` rather than silently absorbed.

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

Failures are loud and batch-wide. A row that fails a validation rule, an
expression that cannot be evaluated, an unparseable cast, or a null in a
non-nullable column fails the whole batch with a :class:`TransformationError`.
The engine's ``error_strategy`` -- not this module -- decides retry vs DLQ.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Annotated, Any, Final, Literal

import pyarrow as pa
import pyarrow.compute as pc
from analitiq.contracts.shared.common import StrictModel
from analitiq.contracts.stream import (
    ArrowFieldSpec,
    AssignmentTarget,
    ConstantAssignmentValue,
    Validation,
    ValidationRule,
)
from pydantic import Field, ValidationError, model_validator

from cdk.type_map.arrow import (
    classify_arrow_conversion,
    first_blocked_nested_leaf,
    resolve_arrow_type,
)
from cdk.type_map.exceptions import InvalidTypeMapError

from .exceptions import TransformationError

# A compiled expression: given the source batch, return one value column.
_ExprFn = Callable[[pa.RecordBatch], pa.Array]


# ---------------------------------------------------------------------------
# The mapping document
# ---------------------------------------------------------------------------


class ExpressionValue(StrictModel):
    """``{"kind": "expression", "expression": {...}}`` -- assign from a read.

    Widens the contract's expression variant on one axis only: the contract
    publishes the ``get``/``pipe``/``fn`` nodes an authoring UI can offer,
    while the engine compiles a larger op set (see :func:`_compile_expr`). The
    AST therefore stays an untyped mapping here and is validated -- op by op,
    arity, and every key the node carries -- at compile time, where the
    vocabulary is defined. Widening the type does not open the document: a key
    no op declares is refused there by name, exactly as it is at this level.
    """

    kind: Literal["expression"]
    expression: dict[str, Any]


# The `kind` discriminator is required and has no default. A value block that
# omitted it used to be read as an expression, so a constant the compiler could
# not find became an unknown-op failure about the AST rather than a statement
# about the missing discriminator.
AssignmentValue = Annotated[
    ExpressionValue | ConstantAssignmentValue,
    Field(discriminator="kind"),
]


class MappingAssignment(StrictModel):
    """One assignment: how to build, type and validate a single target field."""

    # `target`, `constant` and `validate` are the contract's own models, so the
    # target's single-segment path rule, the Arrow container-shape rules and
    # the validation-rule grammar are enforced from one definition rather than
    # a second engine-side spelling of the same shapes.
    target: AssignmentTarget
    value: AssignmentValue
    # `Validation.error_handling` rides along unused: see the module docstring
    # -- the engine handles failures per batch, so there is no assignment-scoped
    # grain for it to change.
    validation: Validation | None = Field(default=None, alias="validate")

    @model_validator(mode="before")
    @classmethod
    def _reject_multi_segment_target(cls, data: Any) -> Any:
        """Say plainly what a dotted target path is wrong about.

        The contract already refuses it, by a published regex whose message is
        the regex. The rule is not restated here -- only the reason, because a
        dotted target is the one mapping mistake with a right answer the author
        cannot guess from the pattern.
        """
        if isinstance(data, Mapping):
            target = data.get("target")
            # A non-object `target` is the contract's error to report, by type;
            # reading `.path` off it here would escape this model as a raw
            # AttributeError instead of a named field failure.
            path = target.get("path") if isinstance(target, Mapping) else None
            if isinstance(path, str) and "." in path:
                raise ValueError(
                    f"target.path {path!r} has more than one segment; a target "
                    f"names one field on the destination record root, and "
                    f"nesting beneath it is declared with arrow_type 'Object' "
                    f"plus 'properties' (or 'List' plus 'items')"
                )
        return data


def _declared_child(
    node: AssignmentTarget | ArrowFieldSpec, token: str
) -> ArrowFieldSpec | None:
    """Return the field *token* names under *node*, or ``None`` if undeclared.

    Mirrors the contract's resolution walk (``StreamMapping``): a ``List``
    node declares its element shape in ``items`` rather than naming it, so
    the element is stepped through transparently -- a rule on a list of
    objects addresses the object's fields. ``enforce_container_shape`` keeps
    ``properties`` and ``items`` mutually exclusive, so the walk is
    unambiguous and bounded by the declared nesting depth.
    """
    while node.properties is None and node.items is not None:
        node = node.items
    return (node.properties or {}).get(token)


class MappingDocument(StrictModel):
    """A stream's mapping, closed at every level."""

    assignments: list[MappingAssignment] = Field(default_factory=list)

    @model_validator(mode="after")
    def _assignment_targets_unique(self) -> MappingDocument:
        """Refuse two assignments building one field (contract RULE-STRM-002).

        Arrow accepts duplicate field names, so without this the batch would
        carry two columns under one name and array position would decide the
        destination field's value.
        """
        counts = Counter(a.target.path for a in self.assignments)
        dups = sorted(path for path, count in counts.items() if count > 1)
        if dups:
            raise ValueError(
                f"assignments declare duplicate target.path values {dups!r}; "
                f"each destination field is built by exactly one assignment"
            )
        return self

    @model_validator(mode="after")
    def _rule_fields_resolve(self) -> MappingDocument:
        """Refuse a validation rule addressing a field no assignment declares.

        Mirrors the contract (``StreamMapping``): a rule's ``field`` is a
        token array whose first token names any ``target.path`` this mapping
        declares -- rules are authored per assignment but grade the record
        the assignments build together -- and each later token names a field
        under that target's ``properties``, descending through ``items`` for
        a ``List``. Unchecked, a typo would name nothing and the rule would
        silently grade no value at all, which reads exactly like a passing
        rule.
        """
        declared = {a.target.path: a.target for a in self.assignments}
        for i, assignment in enumerate(self.assignments):
            if assignment.validation is None:
                continue
            for j, rule in enumerate(assignment.validation.rules):
                where = f"assignments[{i}].validate.rules[{j}].field"
                head, *rest = rule.field
                node: AssignmentTarget | ArrowFieldSpec | None = declared.get(head)
                if node is None:
                    raise ValueError(
                        f"{where} {rule.field!r} names no assignment target: "
                        f"{head!r} is not a declared assignments[].target.path "
                        f"(declared: {sorted(declared)})"
                    )
                walked = [head]
                for token in rest:
                    child = _declared_child(node, token)
                    if child is None:
                        raise ValueError(
                            f"{where} {rule.field!r} does not resolve: "
                            f"{walked!r} declares no field {token!r} under its "
                            f"`properties`"
                        )
                    node = child
                    walked.append(token)
        return self

    @classmethod
    def parse(cls, document: Mapping[str, Any]) -> MappingDocument:
        """Read a mapping document, rejecting anything the transform cannot run.

        An unknown key, a missing ``kind``, a dotted target path or a malformed
        Arrow declaration all fail here naming the field, so a document written
        against a different contract pin is diagnosable from the message.
        """
        try:
            return cls.model_validate(document)
        except ValidationError as e:
            raise TransformationError(
                f"mapping document is invalid: {_field_errors(e)}"
            ) from e


def _field_errors(error: ValidationError) -> str:
    """Render a pydantic failure as ``field: reason`` pairs, field first."""
    return "; ".join(
        f"{'.'.join(str(part) for part in err['loc']) or '<root>'}: {err['msg']}"
        for err in error.errors()
    )


def build_output_schema(assignments: list[MappingAssignment]) -> pa.Schema:
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
        rules: list[ValidationRule],
    ) -> None:
        self.output_schema = output_schema
        self._steps = steps
        self._rules = rules

    def run(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        """Apply the transform to *batch*, returning the output batch.

        Raises :class:`TransformationError` if any column fails to build, any
        validation rule fails on any row, any conversion is rejected, or a
        non-nullable column ends up with nulls. The error names the column and
        (for validation) the offending rows.

        Every column is built before any rule runs: a rule's ``field`` may
        address any declared target, not just its own assignment's, so
        validation needs the whole record the assignments build together.
        Rules run against the built (pre-conversion) values, exactly as they
        did when each rule was bound to its own column.
        """
        errors: list[str] = []
        built = {step.field.name: step.build(batch) for step in self._steps}

        for rule in self._rules:
            errors.extend(_rule_errors(built, rule))

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
            shown = "; ".join(errors[:5])
            suffix = f" (+{len(errors) - 5} more)" if len(errors) > 5 else ""
            raise TransformationError(
                f"transform produced {len(errors)} error(s): {shown}{suffix}"
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


def compile_mapping(document: MappingDocument) -> CompiledTransform:
    """Compile a stream's mapping into a :class:`CompiledTransform`.

    Static work (schema building, expression compilation, validation setup)
    happens here once; the returned object is applied per batch. Raises
    :class:`TransformationError` for an expression the AST vocabulary does not
    define. Document-shape failures are raised earlier, by
    :meth:`MappingDocument.parse`.
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
    # addresses any declared target (document-checked at parse), so it is
    # resolved against the built record, not its own assignment's column.
    rules = [
        rule
        for assignment in assignments
        if assignment.validation is not None
        for rule in assignment.validation.rules
    ]
    return CompiledTransform(output_schema, steps, rules)


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


# Expression AST -> vectorized compute. Each compiler returns a closure over the
# batch so the (static) AST walk happens once at compile time, not per batch.


# Every key an AST node of each op may carry. Most ops take only `args`; `get`,
# `const` and `fn` carry their own payload. This table is what closes the
# expression sub-tree: it is the one part of the document pydantic does not
# check (the engine's op set is wider than the contract's published
# get/pipe/fn), so an unknown key is rejected here, alongside the op vocabulary
# and the arity, instead of being dropped on the way to the compiler.
_ARGS_ONLY_OPS: Final[tuple[str, ...]] = (
    "pipe",
    "if",
    "eq",
    "neq",
    "gt",
    "gte",
    "lt",
    "lte",
    "and",
    "or",
    "not",
    "concat",
    "coalesce",
)
_EXPR_NODE_KEYS: Final[dict[str, frozenset[str]]] = {
    "get": frozenset({"op", "path"}),
    "const": frozenset({"op", "value"}),
    "fn": frozenset({"op", "name", "version", "args"}),
    **{op: frozenset({"op", "args"}) for op in _ARGS_ONLY_OPS},
}


def _expect_node(expr: Any) -> str:
    """Return an AST node's ``op``, refusing an unknown op or an unknown key."""
    if not isinstance(expr, Mapping):
        raise TransformationError(
            f"expression node must be an object with an 'op', got "
            f"{type(expr).__name__}: {expr!r}"
        )
    op = expr.get("op")
    if not isinstance(op, str) or op not in _EXPR_NODE_KEYS:
        raise TransformationError(f"Unknown expression op: {op!r}")
    allowed = _EXPR_NODE_KEYS[op]
    unknown = sorted(set(expr) - allowed)
    if unknown:
        raise TransformationError(
            f"{op} expression carries unknown key(s) {unknown}; a {op} node "
            f"takes {sorted(allowed)}"
        )
    return op


def _variadic(expr: Any, op: str) -> list[_ExprFn]:
    """Compile a variadic node's args, refusing an empty list.

    Four ops take one-or-more args and each used to spell this refusal
    itself, so the message drifted with the op name in front of it.
    """
    args = expr.get("args") or []
    if not args:
        raise TransformationError(f"{op} expression requires at least 1 arg, got 0")
    return [_compile_expr(a) for a in args]


def _compile_get(expr: Any, _op: str) -> _ExprFn:
    path = _expect_token_path(expr.get("path"))
    return lambda batch: _get_path(batch, path)


def _compile_const_node(expr: Any, _op: str) -> _ExprFn:
    value = expr.get("value")
    return lambda batch: pa.array([value] * batch.num_rows)


def _compile_pipe(expr: Any, op: str) -> _ExprFn:
    args = expr.get("args") or []
    if not args:
        raise TransformationError(f"{op} expression requires at least 1 arg, got 0")
    seed = _compile_expr(args[0])
    stages = [_compile_fn(node) for node in args[1:]]

    def run_pipe(batch: pa.RecordBatch) -> pa.Array:
        value = seed(batch)
        for stage in stages:
            value = stage(value)
        return value

    return run_pipe


def _compile_bare_fn(expr: Any, _op: str) -> _ExprFn:
    # A top-level fn (e.g. ``now()``) with no pipe seed: the per-record
    # evaluator applied it to a ``None`` input, so here it runs over an
    # all-null column. Zero-input functions like ``now`` ignore it.
    fn = _compile_fn(expr)
    return lambda batch: fn(pa.nulls(batch.num_rows))


def _compile_if(expr: Any, op: str) -> _ExprFn:
    cond, then_, else_ = (_compile_expr(a) for a in _expect_args(expr, op, 3))
    return lambda batch: _if_else(cond(batch), then_(batch), else_(batch))


def _compile_eq(expr: Any, op: str) -> _ExprFn:
    left, right = (_compile_expr(a) for a in _expect_args(expr, op, 2))
    return lambda batch: _equal(left(batch), right(batch))


def _compile_neq(expr: Any, op: str) -> _ExprFn:
    left, right = (_compile_expr(a) for a in _expect_args(expr, op, 2))
    return lambda batch: pc.invert(_equal(left(batch), right(batch)))


#: The ordering kernels, keyed by the op that selects them.
_ORDER_KERNELS: Final[dict[str, Callable[..., Any]]] = {
    "gt": pc.greater,
    "gte": pc.greater_equal,
    "lt": pc.less,
    "lte": pc.less_equal,
}


def _compile_order(expr: Any, op: str) -> _ExprFn:
    left, right = (_compile_expr(a) for a in _expect_args(expr, op, 2))
    kernel = _ORDER_KERNELS[op]
    return lambda batch: _compare(kernel, left(batch), right(batch), op)


def _compile_bool_reduce(expr: Any, op: str) -> _ExprFn:
    operands = _variadic(expr, op)
    reduce = pc.and_ if op == "and" else pc.or_
    return lambda batch: _bool_reduce(reduce, [o(batch) for o in operands])


def _compile_not(expr: Any, op: str) -> _ExprFn:
    inner = _compile_expr(_expect_args(expr, op, 1)[0])
    return lambda batch: pc.invert(_truthy(inner(batch)))


def _compile_concat(expr: Any, op: str) -> _ExprFn:
    parts = _variadic(expr, op)
    return lambda batch: _concat([p(batch) for p in parts])


def _compile_coalesce(expr: Any, op: str) -> _ExprFn:
    parts = _variadic(expr, op)
    return lambda batch: _coalesce([p(batch) for p in parts])


#: One compiler per op. Keyed rather than branched so the set of compilable
#: ops is a value the drift guard below can compare against the grammar --
#: an op added to `_EXPR_NODE_KEYS` without a compiler fails at import, not
#: on the batch that first carries it.
_EXPR_COMPILERS: Final[dict[str, Callable[[Any, str], _ExprFn]]] = {
    "get": _compile_get,
    "const": _compile_const_node,
    "pipe": _compile_pipe,
    "fn": _compile_bare_fn,
    "if": _compile_if,
    "eq": _compile_eq,
    "neq": _compile_neq,
    "gt": _compile_order,
    "gte": _compile_order,
    "lt": _compile_order,
    "lte": _compile_order,
    "and": _compile_bool_reduce,
    "or": _compile_bool_reduce,
    "not": _compile_not,
    "concat": _compile_concat,
    "coalesce": _compile_coalesce,
}

if set(_EXPR_COMPILERS) != set(_EXPR_NODE_KEYS):
    raise AssertionError(
        "every grammar op needs a compiler and vice versa; unmatched: "
        f"{sorted(set(_EXPR_COMPILERS) ^ set(_EXPR_NODE_KEYS))}"
    )


def _compile_expr(expr: Any) -> _ExprFn:
    """Compile one expression AST node into a vectorized column builder."""
    op = _expect_node(expr)
    return _EXPR_COMPILERS[op](expr, op)


def _compile_fn(node: Any) -> Callable[[pa.Array], pa.Array]:
    """Compile a ``fn`` AST node (a pipe stage) into a vectorized column function."""
    op = _expect_node(node)
    if op != "fn":
        raise TransformationError(f"Expected fn op in pipe stage, got: {op!r}")
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

    def apply(column: pa.Array) -> pa.Array:
        # A wrong argument count is a mapping authoring error; surface it as the
        # TransformationError contract rather than a raw Python TypeError.
        try:
            return kernel(column, *args)
        except TypeError as e:
            raise TransformationError(
                f"function {name!r} called with wrong arguments: {e}"
            ) from e

    return apply


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


def _equal(left: pa.Array, right: pa.Array) -> pa.Array:
    """Element-wise equality with the per-record evaluator's null semantics.

    Python `==` treats two ``None`` operands as equal and a ``None`` against a
    present value as unequal; Arrow's ``pc.equal`` instead yields ``null``
    whenever either side is null. This restores the former: ``True`` where both
    are null, otherwise the kernel result with a one-sided null counting as not
    equal. (``neq`` is the inversion of this.)
    """
    left, right = _unify_null_typed([left, right])
    both_null = pc.and_(pc.is_null(left), pc.is_null(right))
    try:
        equal = pc.fill_null(pc.equal(left, right), False)
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as e:
        raise TransformationError(
            f"eq expression cannot compare {left.type} and {right.type}: {e}"
        ) from e
    return pc.if_else(both_null, pa.scalar(True), equal)


def _bool_reduce(
    kernel: Callable[[Any, Any], pa.Array], operands: list[pa.Array]
) -> pa.Array:
    """Reduce boolean operands with *kernel*. None/0/'' count as False (truthiness)."""
    result = _truthy(operands[0])
    for operand in operands[1:]:
        result = kernel(result, _truthy(operand))
    return result


def _truthy(array: pa.Array) -> pa.Array:
    """Coerce a column to Python boolean truthiness: null counts as False.

    Matches the per-record evaluator, which applied Python truthiness to
    arbitrary values: an empty string / zero / null is false, any other value is
    true. A bare Arrow ``cast(_, bool)`` would instead reject strings outright,
    so strings (non-empty -> true) and numerics (non-zero -> true) are handled
    explicitly.
    """
    if pa.types.is_boolean(array.type):
        return pc.fill_null(array, False)
    if pa.types.is_string(array.type) or pa.types.is_large_string(array.type):
        return pc.fill_null(pc.greater(pc.utf8_length(array), 0), False)
    if pa.types.is_temporal(array.type):
        # A date/time/timestamp value is always truthy in Python; only a null
        # (missing) is false. Arrow's cast-to-bool would reject it or read the
        # physical zero (epoch/midnight) as false.
        return pc.is_valid(array)
    if pa.types.is_null(array.type):
        return pa.array([False] * len(array))
    try:
        return pc.fill_null(pc.cast(array, pa.bool_()), False)
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as e:
        raise TransformationError(
            f"value of type {array.type} has no boolean truth value: {e}"
        ) from e


def _concat(parts: list[pa.Array]) -> pa.Array:
    """Concatenate columns as strings, dropping nulls (mirrors per-record concat)."""
    try:
        strings = [_string_form(part) for part in parts]
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as e:
        raise TransformationError(f"concat expression cannot stringify: {e}") from e
    return pc.binary_join_element_wise(*strings, pa.scalar(""), null_handling="skip")


def _string_form(column: pa.Array) -> pa.Array:
    """Render a column as strings the way the per-record ``str()`` did.

    Booleans become ``"True"``/``"False"`` rather than Arrow's lowercase
    ``"true"``/``"false"``; everything else uses Arrow's string cast. Shared by
    ``to_string`` and ``concat`` so the two never diverge on booleans.
    """
    if pa.types.is_boolean(column.type):
        return pc.if_else(column, pa.scalar("True"), pa.scalar("False"))
    return pc.cast(column, pa.string())


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


# A trailing 'Z' or numeric offset, used to strip the zone marker before a naive
# parse. pyarrow 12 has no single string cast that accepts naive, 'Z', and
# offset forms together, so the zone is dropped and the value stamped UTC.
_ISO_ZONE_SUFFIX_RE: Final[str] = r"(Z|[+-]([01]\d|2[0-3]):?[0-5]\d)$"


def _fn_iso_to_datetime(column: pa.Array) -> pa.Array:
    """Parse ISO-8601 strings into timezone-aware (UTC) microsecond timestamps.

    Accepts every form the per-record ``datetime.fromisoformat`` did -- a bare
    date, a naive datetime, and a ``Z``/offset timestamp. The zone marker is
    stripped and the remaining value parsed as a naive timestamp stamped UTC
    (wall-clock, consistent with ``iso_to_date``); on pyarrow 12 no single cast
    accepts all three forms, and a tz-aware cast rejects naive input outright.
    """
    try:
        strings = pc.cast(column, pa.string())
        naive = pc.replace_substring_regex(
            strings, pattern=_ISO_ZONE_SUFFIX_RE, replacement=""
        )
        return pc.assume_timezone(pc.cast(naive, pa.timestamp("us")), "UTC")
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as e:
        raise TransformationError(f"iso_to_datetime failed: {e}") from e


# Anchored ISO-8601 date / datetime shape: a calendar date with an optional
# time and an optional 'Z' or numeric offset. The hour/minute/second and offset
# ranges are constrained (00-23 / 00-59) and a fractional part is tied to the
# seconds group, so a value with trailing junk ("2025-08-16garbage"), an
# out-of-range time ("2025-08-16T99:99:99"), or a fraction without seconds
# ("2025-08-16T10:30.5Z") is rejected before its date prefix is taken; the date's
# month/day are validated by the timestamp cast that follows.
_ISO_8601_RE: Final[str] = (
    r"^\d{4}-\d{2}-\d{2}"
    r"([T ]([01]\d|2[0-3]):[0-5]\d(:[0-5]\d(\.\d+)?)?"
    r"(Z|[+-]([01]\d|2[0-3]):?[0-5]\d)?)?$"
)


def _fn_iso_to_date(column: pa.Array) -> pa.Array:
    """Render the date part of an ISO-8601 value as a ``YYYY-MM-DD`` string.

    The whole value is first validated against the ISO-8601 shape, so a value
    with trailing junk (``"2025-08-16not-a-timestamp"``) is rejected rather than
    silently truncated to its date prefix -- matching the per-record
    ``datetime.fromisoformat`` which parsed the entire string. The leading 10
    characters (the calendar date, present in every ISO form -- a bare date, a
    naive datetime, or a ``Z``/offset timestamp) are then parsed as a naive
    timestamp (which validates the date and keeps the original wall-clock date)
    and reformatted. (A direct string -> date32 cast is unavailable on
    pyarrow 12, so the date is round-tripped through a timestamp.)
    """
    try:
        strings = pc.cast(column, pa.string())
        matches = pc.match_substring_regex(strings, pattern=_ISO_8601_RE)
        malformed = pc.and_(pc.is_valid(strings), pc.invert(matches))
        if pc.any(malformed, min_count=0).as_py():
            raise TransformationError(
                "iso_to_date: value is not a valid ISO-8601 date/datetime"
            )
        date_part = pc.utf8_slice_codeunits(strings, 0, 10)
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
}


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
) -> tuple[pa.Array, list[int] | None, list[int]]:
    """Return a rule's addressed values, their source rows, and null ancestors.

    The first token selects a built column; each later token descends into
    it -- ``struct_field`` for an ``Object`` level, and a ``List`` level is
    flattened first, with ``list_parent_indices`` composing the element ->
    batch-row map. The second element is that map, or ``None`` when no list
    was crossed (value *i* is row *i*).

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
    row_map: list[int] | None = None
    null_ancestors: set[int] = set()
    for token in tokens[1:]:
        while pa.types.is_list(value.type) or pa.types.is_large_list(value.type):
            for i, absent in enumerate(pc.is_null(value).to_pylist()):
                if absent:
                    null_ancestors.add(i if row_map is None else row_map[i])
            parents = pc.list_parent_indices(value).to_pylist()
            row_map = parents if row_map is None else [row_map[i] for i in parents]
            value = pc.list_flatten(value)
        if pa.types.is_null(value.type):
            # A column that carried no value anywhere infers as Arrow's `null`
            # type, which no `struct_field` kernel accepts. Every deeper token
            # addresses only nulls, so the null values stand in for the field.
            break
        value = pc.struct_field(value, token)
    return value, row_map, sorted(null_ancestors)


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
    except (
        KeyError,
        pa.ArrowInvalid,
        pa.ArrowTypeError,
        pa.ArrowNotImplementedError,
    ) as e:
        raise TransformationError(
            f"column {label}: validation rule {rule.type!r} addresses a "
            f"declared field the built value does not carry: {e}"
        ) from e
    present = pc.is_valid(value)
    mask = _rule_failure_mask(value, present, rule, label)
    failing = [i for i, failed in enumerate(mask.to_pylist()) if failed]
    rows = failing if row_map is None else sorted({row_map[i] for i in failing})
    if rule.type in ("not_null", "required"):
        rows = sorted(set(rows) | set(null_ancestors))
    if not rows:
        return []
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
            case "not_null" | "required":
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
                return _range_failure_mask(value, present, rule, label)
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
    value: pa.Array, present: pa.Array, rule: ValidationRule, label: str
) -> pa.Array:
    """Fail rows outside the bounds carried in the rule's ``value`` object."""
    bounds = rule.value
    if not isinstance(bounds, Mapping) or not {"min", "max"} & set(bounds):
        raise TransformationError(
            f"column {label}: validation rule 'range' needs a value "
            f"object carrying 'min' and/or 'max'; got {bounds!r}"
        )
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


def _expect_token_path(path: Any) -> list[str]:
    """Accept a source path only as an ordered array of field-name tokens.

    A string is refused rather than split. ``"a.b"`` names one field on some
    sources and two on others, and the answer is the author's to give: ``["a",
    "b"]`` is nested, ``["a.b"]`` is a single field whose name contains a dot.
    """
    if (
        not isinstance(path, list)
        or not path
        or not all(isinstance(segment, str) and segment for segment in path)
    ):
        raise TransformationError(
            f"get expression path must be a non-empty array of field-name "
            f"tokens, outermost first (e.g. ['address', 'city']); got {path!r}"
        )
    return path
