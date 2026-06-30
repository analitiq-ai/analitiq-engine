"""Behavior coverage for the vectorized, Arrow-native transform.

The per-record evaluator (``DataTransformer``/``AssignmentTransformer``) was
replaced by a single vectorized path: ``compile_transform(assignments)`` builds
a ``CompiledTransform`` once and ``.run(batch)`` applies it to a
``pa.RecordBatch`` synchronously, raising ``TransformationError`` on any
failure. These tests assert that contract: every expression op, the full
function catalog, the conversion matrix gating, fail-loud batch-wide semantics,
and the static (compile-time) arity checks.
"""

from datetime import datetime
from decimal import Decimal

import pyarrow as pa
import pytest

from src.engine.data_transformer import (
    _FUNCTION_CATALOG,
    build_output_schema,
    compile_transform,
)
from src.engine.exceptions import TransformationError

# --------------------------------------------------------------------------- #
# Local builders -- keep each test reading as the mapping shape the engine sees #
# --------------------------------------------------------------------------- #


def _get(path):
    """An ``op: get`` expression node for a (possibly nested) source path."""
    return {"op": "get", "path": path if isinstance(path, list) else [path]}


def _expr(node):
    """Wrap an expression AST node as an assignment ``value`` block."""
    return {"kind": "expr", "expr": node}


def _const(value):
    """A ``const`` value block (materialised at the target type)."""
    return {"kind": "const", "const": {"value": value}}


def _const_node(value):
    """An expression-level ``op: const`` node (for use inside other ops)."""
    return {"op": "const", "value": value}


def _target(name, arrow_type, nullable=True, **extra):
    t = {"path": [name], "arrow_type": arrow_type, "nullable": nullable}
    t.update(extra)
    return t


def _assignment(name, arrow_type, value, nullable=True, validate=None, **target_extra):
    a = {"target": _target(name, arrow_type, nullable, **target_extra), "value": value}
    if validate is not None:
        a["validate"] = validate
    return a


def _run(records, assignments):
    """Compile *assignments* and run them over *records*, returning pylist rows.

    Source types are inferred by ``from_pylist``; tests that need a specific
    source Arrow type build the batch explicitly instead.
    """
    batch = pa.RecordBatch.from_pylist(records)
    return compile_transform(assignments).run(batch).to_pylist()


class TestExpressionOps:
    """Each expression op evaluates as a vectorized column over the batch."""

    def test_get_renames_top_level_column(self):
        out = _run([{"a": 1}, {"a": 2}], [_assignment("b", "Int64", _expr(_get("a")))])
        assert out == [{"b": 1}, {"b": 2}]

    def test_get_drills_into_struct_source_column(self):
        batch = pa.record_batch(
            [pa.array([{"inner": "v1"}, {"inner": "v2"}])], names=["outer"]
        )
        out = compile_transform(
            [_assignment("x", "Utf8", _expr(_get(["outer", "inner"])))]
        ).run(batch)
        assert out.to_pylist() == [{"x": "v1"}, {"x": "v2"}]

    def test_const_value_kind_broadcasts_literal(self):
        out = _run([{"a": 1}, {"a": 2}], [_assignment("s", "Utf8", _const("X"))])
        assert out == [{"s": "X"}, {"s": "X"}]

    def test_pipe_chains_functions_left_to_right(self):
        node = {
            "op": "pipe",
            "args": [
                _get("e"),
                {"op": "fn", "name": "trim", "version": 1, "args": []},
                {"op": "fn", "name": "lower", "version": 1, "args": []},
            ],
        }
        out = _run([{"e": "  Foo@BAR.com "}], [_assignment("e", "Utf8", _expr(node))])
        assert out == [{"e": "foo@bar.com"}]

    def test_if_eq_select_branch_per_row(self):
        node = {
            "op": "if",
            "args": [
                {"op": "eq", "args": [_get("active"), _const_node(True)]},
                _const_node("yes"),
                _const_node("no"),
            ],
        }
        out = _run(
            [{"active": True}, {"active": False}],
            [_assignment("label", "Utf8", _expr(node))],
        )
        assert out == [{"label": "yes"}, {"label": "no"}]

    @pytest.mark.parametrize(
        "op,left,right,expected",
        [
            ("eq", 1, 1, True),
            ("eq", 1, 2, False),
            ("neq", 1, 2, True),
            ("neq", 1, 1, False),
            ("gt", 5, 3, True),
            ("gt", 3, 5, False),
            ("gt", 5, 5, False),
            ("gte", 5, 5, True),
            ("gte", 3, 5, False),
            ("lt", 3, 5, True),
            ("lt", 5, 5, False),
            ("lte", 3, 3, True),
            ("lte", 5, 3, False),
        ],
    )
    def test_comparison_ops_return_boolean(self, op, left, right, expected):
        node = {"op": op, "args": [_const_node(left), _const_node(right)]}
        out = _run([{"x": 0}], [_assignment("r", "Boolean", _expr(node))])
        assert out == [{"r": expected}]

    def test_and_or_not_evaluate_eagerly_to_result(self):
        """and/or/if do NOT short-circuit -- the column result is what matters.

        ``and`` of [true, false] is false; ``or`` of [false, true] is true;
        ``not`` of true is false -- all computed across the whole column.
        """
        and_node = {"op": "and", "args": [_get("a"), _get("b")]}
        or_node = {"op": "or", "args": [_get("b"), _get("a")]}
        not_node = {"op": "not", "args": [_get("a")]}
        records = [{"a": True, "b": False}]
        assert _run(records, [_assignment("r", "Boolean", _expr(and_node))]) == [
            {"r": False}
        ]
        assert _run(records, [_assignment("r", "Boolean", _expr(or_node))]) == [
            {"r": True}
        ]
        assert _run(records, [_assignment("r", "Boolean", _expr(not_node))]) == [
            {"r": False}
        ]

    def test_concat_joins_strings_and_skips_nulls(self):
        node = {"op": "concat", "args": [_get("a"), _get("b"), _get("c")]}
        out = _run(
            [{"a": "hello", "b": None, "c": "world"}],
            [_assignment("o", "Utf8", _expr(node))],
        )
        assert out == [{"o": "helloworld"}]

    def test_coalesce_returns_first_non_null(self):
        node = {"op": "coalesce", "args": [_get("a"), _get("b")]}
        out = _run(
            [{"a": None, "b": "fallback"}], [_assignment("v", "Utf8", _expr(node))]
        )
        assert out == [{"v": "fallback"}]

    def test_coalesce_all_null_stays_null(self):
        node = {"op": "coalesce", "args": [_get("a"), _get("b")]}
        out = _run([{"a": None, "b": None}], [_assignment("v", "Utf8", _expr(node))])
        assert out == [{"v": None}]

    def test_unknown_op_raises_at_compile(self):
        with pytest.raises(TransformationError, match="Unknown expression op"):
            compile_transform(
                [_assignment("o", "Utf8", _expr({"op": "frobnicate", "args": []}))]
            )

    def test_incompatible_comparison_raises_at_run(self):
        """Comparing across incompatible Arrow types is a per-batch run failure,
        not a static one: the operand types are only known once the columns are
        built."""
        node = {"op": "gt", "args": [_const_node(5), _const_node("not-a-number")]}
        compiled = compile_transform([_assignment("o", "Boolean", _expr(node))])
        with pytest.raises(TransformationError, match="gt expression cannot compare"):
            compiled.run(pa.RecordBatch.from_pylist([{"x": 0}]))


class TestExpressionArityIsStatic:
    """Wrong operator arity is a config defect caught at compile time -- before
    any batch is seen -- because the AST is walked once in ``compile_transform``.
    """

    @pytest.mark.parametrize("op", ["eq", "neq", "gt", "gte", "lt", "lte"])
    def test_binary_ops_require_two_args(self, op):
        node = {"op": op, "args": [_const_node(1)]}
        with pytest.raises(
            TransformationError, match=f"{op} expression requires 2 args"
        ):
            compile_transform([_assignment("o", "Boolean", _expr(node))])

    def test_if_requires_three_args(self):
        node = {"op": "if", "args": [_const_node(True)]}
        with pytest.raises(TransformationError, match="if expression requires 3 args"):
            compile_transform([_assignment("o", "Utf8", _expr(node))])

    def test_not_requires_one_arg(self):
        node = {"op": "not", "args": []}
        with pytest.raises(TransformationError, match="not expression requires 1 args"):
            compile_transform([_assignment("o", "Boolean", _expr(node))])

    @pytest.mark.parametrize("op", ["and", "or", "concat", "coalesce", "pipe"])
    def test_variadic_ops_require_at_least_one_arg(self, op):
        node = {"op": op, "args": []}
        with pytest.raises(TransformationError, match="at least 1 arg"):
            compile_transform([_assignment("o", "Utf8", _expr(node))])

    @pytest.mark.parametrize("op", ["and", "or"])
    def test_missing_args_key_is_treated_as_empty(self, op):
        with pytest.raises(TransformationError, match="at least 1 arg"):
            compile_transform([_assignment("o", "Boolean", _expr({"op": op}))])

    def test_unknown_value_kind_raises_at_compile(self):
        with pytest.raises(TransformationError, match="unknown value kind"):
            compile_transform([_assignment("o", "Utf8", {"kind": "mystery"})])


class TestFunctionCatalog:
    """Every catalog kernel maps a ``pa.Array`` to a ``pa.Array``. Kernels are
    exercised directly so the input Arrow type is exact and unambiguous."""

    def _v1(self, name):
        return _FUNCTION_CATALOG[name][1]

    def test_catalog_has_the_expected_thirteen_functions(self):
        assert set(_FUNCTION_CATALOG) == {
            "iso_to_date",
            "iso_to_datetime",
            "iso_to_timestamp",
            "trim",
            "lower",
            "upper",
            "to_int",
            "to_float",
            "to_string",
            "abs",
            "now",
            "default",
            "coalesce",
        }

    def test_trim_lower_upper(self):
        assert self._v1("trim")(pa.array(["  x  "])).to_pylist() == ["x"]
        assert self._v1("lower")(pa.array(["AbC"])).to_pylist() == ["abc"]
        assert self._v1("upper")(pa.array(["AbC"])).to_pylist() == ["ABC"]

    @pytest.mark.parametrize("name", ["trim", "lower", "upper", "to_string", "abs"])
    def test_null_input_element_stays_null(self, name):
        """A null row passes through every elementwise kernel as null -- it is
        never coerced to "" or 0, which would defeat the nullability check."""
        if name == "abs":
            array = pa.array([None, -5], pa.int64())
        else:
            array = pa.array([None, "a"], pa.string())
        assert self._v1(name)(array).to_pylist()[0] is None

    def test_to_int_truncates_toward_zero(self):
        out = self._v1("to_int")(pa.array(["3.9", "-3.9", None])).to_pylist()
        assert out == [3, -3, None]

    def test_to_int_raises_on_unparseable(self):
        with pytest.raises(TransformationError, match="to_int"):
            self._v1("to_int")(pa.array(["abc"]))

    def test_to_float_parses_and_passes_null(self):
        assert self._v1("to_float")(pa.array(["1.5", None])).to_pylist() == [1.5, None]

    def test_to_float_raises_on_unparseable(self):
        with pytest.raises(TransformationError, match="to_float"):
            self._v1("to_float")(pa.array(["xyz"]))

    def test_to_string_formats_numbers(self):
        out = self._v1("to_string")(pa.array([1, 2, None], pa.int64())).to_pylist()
        assert out == ["1", "2", None]

    def test_abs_raises_on_non_numeric(self):
        with pytest.raises(TransformationError, match="abs"):
            self._v1("abs")(pa.array(["hello"]))

    def test_default_fills_nulls_with_literal(self):
        out = self._v1("default")(pa.array([1, None, 3], pa.int64()), 99).to_pylist()
        assert out == [1, 99, 3]

    def test_coalesce_kernel_uses_literal_alternatives(self):
        out = self._v1("coalesce")(pa.array([None, 2], pa.int64()), 99).to_pylist()
        assert out == [99, 2]

    def test_iso_to_date_renders_naive_date_part(self):
        out = self._v1("iso_to_date")(
            pa.array(["2026-05-12T10:30:00", "2026-05-12", None])
        ).to_pylist()
        assert out == ["2026-05-12", "2026-05-12", None]

    def test_iso_to_date_raises_on_unparseable(self):
        with pytest.raises(TransformationError, match="iso_to_date"):
            self._v1("iso_to_date")(pa.array(["not-a-date"]))

    @pytest.mark.parametrize("name", ["iso_to_datetime", "iso_to_timestamp"])
    def test_iso_to_datetime_parses_tz_aware_utc(self, name):
        out = self._v1(name)(pa.array(["2026-05-12T10:30:00Z"]))
        assert out.type == pa.timestamp("us", tz="UTC")
        value = out.to_pylist()[0]
        assert value.tzinfo is not None
        assert value.utcoffset().total_seconds() == 0

    @pytest.mark.parametrize("name", ["iso_to_datetime", "iso_to_timestamp"])
    def test_iso_to_datetime_raises_on_unparseable(self, name):
        # iso_to_timestamp aliases the iso_to_datetime kernel, so the raised
        # message names the shared kernel; assert the parse failure itself.
        with pytest.raises(TransformationError, match="Failed to parse"):
            self._v1(name)(pa.array(["not-a-timestamp"]))

    def test_now_returns_tz_aware_utc_column(self):
        """``now`` broadcasts the current UTC instant to every row as a tz-aware
        timestamp column -- a naive value would corrupt incremental sync."""
        out = self._v1("now")(pa.array([0, 0, 0]))
        assert out.type == pa.timestamp("us", tz="UTC")
        assert len(out) == 3
        first = out.to_pylist()[0]
        assert first.tzinfo is not None
        assert first.utcoffset().total_seconds() == 0

    def test_iso_to_date_accepts_every_iso_form(self):
        # Regression: a tz-suffixed value (the real Wise 'created' carries 'Z')
        # must not raise. iso_to_date renders the wall-clock date for a bare
        # date, a naive datetime, a 'Z' instant, and an offset alike.
        out = self._v1("iso_to_date")(
            pa.array(
                [
                    "2025-08-16T10:30:00Z",
                    "2025-08-16T10:30:00+00:00",
                    "2025-08-16T23:30:00+02:00",
                    "2025-08-16T10:30:00",
                    "2025-08-16",
                    None,
                ]
            )
        ).to_pylist()
        assert out == ["2025-08-16"] * 5 + [None]

    def test_iso_to_date_fails_loud_on_non_date(self):
        with pytest.raises(TransformationError, match="iso_to_date"):
            self._v1("iso_to_date")(pa.array(["not-a-date"]))


class TestFunctionVersionDispatch:
    """A pipe ``fn`` stage dispatches by ``version`` (default 1). Unknown name
    or unregistered version is a config defect caught at compile time."""

    def _pipe(self, fn_node):
        return _expr({"op": "pipe", "args": [_get("v"), fn_node]})

    def test_version_defaults_to_one(self):
        out = _run(
            [{"v": "  hi  "}],
            [_assignment("o", "Utf8", self._pipe({"op": "fn", "name": "trim"}))],
        )
        assert out == [{"o": "hi"}]

    def test_explicit_version_one_dispatches(self):
        out = _run(
            [{"v": "  hi  "}],
            [
                _assignment(
                    "o",
                    "Utf8",
                    self._pipe({"op": "fn", "name": "trim", "version": 1, "args": []}),
                )
            ],
        )
        assert out == [{"o": "hi"}]

    def test_unregistered_version_raises_naming_registered_versions(self):
        node = {"op": "fn", "name": "trim", "version": 99, "args": []}
        with pytest.raises(TransformationError, match=r"version 99.*\[1\]"):
            compile_transform([_assignment("o", "Utf8", self._pipe(node))])

    def test_unknown_function_raises(self):
        node = {"op": "fn", "name": "nonexistent_fn", "version": 1, "args": []}
        with pytest.raises(TransformationError, match="Unknown function"):
            compile_transform([_assignment("o", "Utf8", self._pipe(node))])

    def test_non_fn_op_in_pipe_stage_raises(self):
        node = _expr({"op": "pipe", "args": [_const_node("x"), _get("y")]})
        with pytest.raises(TransformationError, match="Expected fn op"):
            compile_transform([_assignment("o", "Utf8", node)])


class TestConversionMatrix:
    """Type conversion has one authority: the conversion matrix gates each
    source->target retype, executed by the same ``pc.cast(safe=True)`` the
    destination uses."""

    def test_identity_rename_passes_through(self):
        out = _run([{"a": 1}, {"a": 2}], [_assignment("a", "Int64", _expr(_get("a")))])
        assert out == [{"a": 1}, {"a": 2}]

    def test_auto_widening_int32_to_int64(self):
        batch = pa.record_batch([pa.array([1, 2], pa.int32())], names=["a"])
        out = compile_transform([_assignment("a", "Int64", _expr(_get("a")))]).run(
            batch
        )
        assert out.to_pylist() == [{"a": 1}, {"a": 2}]
        assert out.schema.field("a").type == pa.int64()

    def test_string_to_int_auto_parses(self):
        out = _run(
            [{"a": "1"}, {"a": "2"}], [_assignment("a", "Int64", _expr(_get("a")))]
        )
        assert out == [{"a": 1}, {"a": 2}]

    def test_lossless_float_to_int_succeeds(self):
        out = _run([{"a": 1.0}], [_assignment("a", "Int64", _expr(_get("a")))])
        assert out == [{"a": 1}]

    def test_int_to_string_is_explicit_and_raises(self):
        with pytest.raises(TransformationError, match="to_string"):
            _run([{"a": 1}], [_assignment("s", "Utf8", _expr(_get("a")))])

    def test_lossy_float_to_int_is_rejected(self):
        with pytest.raises(TransformationError):
            _run([{"a": 1.5}], [_assignment("a", "Int64", _expr(_get("a")))])

    def test_narrowing_overflow_is_rejected(self):
        batch = pa.record_batch([pa.array([300], pa.int64())], names=["a"])
        compiled = compile_transform([_assignment("a", "Int8", _expr(_get("a")))])
        with pytest.raises(TransformationError):
            compiled.run(batch)

    def test_naive_timestamp_to_utc(self):
        naive = pa.array(
            [datetime(2025, 8, 16, 10, 30)],  # noqa: DTZ001 -- naive on purpose
            pa.timestamp("us"),
        )
        batch = pa.record_batch([naive], names=["created"])
        assignment = _assignment(
            "created_at", "Timestamp(MICROSECOND, UTC)", _expr(_get("created"))
        )
        out = compile_transform([assignment]).run(batch)
        assert out.schema.field("created_at").type == pa.timestamp("us", tz="UTC")


class TestFailLoudSemantics:
    """A row failing any rule, a null in a non-nullable column, or a missing
    source column are handled batch-wide and loudly -- the transform never does
    per-row DLQ routing (that is the engine's error_strategy downstream)."""

    def test_non_nullable_null_fails_the_batch(self):
        with pytest.raises(TransformationError, match="not nullable"):
            _run(
                [{"a": None}],
                [_assignment("a", "Utf8", _expr(_get("a")), nullable=False)],
            )

    def test_missing_source_column_yields_all_null(self):
        out = _run([{"a": 1}], [_assignment("m", "Utf8", _expr(_get("missing")))])
        assert out == [{"m": None}]

    def test_validate_not_null_one_bad_row_fails_whole_batch(self):
        """A single failing row fails the entire batch even with on_error dlq:
        the transform has no per-row routing."""
        with pytest.raises(TransformationError, match="not_null"):
            _run(
                [{"a": "v"}, {"a": None}],
                [
                    _assignment(
                        "a",
                        "Utf8",
                        _expr(_get("a")),
                        validate={"rules": [{"type": "not_null"}], "on_error": "dlq"},
                    )
                ],
            )

    def test_validate_pattern_failure_fails_whole_batch(self):
        with pytest.raises(TransformationError, match="pattern"):
            _run(
                [{"a": "abc"}, {"a": "123"}],
                [
                    _assignment(
                        "a",
                        "Utf8",
                        _expr(_get("a")),
                        validate={"rules": [{"type": "pattern", "value": "[a-z]+"}]},
                    )
                ],
            )


class TestCompiledReuseAndConsts:
    def test_one_compiled_transform_reused_across_batch_sizes(self):
        """A transform is compiled once and applied to batches of different row
        counts -- including an empty batch -- with no per-batch state leaking."""
        assignments = [
            _assignment("amount", "Float64", _expr(_get("v"))),
            _assignment("kind", "Utf8", _const("txn")),
            _assignment("missing", "Utf8", _expr(_get("absent"))),
        ]
        compiled = compile_transform(assignments)
        for values in ([1.0, 2.0, 3.0], [4.0], []):
            batch = pa.record_batch([pa.array(values, pa.float64())], names=["v"])
            out = compiled.run(batch)
            assert out.num_rows == len(values)
            assert out.to_pylist() == [
                {"amount": v, "kind": "txn", "missing": None} for v in values
            ]

    def test_decimal_passthrough_and_scalar_consts(self):
        """Decimal columns pass through unchanged (this codebase's known sharp
        edge), alongside bool and None constants."""
        prices = pa.array([Decimal("1.23"), Decimal("4.56")], pa.decimal128(10, 2))
        batch = pa.record_batch([prices], names=["price"])
        assignments = [
            _assignment("price", "Decimal128(10, 2)", _expr(_get("price"))),
            _assignment("active", "Boolean", _const(True)),
            _assignment("note", "Utf8", _const(None)),
        ]
        out = compile_transform(assignments).run(batch)
        assert out.column("price").to_pylist() == [Decimal("1.23"), Decimal("4.56")]
        assert out.column("active").to_pylist() == [True, True]
        assert out.column("note").null_count == 2

    def test_const_none_into_non_nullable_fails(self):
        with pytest.raises(TransformationError, match="not nullable"):
            _run(
                [{"x": 1}],
                [_assignment("c", "Utf8", _const(None), nullable=False)],
            )


class TestBuildOutputSchema:
    def test_missing_arrow_type_raises(self):
        with pytest.raises(TransformationError, match="missing target.arrow_type"):
            build_output_schema([{"target": {"path": ["o"]}, "value": _const(1)}])

    def test_scalar_schema_carries_nullability(self):
        schema = build_output_schema(
            [
                _assignment("a", "Int64", _expr(_get("a")), nullable=False),
                _assignment("b", "Utf8", _expr(_get("b")), nullable=True),
            ]
        )
        assert schema.field("a").type == pa.int64()
        assert not schema.field("a").nullable
        assert schema.field("b").nullable


class TestPerRecordParity:
    """Edge semantics that must match the deleted per-record evaluator.

    Vectorized Arrow kernels diverge from Python at the edges (null equality,
    string truthiness, boolean formatting, all-null fallbacks); each kernel here
    is steered back to the per-record behavior so existing mappings keep working.
    """

    def _v1(self, name):
        return _FUNCTION_CATALOG[name][1]

    def test_to_string_renders_bool_as_python_str(self):
        out = self._v1("to_string")(pa.array([True, False, None])).to_pylist()
        assert out == ["True", "False", None]

    def test_string_condition_uses_python_truthiness(self):
        node = {
            "op": "if",
            "args": [_get("s"), _const_node("Y"), _const_node("N")],
        }
        out = _run(
            [{"s": "x"}, {"s": ""}, {"s": None}],
            [_assignment("r", "Utf8", _expr(node))],
        )
        assert out == [{"r": "Y"}, {"r": "N"}, {"r": "N"}]

    def test_eq_treats_two_nulls_as_equal(self):
        node = {
            "op": "if",
            "args": [
                {"op": "eq", "args": [_get("a"), _const_node(None)]},
                _const_node("missing"),
                _const_node("present"),
            ],
        }
        out = _run(
            [{"a": None}, {"a": 5}],
            [_assignment("r", "Utf8", _expr(node))],
        )
        assert out == [{"r": "missing"}, {"r": "present"}]

    def test_default_fills_a_missing_source_column(self):
        node = {
            "op": "pipe",
            "args": [_get("absent"), {"op": "fn", "name": "default", "args": ["N/A"]}],
        }
        out = _run([{"x": 1}], [_assignment("v", "Utf8", _expr(node))])
        assert out == [{"v": "N/A"}]

    def test_coalesce_fn_emits_fallback_for_all_null_input(self):
        node = {
            "op": "pipe",
            "args": [_get("absent"), {"op": "fn", "name": "coalesce", "args": ["fb"]}],
        }
        out = _run([{"x": 1}], [_assignment("v", "Utf8", _expr(node))])
        assert out == [{"v": "fb"}]

    def test_iso_to_date_rejects_trailing_junk(self):
        with pytest.raises(TransformationError, match="iso_to_date"):
            self._v1("iso_to_date")(pa.array(["2025-08-16not-a-timestamp"]))
