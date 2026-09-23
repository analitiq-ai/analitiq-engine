"""Behavior coverage for the mapping module: one document, one transform.

A contract ``StreamMapping`` is compiled once by ``compile_mapping`` into a
``CompiledTransform``; ``.run(batch)`` applies it to a ``pa.RecordBatch``
synchronously, raising ``TransformationError`` on any failure. These tests
assert that behaviour: token-array path reads, every expression form, the
function kernels, the conversion-matrix gating, and fail-loud batch-wide
semantics.
"""

import ast
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pytest
from analitiq.contracts.stream import StreamMapping

import src
from src.engine.batch_policy import ErrorStrategy
from src.engine.exceptions import TransformationError
from src.engine.mapping import (
    _FUNCTIONS,
    ValidationFailure,
    build_output_schema,
    compile_mapping,
)

# --------------------------------------------------------------------------- #
# Local builders -- keep each test reading as the mapping shape the engine sees #
# --------------------------------------------------------------------------- #


def _get(path):
    """An ``op: get`` expression node for a (possibly nested) source path."""
    return {"op": "get", "path": path if isinstance(path, list) else [path]}


def _expr(node):
    """Wrap an expression node as an assignment ``value`` block."""
    return {"kind": "expression", "expression": node}


def _const(value, arrow_type="Utf8"):
    """A constant value block (materialised at the target type)."""
    return {"kind": "constant", "constant": {"value": value, "arrow_type": arrow_type}}


def _rule(rule_type, field="v", **extra):
    """A validation rule in the contract's spelling.

    ``field`` is the contract's ordered token array; a plain string is a
    convenience for the common single-token address.
    """
    tokens = [field] if isinstance(field, str) else list(field)
    return {"type": rule_type, "field": tokens, **extra}


def _target(name, arrow_type, nullable=True, **extra):
    t = {"path": name, "arrow_type": arrow_type, "nullable": nullable}
    t.update(extra)
    return t


def _assignment(name, arrow_type, value, nullable=True, validate=None, **target_extra):
    a = {"target": _target(name, arrow_type, nullable, **target_extra), "value": value}
    if validate is not None:
        a["validate"] = validate
    return a


def _document(assignments):
    """Read a list of assignments as a mapping document."""
    return StreamMapping.model_validate({"assignments": assignments})


def _compile(assignments, default_strategy=ErrorStrategy.FAIL):
    """Read and compile *assignments* -- the whole document-to-transform route."""
    return compile_mapping(_document(assignments), default_strategy=default_strategy)


def _run(records, assignments):
    """Compile *assignments* and run them over *records*, returning pylist rows.

    Source types are inferred by ``from_pylist``; tests that need a specific
    source Arrow type build the batch explicitly instead.
    """
    batch = pa.RecordBatch.from_pylist(records)
    return _compile(assignments).run(batch).to_pylist()


class TestTokenArrayPaths:
    """A source path is an array of tokens from the document to the batch read.

    The whole point of the array: there is no split step, so a `get` cannot
    behave differently depending on where in the expression it sits. A dotted
    string used to reach the reader unsplit from inside a `pipe`, where
    ``path[0]`` was the first LETTER of the field name and the column came out
    silently all-null.
    """

    def test_nested_get_inside_pipe_reads_the_nested_field(self):
        batch = pa.record_batch(
            [pa.array([{"zip": 10115}, {"zip": 1001}])], names=["address"]
        )
        node = {
            "op": "pipe",
            "args": [_get(["address", "zip"]), {"op": "fn", "name": "to_string"}],
        }
        out = _compile([_assignment("zip", "Utf8", _expr(node))]).run(batch)
        assert out.to_pylist() == [{"zip": "10115"}, {"zip": "1001"}]

    def test_a_single_token_reads_a_field_whose_name_contains_a_dot(self):
        batch = pa.record_batch([pa.array(["v"])], names=["a.b"])
        out = _compile([_assignment("x", "Utf8", _expr(_get(["a.b"])))]).run(batch)
        assert out.to_pylist() == [{"x": "v"}]


class TestTargetIsOneSegment:
    """A target names one field on the destination record root."""

    def test_nesting_is_declared_with_object_plus_properties(self):
        schema = build_output_schema(
            _document(
                [
                    _assignment(
                        "address",
                        "Object",
                        _expr(_get("address")),
                        properties={"city": {"arrow_type": "Utf8"}},
                    )
                ]
            ).assignments
        )
        assert pa.types.is_struct(schema.field("address").type)


def _splits_on_a_dot(tree: ast.AST) -> list[int]:
    """Line numbers of every ``<something>.split(".")`` call in *tree*."""
    return [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "split"
        and len(node.args) == 1
        and isinstance(node.args[0], ast.Constant)
        and node.args[0].value == "."
    ]


class TestNoModuleSplitsAPath:
    """No module on the mapping route turns a string into path segments.

    Structural rather than behavioural because the defect was structural: one
    commit taught the translator to split a dotted path, and a later one taught
    the reader to expect tokens. Each was self-consistent; together they made a
    nested read return nulls. A test that only exercises the reader cannot see
    a second module re-introducing the split upstream.
    """

    # The modules a mapping document travels through. Listed rather than
    # inferred because `runner.py` -- the one that introduced the split --
    # carries the document without importing the module that defines it.
    ROUTE = (
        "engine/mapping.py",
        "engine/pipeline_config_prep.py",
        "engine/stream_processor.py",
        "models/resolved.py",
        "runner.py",
    )

    @staticmethod
    def _modules() -> dict[str, ast.AST]:
        """The named route, plus anything that imports the mapping module."""
        root = Path(src.__file__).parent
        sources = {
            path.relative_to(root).as_posix(): path.read_text(encoding="utf-8")
            for path in sorted(root.rglob("*.py"))
        }
        selected = {
            name: text
            for name, text in sources.items()
            if name in TestNoModuleSplitsAPath.ROUTE or "engine.mapping" in text
        }
        missing = set(TestNoModuleSplitsAPath.ROUTE) - set(selected)
        assert not missing, f"the mapping route moved: {sorted(missing)}"
        return {name: ast.parse(text) for name, text in selected.items()}

    def test_no_module_splits_a_path_on_a_dot(self):
        offenders = {
            name: lines
            for name, tree in self._modules().items()
            if (lines := _splits_on_a_dot(tree))
        }
        assert not offenders, (
            f"a path is an array of tokens; these modules split a string "
            f"instead: {offenders}"
        )


class TestExpressionOps:
    """Each expression op evaluates as a vectorized column over the batch."""

    def test_get_renames_top_level_column(self):
        out = _run([{"a": 1}, {"a": 2}], [_assignment("b", "Int64", _expr(_get("a")))])
        assert out == [{"b": 1}, {"b": 2}]

    def test_get_drills_into_struct_source_column(self):
        batch = pa.record_batch(
            [pa.array([{"inner": "v1"}, {"inner": "v2"}])], names=["outer"]
        )
        out = _compile([_assignment("x", "Utf8", _expr(_get(["outer", "inner"])))]).run(
            batch
        )
        assert out.to_pylist() == [{"x": "v1"}, {"x": "v2"}]

    def test_const_value_kind_broadcasts_literal(self):
        out = _run([{"a": 1}, {"a": 2}], [_assignment("s", "Utf8", _const("X"))])
        assert out == [{"s": "X"}, {"s": "X"}]

    def test_pipe_applies_its_conversion_stage_to_the_seed(self):
        node = {
            "op": "pipe",
            "args": [_get("n"), {"op": "fn", "name": "to_string"}],
        }
        out = _run([{"n": 7}], [_assignment("n", "Utf8", _expr(node))])
        assert out == [{"n": "7"}]


class TestFunctionCatalog:
    """Every catalog kernel maps a ``pa.Array`` to a ``pa.Array``. Kernels are
    exercised directly so the input Arrow type is exact and unambiguous."""

    @pytest.mark.parametrize("name", sorted(_FUNCTIONS))
    def test_null_input_element_stays_null(self, name):
        """A null row passes through every elementwise kernel as null -- it is
        never coerced to "", which would defeat the nullability check."""
        array = pa.array([None, "a"], pa.string())
        assert _FUNCTIONS[name](array).to_pylist()[0] is None

    def test_to_string_formats_numbers(self):
        out = _FUNCTIONS["to_string"](pa.array([1, 2, None], pa.int64())).to_pylist()
        assert out == ["1", "2", None]


class TestConversionMatrix:
    """Type conversion has one authority: the conversion matrix gates each
    source->target retype, executed by the same ``pc.cast(safe=True)`` the
    destination uses."""

    def test_identity_rename_passes_through(self):
        out = _run([{"a": 1}, {"a": 2}], [_assignment("a", "Int64", _expr(_get("a")))])
        assert out == [{"a": 1}, {"a": 2}]

    def test_auto_widening_int32_to_int64(self):
        batch = pa.record_batch([pa.array([1, 2], pa.int32())], names=["a"])
        out = _compile([_assignment("a", "Int64", _expr(_get("a")))]).run(batch)
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
        compiled = _compile([_assignment("a", "Int8", _expr(_get("a")))])
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
        out = _compile([assignment]).run(batch)
        assert out.schema.field("created_at").type == pa.timestamp("us", tz="UTC")


class TestFailLoudSemantics:
    """A row failing any rule, a null in a non-nullable column, or a missing
    source column are handled batch-wide and loudly -- the transform never does
    per-row DLQ routing; a rule failure carries the strategy the stream
    disposes of the whole batch under."""

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
        """A single failing row fails the entire batch: the transform has no
        per-row routing."""
        with pytest.raises(TransformationError, match="not_null"):
            _run(
                [{"a": "v"}, {"a": None}],
                [
                    _assignment(
                        "a",
                        "Utf8",
                        _expr(_get("a")),
                        validate={"rules": [_rule("not_null", field="a")]},
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
                        validate={
                            "rules": [_rule("pattern", field="a", value="[a-z]+")]
                        },
                    )
                ],
            )


class TestValidationRules:
    """A rule's wire shape: which column it guards and where its parameter lives."""

    def _validated(self, rules, arrow_type="Int64"):
        return [
            _assignment(
                "v",
                arrow_type,
                _expr(_get("v")),
                validate={"rules": rules},
            )
        ]

    def test_rule_naming_another_declared_target_grades_that_column(self):
        """Rules grade the record the assignments build together.

        A rule is authored per assignment but may address any declared
        target, so the failing column named in the error is the addressed
        one, not the one the rule rides on.
        """
        assignments = [
            _assignment("a", "Int64", _expr(_get("a"))),
            _assignment(
                "b",
                "Int64",
                _expr(_get("b")),
                validate={"rules": [_rule("not_null", field="a")]},
            ),
        ]
        assert _run([{"a": 1, "b": 2}], assignments) == [{"a": 1, "b": 2}]
        with pytest.raises(TransformationError, match=r"column 'a'.*not_null"):
            _run([{"a": None, "b": 2}], assignments)

    def test_rule_addressing_a_nested_field_grades_it(self):
        """A multi-token field descends into the target's declared properties."""
        assignments = [
            _assignment(
                "address",
                "Object",
                _expr(_get("address")),
                properties={"city": {"arrow_type": "Utf8"}},
                validate={
                    "rules": [_rule("min_length", field=["address", "city"], value=4)]
                },
            )
        ]
        assert _run([{"address": {"city": "Kyiv"}}], assignments) == [
            {"address": {"city": "Kyiv"}}
        ]
        with pytest.raises(
            TransformationError, match=r"\['address', 'city'\].*min_length"
        ):
            _run([{"address": {"city": "Ur"}}], assignments)

    def test_rule_addressing_through_a_list_fails_the_element_rows(self):
        """A `List` level is stepped through; a row fails if any element does."""
        assignments = [
            _assignment(
                "lines",
                "List",
                _expr(_get("lines")),
                items={
                    "arrow_type": "Object",
                    "properties": {"sku": {"arrow_type": "Utf8"}},
                },
                validate={"rules": [_rule("not_null", field=["lines", "sku"])]},
            )
        ]
        clean = [{"lines": [{"sku": "A-1"}, {"sku": "A-2"}]}]
        assert _run(clean, assignments) == clean
        with pytest.raises(TransformationError, match=r"1 row\(s\) fail rule"):
            _run(
                [
                    {"lines": [{"sku": "A-1"}]},
                    {"lines": [{"sku": None}, {"sku": None}]},
                ],
                assignments,
            )

    @pytest.mark.parametrize("rule_type", ["not_null", "required"])
    def test_a_null_list_ancestor_fails_like_a_null_struct_parent(self, rule_type):
        """One verdict for "an ancestor of the addressed field is null".

        `list_flatten` drops a null list's elements where a null struct
        propagates null children, so without folding the dropped rows back
        in, the same data would pass or fail on how Arrow typed the batch.
        Here rows 0 (null list) and 2 (null element) fail together; the
        empty list at row 3 is data -- zero elements, nothing null.
        """
        assignments = [
            _assignment(
                "lines",
                "List",
                _expr(_get("lines")),
                items={
                    "arrow_type": "Object",
                    "properties": {"sku": {"arrow_type": "Utf8"}},
                },
                validate={"rules": [_rule(rule_type, field=["lines", "sku"])]},
            )
        ]
        with pytest.raises(TransformationError, match=r"rows \[0, 2\]"):
            _run(
                [
                    {"lines": None},
                    {"lines": [{"sku": "A-1"}]},
                    {"lines": [{"sku": None}]},
                    {"lines": []},
                ],
                assignments,
            )

    def test_null_ancestors_two_list_levels_deep_map_to_their_rows(self):
        """The row map and the null-ancestor set compose across levels."""
        assignments = [
            _assignment(
                "orders",
                "List",
                _expr(_get("orders")),
                items={
                    "arrow_type": "List",
                    "items": {
                        "arrow_type": "Object",
                        "properties": {"sku": {"arrow_type": "Utf8"}},
                    },
                },
                validate={"rules": [_rule("not_null", field=["orders", "sku"])]},
            )
        ]
        with pytest.raises(TransformationError, match=r"rows \[0, 1, 3\]"):
            _run(
                [
                    {"orders": None},
                    {"orders": [None]},
                    {"orders": [[{"sku": "A"}]]},
                    {"orders": [[{"sku": None}]]},
                    {"orders": [[]]},
                ],
                assignments,
            )

    def test_a_null_list_ancestor_is_exempt_from_value_rules(self):
        """Value rules keep the null exemption through a list level too."""
        assignments = [
            _assignment(
                "lines",
                "List",
                _expr(_get("lines")),
                items={
                    "arrow_type": "Object",
                    "properties": {"sku": {"arrow_type": "Utf8"}},
                },
                validate={
                    "rules": [_rule("min_length", field=["lines", "sku"], value=3)]
                },
            )
        ]
        clean = [{"lines": None}, {"lines": [{"sku": "A-1"}]}, {"lines": []}]
        assert _run(clean, assignments) == clean
        with pytest.raises(TransformationError, match=r"rows \[1\]"):
            _run(
                [{"lines": None}, {"lines": [{"sku": "x"}]}],
                assignments,
            )

    def test_nested_not_null_over_a_valueless_column_fails_every_row(self):
        """A column carrying no value anywhere infers as Arrow's `null` type.

        No `struct_field` kernel accepts it, but every deeper token
        addresses only nulls -- the same "ancestor is null" fact however
        the batch was typed, so `not_null` fails these rows exactly as it
        does when the column arrives typed. An empty list stays data.
        """
        assignments = [
            _assignment(
                "lines",
                "List",
                _expr(_get("lines")),
                items={
                    "arrow_type": "Object",
                    "properties": {"sku": {"arrow_type": "Utf8"}},
                },
                validate={"rules": [_rule("not_null", field=["lines", "sku"])]},
            )
        ]
        with pytest.raises(TransformationError, match=r"rows \[0\]"):
            _run([{"lines": None}, {"lines": []}], assignments)

    def test_rule_on_a_field_the_source_does_not_carry_fails_loud_at_run(self):
        """A declared field the built value lacks is no silent grade.

        Rules grade the built (pre-conversion) values, so a field the target
        declares but the source struct does not carry raises out of the
        field walk and is classified, never treated as an all-pass rule over
        nothing.
        """
        with pytest.raises(TransformationError, match="the built value does not carry"):
            _run(
                [{"address": {"city": "Kyiv"}}],
                [
                    _assignment(
                        "address",
                        "Object",
                        _expr(_get("address")),
                        properties={
                            "city": {"arrow_type": "Utf8"},
                            "zip": {"arrow_type": "Utf8"},
                        },
                        validate={
                            "rules": [_rule("not_null", field=["address", "zip"])]
                        },
                    )
                ],
            )

    def test_range_bounds_come_from_the_rule_value_object(self):
        rules = [_rule("range", value={"min": 1, "max": 5})]
        assert _run([{"v": 1}, {"v": 5}], self._validated(rules)) == [
            {"v": 1},
            {"v": 5},
        ]
        with pytest.raises(TransformationError, match=r"fail rule 'range'"):
            _run([{"v": 3}, {"v": 6}], self._validated(rules))

    def test_range_accepts_a_one_sided_bound(self):
        with pytest.raises(TransformationError, match=r"fail rule 'range'"):
            _run([{"v": 9}], self._validated([_rule("range", value={"min": 10})]))
        assert _run(
            [{"v": 9}], self._validated([_rule("range", value={"max": 10})])
        ) == [{"v": 9}]


class TestValidationErrorStrategy:
    """Each rule fails under its assignment's ``error_handling.strategy``
    override, else the pipeline default the transform was compiled with
    (issue #468). The failure is still batch-wide; the strategy it carries is
    what the stream disposes of the batch under."""

    @staticmethod
    def _validated(rules, error_handling=None, name="v"):
        validate = {"rules": rules}
        if error_handling is not None:
            validate["error_handling"] = error_handling
        return _assignment(name, "Int64", _expr(_get(name)), validate=validate)

    def _failure(self, records, assignments, default=ErrorStrategy.FAIL):
        with pytest.raises(ValidationFailure) as info:
            _compile(assignments, default).run(pa.RecordBatch.from_pylist(records))
        return info.value

    def test_override_strategy_is_carried(self):
        failure = self._failure(
            [{"v": None}],
            [self._validated([_rule("not_null")], {"strategy": "skip"})],
        )
        assert failure.strategy is ErrorStrategy.SKIP
        assert "not_null" in str(failure)

    def test_override_wins_over_the_pipeline_default(self):
        failure = self._failure(
            [{"v": None}],
            [self._validated([_rule("not_null")], {"strategy": "dlq"})],
            default=ErrorStrategy.SKIP,
        )
        assert failure.strategy is ErrorStrategy.DLQ

    @pytest.mark.parametrize("default", list(ErrorStrategy))
    def test_no_override_takes_the_pipeline_default(self, default):
        """Absent means the pipeline default, never the contract block's own
        default value."""
        failure = self._failure(
            [{"v": None}], [self._validated([_rule("not_null")])], default=default
        )
        assert failure.strategy is default

    @pytest.mark.parametrize("default", list(ErrorStrategy))
    def test_block_without_strategy_takes_the_pipeline_default(self, default):
        """A block that sets only retry fields overrides nothing: the contract
        model's own ``dlq`` default is not the author's choice."""
        failure = self._failure(
            [{"v": None}],
            [self._validated([_rule("not_null")], {"max_retries": 0})],
            default=default,
        )
        assert failure.strategy is default

    def test_retry_fields_change_nothing(self):
        """A rule is deterministic; the override's retry fields are not read."""
        failure = self._failure(
            [{"v": None}],
            [
                self._validated(
                    [_rule("not_null")],
                    {"strategy": "skip", "max_retries": 3, "retry_delay_seconds": 9},
                )
            ],
        )
        assert failure.strategy is ErrorStrategy.SKIP

    def test_strictest_strategy_wins_across_failed_rules(self):
        """One batch, several failed rules under different strategies: the
        batch takes the strictest verdict (fail > dlq > skip)."""
        assignments = [
            self._validated([_rule("not_null", field="a")], {"strategy": "skip"}, "a"),
            self._validated([_rule("not_null", field="b")], {"strategy": "dlq"}, "b"),
            self._validated([_rule("range", field="c", value={"min": 0})], None, "c"),
        ]
        both_dropped = self._failure(
            [{"a": None, "b": None, "c": 1}], assignments, default=ErrorStrategy.FAIL
        )
        assert both_dropped.strategy is ErrorStrategy.DLQ
        only_a = self._failure(
            [{"a": None, "b": 1, "c": 1}], assignments, default=ErrorStrategy.FAIL
        )
        assert only_a.strategy is ErrorStrategy.SKIP
        with_default = self._failure(
            [{"a": None, "b": None, "c": -1}], assignments, default=ErrorStrategy.FAIL
        )
        assert with_default.strategy is ErrorStrategy.FAIL

    def test_passing_batch_is_untouched_by_the_override(self):
        assignments = [self._validated([_rule("not_null")], {"strategy": "skip"})]
        assert _run([{"v": 1}], assignments) == [{"v": 1}]

    def test_mapping_defect_on_the_same_batch_is_not_a_validation_failure(self):
        """A null in a non-nullable column is a mapping defect no strategy
        relaxes: the batch fails as a TransformationError that also names the
        rule failure, never as a droppable ValidationFailure."""
        assignments = [
            self._validated([_rule("not_null")], {"strategy": "skip"}),
            _assignment("n", "Utf8", _expr(_get("n")), nullable=False),
        ]
        with pytest.raises(TransformationError) as info:
            _compile(assignments).run(
                pa.RecordBatch.from_pylist([{"v": None, "n": None}])
            )
        assert not isinstance(info.value, ValidationFailure)
        assert "not nullable" in str(info.value)
        assert "not_null" in str(info.value)


class TestCompiledReuseAndConsts:
    def test_one_compiled_transform_reused_across_batch_sizes(self):
        """A transform is compiled once and applied to batches of different row
        counts -- including an empty batch -- with no per-batch state leaking."""
        assignments = [
            _assignment("amount", "Float64", _expr(_get("v"))),
            _assignment("kind", "Utf8", _const("txn")),
            _assignment("missing", "Utf8", _expr(_get("absent"))),
        ]
        compiled = _compile(assignments)
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
        out = _compile(assignments).run(batch)
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
    def test_scalar_schema_carries_nullability(self):
        schema = build_output_schema(
            _document(
                [
                    _assignment("a", "Int64", _expr(_get("a")), nullable=False),
                    _assignment("b", "Utf8", _expr(_get("b")), nullable=True),
                ]
            ).assignments
        )
        assert schema.field("a").type == pa.int64()
        assert not schema.field("a").nullable
        assert schema.field("b").nullable

    def test_arrow_type_the_engine_grammar_rejects_names_the_assignment(self):
        """The contract's type pattern admits a zone the CDK cannot resolve."""
        assignments = _document(
            [_assignment("t", "Timestamp(MILLISECOND, Not/AZone)", _expr(_get("t")))]
        ).assignments
        with pytest.raises(
            TransformationError, match=r"assignment\[0\] target='t'.*arrow_type"
        ):
            build_output_schema(assignments)


class TestPerRecordParity:
    """Edge semantics that must match the deleted per-record evaluator.

    Vectorized Arrow kernels diverge from Python at the edges (boolean
    formatting); each kernel here is steered back to the per-record behavior.
    """

    def test_to_string_renders_bool_as_python_str(self):
        out = _FUNCTIONS["to_string"](pa.array([True, False, None])).to_pylist()
        assert out == ["True", "False", None]

    def test_pattern_validation_matches_python_bool_spelling(self):
        # A `True|False` pattern on a Boolean column must pass: booleans render as
        # "True"/"False" for validation, matching to_string.
        out = _run(
            [{"b": True}, {"b": False}],
            [
                _assignment(
                    "b",
                    "Boolean",
                    _expr(_get("b")),
                    validate={
                        "rules": [_rule("pattern", field="b", value="True|False")]
                    },
                )
            ],
        )
        assert out == [{"b": True}, {"b": False}]
