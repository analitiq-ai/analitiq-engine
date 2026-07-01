"""Conformance tests for the permitted-type-conversion matrix.

The matrix (:mod:`cdk.type_map.conversions`) is the single policy every build
boundary consults. These tests pin three things so it cannot silently rot:

1. the committed published artifact equals what the canonical table generates
   (drift guard);
2. every ``explicit`` conversion names a function that actually exists in the
   engine's mapping ``FUNCTION_CATALOG``;
3. each declared mode matches what the real builders do -- ``cast_arrow_batch``
   (destination) and the compiled transform retype accept/reject the same pair
   identically, which is the divergence the matrix exists to remove.
"""

from __future__ import annotations

from collections.abc import Callable

import pyarrow as pa
import pytest

from cdk.schema_contract import SchemaContract
from cdk.type_map.arrow import arrow_family, parse_arrow_type
from cdk.type_map.conversions import (
    ARROW_FAMILIES,
    build_conversion_matrix,
    classify_conversion,
    load_published_matrix,
    render_conversion_matrix,
)
from cdk.type_map.exceptions import InvalidTypeMapError
from src.engine.data_transformer import _FUNCTION_CATALOG, compile_transform
from src.engine.exceptions import TransformationError

_VALID_MODES = {"identity", "auto", "explicit", "forbidden"}


class TestPublishedArtifactDrift:
    """The committed grid must equal what the canonical table generates."""

    def test_committed_json_matches_canonical_render(self) -> None:
        from cdk.type_map.conversions import CONVERSION_MATRIX_PATH

        assert CONVERSION_MATRIX_PATH.read_text() == render_conversion_matrix(), (
            "conversion_matrix.json is stale; regenerate with "
            "CONVERSION_MATRIX_PATH.write_text(render_conversion_matrix())"
        )

    def test_loaded_matrix_equals_built_matrix(self) -> None:
        assert load_published_matrix() == build_conversion_matrix()

    def test_grid_covers_every_family_pair(self) -> None:
        matrix = build_conversion_matrix()
        assert set(matrix) == set(ARROW_FAMILIES)
        for source, row in matrix.items():
            assert set(row) == set(ARROW_FAMILIES), source


class TestMatrixInvariants:
    """Structural rules every entry obeys."""

    def test_every_entry_is_well_formed(self) -> None:
        for source in ARROW_FAMILIES:
            for target in ARROW_FAMILIES:
                conv = classify_conversion(source, target)
                assert conv.mode in _VALID_MODES, (source, target)
                assert isinstance(conv.runtime_checked, bool)
                # An fn is present iff the conversion is explicit.
                if conv.mode == "explicit":
                    assert conv.fn, (source, target)
                else:
                    assert conv.fn is None, (source, target)

    def test_diagonal_is_identity(self) -> None:
        for family in ARROW_FAMILIES:
            assert classify_conversion(family, family).mode == "identity"

    def test_unknown_family_raises(self) -> None:
        with pytest.raises(InvalidTypeMapError):
            classify_conversion("NotAType", "Utf8")
        with pytest.raises(InvalidTypeMapError):
            classify_conversion("Utf8", "NotAType")


class TestMatrixBoundToFunctionCatalog:
    """Every explicit conversion must name a real mapping function."""

    def test_explicit_fns_exist_in_catalog(self) -> None:
        catalog = _FUNCTION_CATALOG
        named = {
            conv["fn"]
            for row in build_conversion_matrix().values()
            for conv in row.values()
            if conv["fn"] is not None
        }
        assert named, "expected at least one explicit conversion fn"
        missing = named - set(catalog)
        assert not missing, f"matrix names fns absent from FUNCTION_CATALOG: {missing}"


class TestNamedCells:
    """The conversions the ticket / ADR call out by name."""

    @pytest.mark.parametrize(
        ("source", "target", "mode", "fn", "runtime_checked"),
        [
            ("Int64", "Utf8", "explicit", "to_string", False),
            ("Boolean", "Utf8", "explicit", "to_string", False),
            ("Float64", "Utf8", "explicit", "to_string", False),
            ("Timestamp", "Utf8", "explicit", "to_string", False),
            # Parsing a string into a scalar stays implicit: API sources ship
            # every value as a JSON string and the engine has always parsed them.
            ("Utf8", "Int64", "auto", None, True),
            ("Utf8", "Float64", "auto", None, True),
            ("Int32", "Int64", "auto", None, True),
            ("Int64", "Int32", "auto", None, True),
            ("Float64", "Int64", "auto", None, True),
            ("Object", "Int64", "forbidden", None, False),
            ("List", "Int64", "forbidden", None, False),
            # One nested shape never becomes another.
            ("Object", "List", "forbidden", None, False),
            ("Int64", "Null", "forbidden", None, False),
            ("Null", "Int64", "auto", None, False),
            # Null fills any typed column, nested included (decided before the
            # nested-forbidden guard), so an all-null source can feed a struct.
            ("Null", "Object", "auto", None, False),
            ("Null", "List", "auto", None, False),
            # Json is an opaque blob: no scalar conversion either way.
            ("Json", "Int64", "forbidden", None, False),
            ("Int64", "Json", "forbidden", None, False),
        ],
    )
    def test_named_cell(
        self,
        source: str,
        target: str,
        mode: str,
        fn: str | None,
        runtime_checked: bool,
    ) -> None:
        conv = classify_conversion(source, target)
        assert conv.mode == mode
        assert conv.fn == fn
        assert conv.runtime_checked == runtime_checked


class TestArrowFamilyRoundTrip:
    """A built DataType maps back to the family it was parsed from."""

    @pytest.mark.parametrize(
        "canonical",
        [
            "Null",
            "Int8",
            "Int64",
            "UInt8",
            "UInt16",
            "UInt32",
            "UInt64",
            "Float16",
            "Float32",
            "Float64",
            "Utf8",
            "LargeUtf8",
            "Boolean",
            "Binary",
            "LargeBinary",
            "FixedSizeBinary(16)",
            "Date32",
            "Date64",
            "Time32(SECOND)",
            "Time64(MICROSECOND)",
            "Timestamp(MICROSECOND, UTC)",
            "Duration(SECOND)",
            "Decimal128(10, 2)",
            "Decimal256(40, 2)",
        ],
    )
    def test_family_round_trip(self, canonical: str) -> None:
        family = canonical.split("(")[0]
        assert arrow_family(parse_arrow_type(canonical)) == family

    def test_binary_trio_disambiguated(self) -> None:
        # is_fixed_size_binary / is_large_binary / is_binary are adjacent,
        # first-match-wins probes; a reorder would silently misclassify these.
        assert arrow_family(pa.binary()) == "Binary"
        assert arrow_family(pa.large_binary()) == "LargeBinary"
        assert arrow_family(pa.binary(16)) == "FixedSizeBinary"

    def test_struct_and_list_families(self) -> None:
        assert arrow_family(pa.struct([("a", pa.int64())])) == "Object"
        assert arrow_family(pa.list_(pa.int64())) == "List"
        assert arrow_family(pa.large_list(pa.int64())) == "List"

    def test_out_of_vocabulary_type_fails_loud(self) -> None:
        # A live Arrow type outside the published vocabulary must raise, not
        # resolve to a silent default, so it cannot slip through a boundary.
        for dtype in (
            pa.map_(pa.string(), pa.int64()),
            pa.union([pa.field("a", pa.int64())], mode="sparse"),
        ):
            with pytest.raises(InvalidTypeMapError):
                arrow_family(dtype)

    def test_dictionary_classifies_by_value_type(self) -> None:
        # Some ADBC drivers dictionary-encode low-cardinality columns; a
        # dictionary is its value type for conversion purposes (pc.cast decodes).
        assert arrow_family(pa.dictionary(pa.int32(), pa.string())) == "Utf8"
        assert arrow_family(pa.dictionary(pa.int8(), pa.int64())) == "Int64"


def _dest(arrow_type: str, *, nullable: bool = True) -> SchemaContract:
    return SchemaContract(
        {"columns": [{"name": "c", "arrow_type": arrow_type, "nullable": nullable}]}
    )


def _cast(arrow_type: str, column: pa.Array) -> pa.RecordBatch:
    batch = pa.RecordBatch.from_arrays([column], names=["c"])
    return _dest(arrow_type).cast_arrow_batch(batch)


class TestDestinationBoundaryConformance:
    """cast_arrow_batch behaves as the declared mode says it should."""

    def test_explicit_int_to_string_is_rejected(self) -> None:
        # The incident: a destination string column fed an int column. Now it
        # fails loud naming the function, instead of silently stringifying.
        with pytest.raises(ValueError, match="to_string"):
            _cast("Utf8", pa.array([1, 2, 3], pa.int64()))

    def test_forbidden_struct_to_int_is_rejected(self) -> None:
        col = pa.array([{"a": 1}], pa.struct([("a", pa.int64())]))
        with pytest.raises(ValueError, match="not a permitted conversion"):
            _cast("Int64", col)

    def test_auto_widening_builds(self) -> None:
        out = _cast("Int64", pa.array([1, 2], pa.int32()))
        assert out.column(0).type == pa.int64()
        assert out.column(0).to_pylist() == [1, 2]

    def test_auto_string_parse_builds(self) -> None:
        # API sources ship JSON strings; the destination parses them. This is
        # why string->scalar stays auto -- protect it from a silent flip.
        out = _cast("Int64", pa.array(["1", "2"], pa.string()))
        assert out.column(0).to_pylist() == [1, 2]

    def test_runtime_checked_unparseable_string_is_rejected(self) -> None:
        with pytest.raises(ValueError):
            _cast("Int64", pa.array(["1", "x"], pa.string()))

    def test_runtime_checked_narrowing_overflow_is_rejected(self) -> None:
        with pytest.raises(ValueError):
            _cast("Int32", pa.array([2**40], pa.int64()))

    def test_runtime_checked_lossy_float_to_int_is_rejected(self) -> None:
        with pytest.raises(ValueError):
            _cast("Int64", pa.array([1.5], pa.float64()))

    def test_same_family_unit_change_builds(self) -> None:
        col = pa.array([0, 1_000], pa.timestamp("s"))
        out = _cast("Timestamp(MICROSECOND)", col)
        assert pa.types.is_timestamp(out.column(0).type)

    def test_all_null_source_fills_a_nested_target(self) -> None:
        # An all-null (null-typed) source column targeting a nullable struct must
        # fill typed nulls, not reject: Null fills any typed column, nested too.
        struct = "Object"
        contract = SchemaContract(
            {
                "columns": [
                    {
                        "name": "c",
                        "arrow_type": struct,
                        "nullable": True,
                        "properties": {"a": {"arrow_type": "Int64"}},
                    }
                ]
            }
        )
        batch = pa.RecordBatch.from_arrays([pa.nulls(2)], names=["c"])
        out = contract.cast_arrow_batch(batch)
        assert out.column(0).to_pylist() == [None, None]
        assert pa.types.is_struct(out.column(0).type)

    def test_dictionary_encoded_column_decodes(self) -> None:
        # Regression: an ADBC dictionary-encoded text column targeting Utf8 must
        # decode (classify by value type), not raise on the pre-cast lookup.
        out = _cast("Utf8", pa.array(["a", "b", "a"]).dictionary_encode())
        assert out.column(0).to_pylist() == ["a", "b", "a"]
        assert out.column(0).type == pa.string()


def _retype_batch(target_arrow_type: str, column: pa.Array) -> pa.RecordBatch:
    assignments = [
        {
            "target": {"path": ["c"], "arrow_type": target_arrow_type},
            "value": {"kind": "expr", "expr": {"op": "get", "path": ["src"]}},
        }
    ]
    batch = pa.RecordBatch.from_arrays([column], names=["src"])
    return compile_transform(assignments).run(batch)


class TestTransformBoundaryConformance:
    """The compiled transform retype behaves like the destination cast."""

    def test_explicit_int_to_string_is_rejected(self) -> None:
        with pytest.raises(TransformationError, match="to_string"):
            _retype_batch("Utf8", pa.array([1, 2], pa.int64()))

    def test_forbidden_nested_source_to_scalar_is_rejected(self) -> None:
        # A get from a struct source column into a scalar target reaches the
        # matrix's forbidden gate -- it must fail loud, not crash obscurely.
        col = pa.array([{"a": 1}], pa.struct([("a", pa.int64())]))
        with pytest.raises(TransformationError, match="not a permitted conversion"):
            _retype_batch("Int64", col)

    def test_auto_widening_builds(self) -> None:
        out = _retype_batch("Int64", pa.array([1, 2], pa.int32()))
        assert out.column(0).type == pa.int64()

    def test_auto_string_parse_builds(self) -> None:
        out = _retype_batch("Int64", pa.array(["1", "2"], pa.string()))
        assert out.column(0).to_pylist() == [1, 2]

    def test_runtime_checked_lossy_float_to_int_is_rejected(self) -> None:
        # Regression guard: a bare pa.array here would silently truncate 1.5->1,
        # which the destination rejects. The fast path must reject it too.
        with pytest.raises(TransformationError):
            _retype_batch("Int64", pa.array([1.5], pa.float64()))


def _boundary_outcome(build: Callable[[], list]) -> tuple:
    """Run one boundary build; return ('ok', values) or ('reject',).

    Normalizes the two boundaries' distinct exception types (ValueError vs
    TransformationError) so their outcomes can be compared directly.
    """
    try:
        return ("ok", build())
    except (ValueError, TransformationError):
        return ("reject",)


class TestBoundariesAgree:
    """The matrix's promise: a retype and the destination cast of the same
    column produce the SAME outcome -- the same value, or both fail loud."""

    @pytest.mark.parametrize(
        ("source_type", "values", "target"),
        [
            (pa.int32(), [1, 2], "Int64"),  # auto widen -> both [1, 2]
            (pa.string(), ["1", "2"], "Int64"),  # auto parse -> both [1, 2]
            (pa.float64(), [1.0, 2.0], "Int64"),  # lossless float -> both [1, 2]
            (pa.float64(), [1.5], "Int64"),  # lossy float -> both reject
            (pa.string(), ["x"], "Int64"),  # unparseable -> both reject
            (pa.int64(), [2**40], "Int32"),  # overflow -> both reject
            (pa.int64(), [1, 2], "Utf8"),  # explicit -> both reject
            (pa.struct([("a", pa.int64())]), [{"a": 1}], "Int64"),  # forbidden
        ],
    )
    def test_transform_and_destination_match(
        self, source_type: pa.DataType, values: list, target: str
    ) -> None:
        col = pa.array(values, source_type)
        destination = _boundary_outcome(
            lambda: _cast(target, col).column(0).to_pylist()
        )
        transform = _boundary_outcome(
            lambda: _retype_batch(target, col).column(0).to_pylist()
        )
        assert destination == transform, (
            f"{source_type} -> {target}: destination={destination} "
            f"transform={transform}"
        )


class TestCrossKindAllowlist:
    """Item 1 (#297): the cross-kind catch-all is an explicit stable-cast
    allowlist. The published grid promises only casts the runtime performs
    identically on every supported pyarrow version; every impossible or
    version-dependent pair publishes as ``forbidden`` instead of a fake ``auto``.
    """

    # Cross-kind pairs pyarrow cannot cast (Binary -> Int64), or that only some
    # pyarrow versions can (Utf8 -> Date32, unimplemented on 12), or that the
    # allowlist deliberately excludes (Float64 -> Boolean; only bool <-> int is
    # offered). The old catch-all mislabelled every one of these ``auto``.
    @pytest.mark.parametrize(
        ("source", "target"),
        [
            ("Binary", "Int64"),
            ("Binary", "Utf8"),
            ("Duration", "Date32"),
            ("Time64", "Boolean"),
            ("Utf8", "Date32"),
            ("Utf8", "Boolean"),
            ("Utf8", "Timestamp"),
            ("Int64", "Date32"),
            ("Int64", "Timestamp"),
            ("Float64", "Boolean"),
            ("Decimal128", "Boolean"),
            ("Timestamp", "Int64"),
            ("Date32", "Int64"),
        ],
    )
    def test_impossible_cross_kind_pair_is_forbidden(
        self, source: str, target: str
    ) -> None:
        conv = classify_conversion(source, target)
        assert conv.mode == "forbidden", (source, target)
        assert conv.fn is None
        assert conv.runtime_checked is False

    # The four allowlist categories: numeric <-> numeric, bool <-> int,
    # string -> numeric parse, date <-> timestamp. Each stays auto + runtime
    # checked -- attempted, the safe-cast rejecting a row/width it cannot convert.
    @pytest.mark.parametrize(
        ("source", "target"),
        [
            ("Int64", "Float64"),
            ("Float64", "Int64"),
            ("Int64", "Decimal128"),
            ("Decimal128", "Int64"),
            ("Float64", "Decimal128"),
            ("Decimal128", "Float64"),
            ("Boolean", "Int64"),
            ("Int64", "Boolean"),
            ("Utf8", "Int64"),
            ("Utf8", "Float64"),
            ("Utf8", "Decimal128"),
            ("Date32", "Timestamp"),
            ("Timestamp", "Date32"),
        ],
    )
    def test_allowlisted_cross_kind_pair_is_auto(
        self, source: str, target: str
    ) -> None:
        conv = classify_conversion(source, target)
        assert conv.mode == "auto", (source, target)
        assert conv.fn is None
        assert conv.runtime_checked is True

    def test_no_cross_kind_pair_is_auto_without_being_runtime_checked(self) -> None:
        # A cross-kind auto is always a runtime-checked cast (it can reject a
        # row); a non-checked auto would be a same-kind widen, never cross-kind.
        for source in ARROW_FAMILIES:
            for target in ARROW_FAMILIES:
                conv = classify_conversion(source, target)
                if conv.mode == "auto" and not conv.runtime_checked:
                    # The only non-checked auto is a Null source filling a column.
                    assert source == "Null", (source, target)


def _retype_nested(target: dict, column: pa.Array) -> pa.RecordBatch:
    """Run the transform retype into a nested (``Object``/``List``) target."""
    assignments = [
        {
            "target": {"path": ["c"], **target},
            "value": {"kind": "expr", "expr": {"op": "get", "path": ["src"]}},
        }
    ]
    batch = pa.RecordBatch.from_arrays([column], names=["src"])
    return compile_transform(assignments).run(batch)


_STRUCT_INT = pa.struct([("a", pa.int64())])
_OBJECT_STR = {"arrow_type": "Object", "properties": {"a": {"arrow_type": "Utf8"}}}


class TestNestedLeafGating:
    """Item 2 (#297): a scalar leaf inside a nested target clears the same matrix
    a top-level scalar retype does -- ``_cast_structural`` no longer lets
    ``pc.cast`` silently convert a struct/list child the matrix would gate.
    """

    def test_explicit_int_to_string_struct_leaf_fails_loud(self) -> None:
        col = pa.array([{"a": 1}], _STRUCT_INT)
        with pytest.raises(TransformationError, match="to_string"):
            _retype_nested(_OBJECT_STR, col)

    def test_explicit_int_to_string_list_leaf_fails_loud(self) -> None:
        col = pa.array([[1, 2]], pa.list_(pa.int64()))
        with pytest.raises(TransformationError, match="to_string"):
            _retype_nested({"arrow_type": "List", "items": {"arrow_type": "Utf8"}}, col)

    def test_forbidden_struct_leaf_fails_loud(self) -> None:
        col = pa.array([{"a": 1}], _STRUCT_INT)
        target = {"arrow_type": "Object", "properties": {"a": {"arrow_type": "Binary"}}}
        with pytest.raises(TransformationError, match="not a permitted conversion"):
            _retype_nested(target, col)

    def test_auto_widening_struct_leaf_builds(self) -> None:
        col = pa.array([{"a": 1}], pa.struct([("a", pa.int32())]))
        target = {"arrow_type": "Object", "properties": {"a": {"arrow_type": "Int64"}}}
        out = _retype_nested(target, col)
        assert out.column(0).to_pylist() == [{"a": 1}]

    def test_deeply_nested_explicit_leaf_names_its_path(self) -> None:
        # The gate walks to the leaf; the error names where the offending leaf is.
        inner = pa.struct([("zip", pa.int64())])
        col = pa.array([{"addr": {"zip": 90210}}], pa.struct([("addr", inner)]))
        target = {
            "arrow_type": "Object",
            "properties": {
                "addr": {
                    "arrow_type": "Object",
                    "properties": {"zip": {"arrow_type": "Utf8"}},
                }
            },
        }
        with pytest.raises(TransformationError, match=r"addr\.zip"):
            _retype_nested(target, col)

    def test_scalar_and_nested_leaf_gate_identically(self) -> None:
        # Parity: the same Int64 -> Utf8 intent fails loud whether it is a
        # top-level column or a struct child -- the divergence item 2 removes.
        scalar = _boundary_outcome(
            lambda: _retype_batch("Utf8", pa.array([1], pa.int64()))
            .column(0)
            .to_pylist()
        )
        nested = _boundary_outcome(
            lambda: _retype_nested(_OBJECT_STR, pa.array([{"a": 1}], _STRUCT_INT))
            .column(0)
            .to_pylist()
        )
        assert scalar == ("reject",)
        assert nested == ("reject",)
