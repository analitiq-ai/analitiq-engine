"""Conformance tests for the permitted-type-conversion matrix.

The matrix (:mod:`cdk.type_map.conversions`) is the single policy every build
boundary consults. These tests pin three things so it cannot silently rot:

1. the committed published artifact equals what the canonical table generates
   (drift guard);
2. every ``explicit`` conversion names a function that actually exists in the
   engine's mapping ``FUNCTION_CATALOG``;
3. each declared mode matches what the real builders do -- ``cast_arrow_batch``
   (destination) and ``run_arrow_transform`` (transform) accept/reject the same
   pair identically, which is the divergence the matrix exists to remove.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from cdk.schema_contract import SchemaContract
from cdk.type_map.arrow import arrow_family, classify_arrow_conversion, parse_arrow_type
from cdk.type_map.conversions import (
    ARROW_FAMILIES,
    build_conversion_matrix,
    classify_conversion,
    load_published_matrix,
    render_conversion_matrix,
)
from cdk.type_map.exceptions import InvalidTypeMapError
from src.engine.data_transformer import (
    AssignmentTransformer,
    build_output_schema,
    plan_arrow_transform,
    run_arrow_transform,
)

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
        catalog = AssignmentTransformer.FUNCTION_CATALOG
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
        ("source", "target", "mode", "fn"),
        [
            ("Int64", "Utf8", "explicit", "to_string"),
            ("Boolean", "Utf8", "explicit", "to_string"),
            ("Float64", "Utf8", "explicit", "to_string"),
            ("Timestamp", "Utf8", "explicit", "to_string"),
            # Parsing a string into a scalar stays implicit: API sources ship
            # every value as a JSON string and the engine has always parsed them.
            ("Utf8", "Int64", "auto", None),
            ("Utf8", "Float64", "auto", None),
            ("Int32", "Int64", "auto", None),
            ("Int64", "Int32", "auto", None),
            ("Object", "Int64", "forbidden", None),
            ("List", "Int64", "forbidden", None),
            ("Int64", "Null", "forbidden", None),
            ("Null", "Int64", "auto", None),
        ],
    )
    def test_named_cell(
        self, source: str, target: str, mode: str, fn: str | None
    ) -> None:
        conv = classify_conversion(source, target)
        assert conv.mode == mode
        assert conv.fn == fn


class TestArrowFamilyRoundTrip:
    """A built DataType maps back to the family it was parsed from."""

    @pytest.mark.parametrize(
        "canonical",
        [
            "Int8",
            "Int64",
            "UInt32",
            "Float32",
            "Float64",
            "Utf8",
            "LargeUtf8",
            "Boolean",
            "Binary",
            "Date32",
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

    def test_struct_and_list_families(self) -> None:
        assert arrow_family(pa.struct([("a", pa.int64())])) == "Object"
        assert arrow_family(pa.list_(pa.int64())) == "List"
        assert arrow_family(pa.large_list(pa.int64())) == "List"


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

    def test_runtime_checked_narrowing_overflow_is_rejected(self) -> None:
        with pytest.raises(ValueError):
            _cast("Int32", pa.array([2**40], pa.int64()))

    def test_same_family_unit_change_builds(self) -> None:
        col = pa.array([0, 1_000], pa.timestamp("s"))
        out = _cast("Timestamp(MICROSECOND)", col)
        assert pa.types.is_timestamp(out.column(0).type)


def _retype_batch(target_arrow_type: str, column: pa.Array) -> pa.RecordBatch:
    assignments = [
        {
            "target": {"path": ["c"], "arrow_type": target_arrow_type},
            "value": {"kind": "expr", "expr": {"op": "get", "path": ["src"]}},
        }
    ]
    plan = plan_arrow_transform(assignments)
    schema = build_output_schema(assignments)
    batch = pa.RecordBatch.from_arrays([column], names=["src"])
    return run_arrow_transform(batch, plan, schema)


class TestTransformBoundaryConformance:
    """run_arrow_transform agrees with the destination on the same pairs."""

    def test_explicit_int_to_string_is_rejected(self) -> None:
        from src.engine.exceptions import TransformationError

        with pytest.raises(TransformationError, match="to_string"):
            _retype_batch("Utf8", pa.array([1, 2], pa.int64()))

    def test_auto_widening_builds(self) -> None:
        out = _retype_batch("Int64", pa.array([1, 2], pa.int32()))
        assert out.column(0).type == pa.int64()


class TestBoundariesAgree:
    """The matrix exists so the two boundaries cannot disagree on a pair."""

    @pytest.mark.parametrize(
        ("source", "target"),
        [
            (pa.int64(), pa.string()),  # explicit: both reject
            (pa.struct([("a", pa.int64())]), pa.int64()),  # forbidden: both reject
        ],
    )
    def test_explicit_and_forbidden_reject_on_both_boundaries(
        self, source: pa.DataType, target: pa.DataType
    ) -> None:
        conv = classify_arrow_conversion(source, target)
        assert conv.mode in {"explicit", "forbidden"}
        # Destination rejects.
        with pytest.raises(ValueError):
            _cast(str_arrow_type(target), pa.array(_sample(source), source))
        # Transform rejects (same pair) -- only when the fast path applies
        # (scalar source). Nested sources never reach run_arrow_transform.
        if not (pa.types.is_struct(source) or pa.types.is_list(source)):
            from src.engine.exceptions import TransformationError

            with pytest.raises(TransformationError):
                _retype_batch(str_arrow_type(target), pa.array(_sample(source), source))


def str_arrow_type(dtype: pa.DataType) -> str:
    """Canonical arrow_type string for a scalar DataType used in these tests."""
    return {
        pa.string(): "Utf8",
        pa.int64(): "Int64",
    }[dtype]


def _sample(dtype: pa.DataType) -> list:
    if pa.types.is_integer(dtype):
        return [1, 2]
    if pa.types.is_struct(dtype):
        return [{"a": 1}]
    raise AssertionError(f"no sample for {dtype}")
