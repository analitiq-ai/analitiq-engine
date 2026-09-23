"""Unit tests for the connector-owned type-map and ssl-mode-map subsystem.

Covers every acceptance bullet from GH #28:
- exact-match rules
- regex rules with named-capture substitution
- specificity ordering (first-match-wins)
- whitespace / case normalization
- hard error on unmapped native types
- SSL mode lookup + canonical-value validation
- type-map document parsing
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import pytest

from cdk.conformance.fakes import type_map_document
from cdk.type_map import (
    InvalidTypeMapError,
    TypeMapper,
    UnmappedTypeError,
    normalize_arrow_type,
    normalize_native_type,
    parse_arrow_type,
)
from cdk.type_map.loader import parse_type_mapper, read_raw_type_map
from cdk.type_map.rules import parse_rules, parse_write_rules

# ---------------------------------------------------------------------------
# normalize_native_type
# ---------------------------------------------------------------------------


class TestNormalizeNativeType:
    def test_strips_outer_whitespace(self):
        assert normalize_native_type("  BIGINT  ") == "BIGINT"

    def test_collapses_internal_runs(self):
        assert normalize_native_type("VARCHAR  ( 50 )") == "VARCHAR ( 50 )"
        assert (
            normalize_native_type("TIMESTAMP\tWITH\nTIME  ZONE")
            == "TIMESTAMP WITH TIME ZONE"
        )

    def test_uppercases(self):
        assert normalize_native_type("bigint") == "BIGINT"
        assert normalize_native_type("Varchar(50)") == "VARCHAR(50)"

    def test_rejects_non_string(self):
        # A lookup input is whatever a driver reported as a column's declared
        # type, so it is not guaranteed to be a string. It must fail as a typed
        # error at the mapper boundary, naming what arrived -- not as a bare
        # AttributeError from inside a str operation, which would escape the
        # UnmappedTypeError handling that names the schema, table and column.
        with pytest.raises(TypeError, match="native type must be a string"):
            normalize_native_type(None)  # type: ignore[arg-type]

    def test_guards_symmetrically_with_the_arrow_side(self):
        # The two halves of one concept must not diverge; that divergence is
        # what this whole change set exists to remove.
        with pytest.raises(TypeError, match="arrow type must be a string"):
            normalize_arrow_type(None)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# TypeMapper — forward lookup
# ---------------------------------------------------------------------------


def _mapper(rules: list[dict]) -> TypeMapper:
    return TypeMapper("test", parse_rules(rules, source="<test>"))


class TestTypeMapperExact:
    def test_exact_hit(self):
        m = _mapper([{"match": "exact", "native_type": "JSONB", "arrow_type": "Utf8"}])
        assert m.to_arrow_type("JSONB") == "Utf8"

    def test_normalization_lower_to_upper(self):
        m = _mapper(
            [{"match": "exact", "native_type": "BIGINT", "arrow_type": "Int64"}]
        )
        assert m.to_arrow_type("bigint") == "Int64"

    def test_normalization_internal_whitespace(self):
        m = _mapper(
            [
                {
                    "match": "exact",
                    "native_type": "DOUBLE PRECISION",
                    "arrow_type": "Float64",
                }
            ]
        )
        assert m.to_arrow_type("double   precision") == "Float64"

    def test_unmapped_raises(self):
        m = _mapper([{"match": "exact", "native_type": "TEXT", "arrow_type": "Utf8"}])
        with pytest.raises(UnmappedTypeError) as exc:
            m.to_arrow_type("MONEY")
        assert exc.value.direction == "forward"
        assert exc.value.value == "MONEY"
        assert "MONEY" in str(exc.value)


class TestTypeMapperRegex:
    def test_regex_with_named_capture(self):
        m = _mapper(
            [
                {
                    "match": "regex",
                    "native_type": (
                        r"^NUMERIC\(\s*(?<p>[1-9]|[12][0-9]|3[0-8])\s*,"
                        r"\s*(?<s>[0-9]|[12][0-9]|3[0-8])\s*\)$"
                    ),
                    "arrow_type": "Decimal128(${p}, ${s})",
                }
            ]
        )
        assert m.to_arrow_type("NUMERIC(18, 2)") == "Decimal128(18, 2)"
        assert m.to_arrow_type("numeric( 10,4 )") == "Decimal128(10, 4)"

    def test_regex_without_tokens(self):
        m = _mapper(
            [
                {
                    "match": "regex",
                    "native_type": r"^VARCHAR\(\s*\d+\s*\)$",
                    "arrow_type": "Utf8",
                }
            ]
        )
        assert m.to_arrow_type("VARCHAR(50)") == "Utf8"
        assert m.to_arrow_type("varchar(1024)") == "Utf8"

    def test_absent_optional_capture_raises_instead_of_rendering_none(self):
        # The read side has no per-column hint to fall back on (unlike the
        # write side's params=), so a non-participating optional capture
        # referenced by a token must raise, not silently render "None" or
        # crash inside re.sub().
        m = _mapper(
            [
                {
                    "match": "regex",
                    "native_type": r"^NUM(?:\((?<p>[1-9]|[12][0-9]|3[0-8])\))?$",
                    "arrow_type": "Decimal128(${p}, 0)",
                }
            ]
        )
        # Group present -> capture wins.
        assert m.to_arrow_type("NUM(10)") == "Decimal128(10, 0)"
        # Group absent -> raise, not "Decimal128(, 0)" or a crash.
        with pytest.raises(InvalidTypeMapError, match="render hint"):
            m.to_arrow_type("NUM")


class TestTypeMapperReDoSBound:
    """#504: a nested-quantifier native_type pattern must not go super-linear.

    ``^(A+)+B$`` is a pattern RE2 accepts, and under
    Python's backtracking ``re`` it runs in time exponential in input
    length on an input with no trailing ``B`` (nothing to anchor the
    backtrack search). The runtime lookup (:meth:`TypeMapper.to_arrow_type`)
    must bound this, not just the load-time compile.
    """

    def test_nested_quantifier_bounded_at_runtime_lookup(self):
        m = _mapper(
            [{"match": "regex", "native_type": r"^(A+)+B$", "arrow_type": "Utf8"}]
        )
        # Measured directly against stdlib `re.fullmatch` on this exact
        # pattern: 30 chars takes ~33s (roughly doubling per character in
        # this range) -- long enough to fail this assertion unmistakably on
        # a regression, short enough that a regression still fails the test
        # run rather than hanging the CI job for hours.
        adversarial = "A" * 30
        start = time.perf_counter()
        with pytest.raises(UnmappedTypeError):
            m.to_arrow_type(adversarial)
        elapsed = time.perf_counter() - start
        assert elapsed < 1.0, (
            f"nested-quantifier match took {elapsed:.3f}s for 30 chars; "
            f"expected linear-time (RE2), not exponential (backtracking re)"
        )

    def test_nested_quantifier_still_matches_admissible_input(self):
        # The bound above must not come from refusing to match at all.
        m = _mapper(
            [{"match": "regex", "native_type": r"^(A+)+B$", "arrow_type": "Utf8"}]
        )
        assert m.to_arrow_type("A" * 50 + "B") == "Utf8"


class TestTypeMapperLoneSurrogateInLookupInput:
    """#504: `to_arrow_type`/`to_native_type`'s input is runtime data (whatever
    a driver or API schema reported), not a pydantic-validated document field
    like the rule's own pattern -- pydantic rejects a lone surrogate in an
    authored `native_type`/`arrow_type`, but nothing validates the lookup
    input the same way. re2 encodes the match subject to UTF-8 internally, so
    a lone surrogate there raises UnicodeEncodeError instead of matching or
    not matching; that must surface as the ordinary miss (UnmappedTypeError),
    not escape as a raw UnicodeEncodeError.
    """

    def test_lone_surrogate_in_to_arrow_type_input_is_a_miss(self):
        m = _mapper(
            [
                {
                    "match": "regex",
                    "native_type": r"^VARCHAR\(\d+\)$",
                    "arrow_type": "Utf8",
                }
            ]
        )
        with pytest.raises(UnmappedTypeError):
            m.to_arrow_type("VARCHAR(\ud800)")

    def test_lone_surrogate_in_to_native_type_input_is_a_miss(self):
        m = _write_mapper(
            [{"match": "regex", "arrow_type": r"^Utf8\(\d+\)$", "native_type": "X"}]
        )
        with pytest.raises(UnmappedTypeError):
            m.to_native_type("Utf8(\ud800)")


class TestSpecificityOrdering:
    """First-match-wins: narrower exact rules must sit above broader regexes."""

    def test_boolean_exact_beats_int8_regex(self):
        m = _mapper(
            [
                {
                    "match": "exact",
                    "native_type": "TINYINT(1)",
                    "arrow_type": "Boolean",
                },
                {
                    "match": "regex",
                    "native_type": r"^TINYINT(\(\d+\))?$",
                    "arrow_type": "Int8",
                },
            ]
        )
        assert m.to_arrow_type("TINYINT(1)") == "Boolean"
        assert m.to_arrow_type("TINYINT(4)") == "Int8"
        assert m.to_arrow_type("TINYINT") == "Int8"

    def test_reordering_changes_result(self):
        m = _mapper(
            [
                {
                    "match": "regex",
                    "native_type": r"^TINYINT(\(\d+\))?$",
                    "arrow_type": "Int8",
                },
                {
                    "match": "exact",
                    "native_type": "TINYINT(1)",
                    "arrow_type": "Boolean",
                },
            ]
        )
        # Broader rule now wins — the exact rule is shadowed.
        assert m.to_arrow_type("TINYINT(1)") == "Int8"


# ---------------------------------------------------------------------------
# parse_arrow_type parser
# ---------------------------------------------------------------------------


class TestParseArrowType:
    def test_primitives(self):
        import pyarrow as pa

        assert parse_arrow_type("Int64") == pa.int64()
        assert parse_arrow_type("Boolean") == pa.bool_()
        assert parse_arrow_type("Utf8") == pa.string()
        assert parse_arrow_type("Date32") == pa.date32()

    @pytest.mark.parametrize(
        "canonical, expected",
        [
            ("Null", "null"),
            ("UInt8", "uint8"),
            ("UInt16", "uint16"),
            ("UInt32", "uint32"),
            ("UInt64", "uint64"),
            ("Float16", "halffloat"),
            ("Float32", "float"),
            ("Float64", "double"),
            ("LargeUtf8", "large_string"),
            ("Binary", "binary"),
            ("LargeBinary", "large_binary"),
            ("Date64", "date64[ms]"),
        ],
    )
    def test_primitive_parser_coverage(self, canonical, expected):
        """Exercise every primitive family parse_arrow_type dispatches on.

        These cases previously went untested at the parser level — they
        were only indirectly hit via sql_types' Arrow → SQLAlchemy map.
        """
        assert str(parse_arrow_type(canonical)) == expected

    def test_timestamp_with_tz(self):
        import pyarrow as pa

        assert parse_arrow_type("Timestamp(us, UTC)") == pa.timestamp("us", tz="UTC")
        assert parse_arrow_type("Timestamp(ms)") == pa.timestamp("ms")

    def test_decimal(self):
        import pyarrow as pa

        assert parse_arrow_type("Decimal128(18, 2)") == pa.decimal128(18, 2)

    def test_time32(self):
        import pyarrow as pa

        assert parse_arrow_type("Time32(s)") == pa.time32("s")
        assert parse_arrow_type("Time32(ms)") == pa.time32("ms")

    def test_time64(self):
        import pyarrow as pa

        assert parse_arrow_type("Time64(us)") == pa.time64("us")
        assert parse_arrow_type("Time64(ns)") == pa.time64("ns")

    def test_time_rejects_wrong_unit(self):
        with pytest.raises(InvalidTypeMapError, match="requires exactly one unit"):
            parse_arrow_type("Time32(us)")  # us is Time64-only
        with pytest.raises(InvalidTypeMapError, match="requires exactly one unit"):
            parse_arrow_type("Time64(s)")  # s is Time32-only

    def test_fixed_size_binary(self):
        import pyarrow as pa

        assert parse_arrow_type("FixedSizeBinary(16)") == pa.binary(16)

    def test_fixed_size_binary_rejects_non_integer(self):
        with pytest.raises(InvalidTypeMapError, match="byte_width is not an integer"):
            parse_arrow_type("FixedSizeBinary(abc)")

    def test_decimal256(self):
        import pyarrow as pa

        assert parse_arrow_type("Decimal256(38, 10)") == pa.decimal256(38, 10)

    def test_decimal_rejects_non_integer_params(self):
        with pytest.raises(InvalidTypeMapError, match="precision is not an integer"):
            parse_arrow_type("Decimal128(a, b)")

    def test_timestamp_requires_unit(self):
        with pytest.raises(InvalidTypeMapError, match="at least a unit"):
            parse_arrow_type("Timestamp()")

    def test_timestamp_rejects_bad_unit(self):
        with pytest.raises(InvalidTypeMapError, match="unit must be one of"):
            parse_arrow_type("Timestamp(xs)")

    def test_unknown_family_rejected(self):
        with pytest.raises(InvalidTypeMapError, match="not supported"):
            parse_arrow_type("Nope")

    def test_unbalanced_parens_rejected(self):
        with pytest.raises(InvalidTypeMapError, match="unbalanced"):
            parse_arrow_type("Int64(")

    def test_object_marker_rejected_at_string_parser(self):
        # parse_arrow_type only sees the string; Object needs the property's
        # sub-schema, which only resolve_arrow_type / SchemaContract have.
        with pytest.raises(InvalidTypeMapError, match="nested type"):
            parse_arrow_type("Object")

    def test_list_marker_rejected_at_string_parser(self):
        with pytest.raises(InvalidTypeMapError, match="nested type"):
            parse_arrow_type("List")

    def test_json_marker_resolves_to_large_string(self):
        import pyarrow as pa

        # Opaque-blob marker: shape unknown, wire type is a JSON-encoded
        # string. Handlers undo the encoding at write time.
        assert parse_arrow_type("Json") == pa.large_string()


# ---------------------------------------------------------------------------
# resolve_arrow_type — JSON-Schema-shaped walker
# ---------------------------------------------------------------------------


class TestResolveArrowType:
    def test_scalar_forwards_to_parse(self):
        import pyarrow as pa

        from cdk.type_map import resolve_arrow_type

        assert resolve_arrow_type({"arrow_type": "Int64"}) == pa.int64()

    def test_object_builds_struct(self):
        import pyarrow as pa

        from cdk.type_map import resolve_arrow_type

        dt = resolve_arrow_type(
            {
                "arrow_type": "Object",
                "properties": {
                    "id": {"arrow_type": "Utf8"},
                    "objectName": {"arrow_type": "Utf8"},
                },
            }
        )
        assert pa.types.is_struct(dt)
        assert [f.name for f in dt] == ["id", "objectName"]

    def test_object_respects_required(self):
        from cdk.type_map import resolve_arrow_type

        dt = resolve_arrow_type(
            {
                "arrow_type": "Object",
                "required": ["id"],
                "properties": {
                    "id": {"arrow_type": "Utf8"},
                    "objectName": {"arrow_type": "Utf8"},
                },
            }
        )
        names = {f.name: f.nullable for f in dt}
        assert names == {"id": False, "objectName": True}

    def test_list_of_scalars(self):
        import pyarrow as pa

        from cdk.type_map import resolve_arrow_type

        dt = resolve_arrow_type(
            {"arrow_type": "List", "items": {"arrow_type": "Int32"}}
        )
        assert pa.types.is_list(dt)
        assert pa.types.is_int32(dt.value_type)

    def test_nested_list_of_objects(self):
        import pyarrow as pa

        from cdk.type_map import resolve_arrow_type

        dt = resolve_arrow_type(
            {
                "arrow_type": "List",
                "items": {
                    "arrow_type": "Object",
                    "properties": {
                        "sku": {"arrow_type": "Utf8"},
                        "qty": {"arrow_type": "Int32"},
                    },
                },
            }
        )
        assert pa.types.is_list(dt)
        assert pa.types.is_struct(dt.value_type)

    def test_missing_arrow_type_raises(self):
        from cdk.type_map import resolve_arrow_type

        with pytest.raises(InvalidTypeMapError, match="missing 'arrow_type'"):
            resolve_arrow_type({})

    def test_object_missing_properties_raises(self):
        from cdk.type_map import resolve_arrow_type

        with pytest.raises(InvalidTypeMapError, match="non-empty 'properties'"):
            resolve_arrow_type({"arrow_type": "Object"})

    def test_list_missing_items_raises(self):
        from cdk.type_map import resolve_arrow_type

        with pytest.raises(InvalidTypeMapError, match="'items' object"):
            resolve_arrow_type({"arrow_type": "List"})


# ---------------------------------------------------------------------------
# Loader — filesystem
# ---------------------------------------------------------------------------


class TestParsingATypeMapDocument:
    """One ``type-map.json`` carries the read and write maps, each optional."""

    _READ = [{"match": "exact", "native_type": "TEXT", "arrow_type": "Utf8"}]
    _WRITE = [{"match": "exact", "arrow_type": "Int64", "native_type": "BIGINT"}]

    def _parse(self, **directions: Any) -> TypeMapper:
        return parse_type_mapper(
            "demo", type_map_document(**directions), source="type-map.json"
        )

    def test_both_sections_parse_both_maps(self):
        mapper = self._parse(read=self._READ, write=self._WRITE)
        assert mapper.connector_slug == "demo"
        assert mapper.to_arrow_type("text") == "Utf8"
        assert mapper.to_native_type("Int64") == "BIGINT"

    def test_write_only_document_parses_the_write_map(self):
        mapper = self._parse(write=self._WRITE)
        assert mapper.has_read_map is False
        assert mapper.to_native_type("Int64") == "BIGINT"
        with pytest.raises(InvalidTypeMapError, match="no read type map"):
            mapper.to_arrow_type("text")

    def test_read_only_document_has_no_write_map(self):
        mapper = self._parse(read=self._READ)
        assert mapper.has_read_map is True
        assert mapper.has_write_map is False

    def test_document_the_contract_model_rejects_is_invalid(self):
        with pytest.raises(InvalidTypeMapError, match="type-map.json"):
            self._parse()


class TestReadingARawTypeMap:
    """The worker-bootstrap read of a definition directory's ``type-map.json``."""

    def test_absent_reads_as_none(self, tmp_path: Path):
        assert read_raw_type_map(tmp_path, "demo") is None

    def test_the_pre_merge_file_names_are_not_read(self, tmp_path: Path):
        (tmp_path / "type-map-read.json").write_text(
            json.dumps(type_map_document(read=[]))
        )
        assert read_raw_type_map(tmp_path, "demo") is None

    def test_malformed_json_raises(self, tmp_path: Path):
        (tmp_path / "type-map.json").write_text("not json")
        with pytest.raises(InvalidTypeMapError, match="not valid JSON"):
            read_raw_type_map(tmp_path, "demo")


# ---------------------------------------------------------------------------
# normalize_arrow_type (write direction, case-preserving)
# ---------------------------------------------------------------------------


class TestNormalizeArrowType:
    def test_preserves_case(self):
        assert normalize_arrow_type("Int64") == "Int64"
        assert normalize_arrow_type("Timestamp(MICROSECOND, UTC)") == (
            "Timestamp(MICROSECOND, UTC)"
        )

    def test_strips_outer_and_collapses_internal_whitespace(self):
        assert normalize_arrow_type("  Decimal128(38,  9) ") == "Decimal128(38, 9)"

    def test_canonicalizes_comma_spacing(self):
        # Spacing-only variants accepted by parse_arrow_type must normalize to
        # the same form so an exact rule matches either spelling.
        assert normalize_arrow_type("Decimal128(38,9)") == "Decimal128(38, 9)"
        assert normalize_arrow_type("Timestamp(MICROSECOND,UTC)") == (
            "Timestamp(MICROSECOND, UTC)"
        )
        assert normalize_arrow_type("Decimal128(38 , 9)") == "Decimal128(38, 9)"

    def test_canonicalizes_paren_adjacent_whitespace(self):
        # parse_arrow_type strips each parsed arg, so these are all valid; they
        # must all normalize to the single canonical spelling.
        assert normalize_arrow_type("Decimal128( 38, 9 )") == "Decimal128(38, 9)"
        assert normalize_arrow_type("Time64( MICROSECOND )") == "Time64(MICROSECOND)"
        assert normalize_arrow_type("Decimal128( 38 , 9 )") == "Decimal128(38, 9)"

    def test_rejects_non_string(self):
        with pytest.raises(TypeError):
            normalize_arrow_type(None)  # type: ignore[arg-type]

    # --- unit alias normalization (issue #125) --------------------------------

    @pytest.mark.parametrize(
        "short, long",
        [
            ("Timestamp(us, UTC)", "Timestamp(MICROSECOND, UTC)"),
            ("Timestamp(ms)", "Timestamp(MILLISECOND)"),
            ("Timestamp(s)", "Timestamp(SECOND)"),
            ("Timestamp(ns)", "Timestamp(NANOSECOND)"),
            ("Time32(s)", "Time32(SECOND)"),
            ("Time32(ms)", "Time32(MILLISECOND)"),
            ("Time64(us)", "Time64(MICROSECOND)"),
            ("Time64(ns)", "Time64(NANOSECOND)"),
            ("Duration(s)", "Duration(SECOND)"),
            ("Duration(ms)", "Duration(MILLISECOND)"),
            ("Duration(us)", "Duration(MICROSECOND)"),
            ("Duration(ns)", "Duration(NANOSECOND)"),
        ],
    )
    def test_short_unit_codes_expand_to_long_form(self, short, long):
        # parse_arrow_type accepts both spellings; normalize_arrow_type must
        # map the short code to the same long-form canonical so an exact write
        # rule authored with the long name matches a lookup using the short name.
        assert normalize_arrow_type(short) == long

    @pytest.mark.parametrize(
        "long_form",
        [
            "Timestamp(MICROSECOND, UTC)",
            "Timestamp(MILLISECOND)",
            "Time32(SECOND)",
            "Time64(NANOSECOND)",
            "Duration(MICROSECOND)",
        ],
    )
    def test_long_form_units_are_idempotent(self, long_form):
        # Long-form spellings must survive normalization unchanged.
        assert normalize_arrow_type(long_form) == long_form

    def test_short_unit_with_paren_whitespace(self):
        # Whitespace normalization and alias expansion compose correctly.
        assert normalize_arrow_type("Time64( us )") == "Time64(MICROSECOND)"
        assert normalize_arrow_type("Timestamp( us , UTC )") == (
            "Timestamp(MICROSECOND, UTC)"
        )

    # --- null timezone normalization (issue #125) -----------------------------

    def test_timestamp_null_tz_folded_to_no_tz(self):
        # Timestamp(unit, null) is timezone-naïve; parse_arrow_type treats it
        # the same as Timestamp(unit).  normalize_arrow_type must fold both
        # into the same string so a write rule for Timestamp(MICROSECOND) also
        # matches Timestamp(MICROSECOND, null).
        assert normalize_arrow_type("Timestamp(MICROSECOND, null)") == (
            "Timestamp(MICROSECOND)"
        )
        assert normalize_arrow_type("Timestamp(us, null)") == "Timestamp(MICROSECOND)"
        assert normalize_arrow_type("Timestamp(ns, null)") == "Timestamp(NANOSECOND)"
        assert normalize_arrow_type("Timestamp(ms, null)") == "Timestamp(MILLISECOND)"

    def test_three_way_composition(self):
        # All three normalization steps must compose: whitespace strip (step 1),
        # short-code expansion (step 2), and null-tz fold (step 3).
        assert normalize_arrow_type("Timestamp( ns , null )") == "Timestamp(NANOSECOND)"
        assert (
            normalize_arrow_type("Timestamp( us , null )") == "Timestamp(MICROSECOND)"
        )

    def test_non_null_tz_is_preserved(self):
        assert normalize_arrow_type("Timestamp(MICROSECOND, UTC)") == (
            "Timestamp(MICROSECOND, UTC)"
        )
        assert normalize_arrow_type("Timestamp(MICROSECOND, America/New_York)") == (
            "Timestamp(MICROSECOND, America/New_York)"
        )

    # --- cross-type unit validation (issue #174) ------------------------------

    @pytest.mark.parametrize(
        "bad_type",
        [
            "Time32(us)",  # short code, Time64-only unit
            "Time32(ns)",  # short code, Time64-only unit
            "Time32(MICROSECOND)",  # long form, Time64-only unit
            "Time32(NANOSECOND)",  # long form, Time64-only unit
        ],
    )
    def test_time32_rejects_time64_units(self, bad_type):
        with pytest.raises(InvalidTypeMapError, match="Time32 accepts"):
            normalize_arrow_type(bad_type)

    @pytest.mark.parametrize(
        "bad_type",
        [
            "Time64(s)",  # short code, Time32-only unit
            "Time64(ms)",  # short code, Time32-only unit
            "Time64(SECOND)",  # long form, Time32-only unit
            "Time64(MILLISECOND)",  # long form, Time32-only unit
        ],
    )
    def test_time64_rejects_time32_units(self, bad_type):
        with pytest.raises(InvalidTypeMapError, match="Time64 accepts"):
            normalize_arrow_type(bad_type)

    @pytest.mark.parametrize(
        "valid_type",
        [
            "Time32(s)",
            "Time32(ms)",
            "Time32(SECOND)",
            "Time32(MILLISECOND)",
            "Time64(us)",
            "Time64(ns)",
            "Time64(MICROSECOND)",
            "Time64(NANOSECOND)",
        ],
    )
    def test_time32_and_time64_accept_valid_units(self, valid_type):
        # Must not raise — valid combinations pass through.
        normalize_arrow_type(valid_type)

    def test_timestamp_accepts_all_units(self):
        # Timestamp is unconstrained; all four units must normalize without error.
        for unit in (
            "s",
            "ms",
            "us",
            "ns",
            "SECOND",
            "MILLISECOND",
            "MICROSECOND",
            "NANOSECOND",
        ):
            normalize_arrow_type(f"Timestamp({unit})")

    def test_duration_accepts_all_units(self):
        # Duration is unconstrained; all four units must normalize without error.
        for unit in (
            "s",
            "ms",
            "us",
            "ns",
            "SECOND",
            "MILLISECOND",
            "MICROSECOND",
            "NANOSECOND",
        ):
            normalize_arrow_type(f"Duration({unit})")


# ---------------------------------------------------------------------------
# TypeMapper.compose — per-type fallback (issue #126)
# ---------------------------------------------------------------------------


def _read_mapper(slug: str, native: str, canonical: str) -> TypeMapper:
    return TypeMapper(
        slug,
        parse_rules(
            [{"match": "exact", "native_type": native, "arrow_type": canonical}],
            source="<r>",
        ),
    )


def _full_mapper(slug: str, native: str, canonical: str) -> TypeMapper:
    return TypeMapper(
        slug,
        parse_rules(
            [{"match": "exact", "native_type": native, "arrow_type": canonical}],
            source="<r>",
        ),
        parse_write_rules(
            [{"match": "exact", "arrow_type": canonical, "native_type": native}],
            source="<w>",
        ),
    )


class TestTypeMapperCompose:
    def test_primary_read_rule_wins(self):
        primary = _read_mapper("conn", "CUSTOM", "Utf8")
        fallback = _read_mapper("pg", "CUSTOM", "Int64")
        composed = TypeMapper.compose(primary, fallback)
        assert composed.to_arrow_type("CUSTOM") == "Utf8"

    def test_fallback_read_rule_used_on_miss(self):
        primary = _read_mapper("conn", "CUSTOM", "Utf8")
        fallback = _read_mapper("pg", "BIGINT", "Int64")
        composed = TypeMapper.compose(primary, fallback)
        assert composed.to_arrow_type("BIGINT") == "Int64"

    def test_both_miss_raises_unmapped(self):
        primary = _read_mapper("conn", "CUSTOM", "Utf8")
        fallback = _read_mapper("pg", "BIGINT", "Int64")
        composed = TypeMapper.compose(primary, fallback)
        with pytest.raises(UnmappedTypeError):
            composed.to_arrow_type("MONEY")

    def test_primary_write_rule_wins(self):
        primary = _full_mapper("conn", "CUSTOM", "Utf8")
        fallback = _full_mapper("pg", "CUSTOM", "Int64")
        composed = TypeMapper.compose(primary, fallback)
        assert composed.to_native_type("Utf8") == "CUSTOM"

    def test_fallback_write_rule_used_on_miss(self):
        primary = _full_mapper("conn", "CUSTOM", "Utf8")
        fallback = _full_mapper("pg", "BIGINT", "Int64")
        composed = TypeMapper.compose(primary, fallback)
        assert composed.to_native_type("Int64") == "BIGINT"

    def test_primary_read_only_inherits_fallback_write_map(self):
        # Primary has read rules only; fallback has both. Composed mapper
        # must expose write rules from the fallback.
        primary = _read_mapper("conn", "CUSTOM", "Utf8")
        fallback = _full_mapper("pg", "BIGINT", "Int64")
        composed = TypeMapper.compose(primary, fallback)
        assert composed.has_write_map is True
        assert composed.to_native_type("Int64") == "BIGINT"

    def test_neither_has_write_map_gives_no_write_map(self):
        primary = _read_mapper("conn", "CUSTOM", "Utf8")
        fallback = _read_mapper("pg", "BIGINT", "Int64")
        composed = TypeMapper.compose(primary, fallback)
        assert composed.has_write_map is False

    def test_composed_slug_is_primary_slug(self):
        primary = _read_mapper("conn:my-pg", "X", "Utf8")
        fallback = _read_mapper("pg", "Y", "Int64")
        composed = TypeMapper.compose(primary, fallback)
        assert composed.connector_slug == "conn:my-pg"

    def test_compose_preserves_regex_rules(self):
        primary = TypeMapper(
            "conn",
            parse_rules(
                [
                    {
                        "match": "regex",
                        "native_type": r"^CUSTOM_(?<n>\d+)$",
                        "arrow_type": "Utf8",
                    },
                ],
                source="<r>",
            ),
        )
        fallback = TypeMapper(
            "pg",
            parse_rules(
                [
                    {
                        "match": "regex",
                        "native_type": r"^VARCHAR\((?<n>\d+)\)$",
                        "arrow_type": "Utf8",
                    },
                ],
                source="<r>",
            ),
        )
        composed = TypeMapper.compose(primary, fallback)
        assert composed.to_arrow_type("CUSTOM_42") == "Utf8"
        assert composed.to_arrow_type("VARCHAR(100)") == "Utf8"


# ---------------------------------------------------------------------------
# TypeMapper — reverse lookup (to_native_type)
# ---------------------------------------------------------------------------


def _write_mapper(write_rules: list[dict]) -> TypeMapper:
    """A mapper with a throwaway read rule plus the given write rules."""
    return TypeMapper(
        "test",
        parse_rules(
            [{"match": "exact", "native_type": "X", "arrow_type": "Utf8"}], source="<r>"
        ),
        parse_write_rules(write_rules, source="<w>"),
    )


class TestToNativeTypeExact:
    def test_exact_hit(self):
        m = _write_mapper(
            [{"match": "exact", "arrow_type": "Int64", "native_type": "BIGINT"}]
        )
        assert m.to_native_type("Int64") == "BIGINT"

    def test_match_is_case_sensitive(self):
        # Arrow vocabulary is mixed-case; "int64" must NOT match "Int64".
        m = _write_mapper(
            [{"match": "exact", "arrow_type": "Int64", "native_type": "BIGINT"}]
        )
        with pytest.raises(UnmappedTypeError):
            m.to_native_type("int64")

    def test_whitespace_tolerated(self):
        m = _write_mapper(
            [{"match": "exact", "arrow_type": "Int64", "native_type": "BIGINT"}]
        )
        assert m.to_native_type("  Int64  ") == "BIGINT"

    def test_exact_rule_for_parameterized_type_matches_spacing_variants(self):
        # An exact rule authored with comma-space still matches the no-space
        # and paren-padded spellings, since all normalize identically.
        m = _write_mapper(
            [
                {
                    "match": "exact",
                    "arrow_type": "Decimal128(38, 9)",
                    "native_type": "NUMERIC(38, 9)",
                }
            ]
        )
        assert m.to_native_type("Decimal128(38, 9)") == "NUMERIC(38, 9)"
        assert m.to_native_type("Decimal128(38,9)") == "NUMERIC(38, 9)"
        assert m.to_native_type("Decimal128( 38, 9 )") == "NUMERIC(38, 9)"

    def test_unmapped_raises_reverse(self):
        m = _write_mapper(
            [{"match": "exact", "arrow_type": "Int64", "native_type": "BIGINT"}]
        )
        with pytest.raises(UnmappedTypeError) as exc:
            m.to_native_type("Float64")
        assert exc.value.direction == "reverse"
        assert exc.value.value == "Float64"
        assert "Float64" in str(exc.value)

    def test_no_write_map_raises(self):
        m = TypeMapper(
            "test",
            parse_rules(
                [{"match": "exact", "native_type": "X", "arrow_type": "Utf8"}],
                source="<r>",
            ),
        )
        assert m.has_write_map is False
        with pytest.raises(InvalidTypeMapError, match="no write type map loaded"):
            m.to_native_type("Int64")

    def test_empty_write_rules_is_no_write_map(self):
        # An explicit empty list is treated as "no write map", not an empty
        # ruleset that would mis-raise UnmappedTypeError.
        m = TypeMapper(
            "test",
            parse_rules(
                [{"match": "exact", "native_type": "X", "arrow_type": "Utf8"}],
                source="<r>",
            ),
            write_rules=[],
        )
        assert m.has_write_map is False
        with pytest.raises(InvalidTypeMapError, match="no write type map loaded"):
            m.to_native_type("Int64")


class TestToNativeTypeUnitAliasNormalization:
    """Write-direction lookup tolerates short unit codes and null tz (issue #125)."""

    def test_exact_rule_long_form_matches_short_code_lookup(self):
        # A connector write map authored with MICROSECOND must match a lookup
        # that arrives with the short code us (e.g. from a hand-built ColumnDef).
        m = _write_mapper(
            [
                {
                    "match": "exact",
                    "arrow_type": "Timestamp(MICROSECOND, UTC)",
                    "native_type": "TIMESTAMPTZ",
                },
            ]
        )
        assert m.to_native_type("Timestamp(us, UTC)") == "TIMESTAMPTZ"
        assert m.to_native_type("Timestamp(MICROSECOND, UTC)") == "TIMESTAMPTZ"

    def test_time64_rule_matches_both_unit_spellings(self):
        # The contract admits only the long-form spelling in a rule, so both
        # lookup spellings must resolve against the long-form rule.
        m = _write_mapper(
            [
                {
                    "match": "exact",
                    "arrow_type": "Time64(MICROSECOND)",
                    "native_type": "TIME",
                },
            ]
        )
        assert m.to_native_type("Time64(MICROSECOND)") == "TIME"
        assert m.to_native_type("Time64(us)") == "TIME"

    def test_null_tz_matches_no_tz_rule(self):
        # Timestamp(unit, null) must match a rule authored for Timestamp(unit).
        m = _write_mapper(
            [
                {
                    "match": "exact",
                    "arrow_type": "Timestamp(MICROSECOND)",
                    "native_type": "TIMESTAMP",
                },
            ]
        )
        assert m.to_native_type("Timestamp(MICROSECOND, null)") == "TIMESTAMP"
        assert m.to_native_type("Timestamp(us, null)") == "TIMESTAMP"

    def test_all_short_codes_normalized(self):
        m = _write_mapper(
            [
                {
                    "match": "exact",
                    "arrow_type": "Time32(SECOND)",
                    "native_type": "T32S",
                },
                {
                    "match": "exact",
                    "arrow_type": "Time32(MILLISECOND)",
                    "native_type": "T32MS",
                },
                {
                    "match": "exact",
                    "arrow_type": "Duration(SECOND)",
                    "native_type": "DUR_S",
                },
                {
                    "match": "exact",
                    "arrow_type": "Duration(MILLISECOND)",
                    "native_type": "DUR_MS",
                },
                {
                    "match": "exact",
                    "arrow_type": "Duration(NANOSECOND)",
                    "native_type": "DUR_NS",
                },
            ]
        )
        assert m.to_native_type("Time32(s)") == "T32S"
        assert m.to_native_type("Time32(ms)") == "T32MS"
        assert m.to_native_type("Duration(s)") == "DUR_S"
        assert m.to_native_type("Duration(ms)") == "DUR_MS"
        assert m.to_native_type("Duration(ns)") == "DUR_NS"

    def test_to_native_type_rejects_cross_type_time_unit(self):
        # Bad canonical supplied at lookup time must raise InvalidTypeMapError,
        # not silently fall through to UnmappedTypeError.
        m = _write_mapper(
            [{"match": "exact", "arrow_type": "Time32(SECOND)", "native_type": "T32S"}]
        )
        with pytest.raises(InvalidTypeMapError, match="Time32 accepts"):
            m.to_native_type("Time32(MICROSECOND)")


class TestToNativeTypeRegex:
    def test_decimal_named_captures(self):
        m = _write_mapper(
            [
                {
                    "match": "regex",
                    "arrow_type": r"^Decimal128\((?<p>\d+),\s*(?<s>\d+)\)$",
                    "native_type": "NUMERIC(${p}, ${s})",
                }
            ]
        )
        assert m.to_native_type("Decimal128(18, 2)") == "NUMERIC(18, 2)"
        assert m.to_native_type("Decimal128(38,9)") == "NUMERIC(38, 9)"

    def test_param_hint_substitution(self):
        m = _write_mapper(
            [
                {
                    "match": "exact",
                    "arrow_type": "Utf8",
                    "native_type": "VARCHAR(${length})",
                }
            ]
        )
        assert m.to_native_type("Utf8", params={"length": "255"}) == "VARCHAR(255)"

    def test_numeric_param_hint_is_stringified(self):
        # Hints sourced from JSON/schema metadata arrive as ints; they must
        # render, not raise a TypeError in the substitution callback.
        m = _write_mapper(
            [
                {
                    "match": "exact",
                    "arrow_type": "Utf8",
                    "native_type": "VARCHAR(${length})",
                }
            ]
        )
        assert m.to_native_type("Utf8", params={"length": 255}) == "VARCHAR(255)"

    def test_none_param_hint_treated_as_missing(self):
        # A null/None hint (nullable or absent metadata field) is dropped, not
        # rendered as literal "None" — so a token needing it fails explicitly.
        m = _write_mapper(
            [
                {
                    "match": "exact",
                    "arrow_type": "Utf8",
                    "native_type": "VARCHAR(${length})",
                }
            ]
        )
        with pytest.raises(InvalidTypeMapError, match="render hint"):
            m.to_native_type("Utf8", params={"length": None})

    def test_missing_hint_raises(self):
        m = _write_mapper(
            [
                {
                    "match": "exact",
                    "arrow_type": "Utf8",
                    "native_type": "VARCHAR(${length})",
                }
            ]
        )
        with pytest.raises(InvalidTypeMapError, match="render hint"):
            m.to_native_type("Utf8")

    def test_regex_rule_missing_hint_raises(self):
        # The looser branch: a regex rule whose native references a token that
        # is neither a capture nor a supplied hint must raise at render time.
        m = _write_mapper(
            [
                {
                    "match": "regex",
                    "arrow_type": r"^Utf8$",
                    "native_type": "VARCHAR(${length})",
                }
            ]
        )
        with pytest.raises(InvalidTypeMapError, match="render hint"):
            m.to_native_type("Utf8")
        assert m.to_native_type("Utf8", params={"length": "64"}) == "VARCHAR(64)"

    def test_absent_optional_capture_falls_back_to_hint(self):
        # An optional capture that does not participate must not shadow a
        # same-named hint nor feed None into substitution.
        m = _write_mapper(
            [
                {
                    "match": "regex",
                    "arrow_type": r"^Utf8(\((?<length>\d+)\))?$",
                    "native_type": "VARCHAR(${length})",
                }
            ]
        )
        # Group present in the canonical -> capture wins.
        assert m.to_native_type("Utf8(10)") == "VARCHAR(10)"
        # Group absent -> the hint supplies the value instead of crashing.
        assert m.to_native_type("Utf8", params={"length": 255}) == "VARCHAR(255)"

    def test_capture_takes_precedence_over_hint(self):
        m = _write_mapper(
            [
                {
                    "match": "regex",
                    "arrow_type": r"^Decimal128\((?<p>\d+),\s*(?<s>\d+)\)$",
                    "native_type": "NUMERIC(${p}, ${s})",
                }
            ]
        )
        # A stray hint with the same name must not override the capture.
        assert m.to_native_type("Decimal128(10, 4)", params={"p": "99"}) == (
            "NUMERIC(10, 4)"
        )

    def test_first_match_wins(self):
        m = _write_mapper(
            [
                {"match": "exact", "arrow_type": "Int64", "native_type": "FIRST"},
                {"match": "regex", "arrow_type": r"^Int\d+$", "native_type": "SECOND"},
            ]
        )
        assert m.to_native_type("Int64") == "FIRST"
        assert m.to_native_type("Int32") == "SECOND"

    def test_nested_quantifier_bounded_at_runtime_lookup(self):
        # #504, write direction: compile_pattern() compiles the arrow_type
        # matcher for write rules through the same RE2 matcher as the read
        # side. Same input size as the read-side test -- see its
        # comment for the measurement.
        m = _write_mapper(
            [{"match": "regex", "arrow_type": r"^(A+)+B$", "native_type": "X"}]
        )
        adversarial = "A" * 30
        start = time.perf_counter()
        with pytest.raises(UnmappedTypeError):
            m.to_native_type(adversarial)
        elapsed = time.perf_counter() - start
        assert elapsed < 1.0, (
            f"nested-quantifier match took {elapsed:.3f}s for 30 chars; "
            f"expected linear-time (RE2), not exponential (backtracking re)"
        )
