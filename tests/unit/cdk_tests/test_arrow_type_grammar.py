"""Conformance tests for the published arrow_type family vocabulary.

The family table (:data:`cdk.type_map.grammar.ARROW_FAMILIES`) is the single
source the parser binds against, the conversion policy classifies from, the
live-type probes derive from, and the published ``arrow_type_grammar.json``
renders from. These tests pin the chain so it cannot silently rot:

1. the committed published artifact equals what the table generates (drift
   guard);
2. every surface derives its family set from the one table — including a
   family added to the table, which must be visible everywhere with no second
   edit;
3. every published claim is exercised against the real parser — allowed
   spellings parse, everything outside the grammar fails loud with the
   engine's typed error (including the ranges pyarrow enforces, so a pyarrow
   upgrade that shifts a bound fails CI instead of silently changing the
   contract);
4. the pyarrow bindings the table names — the factory that builds a family and
   the predicates that recognise it in a live batch — round-trip, so a wrong
   or missing binding fails here rather than misclassifying a column.

Positive parses of the remaining unit spellings (Time64 short forms, the
other Timestamp units) live in ``tests/unit/engine/test_type_map.py``; that
suite and this one together cover the published surface.
"""

from __future__ import annotations

import importlib
import re
from collections.abc import Iterator
from contextlib import contextmanager
from unittest.mock import patch

import pyarrow as pa
import pytest

import cdk.type_map.arrow as arrow_module
from cdk.conformance.roundtrip import probe_canonicals
from cdk.type_map.arrow import arrow_family, parse_arrow_type
from cdk.type_map.conversions import build_conversion_grid, classify_conversion
from cdk.type_map.exceptions import InvalidTypeMapError
from cdk.type_map.grammar import (
    ARROW_FAMILIES,
    ARROW_TYPE_GRAMMAR_PATH,
    GRAMMAR_VERSION,
    ArrowFamily,
    build_arrow_type_grammar,
    load_published_grammar,
    render_arrow_type_grammar,
    unit_families,
)
from cdk.type_map.mapper import TypeMapper
from cdk.type_map.rules import parse_rules, parse_write_rules

# The two halves of the one table, as every consumer splits it.
_SCALAR_FAMILIES = {
    name: spec for name, spec in ARROW_FAMILIES.items() if spec.sub_schema is None
}
_STRUCTURAL_FAMILIES = {
    name: spec for name, spec in ARROW_FAMILIES.items() if spec.sub_schema is not None
}


class TestPublishedArtifactDrift:
    """The committed grammar must equal what the table generates."""

    def test_committed_json_matches_canonical_render(self) -> None:
        assert ARROW_TYPE_GRAMMAR_PATH.read_text() == render_arrow_type_grammar(), (
            "arrow_type_grammar.json is stale; regenerate with "
            "ARROW_TYPE_GRAMMAR_PATH.write_text(render_arrow_type_grammar())"
        )

    def test_loaded_grammar_equals_built_grammar(self) -> None:
        assert load_published_grammar() == build_arrow_type_grammar()

    def test_published_document_states_its_own_version(self) -> None:
        # The publisher reads this field rather than assigning one, so a
        # consumer holding only the bytes can name the vocabulary it got.
        # The format is pinned here too: it is typed in Python but parsed by
        # the publisher, and a version it cannot read is unpublishable.
        assert load_published_grammar()["version"] == GRAMMAR_VERSION
        assert re.fullmatch(r"\d+\.\d+\.\d+", GRAMMAR_VERSION)

    def test_document_carries_the_families_and_nothing_else(self) -> None:
        assert set(load_published_grammar()) == {"version", "families"}


class TestFamilySetConformance:
    """One vocabulary: both published artifacts render the same table."""

    def test_published_grammar_covers_every_family(self) -> None:
        assert set(load_published_grammar()["families"]) == set(ARROW_FAMILIES)

    def test_conversion_grid_axes_equal_the_family_table(self) -> None:
        grid = build_conversion_grid()
        assert set(grid) == set(ARROW_FAMILIES)
        for source, row in grid.items():
            assert set(row) == set(ARROW_FAMILIES), source

    def test_a_family_is_buildable_exactly_when_it_is_not_structural(self) -> None:
        # A structural family's shape comes from its sub-schema, so it has no
        # factory; every scalar family must name one or it could be declared
        # without being buildable.
        for name, spec in ARROW_FAMILIES.items():
            assert (spec.builder is None) == (spec.sub_schema is not None), name
        # The resolved factory table is the one the parser calls, so it is the
        # one that must cover every family declaring a builder.
        assert set(arrow_module._FACTORIES) == {
            name for name, spec in ARROW_FAMILIES.items() if spec.builder is not None
        }

    def test_rules_unit_vocabulary_derives_from_grammar(self) -> None:
        from cdk.type_map import rules

        derived = unit_families()
        assert set(derived) == {"Time32", "Time64", "Duration", "Timestamp"}
        assert derived["Time32"] == frozenset({"SECOND", "MILLISECOND"})
        assert derived["Time64"] == frozenset({"MICROSECOND", "NANOSECOND"})
        # The string-only rule surface consumes this exact mapping.
        assert rules._VALID_UNITS_BY_TYPE == derived


# One representative canonical string per scalar family, derived-checked below
# against the grammar so a new family cannot land without an example here.
_CANONICAL_EXAMPLE: dict[str, str] = {
    "Null": "Null",
    "Boolean": "Boolean",
    "Int8": "Int8",
    "Int16": "Int16",
    "Int32": "Int32",
    "Int64": "Int64",
    "UInt8": "UInt8",
    "UInt16": "UInt16",
    "UInt32": "UInt32",
    "UInt64": "UInt64",
    "Float16": "Float16",
    "Float32": "Float32",
    "Float64": "Float64",
    "Utf8": "Utf8",
    "LargeUtf8": "LargeUtf8",
    "Json": "Json",
    "Binary": "Binary",
    "LargeBinary": "LargeBinary",
    "Date32": "Date32",
    "Date64": "Date64",
    "Time32": "Time32(SECOND)",
    "Time64": "Time64(MICROSECOND)",
    "Duration": "Duration(MILLISECOND)",
    "Timestamp": "Timestamp(MICROSECOND, UTC)",
    "Decimal128": "Decimal128(38, 9)",
    "Decimal256": "Decimal256(76, 10)",
    "FixedSizeBinary": "FixedSizeBinary(16)",
}


class TestEveryFamilyParses:
    """Every scalar family has a parsing example; structural markers reject."""

    def test_example_set_is_complete(self) -> None:
        assert set(_CANONICAL_EXAMPLE) == set(_SCALAR_FAMILIES)

    @pytest.mark.parametrize("canonical", sorted(_CANONICAL_EXAMPLE.values()))
    def test_example_parses(self, canonical: str) -> None:
        assert isinstance(parse_arrow_type(canonical), pa.DataType)

    @pytest.mark.parametrize("family", sorted(_STRUCTURAL_FAMILIES))
    def test_structural_marker_rejected_with_sub_schema_hint(self, family: str) -> None:
        with pytest.raises(InvalidTypeMapError, match="nested type"):
            parse_arrow_type(family)


# The one family with no probe: a live Json column *is* a large_string, so it
# classifies as LargeUtf8. Named here so the exemption stays deliberate.
_UNPROBED_FAMILIES = {"Json"}


class TestFamilyProbes:
    """The live-type probes the table names are real, complete, and exclusive.

    ``arrow_family`` is the inverse of ``parse_arrow_type``: it decides which
    conversion policy a batch column is judged by. A stale probe silently
    misclassifies, so each of these pins it against the type the same table's
    factory builds.
    """

    def test_only_the_documented_family_has_no_probe(self) -> None:
        unprobed = {name for name, spec in ARROW_FAMILIES.items() if not spec.probes}
        assert unprobed == _UNPROBED_FAMILIES

    @pytest.mark.parametrize(
        ("family", "canonical"),
        sorted(
            (family, canonical)
            for family, canonical in _CANONICAL_EXAMPLE.items()
            if family not in _UNPROBED_FAMILIES
        ),
    )
    def test_family_recognises_the_type_its_own_factory_builds(
        self, family: str, canonical: str
    ) -> None:
        assert arrow_family(parse_arrow_type(canonical)) == family

    def test_json_classifies_as_large_utf8(self) -> None:
        # Json is carried on the wire as a JSON-encoded large_string, so the
        # runtime boundaries must judge it by the LargeUtf8 policy; the "Json"
        # row exists only so the published grid can say a declared Json column
        # cannot be retyped.
        assert arrow_family(parse_arrow_type("Json")) == "LargeUtf8"

    @pytest.mark.parametrize(
        ("dtype", "family"),
        [
            (pa.struct([pa.field("x", pa.int64())]), "Object"),
            (pa.list_(pa.int64()), "List"),
            (pa.large_list(pa.int64()), "List"),
        ],
    )
    def test_structural_families_are_probed(
        self, dtype: pa.DataType, family: str
    ) -> None:
        assert arrow_family(dtype) == family

    def test_probes_are_mutually_exclusive(self) -> None:
        # The probes are pyarrow.types predicates on the type id, so no live
        # type may satisfy two families: if one ever did, the answer would
        # depend on the table's ordering rather than on the vocabulary.
        built = [parse_arrow_type(c) for c in _CANONICAL_EXAMPLE.values()]
        built += [pa.struct([pa.field("x", pa.int64())]), pa.list_(pa.int64())]
        for dtype in built:
            matched = [
                name
                for name, spec in ARROW_FAMILIES.items()
                if any(getattr(pa.types, probe)(dtype) for probe in spec.probes)
            ]
            assert len(matched) <= 1, (dtype, matched)

    def test_resolved_probes_cover_exactly_the_declared_ones(self) -> None:
        # The probes are resolved to callables once at import; that resolved
        # table is what arrow_family walks, so it is the thing that must match
        # the vocabulary.
        declared = [
            (getattr(pa.types, probe), name)
            for name, spec in ARROW_FAMILIES.items()
            for probe in spec.probes
        ]
        assert list(arrow_module._FAMILY_PROBES) == declared

    def test_type_outside_the_vocabulary_fails_loud(self) -> None:
        with pytest.raises(InvalidTypeMapError, match="no conversion-matrix family"):
            arrow_family(pa.month_day_nano_interval())


@contextmanager
def _family_added(name: str, spec: ArrowFamily) -> Iterator[None]:
    """Add a family to the one table as if it had always been declared there.

    ``cdk.type_map.arrow`` resolves the table's pyarrow bindings once at
    import, so a family added after that import is only reachable through a
    reload -- which is exactly what a family added to the source table gets.
    The reload on the way out rebuilds those bindings from the restored table.
    """
    try:
        with patch.dict(ARROW_FAMILIES, {name: spec}):
            importlib.reload(arrow_module)
            yield
    finally:
        importlib.reload(arrow_module)


class TestOneTableFeedsEverySurface:
    """A family added to the table alone is visible on every surface.

    The vocabulary used to be re-declared four times (the parameter grammar,
    the conversion kinds, the pyarrow factories, the live-type probes), so
    adding a family meant four edits and forgetting one was silent. This adds
    a family to the one table and reads it back off every surface without
    touching anything else.
    """

    def test_added_family_reaches_every_surface(self) -> None:
        added = ArrowFamily(
            "duration",
            builder="month_day_nano_interval",
            probes=("is_interval",),
        )
        mapper = TypeMapper(
            "acceptance",
            parse_rules(
                [{"match": "exact", "native": "TEXT", "canonical": "Utf8"}],
                source="test",
            ),
            parse_write_rules(
                [{"match": "exact", "canonical": "Utf8", "native": "TEXT"}],
                source="test",
            ),
        )
        with _family_added("Interval", added):
            # The published grammar document.
            assert build_arrow_type_grammar()["families"]["Interval"] == {"params": []}
            # The conversion grid: a new axis, classified by its declared kind.
            grid = build_conversion_grid()
            assert set(grid) == set(ARROW_FAMILIES)
            assert grid["Interval"]["Interval"]["mode"] == "identity"
            assert classify_conversion("Null", "Interval").mode == "auto"
            assert classify_conversion("Interval", "Utf8").fn == "to_string"
            # The parser and its inverse.
            built = parse_arrow_type("Interval")
            assert built == pa.month_day_nano_interval()
            assert arrow_family(built) == "Interval"
            # The conformance kit's canonical probe set.
            assert "Interval" in probe_canonicals(mapper)
        assert "Interval" not in ARROW_FAMILIES


class TestUnitSpellings:
    """Both spellings of every allowed unit parse; disallowed units reject."""

    @pytest.mark.parametrize(
        ("long_form", "short_form"),
        [("SECOND", "s"), ("MILLISECOND", "ms")],
    )
    def test_time32_both_spellings(self, long_form: str, short_form: str) -> None:
        assert parse_arrow_type(f"Time32({long_form})") == parse_arrow_type(
            f"Time32({short_form})"
        )

    def test_duration_accepts_every_unit(self) -> None:
        for unit in ("SECOND", "MILLISECOND", "MICROSECOND", "NANOSECOND"):
            assert isinstance(parse_arrow_type(f"Duration({unit})"), pa.DataType)

    def test_cross_family_unit_rejected(self) -> None:
        with pytest.raises(InvalidTypeMapError, match="requires exactly one unit"):
            parse_arrow_type("Time32(NANOSECOND)")


class TestTimezoneValidation:
    """The tz grammar the artifact publishes is enforced at parse time."""

    @pytest.mark.parametrize(
        "tz", ["UTC", "Europe/Berlin", "Etc/GMT+5", "+02:00", "-11:30", "+00:00"]
    )
    def test_valid_timezone_accepted(self, tz: str) -> None:
        parsed = parse_arrow_type(f"Timestamp(MICROSECOND, {tz})")
        assert parsed == pa.timestamp("us", tz=tz)

    def test_null_sentinel_and_absence_are_both_naive(self) -> None:
        naive = pa.timestamp("us")
        assert parse_arrow_type("Timestamp(MICROSECOND)") == naive
        assert parse_arrow_type("Timestamp(MICROSECOND, null)") == naive

    @pytest.mark.parametrize(
        "tz", ["Not/AZone", "+25:00", "+2:00", "05:00", "utc garbage"]
    )
    def test_invalid_timezone_fails_loud_at_author_time(self, tz: str) -> None:
        # Previously pa.timestamp accepted any tz string and the mistake
        # surfaced only at cast time inside a running pipeline.
        with pytest.raises(InvalidTypeMapError, match="timezone"):
            parse_arrow_type(f"Timestamp(MICROSECOND, {tz})")

    @pytest.mark.parametrize("tz", ["America", "Europe", "Asia"])
    def test_continent_only_typo_raises_typed_error(self, tz: str) -> None:
        # A ZoneInfo construction probe would raise IsADirectoryError here
        # (continents are directories in the tz database); the membership
        # check must keep this the engine's typed error.
        with pytest.raises(InvalidTypeMapError, match="timezone"):
            parse_arrow_type(f"Timestamp(MICROSECOND, {tz})")

    @pytest.mark.parametrize("tz", ["utc", "europe/berlin", "Factory", "localtime"])
    def test_platform_independent_rejection(self, tz: str) -> None:
        # A ZoneInfo probe accepts these on a case-insensitive filesystem
        # (macOS), via tz-database keys Arrow cannot cast (Factory), or via a
        # host-local alias that means a different zone per machine
        # (localtime); the membership check rejects them identically on every
        # platform, so a config validated on a dev Mac cannot die later in
        # the Linux runtime.
        with pytest.raises(InvalidTypeMapError, match="timezone"):
            parse_arrow_type(f"Timestamp(MICROSECOND, {tz})")


class TestIntegerRanges:
    """The published ranges are enforced with the engine's typed error."""

    @pytest.mark.parametrize(
        "canonical",
        ["Decimal128(1, 0)", "Decimal128(38, 38)", "Decimal256(76, 76)"],
    )
    def test_boundary_values_accepted(self, canonical: str) -> None:
        assert isinstance(parse_arrow_type(canonical), pa.DataType)

    @pytest.mark.parametrize(
        ("canonical", "match"),
        [
            ("Decimal128(0, 0)", "precision must be between 1 and 38"),
            ("Decimal128(39, 0)", "precision must be between 1 and 38"),
            ("Decimal128(999, 0)", "precision must be between 1 and 38"),
            ("Decimal256(77, 0)", "precision must be between 1 and 76"),
            ("Decimal128(10, -2)", "scale must be between 0 and precision"),
            ("Decimal128(10, 11)", "scale must be between 0 and precision"),
            ("FixedSizeBinary(0)", "byte_width must be >= 1"),
            ("FixedSizeBinary(-3)", "byte_width must be >= 1"),
        ],
    )
    def test_out_of_range_rejected(self, canonical: str, match: str) -> None:
        with pytest.raises(InvalidTypeMapError, match=match):
            parse_arrow_type(canonical)

    @pytest.mark.parametrize(
        "canonical",
        [
            "Decimal128(+10, 2)",  # sign prefix
            "Decimal128(1_0, 2)",  # underscore separator
            "Decimal128(038, 9)",  # leading zero survives into rendered DDL
            "Decimal128(١٦, 9)",  # unicode digits render corrupt DDL
            "FixedSizeBinary(１６)",  # fullwidth digits
        ],
    )
    def test_non_plain_integer_spelling_rejected(self, canonical: str) -> None:
        # Python's int() accepts all of these; the published grammar says
        # kind "int", and a consumer implementing [0-9]+ would reject them —
        # the exact same-intent divergence this artifact exists to eliminate.
        with pytest.raises(InvalidTypeMapError, match="is not an integer"):
            parse_arrow_type(canonical)

    def test_oversized_digit_string_stays_a_typed_error(self) -> None:
        # Past sys.int_info.default_max_str_digits (4300), int() itself
        # raises; the grammar must keep even absurd inputs inside its typed
        # error contract instead of leaking a bare ValueError.
        huge = "9" * 5000
        with pytest.raises(InvalidTypeMapError, match="out of range"):
            parse_arrow_type(f"Decimal128({huge}, 0)")
        with pytest.raises(InvalidTypeMapError, match="out of range"):
            parse_arrow_type(f"FixedSizeBinary({huge})")


class TestArity:
    """Surplus, missing, and empty parameters all fail loud."""

    def test_parameterless_family_rejects_arguments(self) -> None:
        # The old parser silently ignored these; a parameter on Int64 is an
        # author-time mistake, not noise.
        with pytest.raises(InvalidTypeMapError, match="takes no parameters"):
            parse_arrow_type("Int64(3)")

    @pytest.mark.parametrize("canonical", ["Int64()", "Boolean( )"])
    def test_empty_parens_on_parameterless_family_rejected(
        self, canonical: str
    ) -> None:
        # Every other surface treats "Int64()" as a distinct string from
        # "Int64" (the write-map lookup would miss), so accepting it here
        # would bless a spelling that fails later with a misleading error.
        with pytest.raises(InvalidTypeMapError, match="takes no parameters"):
            parse_arrow_type(canonical)

    def test_empty_parens_on_parameterized_family_keeps_arity_error(self) -> None:
        with pytest.raises(InvalidTypeMapError, match="at least a unit"):
            parse_arrow_type("Timestamp()")
        with pytest.raises(InvalidTypeMapError, match="requires exactly one unit"):
            parse_arrow_type("Time32()")

    def test_surplus_timestamp_argument_rejected(self) -> None:
        with pytest.raises(InvalidTypeMapError, match="requires"):
            parse_arrow_type("Timestamp(us, UTC, extra)")

    def test_missing_decimal_scale_rejected(self) -> None:
        with pytest.raises(InvalidTypeMapError, match="precision, scale"):
            parse_arrow_type("Decimal128(10)")

    def test_empty_parameter_rejected(self) -> None:
        with pytest.raises(InvalidTypeMapError, match="empty parameter"):
            parse_arrow_type("Timestamp(us, )")


class TestPublishedDocumentShape:
    """What consumers read matches what the tests above enforce."""

    def test_scalar_families_carry_params(self) -> None:
        families = load_published_grammar()["families"]
        for family in _SCALAR_FAMILIES:
            assert "params" in families[family], family

    def test_structural_families_carry_sub_schema_key(self) -> None:
        families = load_published_grammar()["families"]
        assert families["Object"] == {"structural": "properties"}
        assert families["List"] == {"structural": "items"}

    def test_unit_params_publish_both_spellings(self) -> None:
        families = load_published_grammar()["families"]
        unit = families["Time32"]["params"][0]
        assert unit["allowed"] == ["SECOND", "MILLISECOND"]
        assert unit["accepted_short_forms"] == {"s": "SECOND", "ms": "MILLISECOND"}

    def test_scale_publishes_precision_bound(self) -> None:
        families = load_published_grammar()["families"]
        scale = families["Decimal128"]["params"][1]
        assert scale == {"name": "scale", "kind": "int", "min": 0, "max": "precision"}

    def test_timezone_publishes_forms_and_sentinel(self) -> None:
        families = load_published_grammar()["families"]
        tz = families["Timestamp"]["params"][1]
        assert tz["kind"] == "timezone"
        assert tz["optional"] is True
        assert tz["null_sentinel"] == "null"
