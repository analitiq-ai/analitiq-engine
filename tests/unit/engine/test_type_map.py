"""Unit tests for the connector-owned type-map and ssl-mode-map subsystem.

Covers every acceptance bullet from GH #28:
- exact-match rules
- regex rules with named-capture substitution
- specificity ordering (first-match-wins)
- whitespace / case normalization
- hard error on unmapped native types
- RE2-subset enforcement (lookaround, backreferences rejected)
- SSL mode lookup + canonical-value validation
- file loading + caching discipline
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from cdk.type_map import (
    InvalidTypeMapError,
    TypeMapNotFoundError,
    TypeMapper,
    UnmappedTypeError,
    load_connection_type_map,
    load_type_map,
    normalize_arrow_type,
    normalize_native_type,
    parse_arrow_type,
)
from cdk.type_map.loader import TYPE_MAP_FILENAME, WRITE_TYPE_MAP_FILENAME
from cdk.type_map.rules import _FORBIDDEN_CONSTRUCTS, parse_rules, parse_write_rules

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
# Read-rule validation (contract shape, then execution safety)
# ---------------------------------------------------------------------------


def _parse_read(rule: dict):
    """Parse one read rule through the real entry point."""
    return parse_rules([rule], source="<test>")


def _parse_write(rule: dict):
    """Parse one write rule through the real entry point."""
    return parse_write_rules([rule], source="<test>")


#: One pattern per construct the engine refuses, mirroring
#: ``_FORBIDDEN_CONSTRUCTS``. Kept in sync by
#: ``test_every_forbidden_construct_has_a_case``.
_NON_RE2_PATTERNS = [
    ("lookahead", r"^FOO(?=BAR)$"),
    ("negative lookahead", r"^FOO(?!BAR)$"),
    ("lookbehind", r"^(?<=BAR)FOO$"),
    ("negative lookbehind", r"^(?<!BAR)FOO$"),
    ("atomic group", r"^(?>FOO)BAR$"),
    ("named backreference", r"^(?<x>\d+)-\k<x>$"),
    ("Python-style named backreference", r"^(?<x>\d+)-(?P=x)$"),
    ("numeric backreference", r"^(?<x>\d+)-\1$"),
]


class TestReadRuleValidation:
    def test_exact_rule_allows_literal_arrow_type(self):
        (rule,) = _parse_read(
            {"match": "exact", "native_type": "BIGINT", "arrow_type": "Int64"}
        )
        assert rule.match == "exact"

    def test_exact_rule_rejects_substitution_tokens(self):
        # An exact rule renders a literal Arrow type; the Arrow vocabulary
        # admits no "$", so the contract's field pattern refuses the template.
        with pytest.raises(InvalidTypeMapError, match=r"exact\.arrow_type"):
            _parse_read(
                {
                    "match": "exact",
                    "native_type": "BIGINT",
                    "arrow_type": "Decimal128(${p}, ${s})",
                }
            )

    @pytest.mark.parametrize(("construct", "pattern"), _NON_RE2_PATTERNS)
    def test_regex_rule_rejects_every_construct_outside_the_re2_subset(
        self, construct, pattern
    ):
        # The engine is the only gate on these: Python compiles all of them and
        # the contract permits any ECMA-262 matcher, so a construct dropped from
        # _FORBIDDEN_CONSTRUCTS would load clean and silently make the rule
        # non-portable.
        with pytest.raises(InvalidTypeMapError):
            _parse_read(
                {"match": "regex", "native_type": pattern, "arrow_type": "Utf8"}
            )

    def test_every_forbidden_construct_has_a_case(self):
        # _NON_RE2_PATTERNS is a hand-written mirror of the guard's own list;
        # this is what stops the two drifting apart.
        declared = {label for _token, label in _FORBIDDEN_CONSTRUCTS}
        covered = {construct for construct, _pattern in _NON_RE2_PATTERNS}
        assert declared <= covered, f"untested constructs: {sorted(declared - covered)}"

    def test_a_re2_incompatible_pattern_outside_the_blocklist_is_refused(self):
        # #504: \Z is not in _FORBIDDEN_CONSTRUCTS (Python compiles it fine)
        # but RE2 refuses to compile it (RE2 spells the equivalent \z) --
        # so this fails only if _assert_executable actually compiles the
        # pattern with re2, not just checks it against the blocklist.
        with pytest.raises(InvalidTypeMapError, match="failed to compile"):
            _parse_read(
                {"match": "regex", "native_type": r"^FOO\Z", "arrow_type": "Utf8"}
            )

    def test_regex_rule_rejects_missing_named_capture(self):
        with pytest.raises(InvalidTypeMapError, match=r"no matching \(\?<p>"):
            _parse_read(
                {
                    "match": "regex",
                    "native_type": r"^FOO\d+$",
                    "arrow_type": "Decimal128(${p}, ${s})",
                }
            )

    def test_regex_rule_rejects_render_that_is_not_an_arrow_type(self):
        # A templated render is shape-checked as a whole: Utf8 takes no
        # parameters, so no substitution can make Utf8(${n}) an Arrow type.
        with pytest.raises(InvalidTypeMapError, match="not a valid Arrow type"):
            _parse_read(
                {
                    "match": "regex",
                    "native_type": r"^FOO(?<n>\d+)$",
                    "arrow_type": "Utf8(${n})",
                }
            )

    def test_regex_rule_rejects_a_capture_wider_than_the_parameter(self):
        # rc24 checks that what a template CAN render is always an Arrow type:
        # an unbounded \d+ in the precision position can match 0 or 40, which
        # Decimal128 does not admit. This is why the reference fixture's NUMERIC
        # captures are bounded alternations rather than \d+ -- the fixture was
        # narrowed to satisfy this rule, and without this test nothing would
        # notice if the rule were relaxed and the narrowing became arbitrary.
        with pytest.raises(InvalidTypeMapError, match="does not admit"):
            _parse_read(
                {
                    "match": "regex",
                    "native_type": r"^NUMERIC\((?<p>\d+), *(?<s>\d+)\)$",
                    "arrow_type": "Decimal128(${p}, ${s})",
                }
            )

    def test_regex_rule_accepts_a_capture_bounded_to_the_parameter(self):
        # The narrowed form the fixtures now carry: every value the capture can
        # match is a legal precision, so the render is an Arrow type whatever
        # the native matches.
        (rule,) = _parse_read(
            {
                "match": "regex",
                "native_type": (
                    r"^NUMERIC\((?<p>[1-9]|[12][0-9]|3[0-8]), *"
                    r"(?<s>[0-9]|[12][0-9]|3[0-8])\)$"
                ),
                "arrow_type": "Decimal128(${p}, ${s})",
            }
        )
        assert rule.match == "regex"

    def test_regex_rule_rejects_malformed_pattern(self):
        with pytest.raises(InvalidTypeMapError, match="not a valid regex"):
            _parse_read({"match": "regex", "native_type": "^[", "arrow_type": "Utf8"})

    def test_unknown_match_kind_rejected(self):
        with pytest.raises(InvalidTypeMapError, match="does not match any of"):
            _parse_read({"match": "partial", "native_type": "x", "arrow_type": "Utf8"})

    @pytest.mark.parametrize(
        "bad_arrow_type",
        [
            "Time32(us)",
            "Time32(MICROSECOND)",
            "Time64(s)",
            "Time64(MILLISECOND)",
            "Timestamp(us, UTC)",
        ],
    )
    def test_exact_rule_rejects_arrow_type_outside_the_vocabulary(self, bad_arrow_type):
        # An exact rule's arrow_type is held to the published Arrow pattern, so
        # a cross-type temporal unit and a short unit code are both refused at
        # the contract, not deferred to parse_arrow_type downstream.
        with pytest.raises(InvalidTypeMapError, match=r"exact\.arrow_type"):
            _parse_read(
                {
                    "match": "exact",
                    "native_type": "TIME",
                    "arrow_type": bad_arrow_type,
                }
            )


class TestParseRules:
    def test_empty_list_rejected(self):
        with pytest.raises(InvalidTypeMapError, match="at least 1 item"):
            parse_rules([], source="<test>")

    def test_whole_document_failure_does_not_invent_a_rule_number(self):
        # An empty list fails the document, not a rule. Rendering it as
        # "rule #?" would send the reader looking for a rule that is not there.
        with pytest.raises(InvalidTypeMapError) as excinfo:
            parse_rules([], source="<test>")
        assert "rule #" not in str(excinfo.value)

    def test_rejection_names_the_offending_value(self):
        # The Arrow vocabulary is published as a `pattern`, so pydantic's own
        # message is the whole 800-character regex. Without the input echoed
        # back, an author is told to go read the grammar rather than which
        # token of theirs was wrong.
        with pytest.raises(InvalidTypeMapError) as excinfo:
            parse_rules(
                [{"match": "exact", "native_type": "TEXT", "arrow_type": "String"}],
                source="<test>",
            )
        assert "'String'" in str(excinfo.value)

    def test_non_object_rejected(self):
        with pytest.raises(InvalidTypeMapError, match="valid dictionary"):
            parse_rules(["oops"], source="<test>")


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

    ``^(A+)+B$`` has no lookahead, lookbehind, atomic group, or
    backreference, so it passes the RE2-subset blocklist -- and under
    Python's backtracking ``re`` it still runs in time exponential in input
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


def _write_connector(
    root: Path,
    slug: str,
    *,
    type_map: list | None = None,
    write_type_map: list | None = None,
) -> None:
    definition = root / slug / "definition"
    definition.mkdir(parents=True, exist_ok=True)
    (definition / "connector.json").write_text(
        json.dumps({"connector_id": "x", "slug": slug, "connector_type": "database"})
    )
    if type_map is not None:
        (definition / TYPE_MAP_FILENAME).write_text(json.dumps(type_map))
    if write_type_map is not None:
        (definition / WRITE_TYPE_MAP_FILENAME).write_text(json.dumps(write_type_map))


class TestLoaders:
    def test_type_map_missing_raises(self, tmp_path: Path):
        _write_connector(tmp_path, "empty")
        with pytest.raises(InvalidTypeMapError, match="required type-map not found"):
            load_type_map(tmp_path, "empty")

    def test_type_map_wrong_root_type(self, tmp_path: Path):
        _write_connector(tmp_path, "bad")
        (tmp_path / "bad" / "definition" / TYPE_MAP_FILENAME).write_text("{}")
        with pytest.raises(InvalidTypeMapError, match="must contain a JSON array"):
            load_type_map(tmp_path, "bad")

    def test_type_map_malformed_json(self, tmp_path: Path):
        _write_connector(tmp_path, "busted")
        (tmp_path / "busted" / "definition" / TYPE_MAP_FILENAME).write_text("not json")
        with pytest.raises(InvalidTypeMapError, match="not valid JSON"):
            load_type_map(tmp_path, "busted")

    def test_type_map_happy_path(self, tmp_path: Path):
        _write_connector(
            tmp_path,
            "demo",
            type_map=[{"match": "exact", "native_type": "TEXT", "arrow_type": "Utf8"}],
        )
        mapper = load_type_map(tmp_path, "demo")
        assert mapper.connector_slug == "demo"
        assert mapper.to_arrow_type("text") == "Utf8"


class TestLoadConnectionTypeMap:
    """Connection-scoped type-map lives under ``connections/{alias}/definition/``."""

    def test_absent_returns_none(self, tmp_path: Path):
        (tmp_path / "my-pg" / "definition").mkdir(parents=True)
        assert load_connection_type_map(tmp_path, "my-pg") is None

    def test_happy_path(self, tmp_path: Path):
        definition = tmp_path / "my-pg" / "definition"
        definition.mkdir(parents=True)
        (definition / TYPE_MAP_FILENAME).write_text(
            json.dumps(
                [
                    {
                        "match": "exact",
                        "native_type": "CUSTOM_ENUM",
                        "arrow_type": "Utf8",
                    },
                ]
            )
        )
        mapper = load_connection_type_map(tmp_path, "my-pg")
        assert mapper is not None
        assert mapper.connector_slug == "connection:my-pg"
        assert mapper.to_arrow_type("CUSTOM_ENUM") == "Utf8"

    def test_malformed_json_raises(self, tmp_path: Path):
        definition = tmp_path / "broken" / "definition"
        definition.mkdir(parents=True)
        (definition / TYPE_MAP_FILENAME).write_text("not json")
        with pytest.raises(InvalidTypeMapError, match="not valid JSON"):
            load_connection_type_map(tmp_path, "broken")

    def test_non_array_root_rejected(self, tmp_path: Path):
        definition = tmp_path / "bad" / "definition"
        definition.mkdir(parents=True)
        (definition / TYPE_MAP_FILENAME).write_text("{}")
        with pytest.raises(InvalidTypeMapError, match="must contain a JSON array"):
            load_connection_type_map(tmp_path, "bad")


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
# Write-rule validation (contract shape, then execution safety)
# ---------------------------------------------------------------------------


class TestWriteRuleValidation:
    def test_exact_rule_allows_literal_native(self):
        (rule,) = _parse_write(
            {"match": "exact", "arrow_type": "Int64", "native_type": "BIGINT"}
        )
        assert rule.match == "exact"

    def test_exact_rule_allows_param_token_in_native(self):
        # Unlike read rules, a write rule's render template may carry tokens fed
        # by per-column hints (e.g. length) rather than regex captures.
        (rule,) = _parse_write(
            {
                "match": "exact",
                "arrow_type": "Utf8",
                "native_type": "VARCHAR(${length})",
            }
        )
        assert rule.native_type == "VARCHAR(${length})"

    def test_regex_rule_allows_non_capture_token(self):
        # ${length} is supplied at render time, not captured — must not raise.
        (rule,) = _parse_write(
            {
                "match": "regex",
                "arrow_type": "^Utf8$",
                "native_type": "VARCHAR(${length})",
            }
        )
        assert rule.match == "regex"

    def test_regex_rule_rejects_lookahead_in_arrow_type(self):
        with pytest.raises(InvalidTypeMapError, match="lookahead"):
            _parse_write(
                {
                    "match": "regex",
                    "arrow_type": "^Foo(?=Bar)$",
                    "native_type": "TEXT",
                }
            )

    def test_regex_rule_rejects_malformed_pattern(self):
        with pytest.raises(InvalidTypeMapError, match="not a valid regex"):
            _parse_write({"match": "regex", "arrow_type": "^[", "native_type": "TEXT"})

    def test_exact_rule_rejects_token_on_match_side(self):
        # An exact rule matches on a literal Arrow type, and the Arrow
        # vocabulary admits no "$" — the contract's field pattern refuses it.
        with pytest.raises(InvalidTypeMapError, match=r"exact\.arrow_type"):
            _parse_write(
                {
                    "match": "exact",
                    "arrow_type": "Decimal128(${p})",
                    "native_type": "NUMERIC",
                }
            )

    def test_regex_rule_rejects_token_on_match_side(self):
        # A regex rule's arrow_type is a matcher, so the contract lets it
        # through; the engine refuses it because the token is compared as
        # literal text and the rule could never fire.
        with pytest.raises(InvalidTypeMapError, match="belong only in the rendered"):
            _parse_write(
                {
                    "match": "regex",
                    "arrow_type": "^Foo${bar}$",
                    "native_type": "TEXT",
                }
            )

    @pytest.mark.parametrize("bad_arrow_type", ["Decimal128(${p)", "Utf8${", "X${p-q}"])
    def test_rejects_malformed_opener_on_match_side(self, bad_arrow_type):
        # Any ${ on an exact rule's match side is a dead-rule footgun, well
        # formed or not, and none of these spellings is an Arrow type.
        with pytest.raises(InvalidTypeMapError, match=r"exact\.arrow_type"):
            _parse_write(
                {
                    "match": "exact",
                    "arrow_type": bad_arrow_type,
                    "native_type": "NUMERIC",
                }
            )

    @pytest.mark.parametrize(
        "bad_native",
        [
            "VARCHAR(${length)",  # unterminated opener
            "VARCHAR(${})",  # empty name
        ],
    )
    def test_contract_rejects_structurally_broken_placeholder(self, bad_native):
        # An opener with no close, and a token with no name at all, are refused
        # by the published contract's own render-value check.
        with pytest.raises(InvalidTypeMapError, match="render value"):
            _parse_write(
                {
                    "match": "exact",
                    "arrow_type": "Utf8",
                    "native_type": bad_native,
                }
            )

    @pytest.mark.parametrize(
        "bad_native",
        [
            "VARCHAR(${length-p})",  # bad character in the name
            "VARCHAR(${length })",  # trailing space in the name
            "VARCHAR($ {length})",  # space between the $ and the brace
        ],
    )
    def test_engine_rejects_placeholder_the_renderer_cannot_resolve(self, bad_native):
        # Well-formed to the contract, but the substitution token does not match
        # it, so it would survive rendering as literal text and land in the
        # emitted DDL. This process does the rendering, so this process refuses.
        with pytest.raises(InvalidTypeMapError, match="malformed substitution token"):
            _parse_write(
                {
                    "match": "exact",
                    "arrow_type": "Utf8",
                    "native_type": bad_native,
                }
            )

    @pytest.mark.parametrize(
        "bad_arrow_type",
        [
            "Time32(us)",
            "Time32(MICROSECOND)",
            "Time64(s)",
            "Time64(MILLISECOND)",
            "Timestamp(us, UTC)",
        ],
    )
    def test_exact_rule_rejects_arrow_type_outside_the_vocabulary(self, bad_arrow_type):
        # An exact write rule matches on a literal Arrow type, so the contract
        # holds it to the published pattern rather than deferring to
        # TypeMapper.__init__'s key pre-computation.
        with pytest.raises(InvalidTypeMapError, match=r"exact\.arrow_type"):
            _parse_write(
                {
                    "match": "exact",
                    "arrow_type": bad_arrow_type,
                    "native_type": "TIME",
                }
            )


class TestParseWriteRules:
    def test_empty_list_rejected(self):
        with pytest.raises(InvalidTypeMapError, match="at least 1 item"):
            parse_write_rules([], source="<test>")

    def test_non_object_rejected(self):
        with pytest.raises(InvalidTypeMapError, match="valid dictionary"):
            parse_write_rules(["oops"], source="<test>")

    def test_execution_safety_error_carries_the_rule_index(self):
        # The engine's own pass builds its own "rule #N" prefix, separately
        # from the one _render_validation_error builds for contract failures.
        # An uncompilable pattern would exercise the contract's path instead,
        # so this drives a construct only the engine refuses.
        with pytest.raises(
            InvalidTypeMapError, match=r"rule #1: .*unsupported construct"
        ):
            parse_write_rules(
                [
                    {"match": "exact", "arrow_type": "Int64", "native_type": "BIGINT"},
                    {
                        "match": "regex",
                        "arrow_type": "^DEC(?=IMAL)$",
                        "native_type": "TEXT",
                    },
                ],
                source="<test>",
            )

    def test_contract_error_wrapped_with_index(self):
        # A contract-level failure (here a bad ``match`` literal) is wrapped
        # with the offending rule's index.
        with pytest.raises(InvalidTypeMapError, match=r"rule #1: "):
            parse_write_rules(
                [
                    {"match": "exact", "arrow_type": "Int64", "native_type": "BIGINT"},
                    {"match": "partial", "arrow_type": "Int32", "native_type": "INT"},
                ],
                source="<test>",
            )


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
        with pytest.raises(InvalidTypeMapError, match="no write-type-map loaded"):
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
        with pytest.raises(InvalidTypeMapError, match="no write-type-map loaded"):
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
        # matcher for write rules through the same re2-backed function as
        # the read side. Same input size as the read-side test -- see its
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


# ---------------------------------------------------------------------------
# Loader — write-type-map.json sibling
# ---------------------------------------------------------------------------


class TestWriteMapLoader:
    def test_sibling_write_map_loaded(self, tmp_path: Path):
        _write_connector(
            tmp_path,
            "demo",
            type_map=[
                {"match": "exact", "native_type": "BIGINT", "arrow_type": "Int64"}
            ],
            write_type_map=[
                {"match": "exact", "arrow_type": "Int64", "native_type": "BIGINT"}
            ],
        )
        mapper = load_type_map(tmp_path, "demo")
        assert mapper.has_write_map is True
        assert mapper.to_native_type("Int64") == "BIGINT"

    def test_absent_write_map_leaves_read_only_mapper(self, tmp_path: Path):
        _write_connector(
            tmp_path,
            "readonly",
            type_map=[
                {"match": "exact", "native_type": "BIGINT", "arrow_type": "Int64"}
            ],
        )
        mapper = load_type_map(tmp_path, "readonly")
        assert mapper.has_write_map is False
        assert mapper.to_arrow_type("BIGINT") == "Int64"

    def test_malformed_write_map_raises_at_load(self, tmp_path):
        # A present-but-broken write map fails fast (caught by registry CI),
        # not silently downgraded. It is NOT a TypeMapNotFoundError, so the
        # connector loader treats it as fatal rather than "no type-map".
        _write_connector(
            tmp_path,
            "busted",
            type_map=[
                {"match": "exact", "native_type": "BIGINT", "arrow_type": "Int64"}
            ],
        )
        (tmp_path / "busted" / "definition" / WRITE_TYPE_MAP_FILENAME).write_text(
            "nope"
        )
        with pytest.raises(InvalidTypeMapError, match="not valid JSON") as exc:
            load_type_map(tmp_path, "busted")
        assert not isinstance(exc.value, TypeMapNotFoundError)

    def test_non_array_write_map_raises_at_load(self, tmp_path):
        _write_connector(
            tmp_path,
            "wrong",
            type_map=[
                {"match": "exact", "native_type": "BIGINT", "arrow_type": "Int64"}
            ],
        )
        (tmp_path / "wrong" / "definition" / WRITE_TYPE_MAP_FILENAME).write_text("{}")
        with pytest.raises(InvalidTypeMapError, match="must contain a JSON array"):
            load_type_map(tmp_path, "wrong")

    def test_absent_read_map_raises_not_found(self, tmp_path):
        # Absence is the benign case the connector loader downgrades to None.
        _write_connector(tmp_path, "apionly")  # connector.json only, no type-map
        with pytest.raises(TypeMapNotFoundError, match="required type-map not found"):
            load_type_map(tmp_path, "apionly")

    def test_connection_scoped_write_map_loaded(self, tmp_path: Path):
        definition = tmp_path / "my-pg" / "definition"
        definition.mkdir(parents=True)
        (definition / TYPE_MAP_FILENAME).write_text(
            json.dumps(
                [{"match": "exact", "native_type": "BIGINT", "arrow_type": "Int64"}]
            )
        )
        (definition / WRITE_TYPE_MAP_FILENAME).write_text(
            json.dumps(
                [{"match": "exact", "arrow_type": "Int64", "native_type": "BIGINT"}]
            )
        )
        mapper = load_connection_type_map(tmp_path, "my-pg")
        assert mapper is not None
        assert mapper.to_native_type("Int64") == "BIGINT"
