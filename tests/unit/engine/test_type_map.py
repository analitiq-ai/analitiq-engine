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
from pathlib import Path

import pytest

from src.engine.type_map import (
    InvalidTypeMapError,
    TypeMapper,
    UnmappedTypeError,
    WriteTypeMapRule,
    parse_arrow_type,
    load_connection_type_map,
    load_type_map,
    normalize_canonical_type,
    normalize_native_type,
)
from src.engine.type_map.rules import TypeMapRule, parse_rules, parse_write_rules

# Repository root, for loading the real connector write-type-maps.
_REPO_ROOT = Path(__file__).resolve().parents[3]
_CONNECTORS_DIR = _REPO_ROOT / "connectors"


# ---------------------------------------------------------------------------
# normalize_native_type
# ---------------------------------------------------------------------------


class TestNormalizeNativeType:
    def test_strips_outer_whitespace(self):
        assert normalize_native_type("  BIGINT  ") == "BIGINT"

    def test_collapses_internal_runs(self):
        assert normalize_native_type("VARCHAR  ( 50 )") == "VARCHAR ( 50 )"
        assert normalize_native_type("TIMESTAMP\tWITH\nTIME  ZONE") == "TIMESTAMP WITH TIME ZONE"

    def test_uppercases(self):
        assert normalize_native_type("bigint") == "BIGINT"
        assert normalize_native_type("Varchar(50)") == "VARCHAR(50)"

    def test_rejects_non_string(self):
        with pytest.raises(TypeError):
            normalize_native_type(None)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# TypeMapRule validation
# ---------------------------------------------------------------------------


class TestTypeMapRuleValidation:
    def test_exact_rule_allows_literal_canonical(self):
        rule = TypeMapRule(match="exact", native="BIGINT", canonical="Int64")
        assert rule.match == "exact"

    def test_exact_rule_rejects_substitution_tokens(self):
        with pytest.raises(InvalidTypeMapError, match=r"exact rule"):
            TypeMapRule(match="exact", native="BIGINT", canonical="Decimal128(${p}, ${s})")

    def test_regex_rule_rejects_lookahead(self):
        with pytest.raises(InvalidTypeMapError, match="lookahead"):
            TypeMapRule(match="regex", native=r"^FOO(?=BAR)$", canonical="Utf8")

    def test_regex_rule_rejects_lookbehind(self):
        with pytest.raises(InvalidTypeMapError, match="lookbehind"):
            TypeMapRule(match="regex", native=r"^(?<=BAR)FOO$", canonical="Utf8")

    def test_regex_rule_rejects_numeric_backreference(self):
        with pytest.raises(InvalidTypeMapError, match="backreference"):
            TypeMapRule(
                match="regex", native=r"^(?<x>\d+)-\1$", canonical="Utf8"
            )

    def test_regex_rule_rejects_named_backreference(self):
        with pytest.raises(InvalidTypeMapError, match="backreference"):
            TypeMapRule(
                match="regex",
                native=r"^(?<x>\d+)-\k<x>$",
                canonical="Utf8",
            )

    def test_regex_rule_rejects_missing_named_capture(self):
        with pytest.raises(InvalidTypeMapError, match="unknown named"):
            TypeMapRule(
                match="regex",
                native=r"^FOO\d+$",
                canonical="Utf8(${n})",
            )

    def test_regex_rule_rejects_malformed_pattern(self):
        with pytest.raises(InvalidTypeMapError, match="failed to compile"):
            TypeMapRule(match="regex", native="^[", canonical="Utf8")

    def test_unknown_match_kind_rejected(self):
        with pytest.raises(Exception):  # pydantic ValidationError
            TypeMapRule(match="partial", native="x", canonical="Utf8")  # type: ignore[arg-type]


class TestParseRules:
    def test_empty_list_rejected(self):
        with pytest.raises(InvalidTypeMapError, match="rule list is empty"):
            parse_rules([], source="<test>")

    def test_non_object_rejected(self):
        with pytest.raises(InvalidTypeMapError, match="not a JSON object"):
            parse_rules(["oops"], source="<test>")


# ---------------------------------------------------------------------------
# TypeMapper — forward lookup
# ---------------------------------------------------------------------------


def _mapper(rules: list[dict]) -> TypeMapper:
    return TypeMapper("test", parse_rules(rules, source="<test>"))


class TestTypeMapperExact:
    def test_exact_hit(self):
        m = _mapper([{"match": "exact", "native": "JSONB", "canonical": "Utf8"}])
        assert m.to_arrow_type("JSONB") == "Utf8"

    def test_normalization_lower_to_upper(self):
        m = _mapper([{"match": "exact", "native": "BIGINT", "canonical": "Int64"}])
        assert m.to_arrow_type("bigint") == "Int64"

    def test_normalization_internal_whitespace(self):
        m = _mapper(
            [{"match": "exact", "native": "DOUBLE PRECISION", "canonical": "Float64"}]
        )
        assert m.to_arrow_type("double   precision") == "Float64"

    def test_unmapped_raises(self):
        m = _mapper([{"match": "exact", "native": "TEXT", "canonical": "Utf8"}])
        with pytest.raises(UnmappedTypeError) as exc:
            m.to_arrow_type("MONEY")
        assert exc.value.direction == "forward"
        assert exc.value.value == "MONEY"
        assert "MONEY" in str(exc.value)


class TestTypeMapperRegex:
    def test_regex_with_named_capture(self):
        m = _mapper([
            {
                "match": "regex",
                "native": r"^NUMERIC\(\s*(?<p>\d+)\s*,\s*(?<s>\d+)\s*\)$",
                "canonical": "Decimal128(${p}, ${s})",
            }
        ])
        assert m.to_arrow_type("NUMERIC(18, 2)") == "Decimal128(18, 2)"
        assert m.to_arrow_type("numeric( 10,4 )") == "Decimal128(10, 4)"

    def test_regex_without_tokens(self):
        m = _mapper([
            {"match": "regex", "native": r"^VARCHAR\(\s*\d+\s*\)$", "canonical": "Utf8"}
        ])
        assert m.to_arrow_type("VARCHAR(50)") == "Utf8"
        assert m.to_arrow_type("varchar(1024)") == "Utf8"


class TestSpecificityOrdering:
    """First-match-wins: narrower exact rules must sit above broader regexes."""

    def test_boolean_exact_beats_int8_regex(self):
        m = _mapper([
            {"match": "exact", "native": "TINYINT(1)", "canonical": "Boolean"},
            {
                "match": "regex",
                "native": r"^TINYINT(\(\d+\))?$",
                "canonical": "Int8",
            },
        ])
        assert m.to_arrow_type("TINYINT(1)") == "Boolean"
        assert m.to_arrow_type("TINYINT(4)") == "Int8"
        assert m.to_arrow_type("TINYINT") == "Int8"

    def test_reordering_changes_result(self):
        m = _mapper([
            {
                "match": "regex",
                "native": r"^TINYINT(\(\d+\))?$",
                "canonical": "Int8",
            },
            {"match": "exact", "native": "TINYINT(1)", "canonical": "Boolean"},
        ])
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
        with pytest.raises(InvalidTypeMapError, match="non-integer parameters"):
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
        from src.engine.type_map import resolve_arrow_type
        import pyarrow as pa

        assert resolve_arrow_type({"arrow_type": "Int64"}) == pa.int64()

    def test_object_builds_struct(self):
        from src.engine.type_map import resolve_arrow_type
        import pyarrow as pa

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
        from src.engine.type_map import resolve_arrow_type

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
        from src.engine.type_map import resolve_arrow_type
        import pyarrow as pa

        dt = resolve_arrow_type(
            {"arrow_type": "List", "items": {"arrow_type": "Int32"}}
        )
        assert pa.types.is_list(dt)
        assert pa.types.is_int32(dt.value_type)

    def test_nested_list_of_objects(self):
        from src.engine.type_map import resolve_arrow_type
        import pyarrow as pa

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
        from src.engine.type_map import resolve_arrow_type

        with pytest.raises(InvalidTypeMapError, match="missing 'arrow_type'"):
            resolve_arrow_type({})

    def test_object_missing_properties_raises(self):
        from src.engine.type_map import resolve_arrow_type

        with pytest.raises(InvalidTypeMapError, match="non-empty 'properties'"):
            resolve_arrow_type({"arrow_type": "Object"})

    def test_list_missing_items_raises(self):
        from src.engine.type_map import resolve_arrow_type

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
        (definition / "type-map.json").write_text(json.dumps(type_map))
    if write_type_map is not None:
        (definition / "write-type-map.json").write_text(json.dumps(write_type_map))


class TestLoaders:
    def test_type_map_missing_raises(self, tmp_path: Path):
        _write_connector(tmp_path, "empty")
        with pytest.raises(InvalidTypeMapError, match="required type-map not found"):
            load_type_map(tmp_path, "empty")

    def test_type_map_wrong_root_type(self, tmp_path: Path):
        _write_connector(tmp_path, "bad")
        (tmp_path / "bad" / "definition" / "type-map.json").write_text("{}")
        with pytest.raises(InvalidTypeMapError, match="must contain a JSON array"):
            load_type_map(tmp_path, "bad")

    def test_type_map_malformed_json(self, tmp_path: Path):
        _write_connector(tmp_path, "busted")
        (tmp_path / "busted" / "definition" / "type-map.json").write_text("not json")
        with pytest.raises(InvalidTypeMapError, match="not valid JSON"):
            load_type_map(tmp_path, "busted")

    def test_type_map_happy_path(self, tmp_path: Path):
        _write_connector(
            tmp_path,
            "demo",
            type_map=[{"match": "exact", "native": "TEXT", "canonical": "Utf8"}],
        )
        mapper = load_type_map(tmp_path, "demo")
        assert mapper.connector_slug == "demo"
        assert mapper.to_arrow_type("text") == "Utf8"

    def test_alternate_connector_dir_layout(self, tmp_path: Path):
        """``connector-{slug}`` layout also resolves."""
        definition = tmp_path / "connector-alt" / "definition"
        definition.mkdir(parents=True)
        (definition / "connector.json").write_text("{}")
        (definition / "type-map.json").write_text(
            json.dumps([{"match": "exact", "native": "TEXT", "canonical": "Utf8"}])
        )
        mapper = load_type_map(tmp_path, "alt")
        assert mapper.to_arrow_type("TEXT") == "Utf8"


class TestLoadConnectionTypeMap:
    """Connection-scoped type-map lives under ``connections/{alias}/definition/``."""

    def test_absent_returns_none(self, tmp_path: Path):
        (tmp_path / "my-pg" / "definition").mkdir(parents=True)
        assert load_connection_type_map(tmp_path, "my-pg") is None

    def test_happy_path(self, tmp_path: Path):
        definition = tmp_path / "my-pg" / "definition"
        definition.mkdir(parents=True)
        (definition / "type-map.json").write_text(
            json.dumps(
                [
                    {"match": "exact", "native": "CUSTOM_ENUM", "canonical": "Utf8"},
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
        (definition / "type-map.json").write_text("not json")
        with pytest.raises(InvalidTypeMapError, match="not valid JSON"):
            load_connection_type_map(tmp_path, "broken")

    def test_non_array_root_rejected(self, tmp_path: Path):
        definition = tmp_path / "bad" / "definition"
        definition.mkdir(parents=True)
        (definition / "type-map.json").write_text("{}")
        with pytest.raises(InvalidTypeMapError, match="must contain a JSON array"):
            load_connection_type_map(tmp_path, "bad")


# ---------------------------------------------------------------------------
# normalize_canonical_type (write direction, case-preserving)
# ---------------------------------------------------------------------------


class TestNormalizeCanonicalType:
    def test_preserves_case(self):
        assert normalize_canonical_type("Int64") == "Int64"
        assert normalize_canonical_type("Timestamp(MICROSECOND, UTC)") == (
            "Timestamp(MICROSECOND, UTC)"
        )

    def test_strips_outer_and_collapses_internal_whitespace(self):
        assert normalize_canonical_type("  Decimal128(38,  9) ") == "Decimal128(38, 9)"

    def test_rejects_non_string(self):
        with pytest.raises(TypeError):
            normalize_canonical_type(None)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# WriteTypeMapRule validation
# ---------------------------------------------------------------------------


class TestWriteTypeMapRuleValidation:
    def test_exact_rule_allows_literal_native(self):
        rule = WriteTypeMapRule(match="exact", canonical="Int64", native="BIGINT")
        assert rule.match == "exact"

    def test_exact_rule_allows_param_token_in_native(self):
        # Unlike read rules, a write rule's render template may carry tokens fed
        # by per-column hints (e.g. length) rather than regex captures.
        rule = WriteTypeMapRule(
            match="exact", canonical="Utf8", native="VARCHAR(${length})"
        )
        assert rule.native == "VARCHAR(${length})"

    def test_regex_rule_allows_non_capture_token(self):
        # ${length} is supplied at render time, not captured — must not raise.
        rule = WriteTypeMapRule(
            match="regex", canonical="^Utf8$", native="VARCHAR(${length})"
        )
        assert rule.match == "regex"

    def test_regex_rule_rejects_lookahead_in_canonical(self):
        with pytest.raises(InvalidTypeMapError, match="lookahead"):
            WriteTypeMapRule(
                match="regex", canonical="^Foo(?=Bar)$", native="TEXT"
            )

    def test_regex_rule_rejects_malformed_pattern(self):
        with pytest.raises(InvalidTypeMapError, match="failed to compile"):
            WriteTypeMapRule(match="regex", canonical="^[", native="TEXT")

    def test_exact_rule_rejects_token_on_match_side(self):
        # A ${...} token in the canonical (match) side would be matched as the
        # literal text and never fire — reject it at load time.
        with pytest.raises(InvalidTypeMapError, match="belong only in the rendered"):
            WriteTypeMapRule(
                match="exact", canonical="Decimal128(${p})", native="NUMERIC"
            )

    def test_regex_rule_rejects_token_on_match_side(self):
        with pytest.raises(InvalidTypeMapError, match="belong only in the rendered"):
            WriteTypeMapRule(
                match="regex", canonical="^Foo${bar}$", native="TEXT"
            )


class TestParseWriteRules:
    def test_empty_list_rejected(self):
        with pytest.raises(InvalidTypeMapError, match="rule list is empty"):
            parse_write_rules([], source="<test>")

    def test_non_object_rejected(self):
        with pytest.raises(InvalidTypeMapError, match="not a JSON object"):
            parse_write_rules(["oops"], source="<test>")

    def test_validator_error_passes_through(self):
        # An InvalidTypeMapError from the rule validator is re-raised as-is.
        with pytest.raises(InvalidTypeMapError, match="failed to compile"):
            parse_write_rules(
                [
                    {"match": "exact", "canonical": "Int64", "native": "BIGINT"},
                    {"match": "regex", "canonical": "^[", "native": "TEXT"},
                ],
                source="<test>",
            )

    def test_pydantic_error_wrapped_with_index(self):
        # A non-InvalidTypeMapError (here a bad ``match`` literal) is wrapped
        # with the offending rule's index.
        with pytest.raises(InvalidTypeMapError, match=r"rule #1 is invalid"):
            parse_write_rules(
                [
                    {"match": "exact", "canonical": "Int64", "native": "BIGINT"},
                    {"match": "partial", "canonical": "Int32", "native": "INT"},
                ],
                source="<test>",
            )


# ---------------------------------------------------------------------------
# TypeMapper — reverse lookup (to_native_type)
# ---------------------------------------------------------------------------


def _write_mapper(write_rules: list[dict]) -> TypeMapper:
    """A mapper with a throwaway read rule plus the given write rules."""
    return TypeMapper(
        "test",
        parse_rules(
            [{"match": "exact", "native": "X", "canonical": "Utf8"}], source="<r>"
        ),
        parse_write_rules(write_rules, source="<w>"),
    )


class TestToNativeTypeExact:
    def test_exact_hit(self):
        m = _write_mapper([{"match": "exact", "canonical": "Int64", "native": "BIGINT"}])
        assert m.to_native_type("Int64") == "BIGINT"

    def test_match_is_case_sensitive(self):
        # Arrow vocabulary is mixed-case; "int64" must NOT match "Int64".
        m = _write_mapper([{"match": "exact", "canonical": "Int64", "native": "BIGINT"}])
        with pytest.raises(UnmappedTypeError):
            m.to_native_type("int64")

    def test_whitespace_tolerated(self):
        m = _write_mapper([{"match": "exact", "canonical": "Int64", "native": "BIGINT"}])
        assert m.to_native_type("  Int64  ") == "BIGINT"

    def test_unmapped_raises_reverse(self):
        m = _write_mapper([{"match": "exact", "canonical": "Int64", "native": "BIGINT"}])
        with pytest.raises(UnmappedTypeError) as exc:
            m.to_native_type("Float64")
        assert exc.value.direction == "reverse"
        assert exc.value.value == "Float64"
        assert "Float64" in str(exc.value)

    def test_no_write_map_raises(self):
        m = TypeMapper(
            "test",
            parse_rules(
                [{"match": "exact", "native": "X", "canonical": "Utf8"}], source="<r>"
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
                [{"match": "exact", "native": "X", "canonical": "Utf8"}], source="<r>"
            ),
            write_rules=[],
        )
        assert m.has_write_map is False
        with pytest.raises(InvalidTypeMapError, match="no write-type-map loaded"):
            m.to_native_type("Int64")


class TestToNativeTypeRegex:
    def test_decimal_named_captures(self):
        m = _write_mapper([
            {
                "match": "regex",
                "canonical": r"^Decimal128\((?<p>\d+),\s*(?<s>\d+)\)$",
                "native": "NUMERIC(${p}, ${s})",
            }
        ])
        assert m.to_native_type("Decimal128(18, 2)") == "NUMERIC(18, 2)"
        assert m.to_native_type("Decimal128(38,9)") == "NUMERIC(38, 9)"

    def test_param_hint_substitution(self):
        m = _write_mapper([
            {"match": "exact", "canonical": "Utf8", "native": "VARCHAR(${length})"}
        ])
        assert m.to_native_type("Utf8", params={"length": "255"}) == "VARCHAR(255)"

    def test_missing_hint_raises(self):
        m = _write_mapper([
            {"match": "exact", "canonical": "Utf8", "native": "VARCHAR(${length})"}
        ])
        with pytest.raises(InvalidTypeMapError, match="render hint"):
            m.to_native_type("Utf8")

    def test_regex_rule_missing_hint_raises(self):
        # The looser branch: a regex rule whose native references a token that
        # is neither a capture nor a supplied hint must raise at render time.
        m = _write_mapper([
            {"match": "regex", "canonical": r"^Utf8$", "native": "VARCHAR(${length})"}
        ])
        with pytest.raises(InvalidTypeMapError, match="render hint"):
            m.to_native_type("Utf8")
        assert m.to_native_type("Utf8", params={"length": "64"}) == "VARCHAR(64)"

    def test_capture_takes_precedence_over_hint(self):
        m = _write_mapper([
            {
                "match": "regex",
                "canonical": r"^Decimal128\((?<p>\d+),\s*(?<s>\d+)\)$",
                "native": "NUMERIC(${p}, ${s})",
            }
        ])
        # A stray hint with the same name must not override the capture.
        assert m.to_native_type("Decimal128(10, 4)", params={"p": "99"}) == (
            "NUMERIC(10, 4)"
        )

    def test_first_match_wins(self):
        m = _write_mapper([
            {"match": "exact", "canonical": "Int64", "native": "FIRST"},
            {"match": "regex", "canonical": r"^Int\d+$", "native": "SECOND"},
        ])
        assert m.to_native_type("Int64") == "FIRST"
        assert m.to_native_type("Int32") == "SECOND"


# ---------------------------------------------------------------------------
# Loader — write-type-map.json sibling
# ---------------------------------------------------------------------------


class TestWriteMapLoader:
    def test_sibling_write_map_loaded(self, tmp_path: Path):
        _write_connector(
            tmp_path,
            "demo",
            type_map=[{"match": "exact", "native": "BIGINT", "canonical": "Int64"}],
            write_type_map=[{"match": "exact", "canonical": "Int64", "native": "BIGINT"}],
        )
        mapper = load_type_map(tmp_path, "demo")
        assert mapper.has_write_map is True
        assert mapper.to_native_type("Int64") == "BIGINT"

    def test_absent_write_map_leaves_read_only_mapper(self, tmp_path: Path):
        _write_connector(
            tmp_path,
            "readonly",
            type_map=[{"match": "exact", "native": "BIGINT", "canonical": "Int64"}],
        )
        mapper = load_type_map(tmp_path, "readonly")
        assert mapper.has_write_map is False
        assert mapper.to_arrow_type("BIGINT") == "Int64"

    def test_malformed_write_map_raises(self, tmp_path: Path):
        _write_connector(
            tmp_path,
            "busted",
            type_map=[{"match": "exact", "native": "BIGINT", "canonical": "Int64"}],
        )
        (tmp_path / "busted" / "definition" / "write-type-map.json").write_text("nope")
        with pytest.raises(InvalidTypeMapError, match="not valid JSON"):
            load_type_map(tmp_path, "busted")

    def test_non_array_write_map_raises(self, tmp_path: Path):
        _write_connector(
            tmp_path,
            "wrong",
            type_map=[{"match": "exact", "native": "BIGINT", "canonical": "Int64"}],
        )
        (tmp_path / "wrong" / "definition" / "write-type-map.json").write_text("{}")
        with pytest.raises(InvalidTypeMapError, match="must contain a JSON array"):
            load_type_map(tmp_path, "wrong")

    def test_connection_scoped_write_map_loaded(self, tmp_path: Path):
        definition = tmp_path / "my-pg" / "definition"
        definition.mkdir(parents=True)
        (definition / "type-map.json").write_text(
            json.dumps([{"match": "exact", "native": "BIGINT", "canonical": "Int64"}])
        )
        (definition / "write-type-map.json").write_text(
            json.dumps([{"match": "exact", "canonical": "Int64", "native": "BIGINT"}])
        )
        mapper = load_connection_type_map(tmp_path, "my-pg")
        assert mapper is not None
        assert mapper.to_native_type("Int64") == "BIGINT"


# ---------------------------------------------------------------------------
# Real connector write-type-maps (#564 acceptance: round-trip the vocabulary)
# ---------------------------------------------------------------------------


def _require_connector_write_map(slug: str) -> TypeMapper:
    """Load a real connector's mapper or skip.

    ``connectors/`` is registry-owned data populated at runtime (gitignored),
    so it is absent in a clean CI checkout. These tests validate the authored
    postgres/snowflake write-maps when present (local dev) and skip otherwise.
    """
    definition = _CONNECTORS_DIR / slug / "definition"
    if not (definition / "type-map.json").is_file():
        pytest.skip(f"connector {slug!r} not populated in {_CONNECTORS_DIR}")
    mapper = load_type_map(_CONNECTORS_DIR, slug)
    if not mapper.has_write_map:
        pytest.skip(f"connector {slug!r} has no write-type-map.json")
    return mapper


class TestRealConnectorWriteMaps:
    def test_postgres_write_map(self):
        m = _require_connector_write_map("postgres")
        assert m.has_write_map is True
        assert m.to_native_type("Boolean") == "BOOLEAN"
        assert m.to_native_type("Int16") == "SMALLINT"
        assert m.to_native_type("Int32") == "INTEGER"
        assert m.to_native_type("Int64") == "BIGINT"
        assert m.to_native_type("Float32") == "REAL"
        assert m.to_native_type("Float64") == "DOUBLE PRECISION"
        assert m.to_native_type("Decimal128(18, 2)") == "NUMERIC(18, 2)"
        assert m.to_native_type("Decimal128(38, 9)") == "NUMERIC(38, 9)"
        assert m.to_native_type("Utf8") == "TEXT"
        assert m.to_native_type("Json") == "JSONB"
        assert m.to_native_type("Binary") == "BYTEA"
        assert m.to_native_type("Date32") == "DATE"
        assert m.to_native_type("Time64(MICROSECOND)") == "TIME"
        assert m.to_native_type("Timestamp(MICROSECOND)") == "TIMESTAMP"
        assert m.to_native_type("Timestamp(MICROSECOND, UTC)") == "TIMESTAMPTZ"

    def test_postgres_round_trips_read_canonicals(self):
        # Every canonical the read map can emit must render to *some* native type
        # (acceptance: round-trip the canonical vocabulary, no silent default).
        m = _require_connector_write_map("postgres")
        for native in ("BOOLEAN", "SMALLINT", "INTEGER", "BIGINT", "REAL",
                       "DOUBLE PRECISION", "NUMERIC(18, 4)", "TEXT", "JSONB",
                       "BYTEA", "DATE", "TIMESTAMPTZ"):
            canonical = m.to_arrow_type(native)
            assert m.to_native_type(canonical)  # non-empty, no raise

    def test_snowflake_write_map(self):
        m = _require_connector_write_map("snowflake")
        assert m.has_write_map is True
        assert m.to_native_type("Boolean") == "BOOLEAN"
        assert m.to_native_type("Int64") == "NUMBER(38, 0)"
        assert m.to_native_type("Float64") == "FLOAT"
        assert m.to_native_type("Decimal128(10, 2)") == "NUMBER(10, 2)"
        assert m.to_native_type("Utf8") == "VARCHAR"
        assert m.to_native_type("Json") == "VARIANT"
        assert m.to_native_type("Binary") == "BINARY"
        assert m.to_native_type("Date32") == "DATE"
        assert m.to_native_type("Time64(NANOSECOND)") == "TIME"
        assert m.to_native_type("Timestamp(NANOSECOND)") == "TIMESTAMP_NTZ"
        assert m.to_native_type("Timestamp(NANOSECOND, UTC)") == "TIMESTAMP_TZ"

    def test_unmapped_canonical_raises_not_defaults(self):
        m = _require_connector_write_map("postgres")
        with pytest.raises(UnmappedTypeError) as exc:
            m.to_native_type("UInt256")
        assert exc.value.direction == "reverse"
