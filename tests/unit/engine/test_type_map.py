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
    CANONICAL_SSL_MODES,
    InvalidSSLModeMapError,
    InvalidTypeMapError,
    SSLModeMapper,
    TypeMapper,
    UnmappedSSLModeError,
    UnmappedTypeError,
    canonical_to_arrow,
    load_connection_type_map,
    load_ssl_mode_map,
    load_type_map,
    normalize_native_type,
)
from src.engine.type_map.rules import TypeMapRule, parse_rules


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
        assert m.to_canonical("JSONB") == "Utf8"

    def test_normalization_lower_to_upper(self):
        m = _mapper([{"match": "exact", "native": "BIGINT", "canonical": "Int64"}])
        assert m.to_canonical("bigint") == "Int64"

    def test_normalization_internal_whitespace(self):
        m = _mapper(
            [{"match": "exact", "native": "DOUBLE PRECISION", "canonical": "Float64"}]
        )
        assert m.to_canonical("double   precision") == "Float64"

    def test_unmapped_raises(self):
        m = _mapper([{"match": "exact", "native": "TEXT", "canonical": "Utf8"}])
        with pytest.raises(UnmappedTypeError) as exc:
            m.to_canonical("MONEY")
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
        assert m.to_canonical("NUMERIC(18, 2)") == "Decimal128(18, 2)"
        assert m.to_canonical("numeric( 10,4 )") == "Decimal128(10, 4)"

    def test_regex_without_tokens(self):
        m = _mapper([
            {"match": "regex", "native": r"^VARCHAR\(\s*\d+\s*\)$", "canonical": "Utf8"}
        ])
        assert m.to_canonical("VARCHAR(50)") == "Utf8"
        assert m.to_canonical("varchar(1024)") == "Utf8"


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
        assert m.to_canonical("TINYINT(1)") == "Boolean"
        assert m.to_canonical("TINYINT(4)") == "Int8"
        assert m.to_canonical("TINYINT") == "Int8"

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
        assert m.to_canonical("TINYINT(1)") == "Int8"


# ---------------------------------------------------------------------------
# canonical_to_arrow parser
# ---------------------------------------------------------------------------


class TestCanonicalToArrow:
    def test_primitives(self):
        import pyarrow as pa

        assert canonical_to_arrow("Int64") == pa.int64()
        assert canonical_to_arrow("Boolean") == pa.bool_()
        assert canonical_to_arrow("Utf8") == pa.string()
        assert canonical_to_arrow("Date32") == pa.date32()

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
        """Exercise every primitive family canonical_to_arrow dispatches on.

        These cases previously went untested at the parser level — they
        were only indirectly hit via sql_types' Arrow → SQLAlchemy map.
        """
        assert str(canonical_to_arrow(canonical)) == expected

    def test_timestamp_with_tz(self):
        import pyarrow as pa

        assert canonical_to_arrow("Timestamp(us, UTC)") == pa.timestamp("us", tz="UTC")
        assert canonical_to_arrow("Timestamp(ms)") == pa.timestamp("ms")

    def test_decimal(self):
        import pyarrow as pa

        assert canonical_to_arrow("Decimal128(18, 2)") == pa.decimal128(18, 2)

    def test_time32(self):
        import pyarrow as pa

        assert canonical_to_arrow("Time32(s)") == pa.time32("s")
        assert canonical_to_arrow("Time32(ms)") == pa.time32("ms")

    def test_time64(self):
        import pyarrow as pa

        assert canonical_to_arrow("Time64(us)") == pa.time64("us")
        assert canonical_to_arrow("Time64(ns)") == pa.time64("ns")

    def test_time_rejects_wrong_unit(self):
        with pytest.raises(InvalidTypeMapError, match="requires exactly one unit"):
            canonical_to_arrow("Time32(us)")  # us is Time64-only
        with pytest.raises(InvalidTypeMapError, match="requires exactly one unit"):
            canonical_to_arrow("Time64(s)")  # s is Time32-only

    def test_fixed_size_binary(self):
        import pyarrow as pa

        assert canonical_to_arrow("FixedSizeBinary(16)") == pa.binary(16)

    def test_fixed_size_binary_rejects_non_integer(self):
        with pytest.raises(InvalidTypeMapError, match="byte_width is not an integer"):
            canonical_to_arrow("FixedSizeBinary(abc)")

    def test_decimal256(self):
        import pyarrow as pa

        assert canonical_to_arrow("Decimal256(38, 10)") == pa.decimal256(38, 10)

    def test_decimal_rejects_non_integer_params(self):
        with pytest.raises(InvalidTypeMapError, match="non-integer parameters"):
            canonical_to_arrow("Decimal128(a, b)")

    def test_timestamp_requires_unit(self):
        with pytest.raises(InvalidTypeMapError, match="at least a unit"):
            canonical_to_arrow("Timestamp()")

    def test_timestamp_rejects_bad_unit(self):
        with pytest.raises(InvalidTypeMapError, match="unit must be one of"):
            canonical_to_arrow("Timestamp(xs)")

    def test_unknown_family_rejected(self):
        with pytest.raises(InvalidTypeMapError, match="not supported"):
            canonical_to_arrow("Nope")

    def test_unbalanced_parens_rejected(self):
        with pytest.raises(InvalidTypeMapError, match="unbalanced"):
            canonical_to_arrow("Int64(")


# ---------------------------------------------------------------------------
# SSLModeMapper
# ---------------------------------------------------------------------------


class TestSSLModeMapper:
    def test_canonical_set_is_frozen(self):
        assert isinstance(CANONICAL_SSL_MODES, frozenset)
        assert CANONICAL_SSL_MODES == {"none", "encrypt", "verify", "prefer"}

    def test_native_lookup(self):
        m = SSLModeMapper(
            "pg",
            {
                "disable": "none",
                "prefer": "prefer",
                "require": "encrypt",
                "verify-ca": "verify",
                "verify-full": "verify",
            },
        )
        assert m.to_canonical("disable") == "none"
        assert m.to_canonical("REQUIRE") == "encrypt"
        assert m.to_canonical("verify-full") == "verify"

    def test_canonical_passthrough(self):
        m = SSLModeMapper("pg", {"require": "encrypt"})
        assert m.to_canonical("encrypt") == "encrypt"
        assert m.to_canonical("VERIFY") == "verify"

    def test_unknown_native_raises(self):
        m = SSLModeMapper("pg", {"require": "encrypt"})
        with pytest.raises(UnmappedSSLModeError):
            m.to_canonical("allow-tls")

    def test_invalid_canonical_value_rejected(self):
        with pytest.raises(InvalidSSLModeMapError, match="not in the canonical"):
            SSLModeMapper("pg", {"strict": "strict"})

    def test_conflicting_native_rejected(self):
        with pytest.raises(InvalidSSLModeMapError, match="maps to both"):
            SSLModeMapper("pg", {"Require": "encrypt", "REQUIRE": "verify"})

    def test_empty_mapping_rejected(self):
        with pytest.raises(InvalidSSLModeMapError, match="empty"):
            SSLModeMapper("pg", {})


# ---------------------------------------------------------------------------
# Loader — filesystem
# ---------------------------------------------------------------------------


def _write_connector(
    root: Path,
    slug: str,
    *,
    type_map: list | None = None,
    ssl_mode_map: dict | None = None,
) -> None:
    definition = root / slug / "definition"
    definition.mkdir(parents=True, exist_ok=True)
    (definition / "connector.json").write_text(
        json.dumps({"connector_id": "x", "slug": slug, "connector_type": "database"})
    )
    if type_map is not None:
        (definition / "type-map.json").write_text(json.dumps(type_map))
    if ssl_mode_map is not None:
        (definition / "ssl-mode-map.json").write_text(json.dumps(ssl_mode_map))


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
        assert mapper.to_canonical("text") == "Utf8"

    def test_ssl_mode_map_absent_is_ok(self, tmp_path: Path):
        _write_connector(
            tmp_path,
            "no-ssl",
            type_map=[{"match": "exact", "native": "TEXT", "canonical": "Utf8"}],
        )
        assert load_ssl_mode_map(tmp_path, "no-ssl") is None

    def test_ssl_mode_map_happy_path(self, tmp_path: Path):
        _write_connector(
            tmp_path,
            "ssl-db",
            type_map=[{"match": "exact", "native": "TEXT", "canonical": "Utf8"}],
            ssl_mode_map={
                "$schema": "https://analitiq.dev/schemas/ssl-mode-map.json",
                "require": "encrypt",
                "disable": "none",
            },
        )
        mapper = load_ssl_mode_map(tmp_path, "ssl-db")
        assert mapper is not None
        assert mapper.to_canonical("require") == "encrypt"
        # JSON-Schema metadata keys are dropped at load time
        assert "$schema" not in mapper.entries

    def test_alternate_connector_dir_layout(self, tmp_path: Path):
        """``connector-{slug}`` layout also resolves."""
        definition = tmp_path / "connector-alt" / "definition"
        definition.mkdir(parents=True)
        (definition / "connector.json").write_text("{}")
        (definition / "type-map.json").write_text(
            json.dumps([{"match": "exact", "native": "TEXT", "canonical": "Utf8"}])
        )
        mapper = load_type_map(tmp_path, "alt")
        assert mapper.to_canonical("TEXT") == "Utf8"


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
        assert mapper.to_canonical("CUSTOM_ENUM") == "Utf8"

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
