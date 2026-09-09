"""Grammar + lookup tests for the connector-level declared facts (issue #401, #513).

``cdk.declarations`` is the typed, fail-loud view of the ``error_map`` and
``concurrency`` blocks: declared content is validated strictly (vocabulary,
key grammar, unknown fields), absence is additive, and the engine-owned
verdict tables cover the whole category vocabulary.

Issue #513 replaced the four closed families (``sqlstate``/``exception``/
``vendor_code``, plus the unchanged ``http``) with a single generic
``key_attrs`` + ``codes`` mechanism: a connector names, in its own
precedence order, which attributes its exception carries a native code on
(or the reserved ``CLASS_NAME_SIGNAL`` to match the exception's class name),
and a flat map from that native code to an engine category. The engine no
longer reads multiple attribute spellings per family (``sqlstate``/
``pgcode``, ``errno``/``vendor_code``/``args[0]``) or carves out ``OSError``
specially -- a connector whose driver needs that now either declares an
unambiguous attribute name or overrides ``classify_error``.
"""

from __future__ import annotations

import pytest

from cdk.declarations import (
    CLASS_NAME_SIGNAL,
    DECLARED_READ_DETERMINISTIC,
    DECLARED_WRITE_VERDICTS,
    ERROR_CATEGORY_VALUES,
    ConnectorDeclarationError,
    ErrorMap,
    parse_declared_concurrency,
    parse_declared_error_map,
    require_declared_category,
)
from cdk.types import AckStatus, FailureCategory

FULL_BLOCK = {
    "key_attrs": ["sqlstate", CLASS_NAME_SIGNAL],
    "codes": {
        "28000": "auth",
        "23505": "write_rejected",
        "OperationalError": "transient",
    },
    "http": {"429": "rate_limited", "401": "auth"},
}


class TestErrorMapParse:
    def test_full_block_parses(self):
        error_map = parse_declared_error_map(FULL_BLOCK)
        assert error_map is not None
        assert error_map.key_attrs == ("sqlstate", CLASS_NAME_SIGNAL)
        assert error_map.codes["28000"] == "auth"
        assert error_map.http[429] == "rate_limited"

    def test_absent_block_stays_undeclared(self):
        assert parse_declared_error_map(None) is None

    def test_empty_block_declares_nothing(self):
        error_map = parse_declared_error_map({})
        assert error_map is not None
        assert error_map.match_http(429) is None
        assert error_map.match_exception(ValueError("x")) is None

    def test_unknown_field_fails(self):
        with pytest.raises(ConnectorDeclarationError, match="unknown fields"):
            parse_declared_error_map({"sqlstate": {"08": "unreachable"}})

    def test_off_vocabulary_category_fails(self):
        with pytest.raises(ConnectorDeclarationError, match="expected one of"):
            parse_declared_error_map(
                {"key_attrs": ["sqlstate"], "codes": {"08": "retry_me"}}
            )

    def test_non_object_block_fails(self):
        with pytest.raises(ConnectorDeclarationError, match="must be an object"):
            parse_declared_error_map("auth")

    def test_non_object_codes_fails(self):
        with pytest.raises(ConnectorDeclarationError, match="must be an object"):
            parse_declared_error_map({"key_attrs": ["sqlstate"], "codes": ["429"]})

    def test_non_object_http_fails(self):
        with pytest.raises(ConnectorDeclarationError, match="must be an object"):
            parse_declared_error_map({"http": ["429"]})

    def test_key_attrs_without_codes_fails(self):
        with pytest.raises(ConnectorDeclarationError, match="key_attrs without codes"):
            parse_declared_error_map({"key_attrs": ["sqlstate"]})

    def test_codes_without_key_attrs_fails(self):
        with pytest.raises(ConnectorDeclarationError, match="key_attrs without codes"):
            parse_declared_error_map({"codes": {"28000": "auth"}})

    def test_empty_key_attrs_list_fails(self):
        with pytest.raises(ConnectorDeclarationError, match="non-empty list"):
            parse_declared_error_map({"key_attrs": [], "codes": {"28000": "auth"}})

    @pytest.mark.parametrize("entry", ["1bad", "bad-name", "", "bad.name", 5])
    def test_malformed_key_attrs_entry_fails(self, entry):
        with pytest.raises(ConnectorDeclarationError, match="malformed entry"):
            parse_declared_error_map({"key_attrs": [entry], "codes": {"28000": "auth"}})

    def test_class_name_signal_is_a_legal_key_attrs_entry(self):
        error_map = parse_declared_error_map(
            {"key_attrs": [CLASS_NAME_SIGNAL], "codes": {"ValueError": "config"}}
        )
        assert error_map is not None
        assert error_map.key_attrs == (CLASS_NAME_SIGNAL,)

    def test_arbitrary_native_code_shapes_are_legal(self):
        # No engine-enforced grammar on codes keys any more: a SQLSTATE, a
        # numeric vendor code, and an S3-style string code are all just map
        # keys now (issue #513's motivating NoSQL/API examples).
        error_map = parse_declared_error_map(
            {
                "key_attrs": ["code"],
                "codes": {"-4002": "unreachable", "NoSuchBucket": "config"},
            }
        )
        assert error_map is not None
        assert error_map.codes["-4002"] == "unreachable"
        assert error_map.codes["NoSuchBucket"] == "config"

    @pytest.mark.parametrize("key", ["42", "999", "4290", "abc"])
    def test_malformed_http_key_fails(self, key):
        with pytest.raises(ConnectorDeclarationError, match="key grammar"):
            parse_declared_error_map({"http": {key: "auth"}})


class TestErrorMapLookup:
    @pytest.fixture()
    def error_map(self) -> ErrorMap:
        parsed = parse_declared_error_map(FULL_BLOCK)
        assert parsed is not None
        return parsed

    def test_declared_attribute_matches(self, error_map):
        exc = Exception("boom")
        exc.sqlstate = "28000"
        match = error_map.match_exception(exc)
        assert match is not None
        assert (match.signal, match.value, match.category) == (
            "sqlstate",
            "28000",
            "auth",
        )

    def test_only_exact_codes_match_no_prefix_fallback(self, error_map):
        # Issue #513: the engine no longer special-cases SQLSTATE-class
        # prefix matching. A connector that wants both a class and a full
        # state declares both entries explicitly.
        exc = Exception("boom")
        exc.sqlstate = "28999"  # not declared; "28000" is, but as a full code
        assert error_map.match_exception(exc) is None

    def test_undeclared_attribute_spelling_does_not_match(self, error_map):
        # Only "sqlstate" is declared in FULL_BLOCK's key_attrs -- "pgcode"
        # is no longer read automatically (issue #513 drops the
        # multi-spelling knowledge _read_sqlstate used to carry).
        exc = Exception("boom")
        exc.pgcode = "28000"
        assert error_map.match_exception(exc) is None

    def test_pgcode_matches_when_declared_as_its_own_key_attr(self):
        error_map = parse_declared_error_map(
            {"key_attrs": ["pgcode"], "codes": {"28000": "auth"}}
        )
        assert error_map is not None
        exc = Exception("boom")
        exc.pgcode = "28000"
        match = error_map.match_exception(exc)
        assert match is not None
        assert match.category == "auth"

    def test_first_declared_key_attr_wins_when_both_present(self):
        error_map = parse_declared_error_map(
            {
                "key_attrs": ["sqlstate", "pgcode"],
                "codes": {"28000": "auth", "99999": "config"},
            }
        )
        assert error_map is not None
        exc = Exception("boom")
        exc.sqlstate = "28000"
        exc.pgcode = "99999"
        match = error_map.match_exception(exc)
        assert match is not None
        assert (match.signal, match.value) == ("sqlstate", "28000")

    def test_boolean_attribute_value_never_matches(self, error_map):
        # getattr on a bool-valued attribute must not stringify to "True"
        # and accidentally collide with a declared code.
        exc = Exception("boom")
        exc.sqlstate = True
        assert error_map.match_exception(exc) is None

    def test_exception_name_matches_through_mro(self, error_map):
        class OperationalError(Exception):
            pass

        class SubOperationalError(OperationalError):
            pass

        match = error_map.match_exception(SubOperationalError("gone away"))
        assert match is not None
        assert (match.signal, match.value, match.category) == (
            CLASS_NAME_SIGNAL,
            "OperationalError",
            "transient",
        )

    def test_cause_chain_is_walked(self, error_map):
        inner = Exception("inner")
        inner.sqlstate = "28000"
        outer = RuntimeError("wrapped")
        outer.__cause__ = inner
        match = error_map.match_exception(outer)
        assert match is not None
        assert match.category == "auth"

    def test_sqlalchemy_orig_link_is_walked(self, error_map):
        # SQLAlchemy's DBAPIError exposes the raw driver exception as
        # .orig — the member carrying the driver's native facts.
        driver_exc = Exception("auth denied")
        driver_exc.sqlstate = "28000"
        wrapper = RuntimeError("(psycopg2.OperationalError) wrapped")
        wrapper.orig = driver_exc
        match = error_map.match_exception(wrapper)
        assert match is not None
        assert match.category == "auth"

    def test_declared_precedence_order_is_the_connectors_own(self, error_map):
        # FULL_BLOCK declares key_attrs=["sqlstate", CLASS_NAME_SIGNAL], so
        # every member is checked for "sqlstate" before any member is
        # checked for the class name — a wrapper's declared class name must
        # not shadow a more specific fact on the driver exception it links.
        class OperationalError(Exception):
            pass

        driver_exc = Exception("password rejected")
        driver_exc.sqlstate = "28000"
        wrapper = OperationalError("wrapped")
        wrapper.orig = driver_exc
        match = error_map.match_exception(wrapper)
        assert match is not None
        assert (match.signal, match.category) == ("sqlstate", "auth")

    def test_http_lookup(self, error_map):
        match = error_map.match_http(429)
        assert match is not None
        assert match.category == "rate_limited"
        assert error_map.match_http(500) is None

    def test_no_key_attrs_declared_matches_nothing(self):
        error_map = parse_declared_error_map({"http": {"429": "rate_limited"}})
        assert error_map is not None
        exc = Exception("boom")
        exc.sqlstate = "28000"
        assert error_map.match_exception(exc) is None

    def test_unclaimed_exception_matches_nothing(self, error_map):
        assert error_map.match_exception(ValueError("nope")) is None


class TestRequireDeclaredCategory:
    def test_valid_category_passes_through(self):
        assert require_declared_category("auth", source="test") == "auth"

    def test_off_vocabulary_category_fails_loud(self):
        with pytest.raises(ConnectorDeclarationError, match="not in the engine"):
            require_declared_category("retry_me", source="test")


class TestConcurrencyParse:
    def test_declared_ceiling_parses(self):
        assert parse_declared_concurrency({"max_connections": 8}) == 8

    def test_absent_block_stays_undeclared(self):
        assert parse_declared_concurrency(None) is None

    def test_unknown_field_fails(self):
        with pytest.raises(ConnectorDeclarationError, match="unknown fields"):
            parse_declared_concurrency({"max_conections": 8})

    @pytest.mark.parametrize("value", [0, -1, "8", 2.5, True])
    def test_non_positive_or_non_int_fails(self, value):
        with pytest.raises(ConnectorDeclarationError, match="positive integer"):
            parse_declared_concurrency({"max_connections": value})


class TestVerdictTables:
    def test_write_verdicts_cover_the_vocabulary(self):
        assert set(DECLARED_WRITE_VERDICTS) == set(ERROR_CATEGORY_VALUES)

    def test_read_verdicts_cover_the_vocabulary(self):
        assert set(DECLARED_READ_DETERMINISTIC) == set(ERROR_CATEGORY_VALUES)

    def test_retryable_write_categories_carry_write_rejected(self):
        # The exhausted-retry path classifies from the category, so the
        # retryable rows must match the undeclared retryable branch.
        for category in ("transient", "unreachable", "rate_limited"):
            status, failure_category = DECLARED_WRITE_VERDICTS[category]
            assert status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
            assert failure_category == FailureCategory.FAILURE_CATEGORY_WRITE_REJECTED

    def test_deterministic_write_categories(self):
        assert DECLARED_WRITE_VERDICTS["auth"] == (
            AckStatus.ACK_STATUS_FATAL_FAILURE,
            FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT,
        )
        assert DECLARED_WRITE_VERDICTS["config"] == (
            AckStatus.ACK_STATUS_FATAL_FAILURE,
            FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT,
        )
        assert DECLARED_WRITE_VERDICTS["write_rejected"] == (
            AckStatus.ACK_STATUS_FATAL_FAILURE,
            FailureCategory.FAILURE_CATEGORY_WRITE_REJECTED,
        )
