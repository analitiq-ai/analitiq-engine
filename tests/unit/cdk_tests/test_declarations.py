"""Grammar + lookup tests for the connector-level declared facts (issue #401).

``cdk.declarations`` is the typed, fail-loud view of the ``error_map`` and
``concurrency`` blocks: declared content is validated strictly (vocabulary,
per-family key grammar, unknown fields), absence is additive, and the
engine-owned verdict tables cover the whole category vocabulary.
"""

from __future__ import annotations

import pytest

from cdk.declarations import (
    DECLARED_READ_DETERMINISTIC,
    DECLARED_WRITE_VERDICTS,
    ERROR_CATEGORY_VALUES,
    ConnectorDeclarationError,
    ErrorMap,
    parse_declared_concurrency,
    parse_declared_error_map,
)
from cdk.types import AckStatus, FailureCategory

FULL_BLOCK = {
    "sqlstate": {"08": "unreachable", "28000": "auth", "23": "write_rejected"},
    "exception": {"OperationalError": "transient"},
    "vendor_code": {"1045": "auth"},
    "http": {"429": "rate_limited", "401": "auth"},
}


class TestErrorMapParse:
    def test_full_block_parses(self):
        error_map = parse_declared_error_map(FULL_BLOCK)
        assert error_map is not None
        assert error_map.sqlstate["28000"] == "auth"
        assert error_map.http[429] == "rate_limited"

    def test_absent_block_stays_undeclared(self):
        assert parse_declared_error_map(None) is None

    def test_empty_block_declares_nothing(self):
        error_map = parse_declared_error_map({})
        assert error_map is not None
        assert error_map.match_http(429) is None

    def test_unknown_family_fails(self):
        with pytest.raises(ConnectorDeclarationError, match="unknown families"):
            parse_declared_error_map({"sql_state": {"08": "unreachable"}})

    def test_off_vocabulary_category_fails(self):
        with pytest.raises(ConnectorDeclarationError, match="expected one of"):
            parse_declared_error_map({"sqlstate": {"08": "retry_me"}})

    def test_non_object_block_fails(self):
        with pytest.raises(ConnectorDeclarationError, match="must be an object"):
            parse_declared_error_map("auth")

    def test_non_object_family_fails(self):
        with pytest.raises(ConnectorDeclarationError, match="must be an object"):
            parse_declared_error_map({"http": ["429"]})

    @pytest.mark.parametrize("key", ["8", "080", "28000X", "08-1", "sqlstate", "28abc"])
    def test_malformed_sqlstate_key_fails(self, key):
        with pytest.raises(ConnectorDeclarationError, match="key grammar"):
            parse_declared_error_map({"sqlstate": {key: "auth"}})

    @pytest.mark.parametrize("key", ["1Operational", "Op-Error", ""])
    def test_malformed_exception_key_fails(self, key):
        with pytest.raises(ConnectorDeclarationError, match="key grammar"):
            parse_declared_error_map({"exception": {key: "transient"}})

    @pytest.mark.parametrize("key", ["1045x", "0x1045", ""])
    def test_malformed_vendor_code_key_fails(self, key):
        with pytest.raises(ConnectorDeclarationError, match="key grammar"):
            parse_declared_error_map({"vendor_code": {key: "auth"}})

    @pytest.mark.parametrize("key", ["42", "999", "4290", "abc"])
    def test_malformed_http_key_fails(self, key):
        with pytest.raises(ConnectorDeclarationError, match="key grammar"):
            parse_declared_error_map({"http": {key: "auth"}})

    def test_negative_vendor_code_is_legal(self):
        error_map = parse_declared_error_map({"vendor_code": {"-4002": "unreachable"}})
        assert error_map is not None
        assert error_map.vendor_code["-4002"] == "unreachable"


class TestErrorMapLookup:
    @pytest.fixture()
    def error_map(self) -> ErrorMap:
        parsed = parse_declared_error_map(FULL_BLOCK)
        assert parsed is not None
        return parsed

    def test_full_sqlstate_wins_over_class(self, error_map):
        exc = Exception("boom")
        exc.sqlstate = "28000"
        match = error_map.match_exception(exc)
        assert match is not None
        assert (match.family, match.identifier, match.category) == (
            "sqlstate",
            "28000",
            "auth",
        )

    def test_sqlstate_class_prefix_matches(self, error_map):
        exc = Exception("boom")
        exc.sqlstate = "08006"
        match = error_map.match_exception(exc)
        assert match is not None
        assert (match.identifier, match.category) == ("08", "unreachable")

    def test_pgcode_spelling_matches(self, error_map):
        exc = Exception("boom")
        exc.pgcode = "23505"
        match = error_map.match_exception(exc)
        assert match is not None
        assert match.category == "write_rejected"

    def test_vendor_code_from_errno(self, error_map):
        exc = Exception("access denied")
        exc.errno = 1045
        match = error_map.match_exception(exc)
        assert match is not None
        assert (match.family, match.category) == ("vendor_code", "auth")

    def test_vendor_code_from_args_first_element(self, error_map):
        # pymysql/MySQLdb expose the code only as args[0], never .errno.
        exc = Exception(1045, "Access denied for user")
        match = error_map.match_exception(exc)
        assert match is not None
        assert (match.family, match.identifier) == ("vendor_code", "1045")

    def test_oserror_args_never_contribute_a_vendor_code(self, error_map):
        vendor_111 = parse_declared_error_map({"vendor_code": {"111": "auth"}})
        assert vendor_111 is not None
        assert vendor_111.match_exception(OSError(111, "refused")) is None

    def test_oserror_errno_is_never_a_vendor_code(self, error_map):
        # ECONNREFUSED is 111 on Linux; a declared vendor code "111" must
        # not claim an operating-system errno.
        vendor_111 = parse_declared_error_map({"vendor_code": {"111": "auth"}})
        assert vendor_111 is not None
        exc = ConnectionRefusedError(111, "Connection refused")
        assert vendor_111.match_exception(exc) is None

    def test_exception_name_matches_through_mro(self, error_map):
        class OperationalError(Exception):
            pass

        class SubOperationalError(OperationalError):
            pass

        match = error_map.match_exception(SubOperationalError("gone away"))
        assert match is not None
        assert (match.identifier, match.category) == (
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

    def test_http_lookup(self, error_map):
        match = error_map.match_http(429)
        assert match is not None
        assert match.category == "rate_limited"
        assert error_map.match_http(500) is None

    def test_names_lookup_for_boundary_collapsed_chains(self, error_map):
        match = error_map.match_names(["ReadError", "OperationalError"])
        assert match is not None
        assert match.category == "transient"
        assert error_map.match_names(["ValueError"]) is None

    def test_unclaimed_exception_matches_nothing(self, error_map):
        assert error_map.match_exception(ValueError("nope")) is None


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
