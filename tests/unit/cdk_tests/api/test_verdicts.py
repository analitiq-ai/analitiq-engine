"""One table for what a status means, in both roles.

Three answers used to live side by side -- the read's transient set, the
write transport's retry set, the write's ack rule -- and they disagreed
about 408, 502 and every 5xx outside four values. The table below is the
test that makes a silent collapse impossible: it asserts all three facts
per status, derived from the one constant.
"""

from __future__ import annotations

import pytest

from cdk.api.verdicts import (
    RETRY_STATUSES,
    classify_exception,
    classify_status,
    declared_retry_statuses,
    http_is_transient,
    read_verdict,
    write_verdict,
)
from cdk.declarations import ErrorMap, parse_declared_error_map
from cdk.exceptions import ReadError, TransientReadError
from cdk.types import AckStatus, FailureCategory

pytestmark = pytest.mark.unit

# status -> (read is deterministic, write ack, the transport re-attempts it)
_TABLE = {
    400: (True, AckStatus.ACK_STATUS_FATAL_FAILURE, False),
    401: (True, AckStatus.ACK_STATUS_FATAL_FAILURE, False),
    404: (True, AckStatus.ACK_STATUS_FATAL_FAILURE, False),
    408: (False, AckStatus.ACK_STATUS_RETRYABLE_FAILURE, True),
    409: (True, AckStatus.ACK_STATUS_FATAL_FAILURE, False),
    422: (True, AckStatus.ACK_STATUS_FATAL_FAILURE, False),
    429: (False, AckStatus.ACK_STATUS_RETRYABLE_FAILURE, True),
    500: (False, AckStatus.ACK_STATUS_RETRYABLE_FAILURE, True),
    501: (False, AckStatus.ACK_STATUS_RETRYABLE_FAILURE, False),
    502: (False, AckStatus.ACK_STATUS_RETRYABLE_FAILURE, True),
    503: (False, AckStatus.ACK_STATUS_RETRYABLE_FAILURE, True),
    504: (False, AckStatus.ACK_STATUS_RETRYABLE_FAILURE, True),
    507: (False, AckStatus.ACK_STATUS_RETRYABLE_FAILURE, False),
}


class TestTheOneTable:
    @pytest.mark.parametrize("status", sorted(_TABLE))
    def test_both_roles_and_the_transport_agree_per_status(self, status: int) -> None:
        deterministic, ack, re_attempted = _TABLE[status]
        error = read_verdict("boom", status=status)
        assert isinstance(error, ReadError if deterministic else TransientReadError)
        assert write_verdict(status=status)[0] == ack
        assert (status in declared_retry_statuses(None)) is re_attempted

    def test_the_transport_set_is_the_constant(self) -> None:
        assert declared_retry_statuses(None) == set(RETRY_STATUSES)

    def test_a_server_error_is_transient_whatever_its_number(self) -> None:
        # The read path used to call anything outside four 5xx values
        # deterministic and fail the whole stream on it.
        assert all(http_is_transient(status) for status in range(500, 600))

    def test_a_failure_with_no_status_never_reached_the_provider(self) -> None:
        assert isinstance(read_verdict("boom"), TransientReadError)
        assert write_verdict()[0] == AckStatus.ACK_STATUS_RETRYABLE_FAILURE


class TestTheDeclarationRevisesTheTransport:
    def test_a_declared_retryable_status_joins_the_set(self) -> None:
        error_map = parse_declared_error_map({"http": {"418": "rate_limited"}})
        assert 418 in declared_retry_statuses(error_map)

    def test_a_declared_fatal_status_leaves_it(self) -> None:
        # Hammering a status the declaration calls fatal burns the ack
        # deadline before the fatal ack ever exists.
        error_map = parse_declared_error_map({"http": {"503": "config"}})
        assert 503 not in declared_retry_statuses(error_map)


class TestClassification:
    def test_the_dialect_goes_first(self) -> None:
        class Dialect:
            def classify(self, status: int, body: object) -> str | None:
                return "rate_limited" if status == 400 else None

        error_map = parse_declared_error_map({"http": {"400": "config"}})
        assert (
            classify_status(400, {}, dialect=Dialect(), error_map=error_map)
            == "rate_limited"
        )

    def test_the_declared_map_decides_when_the_dialect_has_no_opinion(self) -> None:
        error_map = parse_declared_error_map({"http": {"400": "config"}})
        assert classify_status(400, {}, dialect=None, error_map=error_map) == "config"

    def test_an_unclaimed_status_falls_to_the_built_in_rule(self) -> None:
        assert classify_status(400, {}, dialect=None, error_map=None) is None

    def test_a_declared_exception_never_claims_a_response_error(self) -> None:
        # The two families stay disjoint: a broad exception class meant for
        # status-less blips must not turn a deterministic 4xx into an
        # infinite retry.
        error_map = parse_declared_error_map({"exception": {"ValueError": "transient"}})
        assert classify_status(400, {}, dialect=None, error_map=error_map) is None

    def test_a_status_less_error_resolves_by_the_exception_family(self) -> None:
        error_map = parse_declared_error_map({"exception": {"ValueError": "transient"}})
        assert classify_exception(ValueError("x"), error_map=error_map) == "transient"

    def test_an_undeclared_connector_claims_nothing(self) -> None:
        assert classify_exception(ValueError("x"), error_map=None) is None


class TestDeclaredCategorySurvives:
    def test_a_deterministic_category_rides_the_read_error(self) -> None:
        error = read_verdict("boom", status=400, category="config")
        assert isinstance(error, ReadError)
        assert error.declared_category == "config"

    def test_a_retryable_category_beats_the_built_in_status_rule(self) -> None:
        # 400 is deterministic by the built-in rule; the declaration wins.
        error = read_verdict("boom", status=400, category="rate_limited")
        assert isinstance(error, TransientReadError)
        assert error.declared_category == "rate_limited"

    def test_a_declared_category_derives_the_write_ack_and_its_own_category(
        self,
    ) -> None:
        assert write_verdict(status=500, category="config") == (
            AckStatus.ACK_STATUS_FATAL_FAILURE,
            FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT,
        )

    def test_the_two_read_errors_are_not_one_hierarchy(self) -> None:
        # The worker classifies ReadError as deterministic by isinstance, so
        # tidying these into a hierarchy would make every 429 fatal.
        assert not issubclass(TransientReadError, ReadError)
        assert not issubclass(ReadError, TransientReadError)


class TestTheTableIsTotal:
    def test_every_declared_category_has_a_read_and_a_write_verdict(self) -> None:
        from cdk.declarations import ERROR_CATEGORY_VALUES

        for category in ERROR_CATEGORY_VALUES:
            assert read_verdict("x", category=category) is not None
            assert write_verdict(category=category) is not None

    def test_an_error_map_is_what_the_declaration_parses_to(self) -> None:
        assert isinstance(
            parse_declared_error_map({"http": {"429": "transient"}}), ErrorMap
        )
