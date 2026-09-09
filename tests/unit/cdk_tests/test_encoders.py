"""Tests for the write-side encode functions (issue #503).

Complements ``test_encoding_catalogs.py`` (catalog drift) and
``api/test_write_path.py`` (the ``GenericAPIConnector``-level, end-to-end
behavior): this file is about the encode functions themselves, resolved
directly through :func:`resolve_encoder`.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from cdk.type_map.encoders import resolve_encoder
from cdk.type_map.exceptions import InvalidTypeMapError

pytestmark = pytest.mark.unit


class TestEpochEncoderUnitArithmetic:
    """A commit on this same branch shipped ``_encode_epoch`` computing
    NANOSECOND ticks 1000x too small -- found by an anti-pattern audit,
    not by a test. Pinned here per unit so it cannot regress silently
    again.
    """

    # One second after the epoch: the expected tick count in each unit is
    # unambiguous and independently computable, so this is a real
    # assertion, not a round-trip that could hide the same bug on both
    # sides.
    _ONE_SECOND_AFTER_EPOCH = datetime(1970, 1, 1, 0, 0, 1, tzinfo=timezone.utc)

    @pytest.mark.parametrize(
        ("unit", "expected_ticks"),
        [
            ("SECOND", 1),
            ("MILLISECOND", 1_000),
            ("MICROSECOND", 1_000_000),
            ("NANOSECOND", 1_000_000_000),
        ],
    )
    def test_each_unit_produces_the_correct_tick_count(
        self, unit: str, expected_ticks: int
    ) -> None:
        fn = resolve_encoder({"name": "epoch", "unit": unit})
        assert fn(self._ONE_SECOND_AFTER_EPOCH) == expected_ticks

    def test_a_naive_datetime_is_treated_as_utc(self) -> None:
        fn = resolve_encoder({"name": "epoch", "unit": "SECOND"})
        naive = datetime(1970, 1, 1, 0, 0, 1)  # noqa: DTZ001 -- the input under test
        assert fn(naive) == 1

    def test_a_non_datetime_value_is_refused(self) -> None:
        fn = resolve_encoder({"name": "epoch", "unit": "SECOND"})
        with pytest.raises(TypeError, match="expects a datetime"):
            fn("not-a-datetime")


class TestResolveEncoderValidation:
    def test_an_unknown_name_is_refused(self) -> None:
        with pytest.raises(InvalidTypeMapError, match="unknown encoding_write name"):
            resolve_encoder({"name": "bogus"})

    def test_a_non_string_name_is_refused_as_unknown_not_a_crash(self) -> None:
        # dict.get on a non-hashable name (e.g. a list) would otherwise
        # raise a raw TypeError instead of the named InvalidTypeMapError
        # every other malformed encoding_write gets.
        with pytest.raises(InvalidTypeMapError, match="unknown encoding_write name"):
            resolve_encoder({"name": ["not", "a", "string"]})

    def test_none_returns_none(self) -> None:
        assert resolve_encoder(None) is None

    def test_code_sentinel_returns_none_for_the_caller_to_route(self) -> None:
        assert resolve_encoder({"name": "code"}) is None
