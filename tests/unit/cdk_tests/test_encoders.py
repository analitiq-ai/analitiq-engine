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

    def test_a_datetime_subclass_carries_only_its_microsecond_value(self) -> None:
        # LandingBatch.records (cdk.base_handler) materialises the whole
        # batch via RecordBatch.to_pylist() before land() ever calls an
        # encoder, and to_pylist() itself refuses a genuinely
        # sub-microsecond Timestamp(NANOSECOND) value outright unless
        # pandas is installed (not a CDK runtime dependency) -- so any
        # extra attribute a datetime subclass carries is not something
        # this encoder can or should read; only .microsecond is real.
        class _DatetimeSubclass(datetime):
            nanosecond = 789

        fn = resolve_encoder({"name": "epoch", "unit": "NANOSECOND"})
        value = _DatetimeSubclass(1970, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
        assert fn(value) == 1_000_000_000

    def test_a_value_finer_than_the_configured_unit_is_refused(self) -> None:
        # Floor division would otherwise silently change the represented
        # instant (0.5s at unit SECOND becomes 0), not just its precision.
        fn = resolve_encoder({"name": "epoch", "unit": "SECOND"})
        value = datetime(1970, 1, 1, 0, 0, 0, 500_000, tzinfo=timezone.utc)
        with pytest.raises(ValueError, match="not exactly representable"):
            fn(value)

    def test_a_pre_epoch_value_finer_than_the_configured_unit_is_refused(self) -> None:
        # Floor division rounds a negative total toward -inf, not toward
        # the represented instant -- -0.5s at unit SECOND would otherwise
        # become -1, not 0.
        fn = resolve_encoder({"name": "epoch", "unit": "SECOND"})
        value = datetime(1969, 12, 31, 23, 59, 59, 500_000, tzinfo=timezone.utc)
        with pytest.raises(ValueError, match="not exactly representable"):
            fn(value)


class TestEpochEncoderRejectsWhatTheBodySerializerCannotEncode:
    """orjson (cdk.api.http.encode_body) renders a JSON number from either
    an i64 or a u64, not a plain signed 64-bit span. A wide Timestamp
    encoded in a finer unit -- a year-9999 value as NANOSECOND -- produces
    an integer outside even that range; encode_body's own TypeError for it
    is not caught by the per-record body-build boundary
    (cdk.api.generic._write_one_by_one), so it must be refused here, where
    apply_field_encoders' existing (ValueError, TypeError) catch already
    handles it as this one record's failure.
    """

    def test_a_year_9999_timestamp_as_nanosecond_is_refused_not_silently_overflowed(
        self,
    ) -> None:
        fn = resolve_encoder({"name": "epoch", "unit": "NANOSECOND"})
        value = datetime(9999, 1, 1, tzinfo=timezone.utc)
        with pytest.raises(ValueError, match="outside the range"):
            fn(value)

    def test_an_ordinary_value_in_every_unit_still_succeeds(self) -> None:
        value = datetime(2024, 1, 15, 13, 45, 6, tzinfo=timezone.utc)
        for unit in ("SECOND", "MILLISECOND", "MICROSECOND", "NANOSECOND"):
            fn = resolve_encoder({"name": "epoch", "unit": unit})
            assert fn(value) > 0


class TestStrftimeRejectsUnknownDirectivesAtResolveTime:
    """datetime.strftime's own behavior for an unrecognised directive is
    platform-dependent -- glibc (this catalog's Linux CI/prod target)
    silently renders one like '%Q' literally instead of raising -- so a
    malformed pattern must be refused when the encoder is resolved, not
    left to reach data time and render malformed output.
    """

    def test_an_unknown_directive_is_refused_at_resolve_time(self) -> None:
        with pytest.raises(InvalidTypeMapError, match="does not define"):
            resolve_encoder({"name": "strftime", "pattern": "%Q"})

    def test_a_trailing_bare_percent_is_refused(self) -> None:
        with pytest.raises(InvalidTypeMapError, match="bare '%'"):
            resolve_encoder({"name": "strftime", "pattern": "%Y%"})

    def test_the_percent_escape_is_still_accepted(self) -> None:
        fn = resolve_encoder({"name": "strftime", "pattern": "%Y%%"})
        assert fn(datetime(2024, 1, 15, tzinfo=timezone.utc)) == "2024%"

    def test_every_documented_directive_is_still_accepted(self) -> None:
        pattern = "".join(f"%{d}" for d in "aAwdbBmyYHIpMSfzZjUWcxXGuV")
        fn = resolve_encoder({"name": "strftime", "pattern": pattern})
        assert fn(datetime(2024, 1, 15, 13, 45, 6, tzinfo=timezone.utc))


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

    def test_an_unpublished_param_is_refused(self) -> None:
        # iso8601 takes no params; a factory only reads the keys it
        # declares, so an unpublished one would otherwise be silently
        # ignored rather than rendering the (wrong) format it names.
        with pytest.raises(InvalidTypeMapError, match="unknown parameter"):
            resolve_encoder({"name": "iso8601", "pattern": "%Y%m%d"})


class TestBoolMapRejectsIdenticalTokens:
    def test_identical_rendered_tokens_are_refused_at_declaration_time(self) -> None:
        # encode() only ever renders true_values[0]/false_values[0]; if
        # those match, True and False would render the identical wire
        # token, which is not a case-of-overlap in the full lists but is
        # exactly as ambiguous.
        with pytest.raises(InvalidTypeMapError, match="both"):
            resolve_encoder(
                {
                    "name": "bool_map",
                    "true_values": ["Y", "yes"],
                    "false_values": ["Y", "no"],
                }
            )
