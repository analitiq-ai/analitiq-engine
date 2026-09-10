"""Tests for the pyarrow-backed decoder functions (issue #503).

Complements ``test_encoding_catalogs.py`` (catalog drift) and
``destination/test_schema_contract.py`` (the ``SchemaContract``-level
behavior): this file is about the decode functions themselves, resolved
directly through :func:`resolve_decoder` rather than through a contract.
"""

from __future__ import annotations

import time

import pyarrow as pa
import pytest

from cdk.type_map.arrow import resolve_decoder
from cdk.type_map.exceptions import InvalidTypeMapError

pytestmark = pytest.mark.unit


class TestBoolMapRejectsOverlappingTokens:
    def test_a_token_present_in_both_value_sets_is_refused_at_declaration_time(
        self,
    ) -> None:
        field = pa.field("t", pa.bool_(), nullable=True)
        with pytest.raises(InvalidTypeMapError, match="appear in both"):
            resolve_decoder(
                {
                    "encoding": {
                        "name": "bool_map",
                        "true_values": ["Y", "yes"],
                        "false_values": ["N", "yes"],
                    }
                },
                field,
            )


class TestIsoDurationPreservesSubMicrosecondPrecision:
    def test_a_nanosecond_fraction_is_not_truncated(self) -> None:
        field = pa.field("d", pa.duration("ns"), nullable=True)
        fn = resolve_decoder({"encoding": {"name": "iso_duration"}}, field)
        result = fn(field, ["PT1.123456789S"])
        # `.value` (raw ticks), not `.to_pylist()` -- pyarrow refuses to
        # materialize a nanosecond Duration as a Python `timedelta` at all
        # (it caps at microseconds), which is exactly the precision this
        # decoder must not lose before the array even reaches that boundary.
        assert result[0].value == 1_123_456_789

    def test_a_sub_nanosecond_fraction_is_refused_not_truncated(self) -> None:
        # int() on the scaled Decimal would otherwise truncate any
        # fractional tick silently, the same class of loss
        # _iso8601_ns_remainder refuses on the Timestamp side.
        field = pa.field("d", pa.duration("ns"), nullable=True)
        fn = resolve_decoder({"encoding": {"name": "iso_duration"}}, field)
        with pytest.raises(ValueError, match="precision finer than"):
            fn(field, ["PT0.0000000009S"])

    def test_a_comma_fractional_separator_is_accepted(self) -> None:
        # ISO-8601 permits "," as well as "." for the fractional separator,
        # the same as the sibling iso8601 decoder.
        field = pa.field("d", pa.duration("ms"), nullable=True)
        fn = resolve_decoder({"encoding": {"name": "iso_duration"}}, field)
        result = fn(field, ["PT0,5S"])
        assert result[0].value == 500


class TestIsoDurationScalesToTheDestinationUnit:
    def test_a_large_day_count_fits_a_coarse_destination_unit(self) -> None:
        # Accumulating through an intermediate nanosecond total before
        # casting down would overflow int64 here even though the value
        # fits comfortably in Duration(SECOND)'s own much wider range.
        field = pa.field("d", pa.duration("s"), nullable=True)
        fn = resolve_decoder({"encoding": {"name": "iso_duration"}}, field)
        result = fn(field, ["P200000D"])
        assert result[0].value == 200_000 * 86400

    def test_precision_finer_than_the_destination_unit_is_refused(self) -> None:
        field = pa.field("d", pa.duration("s"), nullable=True)
        fn = resolve_decoder({"encoding": {"name": "iso_duration"}}, field)
        with pytest.raises(ValueError, match="precision finer than"):
            fn(field, ["PT0.5S"])


class TestIsoDurationAcceptsASign:
    def test_a_leading_minus_negates_the_duration(self) -> None:
        field = pa.field("d", pa.duration("s"), nullable=True)
        fn = resolve_decoder({"encoding": {"name": "iso_duration"}}, field)
        result = fn(field, ["-PT1S", "PT1S"])
        assert [v.value for v in result] == [-1, 1]

    def test_a_leading_minus_on_a_weeks_duration_is_negated(self) -> None:
        field = pa.field("d", pa.duration("s"), nullable=True)
        fn = resolve_decoder({"encoding": {"name": "iso_duration"}}, field)
        result = fn(field, ["-P1W"])
        assert result[0].value == -604800


class TestIsoDurationRejectsComponentFreeStrings:
    def test_a_designator_with_no_component_is_refused(self) -> None:
        field = pa.field("d", pa.duration("s"), nullable=True)
        fn = resolve_decoder({"encoding": {"name": "iso_duration"}}, field)
        with pytest.raises(ValueError, match="not a valid ISO-8601 duration"):
            fn(field, ["P"])

    @pytest.mark.parametrize("value", ["PT", "P1DT"])
    def test_a_dangling_time_designator_is_refused(self, value: str) -> None:
        # "T" introduces an hour/minute/second section; a "T" with nothing
        # after it ("PT" alone, or "P1DT") names no time component.
        field = pa.field("d", pa.duration("s"), nullable=True)
        fn = resolve_decoder({"encoding": {"name": "iso_duration"}}, field)
        with pytest.raises(ValueError, match="not a valid ISO-8601 duration"):
            fn(field, [value])


class TestIsoDurationRejectsCalendarComponents:
    def test_a_year_or_month_component_is_refused(self) -> None:
        # A Duration is a fixed physical length; a calendar year or month
        # is not (a month is 28-31 days depending which one), so neither
        # has a tick count to convert to.
        field = pa.field("d", pa.duration("s"), nullable=True)
        fn = resolve_decoder({"encoding": {"name": "iso_duration"}}, field)
        with pytest.raises(ValueError, match="calendar year/month"):
            fn(field, ["P1Y2M3D"])

    def test_weeks_cannot_combine_with_other_components(self) -> None:
        # ISO-8601: a weeks designator is exclusive of every other one.
        field = pa.field("d", pa.duration("s"), nullable=True)
        fn = resolve_decoder({"encoding": {"name": "iso_duration"}}, field)
        with pytest.raises(ValueError, match="not a valid ISO-8601 duration"):
            fn(field, ["P1W2D"])


class TestIso8601PreservesNanosecondPrecision:
    """``datetime.fromisoformat``/``time.fromisoformat`` cap at microseconds
    and silently drop anything past the sixth fractional digit -- checked
    only against a Timestamp/Time64 column actually declared at nanosecond
    resolution; a coarser column loses nothing by going through them.
    """

    def test_a_nanosecond_fraction_on_a_timestamp_is_not_dropped(self) -> None:
        field = pa.field("t", pa.timestamp("ns", tz="UTC"), nullable=True)
        fn = resolve_decoder({"encoding": {"name": "iso8601"}}, field)
        result = fn(field, ["1970-01-01T00:00:00.123456789+00:00", None])
        assert result[0].value == 123_456_789
        assert result[1].as_py() is None

    def test_a_nanosecond_fraction_on_a_time64_is_not_dropped(self) -> None:
        field = pa.field("t", pa.time64("ns"), nullable=True)
        fn = resolve_decoder({"encoding": {"name": "iso8601"}}, field)
        result = fn(field, ["00:00:00.000000123"])
        assert result[0].value == 123

    def test_a_microsecond_column_is_unaffected(self) -> None:
        field = pa.field("t", pa.timestamp("us", tz="UTC"), nullable=True)
        fn = resolve_decoder({"encoding": {"name": "iso8601"}}, field)
        result = fn(field, ["2024-01-01T00:00:00.123456+00:00"])
        assert result.to_pylist()[0].microsecond == 123456

    def test_a_comma_fractional_separator_is_not_dropped(self) -> None:
        # ISO-8601 permits "," as well as "." for the fractional separator,
        # and datetime.fromisoformat accepts both.
        field = pa.field("t", pa.timestamp("ns", tz="UTC"), nullable=True)
        fn = resolve_decoder({"encoding": {"name": "iso8601"}}, field)
        result = fn(field, ["1970-01-01T00:00:00,123456789+00:00"])
        assert result[0].value == 123_456_789

    def test_precision_finer_than_nanoseconds_is_refused_not_dropped(self) -> None:
        # fromisoformat drops everything past microseconds silently, and
        # digits 7-9 are added back explicitly -- a 10th+ digit has nowhere
        # to go and must be refused rather than dropped the same way.
        field = pa.field("t", pa.timestamp("ns", tz="UTC"), nullable=True)
        fn = resolve_decoder({"encoding": {"name": "iso8601"}}, field)
        with pytest.raises(ValueError, match="finer than nanoseconds"):
            fn(field, ["1970-01-01T00:00:00.0000000009+00:00"])


class TestEpochDecoderPreservesTheInstantAcrossTargetZones:
    """Epoch ticks are an absolute UTC instant. Decoding into a Timestamp
    column with a non-UTC tz must shift the wall-clock time to that zone,
    never reinterpret the tick count as already being local time there --
    that would silently move the represented instant by the zone's offset.
    """

    def test_epoch_zero_into_a_non_utc_column_is_the_correct_local_instant(
        self,
    ) -> None:
        field = pa.field("t", pa.timestamp("s", tz="America/New_York"), nullable=True)
        fn = resolve_decoder({"encoding": {"name": "epoch", "unit": "SECOND"}}, field)
        result = fn(field, [0])
        # 1970-01-01T00:00:00Z is 1969-12-31T19:00:00 in America/New_York
        # (UTC-5, no DST in effect at that date).
        assert result.to_pylist()[0].isoformat() == "1969-12-31T19:00:00-05:00"


class TestRegexEpochUsesRE2NotBacktrackingRe:
    """``pattern`` is endpoint-authored, untrusted input matched against
    every row of every batch -- it must be compiled and matched with
    ``re2`` (linear-time, cannot backtrack), the same policy issue #504
    already applies to every other author-declared regex in this engine.
    """

    def test_a_catastrophic_backtracking_shaped_pattern_stays_linear(self) -> None:
        # ^(a+)+$ against "aaa...aX" is the textbook case: exponential under
        # Python's backtracking `re` (unusable well before 40 characters),
        # bounded under RE2 regardless of input length.
        field = pa.field("t", pa.timestamp("s"), nullable=True)
        fn = resolve_decoder(
            {
                "encoding": {
                    "name": "regex_epoch",
                    "pattern": r"^(a+)+$",
                    "unit": "SECOND",
                }
            },
            field,
        )
        adversarial_input = "a" * 60 + "X"

        start = time.monotonic()
        with pytest.raises(ValueError, match="does not match"):
            fn(field, [adversarial_input])
        elapsed = time.monotonic() - start

        assert elapsed < 1.0, (
            f"regex_epoch took {elapsed:.3f}s against a catastrophic-"
            f"backtracking-shaped pattern; it must be bounded by RE2, not "
            f"exponential under stdlib re"
        )

    def test_an_uncompilable_pattern_is_refused_at_declaration_time(self) -> None:
        field = pa.field("t", pa.timestamp("s"), nullable=True)
        with pytest.raises(InvalidTypeMapError, match="not a valid regular expression"):
            resolve_decoder(
                {
                    "encoding": {
                        "name": "regex_epoch",
                        "pattern": "(unclosed",
                        "unit": "SECOND",
                    }
                },
                field,
            )

    def test_wrong_capture_group_count_is_refused_at_declaration_time(self) -> None:
        field = pa.field("t", pa.timestamp("s"), nullable=True)
        with pytest.raises(InvalidTypeMapError, match="exactly one capture group"):
            resolve_decoder(
                {
                    "encoding": {
                        "name": "regex_epoch",
                        "pattern": r"(\d+)-(\d+)",
                        "unit": "SECOND",
                    }
                },
                field,
            )

    def test_xero_shaped_pattern_still_decodes_correctly(self) -> None:
        # RE2 accepts the exact pattern the Xero wire format needs: named
        # groups aren't required, and the non-capturing offset suffix is
        # matched but never returned.
        field = pa.field("updated", pa.timestamp("us", tz="UTC"), nullable=True)
        fn = resolve_decoder(
            {
                "encoding": {
                    "name": "regex_epoch",
                    "pattern": r"/Date\((\d+)(?:[+-]\d{4})?\)/",
                    "unit": "MILLISECOND",
                }
            },
            field,
        )
        result = fn(field, ["/Date(1541176290160+0000)/", None])
        assert result.to_pylist()[0].isoformat() == "2018-11-02T16:31:30.160000+00:00"
        assert result.to_pylist()[1] is None


class TestResolveDecoderRejectsUnknownParams:
    def test_a_param_no_factory_reads_is_refused(self) -> None:
        # iso8601 takes no params; a factory only reads the keys it
        # declares, so an unpublished one would otherwise be silently
        # ignored rather than applying the (wrong) format it names.
        field = pa.field("t", pa.timestamp("us", tz="UTC"), nullable=True)
        with pytest.raises(InvalidTypeMapError, match="unknown parameter"):
            resolve_decoder(
                {"encoding": {"name": "iso8601", "pattern": "%Y%m%d"}}, field
            )

    def test_a_misspelled_required_param_is_refused_by_name(self) -> None:
        field = pa.field("t", pa.timestamp("us", tz="UTC"), nullable=True)
        with pytest.raises(InvalidTypeMapError, match="unknown parameter"):
            resolve_decoder({"encoding": {"name": "epoch", "units": "SECOND"}}, field)


class TestDecimalDecoderRaisesValueErrorNotInvalidOperation:
    def test_a_non_decimal_token_raises_value_error(self) -> None:
        # decimal.InvalidOperation is neither a ValueError nor a
        # TypeMapError -- left uncaught, the worker's deterministic-error
        # classifier would never recognize it, misclassifying a repeatable
        # bad-data failure as retryable.
        field = pa.field("amount", pa.decimal128(10, 2), nullable=True)
        fn = resolve_decoder({"encoding": {"name": "decimal"}}, field)
        with pytest.raises(ValueError, match="not a valid decimal"):
            fn(field, ["unknown"])
