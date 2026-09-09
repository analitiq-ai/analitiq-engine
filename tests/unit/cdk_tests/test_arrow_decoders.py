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


class TestIsoDurationRejectsComponentFreeStrings:
    @pytest.mark.parametrize("value", ["P", "PT"])
    def test_a_designator_with_no_component_is_refused(self, value: str) -> None:
        # Every component group is individually optional (so "P3D" and
        # "PT30S" each parse fine alone), which also makes bare "P"/"PT"
        # fullmatch with every group absent -- neither names an actual
        # duration component, and ISO-8601 requires at least one.
        field = pa.field("d", pa.duration("s"), nullable=True)
        fn = resolve_decoder({"encoding": {"name": "iso_duration"}}, field)
        with pytest.raises(ValueError, match="no duration component"):
            fn(field, [value])


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
