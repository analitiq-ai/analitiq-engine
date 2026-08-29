"""The batch-level slot ``response.metadata`` rides in (issue #469)."""

from __future__ import annotations

from decimal import Decimal

import pyarrow as pa
import pytest

from cdk.batch_metadata import (
    RESPONSE_METADATA_KEY,
    response_metadata_of,
    with_response_metadata,
)
from src.worker.readable import _decode_arrow_ipc
from src.worker.source_service import _encode_arrow_ipc

pytestmark = pytest.mark.unit


def _ipc_round_trip(batch: pa.RecordBatch) -> pa.RecordBatch:
    # The production hop: the source worker's encoder to the engine's decoder.
    return _decode_arrow_ipc(_encode_arrow_ipc(batch))


class TestTheSlot:
    def test_a_batch_without_it_answers_none(self) -> None:
        assert response_metadata_of(pa.RecordBatch.from_pylist([{"a": 1}])) is None

    def test_values_survive_the_ipc_hop(self) -> None:
        # The IPC bytes are what cross the process boundary; the slot is
        # only a slot if the far side reads back what the read wrote.
        batch = with_response_metadata(
            pa.RecordBatch.from_pylist([{"a": 1}]),
            {"total": 3, "label": "x", "missing": None},
        )
        assert response_metadata_of(_ipc_round_trip(batch)) == {
            "total": 3,
            "label": "x",
            "missing": None,
        }

    def test_the_empty_last_page_keeps_its_values_across_the_hop(self) -> None:
        # The page that ends a read has no rows; the decoder rebuilds a
        # zero-row batch from the table, and the slot must survive that.
        batch = with_response_metadata(
            pa.RecordBatch.from_pylist([], schema=pa.schema([("a", pa.int64())])),
            {"total": 0},
        )
        decoded = _ipc_round_trip(batch)
        assert decoded.num_rows == 0
        assert response_metadata_of(decoded) == {"total": 0}

    def test_an_empty_declared_block_is_a_dict_not_none(self) -> None:
        batch = with_response_metadata(pa.RecordBatch.from_pylist([{"a": 1}]), {})
        assert response_metadata_of(batch) == {}

    def test_other_schema_metadata_is_kept(self) -> None:
        batch = pa.RecordBatch.from_pylist([{"a": 1}]).replace_schema_metadata(
            {b"other": b"kept"}
        )
        stamped = with_response_metadata(batch, {"total": 1})
        assert stamped.schema.metadata[b"other"] == b"kept"
        assert RESPONSE_METADATA_KEY in stamped.schema.metadata

    def test_a_decimal_lands_as_a_json_number(self) -> None:
        # The same narrowing a Json column applies: the lossless parse made
        # it a Decimal, the document keeps it a number.
        batch = with_response_metadata(
            pa.RecordBatch.from_pylist([{"a": 1}]), {"ratio": Decimal("0.25")}
        )
        assert response_metadata_of(batch) == {"ratio": 0.25}

    @pytest.mark.parametrize(
        "value",
        [
            Decimal("1e400"),
            Decimal("NaN"),
            Decimal("Infinity"),
            float("nan"),
            float("-inf"),
            {"nested": [1, float("inf")]},
        ],
    )
    def test_a_non_finite_number_anywhere_is_refused(self, value) -> None:
        # 1e400 narrows to infinity; a permissive parser hands NaN through
        # as a float. Neither is a token JSON can spell, at any depth.
        with pytest.raises(ValueError, match="not JSON compliant"):
            with_response_metadata(pa.RecordBatch.from_pylist([{"a": 1}]), {"v": value})

    def test_a_value_json_cannot_carry_fails_loud(self) -> None:
        with pytest.raises(TypeError, match="not JSON serializable"):
            with_response_metadata(
                pa.RecordBatch.from_pylist([{"a": 1}]), {"when": object()}
            )

    def test_a_slot_holding_no_object_fails_loud(self) -> None:
        batch = pa.RecordBatch.from_pylist([{"a": 1}]).replace_schema_metadata(
            {RESPONSE_METADATA_KEY: b"[1, 2]"}
        )
        with pytest.raises(ValueError, match="not an object"):
            response_metadata_of(batch)
