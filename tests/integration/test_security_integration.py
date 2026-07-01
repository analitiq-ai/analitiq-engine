"""End-to-end transformation integration tests.

Ported to the vectorized transform: a stream's assignments are compiled once
with ``compile_transform`` and applied to a ``pa.RecordBatch`` with ``.run``.
Every target declares its ``arrow_type``; the source batch is built from the
record dict and the transformed columns are compared to the expected values.
"""

import pyarrow as pa
import pytest

from src.engine.data_transformer import compile_transform
from src.engine.exceptions import TransformationError


def _get(path):
    return {"op": "get", "path": path if isinstance(path, list) else [path]}


def _pipe(source_path, fn_name):
    return {
        "op": "pipe",
        "args": [
            _get(source_path),
            {"op": "fn", "name": fn_name, "version": 1, "args": []},
        ],
    }


def _target(name, arrow_type, nullable=True, **extra):
    t = {"path": [name], "arrow_type": arrow_type, "nullable": nullable}
    t.update(extra)
    return t


def _expr_assignment(name, arrow_type, expr, nullable=True, **target_extra):
    return {
        "target": _target(name, arrow_type, nullable, **target_extra),
        "value": {"kind": "expr", "expr": expr},
    }


def _const_assignment(name, arrow_type, value, nullable=True, **target_extra):
    return {
        "target": _target(name, arrow_type, nullable, **target_extra),
        "value": {"kind": "const", "const": {"value": value}},
    }


_CHECK_ACCOUNT_PROPERTIES = {
    "id": {"arrow_type": "Utf8"},
    "objectName": {"arrow_type": "Utf8"},
}


def _sevdesk_non_date_assignments():
    """The Wise->SevDesk mapping minus the iso_to_date-derived valueDate."""
    return [
        _expr_assignment("amount", "Float64", _get("targetValue"), nullable=False),
        _expr_assignment("paymtPurpose", "Int64", _get("id"), nullable=False),
        _const_assignment("objectName", "Utf8", "CheckAccountTransaction"),
        _const_assignment(
            "checkAccount",
            "Object",
            {"id": "5936402", "objectName": "CheckAccount"},
            properties=_CHECK_ACCOUNT_PROPERTIES,
        ),
        _const_assignment("status", "Utf8", "100"),
    ]


class TestSecurityIntegration:
    """End-to-end transformation tests for the full pipeline."""

    def test_wise_to_sevdesk_non_date_fields(self, sample_wise_record):
        """The non-date fields of the Wise->SevDesk mapping transform exactly:
        a float amount and int purpose pass through their identity types, and
        the const string / nested-object / status fields broadcast per row."""
        batch = pa.RecordBatch.from_pylist([sample_wise_record])
        out = compile_transform(_sevdesk_non_date_assignments()).run(batch).to_pylist()

        assert len(out) == 1
        t = out[0]
        assert t["amount"] == 100.50
        assert t["paymtPurpose"] == 123456
        assert t["objectName"] == "CheckAccountTransaction"
        assert t["checkAccount"] == {"id": "5936402", "objectName": "CheckAccount"}
        assert t["status"] == "100"

    def test_wise_to_sevdesk_transformation(self, sample_wise_record):
        """Complete Wise->SevDesk transformation including the iso_to_date
        valueDate derived from the tz-suffixed Wise 'created' value."""
        assignments = [
            _expr_assignment(
                "valueDate", "Utf8", _pipe("created", "iso_to_date"), nullable=False
            ),
            *_sevdesk_non_date_assignments(),
        ]
        batch = pa.RecordBatch.from_pylist([sample_wise_record])
        out = compile_transform(assignments).run(batch).to_pylist()

        t = out[0]
        assert t["valueDate"] == "2025-08-16"
        assert t["amount"] == 100.50
        assert t["paymtPurpose"] == 123456
        assert t["objectName"] == "CheckAccountTransaction"
        assert t["checkAccount"] == {"id": "5936402", "objectName": "CheckAccount"}
        assert t["status"] == "100"

    def test_batch_processing(self):
        """All records in a batch are transformed; row order is preserved."""
        batch = pa.RecordBatch.from_pylist(
            [
                {"id": 1, "data": "value_one"},
                {"id": 2, "data": "value_two"},
                {"id": 3, "data": "value_three"},
                {"id": 4, "data": "value_four"},
            ]
        )
        assignments = [
            _expr_assignment("record_id", "Int64", _get("id")),
            _expr_assignment("payload", "Utf8", _get("data")),
        ]
        out = compile_transform(assignments).run(batch).to_pylist()

        assert len(out) == 4
        for i, record in enumerate(out):
            assert record["record_id"] == i + 1
            assert record["payload"] == f"value_{['one', 'two', 'three', 'four'][i]}"

    def test_non_nullable_value_date_with_null_source_fails(self):
        """An absent non-nullable valueDate source fails the whole batch -- the
        transform never silently emits a null where the contract forbids one."""
        batch = pa.RecordBatch.from_pylist([{"targetValue": 1.0, "id": 7}])
        assignments = [
            _expr_assignment("valueDate", "Utf8", _get("created"), nullable=False),
            *_sevdesk_non_date_assignments(),
        ]
        with pytest.raises(TransformationError, match="not nullable"):
            compile_transform(assignments).run(batch)
