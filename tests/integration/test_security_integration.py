"""End-to-end security integration tests."""

import pytest

from src.engine.data_transformer import DataTransformer


def _assignment(target_path, expr=None, const=None, nullable=True):
    target = {"path": target_path if isinstance(target_path, list) else [target_path], "nullable": nullable}
    if const is not None:
        value = {"kind": "const", "const": {"value": const}}
    else:
        value = {"kind": "expr", "expr": expr}
    return {"target": target, "value": value}


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


class TestSecurityIntegration:
    """End-to-end security tests for the full transformation pipeline."""

    @pytest.mark.asyncio
    async def test_wise_to_sevdesk_transformation(self, sample_wise_record):
        """Complete Wise→SevDesk transformation using the assignments format."""
        transformer = DataTransformer()
        config = {
            "mapping": {
                "assignments": [
                    _assignment("valueDate", nullable=False,
                                expr=_pipe("created", "iso_to_date")),
                    _assignment("amount", nullable=False,
                                expr=_get("targetValue")),
                    _assignment("paymtPurpose", nullable=False,
                                expr=_get("id")),
                    _assignment("objectName",
                                const="CheckAccountTransaction"),
                    _assignment("checkAccount",
                                const={"id": "5936402", "objectName": "CheckAccount"}),
                    _assignment("status", const="100"),
                ]
            }
        }

        result = await transformer.apply_transformations([sample_wise_record], config)

        assert len(result) == 1
        t = result[0]
        assert t["valueDate"] == "2025-08-16"
        assert t["amount"] == 100.50
        assert t["paymtPurpose"] == 123456
        assert t["objectName"] == "CheckAccountTransaction"
        assert t["checkAccount"] == {"id": "5936402", "objectName": "CheckAccount"}
        assert t["status"] == "100"

    @pytest.mark.asyncio
    async def test_batch_processing(self):
        """All records in a batch are transformed correctly."""
        transformer = DataTransformer()
        batch = [
            {"id": 1, "data": "value_one"},
            {"id": 2, "data": "value_two"},
            {"id": 3, "data": "value_three"},
            {"id": 4, "data": "value_four"},
        ]
        config = {
            "mapping": {
                "assignments": [
                    _assignment("record_id", expr=_get("id")),
                    _assignment("payload", expr=_get("data")),
                ]
            }
        }

        result = await transformer.apply_transformations(batch, config)

        assert len(result) == 4
        for i, record in enumerate(result):
            assert record["record_id"] == i + 1
