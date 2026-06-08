"""End-to-end security integration tests."""

import os
import pytest

from src.engine.data_transformer import DataTransformer
from src.engine.expression_evaluator import SecureExpressionEvaluator


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

    @pytest.mark.asyncio
    async def test_environment_variable_security(self):
        """SecureExpressionEvaluator resolves safe env-var placeholders and
        treats malformed patterns as static strings."""
        evaluator = SecureExpressionEvaluator()

        os.environ["SAFE_VAR"] = "safe_value"

        result = await evaluator.evaluate("${SAFE_VAR}", {}, {})
        assert result == "safe_value"

        unsafe_patterns = [
            "${PATH}/../../../etc/passwd",
            "${HOME}; rm -rf /",
            "${USER}; echo injected",
            "${}",
            "${123}",
            "${invalid-name}",
        ]

        for pattern in unsafe_patterns:
            result = await evaluator.evaluate(pattern, {}, {})
            assert result == pattern

    @pytest.mark.asyncio
    async def test_json_injection_prevention(self):
        """SecureExpressionEvaluator parses valid JSON and rejects invalid JSON."""
        evaluator = SecureExpressionEvaluator()

        safe_json = '{"id": "123", "name": "test"}'
        result = await evaluator.evaluate(safe_json, {}, {})
        assert result == {"id": "123", "name": "test"}

        invalid_json = '{"invalid": json syntax}'
        with pytest.raises(ValueError):
            await evaluator.evaluate(invalid_json, {}, {})

    @pytest.mark.asyncio
    async def test_concat_function_security(self):
        """Concat in SecureExpressionEvaluator treats unresolved names as literals."""
        evaluator = SecureExpressionEvaluator()
        record = {"safe_field": "safe_value", "user_id": 123}

        result = await evaluator.evaluate("concat(user_id, '-', safe_field)", {}, record)
        assert result == "123-safe_value"

        result = await evaluator.evaluate("concat(missing_field, '-', safe_field)", {}, record)
        assert result == "missing_field-safe_value"

        result = await evaluator.evaluate("concat()", {}, record)
        assert result == ""

    @pytest.mark.asyncio
    async def test_function_isolation(self):
        """SecureExpressionEvaluator allows only pre-approved functions."""
        evaluator = SecureExpressionEvaluator()

        allowed_functions = ["now()", "today()", "uuid()"]
        for func in allowed_functions:
            result = await evaluator.evaluate(func, {}, {})
            assert result is not None
            assert isinstance(result, str)

        disallowed_functions = [
            "eval()", "exec()", "import()", "open()",
            "system()", "__import__()", "subprocess()",
            "compile()", "globals()", "locals()",
        ]
        for func in disallowed_functions:
            result = await evaluator.evaluate(func, {}, {})
            assert result == func
