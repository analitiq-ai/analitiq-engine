"""Unit tests for DataTransformer - assignments format only."""

import pytest

from src.engine.data_transformer import DataTransformer
from src.engine.exceptions import TransformationError


def _assignment(target_path, expr=None, const=None, nullable=True):
    """Build an assignment dict in transformer shape."""
    target = {"path": target_path if isinstance(target_path, list) else [target_path], "nullable": nullable}
    if const is not None:
        value = {"kind": "const", "const": {"value": const}}
    else:
        value = {"kind": "expr", "expr": expr}
    return {"target": target, "value": value}


def _get(path):
    """Build a get-expression for the given field path."""
    return {"op": "get", "path": path if isinstance(path, list) else [path]}


def _pipe(source_path, fn_name):
    """Build a pipe expression: get source_path, apply fn_name."""
    return {
        "op": "pipe",
        "args": [
            _get(source_path),
            {"op": "fn", "name": fn_name, "version": 1, "args": []},
        ],
    }


class TestDataTransformer:
    """Test suite for DataTransformer."""

    @pytest.fixture
    def transformer(self):
        return DataTransformer()

    @pytest.fixture
    def sample_batch(self):
        return [
            {
                "id": 123456,
                "created": "2025-08-16T10:30:00Z",
                "targetValue": 100.50,
                "targetCurrency": "EUR",
                "details": {
                    "reference": "Payment for services",
                    "merchant": {"name": "Test Merchant"},
                },
            },
            {
                "id": 789012,
                "created": "2025-08-16T11:00:00Z",
                "targetValue": 250.75,
                "targetCurrency": "USD",
                "details": {
                    "reference": "Invoice payment",
                    "merchant": {"name": "Another Merchant"},
                },
            },
        ]

    @pytest.mark.asyncio
    async def test_no_transformations(self, transformer, sample_batch):
        """Batch passes through unchanged when no assignments configured."""
        result = await transformer.apply_transformations(sample_batch, {"mapping": {}})
        assert result == sample_batch

    @pytest.mark.asyncio
    async def test_field_rename(self, transformer, sample_batch):
        """Simple field rename: source key appears under target name."""
        config = {
            "mapping": {
                "assignments": [
                    _assignment("transaction_id", expr=_get("id")),
                    _assignment("amount", expr=_get("targetValue")),
                    _assignment("currency", expr=_get("targetCurrency")),
                ]
            }
        }
        result = await transformer.apply_transformations(sample_batch, config)

        assert len(result) == 2
        assert result[0]["transaction_id"] == 123456
        assert result[0]["amount"] == 100.50
        assert result[0]["currency"] == "EUR"
        assert result[1]["transaction_id"] == 789012
        assert result[1]["amount"] == 250.75
        assert result[1]["currency"] == "USD"

    @pytest.mark.asyncio
    async def test_field_with_function_transform(self, transformer, sample_batch):
        """Pipe expression applies a named function to the source value."""
        config = {
            "mapping": {
                "assignments": [
                    _assignment("date", expr=_pipe("created", "iso_to_date")),
                    _assignment("amount", expr=_get("targetValue")),
                    _assignment("currency_code", expr=_pipe("targetCurrency", "lower")),
                ]
            }
        }
        result = await transformer.apply_transformations(sample_batch, config)

        assert result[0]["date"] == "2025-08-16"
        assert result[0]["amount"] == 100.50
        assert result[0]["currency_code"] == "eur"

    @pytest.mark.asyncio
    async def test_nested_source_field_access(self, transformer, sample_batch):
        """Multi-element path descends into nested dicts."""
        config = {
            "mapping": {
                "assignments": [
                    _assignment("payment_reference", expr=_get(["details", "reference"])),
                    _assignment("merchant_name", expr=_get(["details", "merchant", "name"])),
                ]
            }
        }
        result = await transformer.apply_transformations(sample_batch, config)

        assert result[0]["payment_reference"] == "Payment for services"
        assert result[0]["merchant_name"] == "Test Merchant"
        assert result[1]["payment_reference"] == "Invoice payment"
        assert result[1]["merchant_name"] == "Another Merchant"

    @pytest.mark.asyncio
    async def test_const_value_assignment(self, transformer, sample_batch):
        """Const assignment writes the same literal value for every record."""
        config = {
            "mapping": {
                "assignments": [
                    _assignment("object_name", const="Transaction"),
                    _assignment("status_code", const=42),
                ]
            }
        }
        result = await transformer.apply_transformations(sample_batch, config)

        assert result[0]["object_name"] == "Transaction"
        assert result[0]["status_code"] == 42
        assert result[1]["object_name"] == "Transaction"
        assert result[1]["status_code"] == 42

    @pytest.mark.asyncio
    async def test_all_function_types(self, transformer):
        """Exercise every function in the AssignmentTransformer catalog."""
        batch = [
            {
                "text": "  Hello World  ",
                "neg": -42.5,
                "date_str": "2025-08-16T10:30:00Z",
                "str_num": "123.45",
                "float_num": 67.89,
            }
        ]
        config = {
            "mapping": {
                "assignments": [
                    _assignment("trimmed", expr=_pipe("text", "trim")),
                    _assignment("lowered", expr=_pipe("text", "lower")),
                    _assignment("uppered", expr=_pipe("text", "upper")),
                    _assignment("positive", expr=_pipe("neg", "abs")),
                    _assignment("as_date", expr=_pipe("date_str", "iso_to_date")),
                    _assignment("as_float", expr=_pipe("str_num", "to_float")),
                    _assignment("as_int", expr=_pipe("float_num", "to_int")),
                    _assignment("as_str", expr=_pipe("neg", "to_string")),
                ]
            }
        }
        result = await transformer.apply_transformations(batch, config)

        assert result[0]["trimmed"] == "Hello World"
        assert result[0]["lowered"] == "  hello world  "
        assert result[0]["uppered"] == "  HELLO WORLD  "
        assert result[0]["positive"] == 42.5
        assert result[0]["as_date"] == "2025-08-16"
        assert result[0]["as_float"] == 123.45
        assert result[0]["as_int"] == 67
        assert result[0]["as_str"] == "-42.5"

    @pytest.mark.asyncio
    async def test_missing_source_field_returns_none(self, transformer, sample_batch):
        """A get on an absent path produces None; nullable target keeps it."""
        config = {
            "mapping": {
                "assignments": [
                    _assignment("should_be_none", nullable=True,
                                expr=_get(["missing", "nested", "field"])),
                    _assignment("also_none", nullable=True,
                                expr=_get(["details", "missing_field"])),
                ]
            }
        }
        result = await transformer.apply_transformations(sample_batch, config)

        assert result[0]["should_be_none"] is None
        assert result[0]["also_none"] is None

    @pytest.mark.asyncio
    async def test_nonnullable_field_raises_on_null(self, transformer):
        """A null value on a non-nullable target raises TransformationError."""
        batch = [{"id": None}]
        config = {
            "mapping": {
                "assignments": [
                    _assignment("id", nullable=False, expr=_get("id")),
                ]
            }
        }
        with pytest.raises(TransformationError):
            await transformer.apply_transformations(batch, config)

    @pytest.mark.asyncio
    async def test_to_int_raises_on_unparseable(self, transformer):
        """_fn_to_int raises on non-numeric input (#183); the batch wrapper
        surfaces it as TransformationError for engine DLQ routing."""
        batch = [{"val": "abc"}]
        config = {"mapping": {"assignments": [_assignment("out", expr=_pipe("val", "to_int"))]}}
        with pytest.raises(TransformationError, match="to_int"):
            await transformer.apply_transformations(batch, config)

    @pytest.mark.asyncio
    async def test_to_float_raises_on_unparseable(self, transformer):
        """_fn_to_float raises on non-numeric input (#183); the batch wrapper
        surfaces it as TransformationError for engine DLQ routing."""
        batch = [{"val": "xyz"}]
        config = {"mapping": {"assignments": [_assignment("out", expr=_pipe("val", "to_float"))]}}
        with pytest.raises(TransformationError, match="to_float"):
            await transformer.apply_transformations(batch, config)

    @pytest.mark.asyncio
    async def test_legacy_keys_emit_warning(self, transformer, sample_batch):
        """apply_transformations warns and returns batch unchanged when legacy keys are present."""
        from unittest.mock import patch
        import sys
        _mod = sys.modules[DataTransformer.__module__]

        config = {"mapping": {"field_mappings": {"id": "transaction_id"}}}
        with patch.object(_mod.logger, "warning") as mock_warn:
            result = await transformer.apply_transformations(sample_batch, config)
        assert result == sample_batch
        mock_warn.assert_called_once()
        assert "field_mappings" in mock_warn.call_args[0][1]

    @pytest.mark.asyncio
    async def test_legacy_keys_warn_even_with_assignments_present(self, transformer):
        """The legacy-key warning fires even when valid assignments accompany the stray keys."""
        from unittest.mock import patch
        import sys
        _mod = sys.modules[DataTransformer.__module__]

        batch = [{"id": 1}]
        config = {
            "mapping": {
                "assignments": [_assignment("out", expr=_get("id"))],
                "computed_fields": {"x": "1 + 1"},
            }
        }
        with patch.object(_mod.logger, "warning") as mock_warn:
            result = await transformer.apply_transformations(batch, config)
        assert result[0]["out"] == 1
        mock_warn.assert_called_once()
        assert "computed_fields" in mock_warn.call_args[0][1]

    @pytest.mark.asyncio
    async def test_iso_date_function_edge_cases(self, transformer):
        """iso_to_date converts ISO variants and raises on invalid input (#184)."""
        batch = [
            {
                "d_utc_z": "2025-08-16T10:30:00Z",
                "d_utc_off": "2025-08-16T10:30:00+00:00",
                "d_tz_off": "2025-08-16T10:30:00+02:00",
                "d_none": None,
            }
        ]
        config = {
            "mapping": {
                "assignments": [
                    _assignment("r_utc_z", expr=_pipe("d_utc_z", "iso_to_date")),
                    _assignment("r_utc_off", expr=_pipe("d_utc_off", "iso_to_date")),
                    _assignment("r_tz_off", expr=_pipe("d_tz_off", "iso_to_date")),
                    _assignment("r_none", expr=_pipe("d_none", "iso_to_date")),
                ]
            }
        }
        result = await transformer.apply_transformations(batch, config)

        assert result[0]["r_utc_z"] == "2025-08-16"
        assert result[0]["r_utc_off"] == "2025-08-16"
        assert result[0]["r_tz_off"] == "2025-08-16"
        assert result[0]["r_none"] is None

    @pytest.mark.asyncio
    async def test_iso_date_raises_on_invalid_input(self, transformer):
        """Invalid input no longer passes through unchanged (#184): the
        batch wrapper surfaces the parse failure as TransformationError."""
        batch = [{"d_bad": "invalid-date"}]
        config = {
            "mapping": {
                "assignments": [
                    _assignment("r_bad", expr=_pipe("d_bad", "iso_to_date")),
                ]
            }
        }
        with pytest.raises(TransformationError, match="iso_to_date"):
            await transformer.apply_transformations(batch, config)

    @pytest.mark.asyncio
    async def test_concat_with_args_returns_correct_result(self, transformer):
        """Mixed get/const args are joined in order."""
        batch = [{"first": "hello", "second": "world"}]
        config = {
            "mapping": {
                "assignments": [
                    _assignment("out", expr={
                        "op": "concat",
                        "args": [_get("first"), {"op": "const", "value": " "}, _get("second")],
                    })
                ]
            }
        }
        result = await transformer.apply_transformations(batch, config)
        assert result[0]["out"] == "hello world"

    @pytest.mark.asyncio
    async def test_concat_skips_none_valued_args(self, transformer):
        """Args that evaluate to None are dropped; surrounding args still join."""
        batch = [{"a": "hello", "b": None, "c": "world"}]
        config = {
            "mapping": {
                "assignments": [
                    _assignment("out", expr={
                        "op": "concat",
                        "args": [_get("a"), _get("b"), _get("c")],
                    })
                ]
            }
        }
        result = await transformer.apply_transformations(batch, config)
        assert result[0]["out"] == "helloworld"

    @pytest.mark.asyncio
    async def test_concat_empty_args_raises(self, transformer):
        """Zero args is always a builder bug; raises rather than silently returning empty string."""
        batch = [{"x": 1}]
        config = {
            "mapping": {
                "assignments": [
                    _assignment("out", expr={"op": "concat", "args": []})
                ]
            }
        }
        with pytest.raises(TransformationError, match="concat.*requires at least 1 arg"):
            await transformer.apply_transformations(batch, config)

    @pytest.mark.asyncio
    async def test_coalesce_with_args_returns_first_non_none(self, transformer):
        """Leading None args are skipped; the first non-None value is returned."""
        batch = [{"a": None, "b": None, "c": "found"}]
        config = {
            "mapping": {
                "assignments": [
                    _assignment("out", expr={
                        "op": "coalesce",
                        "args": [_get("a"), _get("b"), _get("c")],
                    })
                ]
            }
        }
        result = await transformer.apply_transformations(batch, config)
        assert result[0]["out"] == "found"

    @pytest.mark.asyncio
    async def test_coalesce_all_none_returns_none(self, transformer):
        """All-None args is legitimate — None propagates to the nullable check downstream."""
        batch = [{"a": None, "b": None}]
        config = {
            "mapping": {
                "assignments": [
                    _assignment("out", expr={
                        "op": "coalesce",
                        "args": [_get("a"), _get("b")],
                    })
                ]
            }
        }
        result = await transformer.apply_transformations(batch, config)
        assert result[0]["out"] is None

    @pytest.mark.asyncio
    async def test_coalesce_empty_args_raises(self, transformer):
        """Zero args has no meaningful return value; raises rather than silently producing None."""
        batch = [{"x": 1}]
        config = {
            "mapping": {
                "assignments": [
                    _assignment("out", expr={"op": "coalesce", "args": []})
                ]
            }
        }
        with pytest.raises(TransformationError, match="coalesce.*requires at least 1 arg"):
            await transformer.apply_transformations(batch, config)

    @pytest.mark.asyncio
    async def test_and_empty_args_raises(self, transformer):
        """Empty args list would silently return True, masking a misconfigured filter; guard raises instead."""
        batch = [{"x": 1}]
        config = {
            "mapping": {
                "assignments": [
                    _assignment("out", expr={"op": "and", "args": []}),
                ]
            }
        }
        with pytest.raises(TransformationError, match="and expression requires at least 1 arg"):
            await transformer.apply_transformations(batch, config)

    @pytest.mark.asyncio
    async def test_and_missing_args_key_raises(self, transformer):
        """Absent args key is treated identically to an empty list; same guard fires."""
        batch = [{"x": 1}]
        config = {
            "mapping": {
                "assignments": [
                    _assignment("out", expr={"op": "and"}),
                ]
            }
        }
        with pytest.raises(TransformationError, match="and expression requires at least 1 arg"):
            await transformer.apply_transformations(batch, config)

    @pytest.mark.asyncio
    async def test_or_empty_args_raises(self, transformer):
        """Empty args list would silently return False, blocking every record; guard raises instead."""
        batch = [{"x": 1}]
        config = {
            "mapping": {
                "assignments": [
                    _assignment("out", expr={"op": "or", "args": []}),
                ]
            }
        }
        with pytest.raises(TransformationError, match="or expression requires at least 1 arg"):
            await transformer.apply_transformations(batch, config)

    @pytest.mark.asyncio
    async def test_or_missing_args_key_raises(self, transformer):
        """Absent args key is treated identically to an empty list; same guard fires."""
        batch = [{"x": 1}]
        config = {
            "mapping": {
                "assignments": [
                    _assignment("out", expr={"op": "or"}),
                ]
            }
        }
        with pytest.raises(TransformationError, match="or expression requires at least 1 arg"):
            await transformer.apply_transformations(batch, config)

    @pytest.mark.asyncio
    async def test_and_all_true_returns_true(self, transformer):
        batch = [{"a": True}]
        true_expr = {"op": "get", "path": ["a"]}
        config = {
            "mapping": {
                "assignments": [_assignment("out", expr={"op": "and", "args": [true_expr, true_expr]})]
            }
        }
        result = await transformer.apply_transformations(batch, config)
        assert result[0]["out"] is True

    @pytest.mark.asyncio
    async def test_and_short_circuits_on_false(self, transformer):
        batch = [{"a": True, "b": False}]
        config = {
            "mapping": {
                "assignments": [_assignment("out", expr={"op": "and", "args": [
                    {"op": "get", "path": ["a"]},
                    {"op": "get", "path": ["b"]},
                ]})]
            }
        }
        result = await transformer.apply_transformations(batch, config)
        assert result[0]["out"] is False

    @pytest.mark.asyncio
    async def test_or_short_circuits_on_true(self, transformer):
        batch = [{"a": True, "b": False}]
        config = {
            "mapping": {
                "assignments": [_assignment("out", expr={"op": "or", "args": [
                    {"op": "get", "path": ["b"]},
                    {"op": "get", "path": ["a"]},
                ]})]
            }
        }
        result = await transformer.apply_transformations(batch, config)
        assert result[0]["out"] is True

    @pytest.mark.asyncio
    async def test_or_all_false_returns_false(self, transformer):
        batch = [{"b": False}]
        false_expr = {"op": "get", "path": ["b"]}
        config = {
            "mapping": {
                "assignments": [_assignment("out", expr={"op": "or", "args": [false_expr, false_expr]})]
            }
        }
        result = await transformer.apply_transformations(batch, config)
        assert result[0]["out"] is False
