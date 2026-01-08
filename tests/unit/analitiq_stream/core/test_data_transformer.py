"""Unit tests for DataTransformer - NO MOCKING, real functionality only."""

import pytest

from src.core.data_transformer import DataTransformer
from src.core.exceptions import TransformationError


class TestDataTransformer:
    """Test suite for DataTransformer functionality."""

    @pytest.fixture
    def transformer(self):
        """Create a DataTransformer instance."""
        return DataTransformer()

    @pytest.fixture
    def sample_batch(self):
        """Sample batch of records for testing."""
        return [
            {
                "id": 123456,
                "created": "2025-08-16T10:30:00Z",
                "targetValue": 100.50,
                "targetCurrency": "EUR",
                "details": {
                    "reference": "Payment for services",
                    "merchant": {
                        "name": "Test Merchant"
                    }
                }
            },
            {
                "id": 789012,
                "created": "2025-08-16T11:00:00Z",
                "targetValue": 250.75,
                "targetCurrency": "USD",
                "details": {
                    "reference": "Invoice payment",
                    "merchant": {
                        "name": "Another Merchant"
                    }
                }
            }
        ]

    @pytest.mark.asyncio
    async def test_no_transformations(self, transformer, sample_batch):
        """Test batch passes through unchanged when no transformations configured."""
        config = {"mapping": {}}
        
        result = await transformer.apply_transformations(sample_batch, config)
        assert result == sample_batch

    @pytest.mark.asyncio
    async def test_field_mappings_simple(self, transformer, sample_batch):
        """Test simple field mappings without transformations."""
        config = {
            "mapping": {
                "field_mappings": {
                    "id": {"target": "transaction_id"},
                    "targetValue": {"target": "amount"},
                    "targetCurrency": {"target": "currency"}
                }
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
    async def test_field_mappings_with_transformations(self, transformer, sample_batch):
        """Test field mappings with transformations."""
        config = {
            "mapping": {
                "field_mappings": {
                    "created": {
                        "target": "date",
                        "transformations": ["iso_to_date"]
                    },
                    "targetValue": {
                        "target": "amount",
                        "transformations": ["abs"]
                    },
                    "targetCurrency": {
                        "target": "currency_code",
                        "transformations": ["lowercase"]
                    }
                }
            }
        }
        
        result = await transformer.apply_transformations(sample_batch, config)
        
        assert result[0]["date"] == "2025-08-16"
        assert result[0]["amount"] == 100.50
        assert result[0]["currency_code"] == "eur"

    @pytest.mark.asyncio
    async def test_nested_field_access(self, transformer, sample_batch):
        """Test accessing nested fields with dot notation."""
        config = {
            "mapping": {
                "field_mappings": {
                    "details.reference": {"target": "payment_reference"},
                    "details.merchant.name": {"target": "merchant_name"}
                }
            }
        }
        
        result = await transformer.apply_transformations(sample_batch, config)
        
        assert result[0]["payment_reference"] == "Payment for services"
        assert result[0]["merchant_name"] == "Test Merchant"
        assert result[1]["payment_reference"] == "Invoice payment"
        assert result[1]["merchant_name"] == "Another Merchant"

    @pytest.mark.asyncio
    async def test_computed_fields(self, transformer, sample_batch):
        """Test computed fields with secure expressions."""
        config = {
            "mapping": {
                "computed_fields": {
                    "object_name": {"expression": "Transaction"},
                    "account_info": {"expression": '{"id": "5936402", "type": "CheckAccount"}'},
                    "timestamp": {"expression": "now()"},
                    "static_status": {"expression": "active"}
                }
            }
        }
        
        result = await transformer.apply_transformations(sample_batch, config)
        
        assert result[0]["object_name"] == "Transaction"
        assert result[0]["account_info"] == {"id": "5936402", "type": "CheckAccount"}
        assert "T" in result[0]["timestamp"]  # ISO timestamp format
        assert result[0]["static_status"] == "active"

    @pytest.mark.asyncio
    async def test_string_field_mappings(self, transformer, sample_batch):
        """Test simple string field mappings (legacy format)."""
        config = {
            "mapping": {
                "field_mappings": {
                    "id": "transaction_id",
                    "targetValue": "amount"
                }
            }
        }
        
        result = await transformer.apply_transformations(sample_batch, config)
        
        assert result[0]["transaction_id"] == 123456
        assert result[0]["amount"] == 100.50

    @pytest.mark.asyncio
    async def test_all_transformation_types(self, transformer):
        """Test all supported transformation types."""
        batch = [
            {
                "text_field": "  Hello World  ",
                "number_field": -42.5,
                "date_field": "2025-08-16T10:30:00Z",
                "string_number": "123.45",
                "float_number": 67.89
            }
        ]
        
        config = {
            "mapping": {
                "field_mappings": {
                    "text_field": {
                        "target": "clean_text",
                        "transformations": ["strip", "lowercase"]
                    },
                    "number_field": {
                        "target": "positive_number", 
                        "transformations": ["abs"]
                    },
                    "date_field": {
                        "target": "formatted_date",
                        "transformations": ["iso_to_date"]
                    },
                    "string_number": {
                        "target": "as_float",
                        "transformations": ["to_float"]
                    },
                    "float_number": {
                        "target": "as_int",
                        "transformations": ["to_int"]
                    }
                }
            }
        }
        
        result = await transformer.apply_transformations(batch, config)
        
        assert result[0]["clean_text"] == "hello world"
        assert result[0]["positive_number"] == 42.5
        assert result[0]["formatted_date"] == "2025-08-16"
        assert result[0]["as_float"] == 123.45
        assert result[0]["as_int"] == 67

    @pytest.mark.asyncio
    async def test_missing_nested_field(self, transformer, sample_batch):
        """Test handling of missing nested fields."""
        config = {
            "mapping": {
                "field_mappings": {
                    "missing.nested.field": {"target": "should_be_none"},
                    "details.missing_field": {"target": "also_none"}
                }
            }
        }
        
        result = await transformer.apply_transformations(sample_batch, config)
        
        assert result[0]["should_be_none"] is None
        assert result[0]["also_none"] is None

    @pytest.mark.asyncio
    async def test_transformation_error_handling(self, transformer):
        """Test error handling in transformations using real expression evaluator."""
        # Use a record that will cause real errors during transformation
        batch = [{"field": "test_value"}]

        # Create a custom transformer with a broken expression evaluator
        class BrokenExpressionEvaluator:
            async def evaluate(self, expression, record, context):
                raise ValueError("Evaluation failed")

        broken_transformer = DataTransformer()
        broken_transformer.expression_evaluator = BrokenExpressionEvaluator()

        config = {
            "mapping": {
                "computed_fields": {
                    "bad_field": {"expression": "any_expression"}
                }
            }
        }

        with pytest.raises(TransformationError, match="Data transformation failed"):
            await broken_transformer.apply_transformations(batch, config)

    @pytest.mark.asyncio
    async def test_iso_date_transformation_edge_cases(self, transformer):
        """Test ISO date transformation with various input formats."""
        batch = [
            {
                "date1": "2025-08-16T10:30:00Z",       # UTC with Z
                "date2": "2025-08-16T10:30:00+00:00",  # UTC with offset
                "date3": "2025-08-16T10:30:00+02:00",  # Timezone offset
                "date4": "invalid-date",               # Invalid format
                "date5": None                          # None value
            }
        ]
        
        config = {
            "mapping": {
                "field_mappings": {
                    "date1": {"target": "d1", "transformations": ["iso_to_date"]},
                    "date2": {"target": "d2", "transformations": ["iso_to_date"]},
                    "date3": {"target": "d3", "transformations": ["iso_to_date"]},
                    "date4": {"target": "d4", "transformations": ["iso_to_date"]},
                    "date5": {"target": "d5", "transformations": ["iso_to_date"]}
                }
            }
        }
        
        result = await transformer.apply_transformations(batch, config)
        
        assert result[0]["d1"] == "2025-08-16"
        assert result[0]["d2"] == "2025-08-16"
        assert result[0]["d3"] == "2025-08-16"
        assert result[0]["d4"] == "invalid-date"  # Should keep original on error
        assert result[0]["d5"] is None  # Should handle None values

    @pytest.mark.asyncio
    async def test_transformation_type_safety(self, transformer):
        """Test that transformations are applied safely with type checking."""
        batch = [
            {
                "string_field": "test",
                "number_field": 42,
                "none_field": None,
                "bool_field": True
            }
        ]
        
        config = {
            "mapping": {
                "field_mappings": {
                    # These should only apply if type matches
                    "string_field": {"target": "s1", "transformations": ["abs"]},      # Wrong type
                    "number_field": {"target": "n1", "transformations": ["strip"]},    # Wrong type
                    "none_field": {"target": "null1", "transformations": ["upper"]},   # None value
                    "bool_field": {"target": "b1", "transformations": ["to_int"]}      # Type conversion
                }
            }
        }
        
        result = await transformer.apply_transformations(batch, config)
        
        # Should gracefully handle type mismatches
        assert result[0]["s1"] == "test"  # Unchanged due to type mismatch
        assert result[0]["n1"] == 42      # Unchanged due to type mismatch  
        assert result[0]["null1"] is None  # None should be preserved
        assert result[0]["b1"] == 1       # Bool to int conversion