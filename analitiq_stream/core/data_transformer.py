"""Data transformation utilities for the streaming engine."""

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List

from .exceptions import TransformationError
from .expression_evaluator import SecureExpressionEvaluator

logger = logging.getLogger(__name__)


class DataTransformer:
    """Handles field mappings, transformations, and computed fields."""
    
    def __init__(self):
        self.expression_evaluator = SecureExpressionEvaluator()
    
    async def apply_transformations(self, batch: List[Dict[str, Any]], 
                                  config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Apply field mappings and transformations to batch.
        
        Args:
            batch: List of records to transform
            config: Stream configuration with mapping rules
            
        Returns:
            Transformed batch
            
        Raises:
            TransformationError: If transformation fails
        """
        mapping = config.get("mapping", {})
        field_mappings = mapping.get("field_mappings", {})
        computed_fields = mapping.get("computed_fields", {})

        if not field_mappings and not computed_fields:
            return batch  # No transformations to apply

        transformed_batch = []

        try:
            for record in batch:
                transformed_record = {}
                
                # Apply field mappings
                for source_field, mapping_config in field_mappings.items():
                    if isinstance(mapping_config, dict):
                        target_field = mapping_config.get("target", source_field)
                        transformations = mapping_config.get("transformations", [])
                    else:
                        # Simple string mapping
                        target_field = mapping_config
                        transformations = []

                    # Get source value (support nested fields like "details.merchant.name")
                    source_value = self._get_nested_value(record, source_field)
                    
                    # Apply transformations
                    transformed_value = await self._apply_field_transformations(
                        source_value, transformations
                    )
                    
                    # Set target value
                    transformed_record[target_field] = transformed_value

                # Apply computed fields
                for field_name, field_config in computed_fields.items():
                    if isinstance(field_config, dict):
                        expression = field_config.get("expression", "")
                    else:
                        expression = field_config

                    # Secure expression evaluation
                    computed_value = await self.expression_evaluator.evaluate(
                        expression, record, transformed_record
                    )
                    transformed_record[field_name] = computed_value

                transformed_batch.append(transformed_record)

        except Exception as e:
            logger.error(f"Transformation failed: {e}")
            raise TransformationError(f"Data transformation failed: {e}") from e

        return transformed_batch

    def _get_nested_value(self, record: Dict[str, Any], field_path: str) -> Any:
        """
        Get value from nested field path like 'details.merchant.name'.
        
        Args:
            record: Record to extract from
            field_path: Dot-separated field path
            
        Returns:
            Field value or None if not found
        """
        if "." not in field_path:
            return record.get(field_path)

        current = record
        for field in field_path.split("."):
            if isinstance(current, dict) and field in current:
                current = current[field]
            else:
                return None
        return current

    async def _apply_field_transformations(self, value: Any, 
                                         transformations: List[str]) -> Any:
        """
        Apply transformations to a field value with async safety.
        
        Args:
            value: Value to transform
            transformations: List of transformation names
            
        Returns:
            Transformed value
        """
        if not transformations or value is None:
            return value

        for transformation in transformations:
            # Yield control for CPU-intensive operations
            await asyncio.sleep(0)
            
            # Apply transformation using match statement (Python 3.11+)
            match transformation:
                case "abs" if isinstance(value, (int, float)):
                    value = abs(value)
                case "strip" | "trim" if isinstance(value, str):
                    value = value.strip()
                case "lowercase" | "lower" if isinstance(value, str):
                    value = value.lower()
                case "uppercase" | "upper" if isinstance(value, str):
                    value = value.upper()
                case "iso_to_date" if isinstance(value, str):
                    value = await self._parse_iso_date(value)
                case "iso_to_timestamp" if isinstance(value, str):
                    value = await self._parse_iso_timestamp(value)
                case "to_int" if isinstance(value, (str, float)):
                    try:
                        value = int(float(value))
                    except (ValueError, TypeError):
                        logger.warning(f"Failed to convert {value} to int")
                case "to_float" if isinstance(value, (str, int)):
                    try:
                        value = float(value)
                    except (ValueError, TypeError):
                        logger.warning(f"Failed to convert {value} to float")
                case "to_str":
                    value = str(value) if value is not None else ""
                case _:
                    logger.warning(f"Unknown transformation: {transformation}")

        return value

    async def _parse_iso_date(self, value: str) -> str:
        """
        Parse ISO datetime string to date format.
        
        Args:
            value: ISO datetime string
            
        Returns:
            Date string in YYYY-MM-DD format
        """
        try:
            # Handle various ISO formats
            if value.endswith('Z'):
                value = value.replace('Z', '+00:00')
            
            dt = datetime.fromisoformat(value)
            return dt.strftime('%Y-%m-%d')
        except ValueError as e:
            logger.warning(f"Failed to parse ISO date '{value}': {e}")
            return value  # Return original on error

    async def _parse_iso_timestamp(self, value: str) -> str:
        """
        Parse ISO datetime string to timestamp format.
        
        Args:
            value: ISO datetime string
            
        Returns:
            Timestamp string
        """
        try:
            # Handle various ISO formats
            if value.endswith('Z'):
                value = value.replace('Z', '+00:00')
            
            dt = datetime.fromisoformat(value)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except ValueError as e:
            logger.warning(f"Failed to parse ISO timestamp '{value}': {e}")
            return value  # Return original on error