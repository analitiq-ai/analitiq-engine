"""Database connector utilities for data type conversions."""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def convert_python_to_db(value: Any, target_db_type: str) -> Any:
    """
    Convert Python values to database-compatible types based on target database type.
    This is schema-aware conversion that only applies transformations when appropriate.

    Args:
        value: Python value to convert
        target_db_type: Target database type (e.g., 'TIMESTAMPTZ', 'TEXT', 'JSONB')

    Returns:
        Database-compatible value
    """
    if value is None:
        return None

    if target_db_type is None:
        return value

    target_db_type = target_db_type.upper()

    # Only convert strings to datetime objects for timestamp columns
    if target_db_type in ('TIMESTAMPTZ', 'TIMESTAMP'):
        if isinstance(value, str):
            return _parse_datetime_string(value, fallback_to_original=False)
        elif isinstance(value, datetime):
            return value

    # Convert dict/list to JSON string for JSONB columns
    elif target_db_type == 'JSONB' and isinstance(value, (dict, list)):
        return json.dumps(value)

    # For all other types, return as-is (TEXT, VARCHAR, INTEGER, etc.)
    return value


def _looks_like_datetime(value: str) -> bool:
    """Check if a string looks like a datetime without attempting to parse it."""
    if not isinstance(value, str) or len(value) < 10:
        return False

    # Look for common datetime patterns - be more strict
    return (
        value.endswith('Z') or  # ISO with Z timezone
        ('+' in value[-6:] or '-' in value[-6:]) or  # ISO with timezone offset
        ('T' in value and value.count('-') == 2 and len(value) >= 19)  # ISO datetime format (stricter)
    )


def _parse_datetime_string(value: str, fallback_to_original: bool = False) -> Any:
    """
    Parse datetime string with configurable fallback behavior.

    Args:
        value: String to parse as datetime
        fallback_to_original: If True, return original value on parse failure.
                            If False, raise exception so record goes to DLQ.
    """
    from dateutil import parser as date_parser

    try:
        # Handle common datetime formats
        if value.endswith('Z'):
            # ISO format with Z timezone
            return date_parser.isoparse(value)
        elif '+' in value[-6:] or '-' in value[-6:]:
            # ISO format with timezone offset
            return date_parser.isoparse(value)
        elif 'T' in value:
            # ISO format datetime
            return date_parser.isoparse(value)
        else:
            # Try general datetime parsing
            return date_parser.parse(value)
    except (ValueError, TypeError) as e:
        if fallback_to_original:
            # Return original value for schema-less conversion
            return value
        else:
            # Re-raise exception so record goes to DLQ
            raise ValueError(f"Failed to parse datetime string '{value}': {e}") from e


def convert_db_to_python(value: Any) -> Any:
    """
    Convert database values to Python-friendly types.
    Used when reading data from database.
    
    Args:
        value: Database value to convert
        
    Returns:
        Python-compatible value
    """
    if value is None:
        return None
    
    # Convert datetime objects to ISO strings for JSON serialization
    if isinstance(value, datetime):
        return value.isoformat()
    
    # Other database types are typically already Python-compatible
    return value


def convert_record_for_db(record: Dict[str, Any], column_types: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """
    Convert all values in a record for database writing.

    Args:
        record: Dictionary with Python values
        column_types: Optional mapping of column names to database types

    Returns:
        Dictionary with database-compatible values
    """
    converted = {}
    for key, value in record.items():
        target_type = column_types.get(key) if column_types else "TEXT"
        converted[key] = convert_python_to_db(value, target_type)
    return converted


def convert_record_from_db(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert all values in a record from database reading.
    
    Args:
        record: Dictionary with database values
        
    Returns:
        Dictionary with Python-compatible values
    """
    converted = {}
    for key, value in record.items():
        converted[key] = convert_db_to_python(value)
    return converted


def convert_batch_for_db(batch: List[Dict[str, Any]], column_types: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
    """
    Convert all records in a batch for database writing.

    Args:
        batch: List of records with Python values
        column_types: Optional mapping of column names to database types

    Returns:
        List of records with database-compatible values
    """
    return [convert_record_for_db(record, column_types) for record in batch]


def convert_batch_from_db(batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Convert all records in a batch from database reading.
    
    Args:
        batch: List of records with database values
        
    Returns:
        List of records with Python-compatible values
    """
    return [convert_record_from_db(record) for record in batch]


def extract_values_for_columns(record: Dict[str, Any], columns: List[str], 
                              column_types: Optional[Dict[str, str]] = None) -> List[Any]:
    """
    Extract values from record in column order, converting for database with schema awareness.
    
    Args:
        record: Record dictionary
        columns: List of column names in order
        column_types: Optional mapping of column names to database types
        
    Returns:
        List of values in column order, converted for database
    """
    values = []
    for col in columns:
        value = record.get(col)
        target_type = column_types.get(col) if column_types else "TEXT"
        converted_value = convert_python_to_db(value, target_type)
        values.append(converted_value)
    return values


def validate_datetime_conversion(value: str) -> bool:
    """
    Validate if a string can be converted to datetime.

    Args:
        value: String value to validate

    Returns:
        True if string can be parsed as datetime
    """
    if not isinstance(value, str) or len(value) < 10:
        return False

    try:
        from dateutil import parser as date_parser

        if value.endswith('Z') or '+' in value[-6:] or '-' in value[-6:]:
            date_parser.isoparse(value)
            return True
        elif value.count('-') >= 2 and ('T' in value or ' ' in value):
            date_parser.parse(value)
            return True
    except (ValueError, TypeError, ImportError):
        pass

    return False