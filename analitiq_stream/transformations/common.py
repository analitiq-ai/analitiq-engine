"""Common transformation functions for field mapping."""

import hashlib
import re
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any, Callable, Dict, Optional, Union


class TransformationError(Exception):
    """Exception raised when a transformation fails."""
    pass


def abs_transform(value: Any) -> Any:
    """Return absolute value of numeric input."""
    try:
        if isinstance(value, (int, float, Decimal)):
            return abs(value)
        elif isinstance(value, str):
            # Try to convert string to number first
            try:
                return abs(float(value))
            except ValueError:
                raise TransformationError(f"Cannot convert string '{value}' to number for abs()")
        else:
            raise TransformationError(f"abs() not supported for type {type(value)}")
    except Exception as e:
        raise TransformationError(f"abs transformation failed: {str(e)}")


def strip_transform(value: Any) -> str:
    """Strip whitespace from string value."""
    try:
        if value is None:
            return ""
        return str(value).strip()
    except Exception as e:
        raise TransformationError(f"strip transformation failed: {str(e)}")


def upper_transform(value: Any) -> str:
    """Convert string to uppercase."""
    try:
        if value is None:
            return ""
        return str(value).upper()
    except Exception as e:
        raise TransformationError(f"upper transformation failed: {str(e)}")


def lower_transform(value: Any) -> str:
    """Convert string to lowercase."""
    try:
        if value is None:
            return ""
        return str(value).lower()
    except Exception as e:
        raise TransformationError(f"lower transformation failed: {str(e)}")


def iso_to_date_transform(value: Any) -> str:
    """Convert ISO datetime string to date (YYYY-MM-DD)."""
    try:
        if value is None:
            return ""
        
        # Handle datetime objects
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d")
        
        # Handle string ISO format
        iso_string = str(value)
        
        # Handle empty string
        if not iso_string.strip():
            raise TransformationError("Empty string is not a valid ISO date")
            
        # Remove timezone info and parse
        iso_clean = iso_string.replace("Z", "+00:00")
        if "T" in iso_clean:
            # Full datetime format
            dt = datetime.fromisoformat(iso_clean)
            return dt.strftime("%Y-%m-%d")
        else:
            # Already just date format - validate it's a proper date
            try:
                datetime.strptime(iso_clean[:10], "%Y-%m-%d")
                return iso_clean[:10]  # Take first 10 chars (YYYY-MM-DD)
            except ValueError:
                raise TransformationError(f"Invalid date format: {iso_clean[:10]}")
            
    except TransformationError:
        raise
    except Exception as e:
        raise TransformationError(f"iso_to_date transformation failed for '{value}': {str(e)}")


def iso_to_datetime_transform(value: Any) -> str:
    """Convert ISO datetime string to datetime (YYYY-MM-DD HH:MM:SS)."""
    try:
        if value is None:
            return ""
        
        # Handle datetime objects
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        
        # Handle string ISO format
        iso_string = str(value)
        iso_clean = iso_string.replace("Z", "+00:00")
        dt = datetime.fromisoformat(iso_clean)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
        
    except Exception as e:
        raise TransformationError(f"iso_to_datetime transformation failed for '{value}': {str(e)}")


def now_transform(value: Any = None) -> str:
    """Return current timestamp (ignores input value)."""
    try:
        return datetime.now().isoformat()
    except Exception as e:
        raise TransformationError(f"now transformation failed: {str(e)}")


def uuid_transform(value: Any = None) -> str:
    """Generate a UUID4 (ignores input value)."""
    try:
        return str(uuid.uuid4())
    except Exception as e:
        raise TransformationError(f"uuid transformation failed: {str(e)}")


def md5_transform(value: Any) -> str:
    """Generate MD5 hash of the input value."""
    try:
        if value is None:
            value = ""
        input_str = str(value)
        return hashlib.md5(input_str.encode()).hexdigest()
    except Exception as e:
        raise TransformationError(f"md5 transformation failed: {str(e)}")


def truncate_transform(value: Any, length: int = 50) -> str:
    """Truncate string to specified length."""
    try:
        if value is None:
            return ""
        str_value = str(value)
        return str_value[:length]
    except Exception as e:
        raise TransformationError(f"truncate transformation failed: {str(e)}")


def regex_replace_transform(value: Any, pattern: str, replacement: str = "") -> str:
    """Replace text using regex pattern."""
    try:
        if value is None:
            return ""
        str_value = str(value)
        return re.sub(pattern, replacement, str_value)
    except Exception as e:
        raise TransformationError(f"regex_replace transformation failed: {str(e)}")


def coalesce_transform(*values: Any) -> Any:
    """Return first non-null, non-empty value."""
    try:
        for value in values:
            if value is not None and value != "":
                return value
        return ""
    except Exception as e:
        raise TransformationError(f"coalesce transformation failed: {str(e)}")


def number_format_transform(value: Any, decimals: int = 2) -> str:
    """Format number with specified decimal places."""
    try:
        if value is None:
            return "0.00"
        
        # Convert to float first
        if isinstance(value, str):
            num_value = float(value)
        else:
            num_value = float(value)
        
        return f"{num_value:.{decimals}f}"
        
    except Exception as e:
        raise TransformationError(f"number_format transformation failed: {str(e)}")


def boolean_transform(value: Any) -> bool:
    """Convert value to boolean."""
    try:
        if isinstance(value, bool):
            return value
        elif isinstance(value, str):
            return value.lower() in ("true", "1", "yes", "on", "y")
        elif isinstance(value, (int, float)):
            return bool(value)
        else:
            return bool(value)
    except Exception as e:
        raise TransformationError(f"boolean transformation failed: {str(e)}")


# Registry of available transformations
TRANSFORMATION_REGISTRY: Dict[str, Callable] = {
    "abs": abs_transform,
    "strip": strip_transform,
    "upper": upper_transform,
    "lower": lower_transform,
    "iso_to_date": iso_to_date_transform,
    "iso_to_datetime": iso_to_datetime_transform,
    "now": now_transform,
    "uuid": uuid_transform,
    "md5": md5_transform,
    "truncate": truncate_transform,
    "regex_replace": regex_replace_transform,
    "coalesce": coalesce_transform,
    "number_format": number_format_transform,
    "boolean": boolean_transform,
}


def get_transformation_function(name: str) -> Callable:
    """Get transformation function by name."""
    if name not in TRANSFORMATION_REGISTRY:
        available = ", ".join(TRANSFORMATION_REGISTRY.keys())
        raise TransformationError(f"Unknown transformation '{name}'. Available: {available}")
    
    return TRANSFORMATION_REGISTRY[name]


def list_available_transformations() -> Dict[str, str]:
    """List all available transformations with their descriptions."""
    descriptions = {
        "abs": "Return absolute value of numeric input",
        "strip": "Strip whitespace from string value", 
        "upper": "Convert string to uppercase",
        "lower": "Convert string to lowercase",
        "iso_to_date": "Convert ISO datetime string to date (YYYY-MM-DD)",
        "iso_to_datetime": "Convert ISO datetime string to datetime (YYYY-MM-DD HH:MM:SS)",
        "now": "Return current timestamp (ignores input value)",
        "uuid": "Generate a UUID4 (ignores input value)",
        "md5": "Generate MD5 hash of the input value",
        "truncate": "Truncate string to specified length (default: 50)",
        "regex_replace": "Replace text using regex pattern",
        "coalesce": "Return first non-null, non-empty value from multiple inputs",
        "number_format": "Format number with specified decimal places (default: 2)",
        "boolean": "Convert value to boolean",
    }
    
    return descriptions