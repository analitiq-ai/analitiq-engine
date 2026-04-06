"""Transformation registry and execution engine."""

import hashlib
import logging
import re
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Union

from ..models.transformations import TransformationType, TransformationConfig

logger = logging.getLogger(__name__)


class TransformationError(Exception):
    """Exception raised when a transformation fails."""
    pass


class TransformationRegistry:
    """Registry for transformation functions."""
    
    def __init__(self):
        self._transformations: Dict[TransformationType, Callable] = {}
        self._register_builtin_transformations()
    
    def _register_builtin_transformations(self):
        """Register all built-in transformation functions."""

        # String transformations
        self._transformations[TransformationType.STRIP] = self._strip_transform
        self._transformations[TransformationType.UPPER] = self._upper_transform
        self._transformations[TransformationType.LOWER] = self._lower_transform
        self._transformations[TransformationType.TRUNCATE] = self._truncate_transform
        self._transformations[TransformationType.REGEX_REPLACE] = self._regex_replace_transform

        # Numeric transformations
        self._transformations[TransformationType.ABS] = self._abs_transform
        self._transformations[TransformationType.TO_INT] = self._to_int_transform
        self._transformations[TransformationType.TO_FLOAT] = self._to_float_transform
        self._transformations[TransformationType.NUMBER_FORMAT] = self._number_format_transform

        # Boolean transformations
        self._transformations[TransformationType.BOOLEAN] = self._boolean_transform

        # Date/Time transformations
        self._transformations[TransformationType.ISO_TO_DATE] = self._iso_to_date_transform
        self._transformations[TransformationType.ISO_TO_DATETIME] = self._iso_to_datetime_transform
        self._transformations[TransformationType.ISO_TO_TIMESTAMP] = self._iso_to_timestamp_transform
        self._transformations[TransformationType.ISO_STRING_TO_DATETIME] = self._iso_string_to_datetime_transform
        self._transformations[TransformationType.NOW] = self._now_transform

        # Utility transformations
        self._transformations[TransformationType.UUID] = self._uuid_transform
        self._transformations[TransformationType.MD5] = self._md5_transform
        self._transformations[TransformationType.COALESCE] = self._coalesce_transform
    
    def get_transformation(self, transform_type: TransformationType) -> Callable:
        """Get transformation function by type."""
        if transform_type not in self._transformations:
            available = ", ".join([t.value for t in self._transformations.keys()])
            raise TransformationError(f"Unknown transformation '{transform_type.value}'. Available: {available}")
        
        return self._transformations[transform_type]
    
    def apply_transformation(self, value: Any, config: TransformationConfig) -> Any:
        """Apply a single transformation to a value."""
        try:
            transform_func = self.get_transformation(config.type)
            
            # Call transformation function with parameters if any
            if config.parameters:
                return transform_func(value, **config.parameters)
            else:
                return transform_func(value)
                
        except Exception as e:
            raise TransformationError(f"Transformation '{config.type.value}' failed: {str(e)}") from e
    
    def apply_transformations(self, value: Any, transformations: List[TransformationConfig]) -> Any:
        """Apply a list of transformations to a value in order."""
        result = value
        
        for transformation in transformations:
            result = self.apply_transformation(result, transformation)
        
        return result
    
    def register_custom_transformation(self, transform_type: str, transform_func: Callable):
        """Register a custom transformation function."""
        # Convert string to enum for custom transformations
        custom_type = TransformationType(transform_type)
        self._transformations[custom_type] = transform_func
        logger.info(f"Registered custom transformation: {transform_type}")
    
    def list_available_transformations(self) -> List[str]:
        """List all available transformation types."""
        return [t.value for t in self._transformations.keys()]
    
    # Built-in transformation implementations
    
    def _strip_transform(self, value: Any) -> str:
        """Strip whitespace from string value."""
        if value is None:
            return ""
        return str(value).strip()
    
    def _upper_transform(self, value: Any) -> str:
        """Convert string to uppercase."""
        if value is None:
            return ""
        return str(value).upper()
    
    def _lower_transform(self, value: Any) -> str:
        """Convert string to lowercase."""
        if value is None:
            return ""
        return str(value).lower()
    
    def _abs_transform(self, value: Any) -> Any:
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
    
    def _to_int_transform(self, value: Any) -> int:
        """Convert value to integer."""
        if value is None:
            return 0
        try:
            if isinstance(value, str):
                return int(float(value))  # Handle decimal strings
            return int(value)
        except (ValueError, TypeError):
            raise TransformationError(f"Cannot convert '{value}' to integer")
    
    def _to_float_transform(self, value: Any) -> float:
        """Convert value to float."""
        if value is None:
            return 0.0
        try:
            return float(value)
        except (ValueError, TypeError):
            raise TransformationError(f"Cannot convert '{value}' to float")
    
    def _boolean_transform(self, value: Any) -> bool:
        """Convert value to boolean."""
        if isinstance(value, bool):
            return value
        elif isinstance(value, str):
            return value.lower() in ("true", "1", "yes", "on", "y")
        elif isinstance(value, (int, float)):
            return bool(value)
        else:
            return bool(value)
    
    def _iso_to_date_transform(self, value: Any) -> str:
        """Convert ISO datetime string to date (YYYY-MM-DD)."""
        if value is None:
            return ""
        
        # Handle datetime objects
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d")
        
        # Handle string ISO format
        iso_string = str(value)
        
        if not iso_string.strip():
            raise TransformationError("Empty string is not a valid ISO date")
        
        try:
            # Remove timezone info and parse
            iso_clean = iso_string.replace("Z", "+00:00")
            if "T" in iso_clean:
                # Full datetime format
                dt = datetime.fromisoformat(iso_clean)
                return dt.strftime("%Y-%m-%d")
            else:
                # Already just date format - validate it's a proper date
                datetime.strptime(iso_clean[:10], "%Y-%m-%d")
                return iso_clean[:10]  # Take first 10 chars (YYYY-MM-DD)
        except (ValueError, TypeError) as e:
            raise TransformationError(f"Invalid ISO date format '{value}': {str(e)}")
    
    def _iso_to_datetime_transform(self, value: Any) -> str:
        """Convert ISO datetime string to datetime (YYYY-MM-DD HH:MM:SS)."""
        if value is None:
            return ""
        
        # Handle datetime objects
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        
        try:
            # Handle string ISO format
            iso_string = str(value)
            iso_clean = iso_string.replace("Z", "+00:00")
            dt = datetime.fromisoformat(iso_clean)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError) as e:
            raise TransformationError(f"Invalid ISO datetime format '{value}': {str(e)}")
    
    def _iso_to_timestamp_transform(self, value: Any) -> datetime:
        """Convert ISO datetime string to datetime object for database storage."""
        if value is None:
            raise TransformationError("Cannot convert None to datetime")
        
        # Handle datetime objects
        if isinstance(value, datetime):
            return value
        
        try:
            # Handle string ISO format
            iso_string = str(value)
            iso_clean = iso_string.replace("Z", "+00:00")
            dt = datetime.fromisoformat(iso_clean)
            return dt
        except (ValueError, TypeError) as e:
            raise TransformationError(f"Invalid ISO timestamp format '{value}': {str(e)}")
    
    def _iso_string_to_datetime_transform(self, value: Any) -> datetime:
        """Convert ISO timestamp string to datetime object for database storage."""
        return self._iso_to_timestamp_transform(value)

    def _truncate_transform(self, value: Any, length: int = 50) -> str:
        """Truncate string to specified length."""
        try:
            if value is None:
                return ""
            string_value = str(value)
            return string_value[:length] if len(string_value) > length else string_value
        except Exception as e:
            raise TransformationError(f"truncate transformation failed: {str(e)}")

    def _regex_replace_transform(self, value: Any, pattern: str, replacement: str = "", flags: int = 0) -> str:
        """Replace text using regex pattern."""
        try:
            if value is None:
                return ""
            string_value = str(value)
            return re.sub(pattern, replacement, string_value, flags=flags)
        except Exception as e:
            raise TransformationError(f"regex_replace transformation failed: {str(e)}")

    def _number_format_transform(self, value: Any, decimals: int = 2) -> str:
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

    def _now_transform(self, value: Any = None) -> str:
        """Return current timestamp (ignores input value)."""
        try:
            return datetime.now().isoformat()
        except Exception as e:
            raise TransformationError(f"now transformation failed: {str(e)}")

    def _uuid_transform(self, value: Any = None) -> str:
        """Generate a UUID4 (ignores input value)."""
        try:
            return str(uuid.uuid4())
        except Exception as e:
            raise TransformationError(f"uuid transformation failed: {str(e)}")

    def _md5_transform(self, value: Any) -> str:
        """Generate MD5 hash of the input value."""
        try:
            if value is None:
                string_value = ""
            else:
                string_value = str(value)
            return hashlib.md5(string_value.encode('utf-8')).hexdigest()
        except Exception as e:
            raise TransformationError(f"md5 transformation failed: {str(e)}")

    def _coalesce_transform(self, *values: Any) -> Any:
        """Return first non-null, non-empty value from multiple inputs."""
        try:
            for value in values:
                if value is not None and value != "":
                    return value
            return ""
        except Exception as e:
            raise TransformationError(f"coalesce transformation failed: {str(e)}")



# Global registry instance
transformation_registry = TransformationRegistry()


def get_transformation_registry() -> TransformationRegistry:
    """Get the global transformation registry."""
    return transformation_registry


def apply_transformation(value: Any, transform_type: str, parameters: Optional[Dict[str, Any]] = None) -> Any:
    """Apply a single transformation to a value using the global registry."""
    config = TransformationConfig(
        type=TransformationType(transform_type),
        parameters=parameters or {}
    )
    return transformation_registry.apply_transformation(value, config)


def apply_transformations(value: Any, transformations: List[str]) -> Any:
    """Apply multiple transformations to a value using the global registry."""
    configs = [
        TransformationConfig(type=TransformationType(t))
        for t in transformations
    ]
    return transformation_registry.apply_transformations(value, configs)

