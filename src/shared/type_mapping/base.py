"""Base type mapper interface for unified type declaration and coercion.

This module provides the abstract base class for destination-specific type mapping,
combining two responsibilities:
1. Type declaration: Converting JSON Schema types to native SQL types
2. Type coercion: Converting Python values to database-ready values
"""

import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime, date, time, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class BaseTypeMapper(ABC):
    """Abstract base class for destination-specific type mapping.

    Each database dialect implements this class to handle both:
    - Type declaration (JSON Schema -> SQL type string)
    - Value coercion (Python value -> DB-compatible value)
    """

    @property
    @abstractmethod
    def dialect(self) -> str:
        """Return the database dialect this mapper handles."""
        pass

    @abstractmethod
    def json_schema_to_native(self, field_def: Dict[str, Any]) -> str:
        """Convert JSON Schema field definition to native SQL type string.

        Used for DDL generation (CREATE TABLE statements).

        Args:
            field_def: JSON Schema field definition with 'type', 'format', etc.

        Returns:
            Native SQL type string (e.g., 'TIMESTAMPTZ', 'BIGINT', 'JSONB')
        """
        pass

    def json_schema_to_sqlalchemy(self, field_def: Dict[str, Any]) -> Any:
        """Convert JSON Schema field definition to SQLAlchemy type.

        Override in subclasses that need SQLAlchemy type support.

        Args:
            field_def: JSON Schema field definition

        Returns:
            SQLAlchemy type object
        """
        # Default implementation returns Text
        from sqlalchemy import Text
        return Text()

    def build_column_type_mapping(self, schema: Dict[str, Any]) -> Dict[str, str]:
        """Build column name -> SQL type mapping from schema.

        Args:
            schema: JSON Schema with 'properties' dict

        Returns:
            Dict mapping column names to SQL type strings
        """
        mapping = {}
        for field_name, field_def in schema.get("properties", {}).items():
            mapping[field_name] = self.json_schema_to_native(field_def)
        return mapping

    def coerce_value(self, value: Any, target_type: str) -> Any:
        """Coerce Python value to database-compatible type.

        Args:
            value: The Python value to coerce
            target_type: Target SQL type string (e.g., 'TIMESTAMPTZ', 'JSONB')

        Returns:
            Coerced value appropriate for the database
        """
        if value is None:
            return None

        target_upper = target_type.upper()

        # Datetime types
        if target_upper in ("TIMESTAMPTZ", "TIMESTAMP", "DATETIME"):
            return self.coerce_datetime(value)
        if target_upper == "DATE":
            return self.coerce_date(value)
        if target_upper == "TIME":
            return self.coerce_time(value)

        # JSON types
        if target_upper in ("JSONB", "JSON"):
            return self.coerce_json(value)

        # Array types
        if target_upper.endswith("[]") or "ARRAY" in target_upper:
            return self.coerce_array(value)

        # Numeric types
        if target_upper == "BIGINT" or target_upper == "INT8":
            return self.coerce_integer(value)
        if target_upper in ("INTEGER", "INT", "INT4", "SMALLINT"):
            return self.coerce_integer(value)
        if target_upper.startswith("DECIMAL") or target_upper.startswith("NUMERIC"):
            return self.coerce_decimal(value)
        if target_upper in ("FLOAT", "DOUBLE", "DOUBLE PRECISION", "REAL"):
            return self.coerce_number(value)

        # Boolean
        if target_upper in ("BOOLEAN", "BOOL"):
            return self.coerce_boolean(value)

        # UUID
        if target_upper == "UUID":
            return self.coerce_uuid(value)

        # String types - return as-is or convert to string
        if target_upper in ("TEXT", "VARCHAR", "CHAR") or target_upper.startswith("VARCHAR"):
            return self.coerce_string(value)

        # Default: return as-is
        return value

    def coerce_record(self, record: Dict[str, Any], schema: Dict[str, Any]) -> Dict[str, Any]:
        """Coerce all values in a record based on schema.

        Args:
            record: Record with Python values
            schema: JSON Schema defining expected types

        Returns:
            Record with coerced values
        """
        type_mapping = self.build_column_type_mapping(schema)
        result = {}
        for key, value in record.items():
            target_type = type_mapping.get(key, "TEXT")
            result[key] = self.coerce_value(value, target_type)
        return result

    def coerce_records(self, records: List[Dict], schema: Dict) -> List[Dict]:
        """Coerce a batch of records.

        Args:
            records: List of records with Python values
            schema: JSON Schema defining expected types

        Returns:
            List of records with coerced values
        """
        return [self.coerce_record(r, schema) for r in records]

    # Type-specific coercion methods

    def coerce_datetime(self, value: Any) -> Optional[datetime]:
        """Coerce value to datetime.

        Args:
            value: Value to coerce (string, datetime, timestamp)

        Returns:
            datetime object or None
        """
        if value is None:
            return None

        if isinstance(value, datetime):
            return value

        if isinstance(value, date) and not isinstance(value, datetime):
            return datetime.combine(value, time.min)

        if isinstance(value, str):
            return self._parse_datetime_string(value)

        # Try timestamp conversion
        try:
            return datetime.fromtimestamp(float(value))
        except (ValueError, TypeError, OSError):
            logger.warning(f"Cannot coerce '{value}' to datetime")
            return None

    def coerce_date(self, value: Any) -> Optional[date]:
        """Coerce value to date.

        Args:
            value: Value to coerce (string, date, datetime)

        Returns:
            date object or None
        """
        if value is None:
            return None

        if isinstance(value, date):
            return value if not isinstance(value, datetime) else value.date()

        if isinstance(value, datetime):
            return value.date()

        if isinstance(value, str):
            return self._parse_date_string(value)

        return None

    def coerce_time(self, value: Any) -> Optional[time]:
        """Coerce value to time.

        Args:
            value: Value to coerce (string, time, datetime)

        Returns:
            time object or None
        """
        if value is None:
            return None

        if isinstance(value, time):
            return value

        if isinstance(value, datetime):
            return value.time()

        if isinstance(value, str):
            return self._parse_time_string(value)

        return None

    def coerce_json(self, value: Any) -> Any:
        """Coerce value for JSON/JSONB columns.

        Default implementation returns dicts/lists as-is (most drivers handle serialization).

        Args:
            value: Value to coerce

        Returns:
            JSON-compatible value
        """
        if value is None:
            return None

        if isinstance(value, (dict, list)):
            return value

        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value

        return value

    def coerce_array(self, value: Any) -> Optional[List]:
        """Coerce value for array columns.

        Args:
            value: Value to coerce

        Returns:
            List or None
        """
        if value is None:
            return None

        if isinstance(value, list):
            return value

        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    return parsed
            except json.JSONDecodeError:
                pass

        return [value]

    def coerce_integer(self, value: Any) -> Optional[int]:
        """Coerce value to integer.

        Args:
            value: Value to coerce

        Returns:
            int or None
        """
        if value is None:
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            logger.warning(f"Failed to coerce '{value}' to integer")
            return None

    def coerce_number(self, value: Any) -> Optional[float]:
        """Coerce value to float.

        Args:
            value: Value to coerce

        Returns:
            float or None
        """
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            logger.warning(f"Failed to coerce '{value}' to number")
            return None

    def coerce_decimal(self, value: Any) -> Any:
        """Coerce value to Decimal.

        Override in subclasses that need specific Decimal handling.

        Args:
            value: Value to coerce

        Returns:
            Decimal or float
        """
        if value is None:
            return None
        try:
            return Decimal(str(value))
        except Exception:
            logger.warning(f"Failed to coerce '{value}' to Decimal")
            return None

    def coerce_boolean(self, value: Any) -> Optional[bool]:
        """Coerce value to boolean.

        Args:
            value: Value to coerce

        Returns:
            bool or None
        """
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ("true", "1", "yes", "on")
        return bool(value)

    def coerce_uuid(self, value: Any) -> Optional[str]:
        """Coerce value to UUID string.

        Args:
            value: Value to coerce

        Returns:
            UUID string or None
        """
        if value is None:
            return None
        return str(value)

    def coerce_string(self, value: Any) -> Optional[str]:
        """Coerce value to string.

        Args:
            value: Value to coerce

        Returns:
            String or None
        """
        if value is None:
            return None
        return str(value)

    # Helper methods for parsing date/time strings

    def _parse_datetime_string(self, value: str) -> Optional[datetime]:
        """Parse datetime string with multiple format support.

        Args:
            value: Datetime string to parse

        Returns:
            datetime object or None
        """
        if not isinstance(value, str):
            return None

        # Normalize: replace 'Z' with UTC offset
        normalized = value.strip()
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"

        # Try fromisoformat first (handles most ISO 8601)
        try:
            return datetime.fromisoformat(normalized)
        except ValueError:
            pass

        # Fallback formats
        formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y/%m/%d %H:%M:%S",
            "%d-%m-%Y %H:%M:%S",
            "%d/%m/%Y %H:%M:%S",
        ]

        for fmt in formats:
            try:
                return datetime.strptime(normalized, fmt)
            except ValueError:
                continue

        logger.warning(f"Failed to parse datetime string: '{value}'")
        return None

    def _parse_date_string(self, value: str) -> Optional[date]:
        """Parse date string with multiple format support.

        Args:
            value: Date string to parse

        Returns:
            date object or None
        """
        if not isinstance(value, str):
            return None

        normalized = value.strip()

        # Try ISO format first
        try:
            return date.fromisoformat(normalized[:10])
        except ValueError:
            pass

        # Fallback formats
        formats = [
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%d-%m-%Y",
            "%d/%m/%Y",
            "%m-%d-%Y",
            "%m/%d/%Y",
        ]

        for fmt in formats:
            try:
                return datetime.strptime(normalized, fmt).date()
            except ValueError:
                continue

        logger.warning(f"Failed to parse date string: '{value}'")
        return None

    def _parse_time_string(self, value: str) -> Optional[time]:
        """Parse time string with multiple format support.

        Args:
            value: Time string to parse

        Returns:
            time object or None
        """
        if not isinstance(value, str):
            return None

        normalized = value.strip()

        # Try ISO format first
        try:
            return time.fromisoformat(normalized)
        except ValueError:
            pass

        # Fallback formats
        formats = [
            "%H:%M:%S",
            "%H:%M:%S.%f",
            "%H:%M",
            "%I:%M:%S %p",
            "%I:%M %p",
        ]

        for fmt in formats:
            try:
                return datetime.strptime(normalized, fmt).time()
            except ValueError:
                continue

        logger.warning(f"Failed to parse time string: '{value}'")
        return None
