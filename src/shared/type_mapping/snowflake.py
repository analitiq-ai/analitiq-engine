"""Snowflake-specific type mapper.

Handles Snowflake's native types including VARIANT, TIMESTAMP_TZ, and
Snowflake-specific features.
"""

import json
import logging
from datetime import datetime, date, time, timezone
from typing import Any, Dict, Optional

from .base import BaseTypeMapper

logger = logging.getLogger(__name__)


class SnowflakeTypeMapper(BaseTypeMapper):
    """Type mapper for Snowflake databases.

    Snowflake features handled:
    - VARIANT for complex objects and arrays
    - TIMESTAMP_TZ for timezone-aware datetimes
    - TIMESTAMP_NTZ for naive datetimes
    - NUMBER with precision/scale
    """

    @property
    def dialect(self) -> str:
        return "snowflake"

    def json_schema_to_native(self, field_def: Dict[str, Any]) -> str:
        """Convert JSON Schema to Snowflake-specific types.

        Args:
            field_def: JSON Schema field definition

        Returns:
            Snowflake type string
        """
        # Check for explicit database_type first
        if "database_type" in field_def:
            return field_def["database_type"]

        field_type = field_def.get("type", "string")
        field_format = field_def.get("format")

        # Handle nullable types
        if isinstance(field_type, list) and "null" in field_type:
            field_type = [t for t in field_type if t != "null"][0]

        if field_type == "string":
            if field_format == "date-time":
                return "TIMESTAMP_TZ"
            elif field_format == "date":
                return "DATE"
            elif field_format == "time":
                return "TIME"
            else:
                max_length = field_def.get("maxLength")
                if max_length and max_length <= 16777216:
                    return f"VARCHAR({max_length})"
                return "VARCHAR(16777216)"  # Snowflake max VARCHAR

        elif field_type == "integer":
            return "NUMBER(38,0)"  # Snowflake uses NUMBER for integers

        elif field_type == "number":
            precision = field_def.get("precision", 38)
            scale = field_def.get("scale", 9)
            return f"NUMBER({precision},{scale})"

        elif field_type == "boolean":
            return "BOOLEAN"

        elif field_type == "object":
            return "VARIANT"

        elif field_type == "array":
            return "VARIANT"  # Snowflake uses VARIANT for arrays

        return "VARCHAR(16777216)"

    def json_schema_to_sqlalchemy(self, field_def: Dict[str, Any]) -> Any:
        """Convert JSON Schema to SQLAlchemy type for Snowflake.

        Args:
            field_def: JSON Schema field definition

        Returns:
            SQLAlchemy type object
        """
        from sqlalchemy import BigInteger, Boolean, DateTime, Float, String, Text

        try:
            from snowflake.sqlalchemy import VARIANT
        except ImportError:
            # Fall back to Text if Snowflake SQLAlchemy not available
            VARIANT = Text

        if "database_type" in field_def:
            db_type = field_def["database_type"].upper()
            if db_type == "VARIANT":
                return VARIANT()

        field_type = field_def.get("type", "string")
        field_format = field_def.get("format")

        if isinstance(field_type, list) and "null" in field_type:
            field_type = [t for t in field_type if t != "null"][0]

        if field_type == "string":
            if field_format == "date-time":
                return DateTime(timezone=True)
            elif field_format == "date":
                return DateTime()
            max_length = field_def.get("maxLength", 255)
            return String(max_length) if max_length <= 255 else Text()

        elif field_type == "integer":
            return BigInteger()

        elif field_type == "number":
            return Float()

        elif field_type == "boolean":
            return Boolean()

        elif field_type in ("object", "array"):
            return VARIANT()

        return Text()

    def coerce_datetime(self, value: Any) -> Optional[datetime]:
        """Coerce to datetime for Snowflake TIMESTAMP columns.

        Args:
            value: Value to coerce

        Returns:
            datetime object or None
        """
        if value is None:
            return None

        if isinstance(value, datetime):
            return value

        if isinstance(value, date) and not isinstance(value, datetime):
            return datetime.combine(value, time.min, tzinfo=timezone.utc)

        if isinstance(value, str):
            parsed = self._parse_datetime_string(value)
            if parsed:
                # Ensure timezone awareness for TIMESTAMP_TZ
                if parsed.tzinfo is None:
                    return parsed.replace(tzinfo=timezone.utc)
                return parsed
            logger.warning(f"Snowflake: Failed to parse datetime '{value}'")
            return None

        try:
            dt = datetime.fromtimestamp(float(value), tz=timezone.utc)
            return dt
        except (ValueError, TypeError, OSError) as e:
            logger.warning(f"Snowflake: Cannot coerce '{value}' to datetime: {e}")
            return None

    def coerce_json(self, value: Any) -> Any:
        """Coerce value for Snowflake VARIANT columns.

        Snowflake VARIANT accepts Python dicts and lists directly.

        Args:
            value: Value to coerce

        Returns:
            Dict, list, or JSON string
        """
        if value is None:
            return None

        if isinstance(value, (dict, list)):
            return value

        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return {"value": value}

        return {"value": value}

    def coerce_array(self, value: Any) -> Any:
        """Coerce array for Snowflake VARIANT columns.

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
