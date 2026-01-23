"""MySQL-specific type mapper.

Handles MySQL's native types including JSON, DATETIME, and MySQL-specific features.
"""

import json
import logging
from datetime import datetime, date, time
from typing import Any, Dict, List, Optional

from .base import BaseTypeMapper

logger = logging.getLogger(__name__)


class MySQLTypeMapper(BaseTypeMapper):
    """Type mapper for MySQL/MariaDB databases.

    MySQL features handled:
    - JSON type for complex objects
    - DATETIME for timestamps (MySQL doesn't have TIMESTAMPTZ)
    - No native arrays (uses JSON instead)
    - DECIMAL with precision/scale
    """

    @property
    def dialect(self) -> str:
        return "mysql"

    def json_schema_to_native(self, field_def: Dict[str, Any]) -> str:
        """Convert JSON Schema to MySQL-specific types.

        Args:
            field_def: JSON Schema field definition

        Returns:
            MySQL type string
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
                return "DATETIME(6)"  # Microsecond precision
            elif field_format == "date":
                return "DATE"
            elif field_format == "time":
                return "TIME(6)"
            elif field_format == "uuid":
                return "CHAR(36)"  # MySQL doesn't have native UUID
            else:
                max_length = field_def.get("maxLength", 255)
                if max_length <= 255:
                    return f"VARCHAR({max_length})"
                elif max_length <= 65535:
                    return "TEXT"
                elif max_length <= 16777215:
                    return "MEDIUMTEXT"
                else:
                    return "LONGTEXT"

        elif field_type == "integer":
            return "BIGINT"

        elif field_type == "number":
            precision = field_def.get("precision", 15)
            scale = field_def.get("scale", 2)
            return f"DECIMAL({precision},{scale})"

        elif field_type == "boolean":
            return "TINYINT(1)"  # MySQL uses TINYINT for boolean

        elif field_type == "object":
            return "JSON"

        elif field_type == "array":
            return "JSON"  # MySQL stores arrays as JSON

        return "TEXT"

    def json_schema_to_sqlalchemy(self, field_def: Dict[str, Any]) -> Any:
        """Convert JSON Schema to SQLAlchemy type for MySQL.

        Args:
            field_def: JSON Schema field definition

        Returns:
            SQLAlchemy type object
        """
        from sqlalchemy import BigInteger, Boolean, DateTime, Float, String, Text
        from sqlalchemy.dialects.mysql import JSON, TINYINT

        if "database_type" in field_def:
            db_type = field_def["database_type"].upper()
            if db_type == "JSON":
                return JSON()

        field_type = field_def.get("type", "string")
        field_format = field_def.get("format")

        if isinstance(field_type, list) and "null" in field_type:
            field_type = [t for t in field_type if t != "null"][0]

        if field_type == "string":
            if field_format == "date-time":
                return DateTime()
            elif field_format == "date":
                return DateTime()
            elif field_format == "uuid":
                return String(36)
            max_length = field_def.get("maxLength", 255)
            return String(max_length) if max_length <= 255 else Text()

        elif field_type == "integer":
            return BigInteger()

        elif field_type == "number":
            return Float()

        elif field_type == "boolean":
            return TINYINT(1)

        elif field_type in ("object", "array"):
            return JSON()

        return Text()

    def coerce_datetime(self, value: Any) -> Optional[datetime]:
        """Coerce to datetime for MySQL DATETIME columns.

        Args:
            value: Value to coerce

        Returns:
            datetime object or None
        """
        if value is None:
            return None

        if isinstance(value, datetime):
            # MySQL DATETIME doesn't store timezone, strip it
            return value.replace(tzinfo=None)

        if isinstance(value, date) and not isinstance(value, datetime):
            return datetime.combine(value, time.min)

        if isinstance(value, str):
            parsed = self._parse_datetime_string(value)
            if parsed:
                # Strip timezone for MySQL
                return parsed.replace(tzinfo=None)
            logger.warning(f"MySQL: Failed to parse datetime '{value}'")
            return None

        try:
            return datetime.fromtimestamp(float(value))
        except (ValueError, TypeError, OSError) as e:
            logger.warning(f"MySQL: Cannot coerce '{value}' to datetime: {e}")
            return None

    def coerce_json(self, value: Any) -> Optional[str]:
        """Coerce value for MySQL JSON columns.

        MySQL requires JSON as string for some drivers.

        Args:
            value: Value to coerce

        Returns:
            JSON string or None
        """
        if value is None:
            return None

        if isinstance(value, (dict, list)):
            return json.dumps(value)

        if isinstance(value, str):
            # Validate it's valid JSON
            try:
                json.loads(value)
                return value
            except json.JSONDecodeError:
                return json.dumps({"value": value})

        return json.dumps({"value": value})

    def coerce_array(self, value: Any) -> Optional[str]:
        """Coerce array for MySQL (stored as JSON).

        MySQL doesn't have native arrays, uses JSON instead.

        Args:
            value: Value to coerce

        Returns:
            JSON array string or None
        """
        if value is None:
            return None

        if isinstance(value, list):
            return json.dumps(value)

        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    return value
            except json.JSONDecodeError:
                pass

        return json.dumps([value])

    def coerce_boolean(self, value: Any) -> Optional[int]:
        """Coerce to boolean for MySQL TINYINT(1).

        MySQL uses 1/0 for boolean values.

        Args:
            value: Value to coerce

        Returns:
            1 or 0 or None
        """
        if value is None:
            return None
        if isinstance(value, bool):
            return 1 if value else 0
        if isinstance(value, int):
            return 1 if value else 0
        if isinstance(value, str):
            return 1 if value.lower() in ("true", "1", "yes", "on") else 0
        return 1 if value else 0
