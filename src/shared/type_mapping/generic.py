"""Generic SQL type mapper for unknown databases.

Provides safe fallback type mappings that should work across most SQL databases.
"""

import json
import logging
from datetime import datetime, date, time
from typing import Any, Dict, Optional

from .base import BaseTypeMapper

logger = logging.getLogger(__name__)


class GenericSQLTypeMapper(BaseTypeMapper):
    """Generic type mapper for SQL databases.

    Uses conservative type mappings that should work across most SQL databases:
    - VARCHAR/TEXT for strings
    - BIGINT for integers
    - DECIMAL for numbers
    - TIMESTAMP for datetimes
    - TEXT for complex types (JSON-serialized)
    """

    @property
    def dialect(self) -> str:
        return "generic"

    def json_schema_to_native(self, field_def: Dict[str, Any]) -> str:
        """Convert JSON Schema to generic SQL types.

        Args:
            field_def: JSON Schema field definition

        Returns:
            Generic SQL type string
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
                return "TIMESTAMP"
            elif field_format == "date":
                return "DATE"
            elif field_format == "time":
                return "TIME"
            else:
                max_length = field_def.get("maxLength", 255)
                return f"VARCHAR({max_length})" if max_length <= 4000 else "TEXT"

        elif field_type == "integer":
            return "BIGINT"

        elif field_type == "number":
            precision = field_def.get("precision", 15)
            scale = field_def.get("scale", 2)
            return f"DECIMAL({precision},{scale})"

        elif field_type == "boolean":
            return "BOOLEAN"

        elif field_type in ("object", "array"):
            return "TEXT"  # Store as JSON string

        return "TEXT"

    def json_schema_to_sqlalchemy(self, field_def: Dict[str, Any]) -> Any:
        """Convert JSON Schema to SQLAlchemy type.

        Args:
            field_def: JSON Schema field definition

        Returns:
            SQLAlchemy type object
        """
        from sqlalchemy import BigInteger, Boolean, DateTime, Float, String, Text

        field_type = field_def.get("type", "string")
        field_format = field_def.get("format")

        if isinstance(field_type, list) and "null" in field_type:
            field_type = [t for t in field_type if t != "null"][0]

        if field_type == "string":
            if field_format in ("date-time", "date"):
                return DateTime()
            max_length = field_def.get("maxLength", 255)
            return String(max_length) if max_length <= 255 else Text()

        elif field_type == "integer":
            return BigInteger()

        elif field_type == "number":
            return Float()

        elif field_type == "boolean":
            return Boolean()

        return Text()

    def coerce_json(self, value: Any) -> Optional[str]:
        """Coerce value for TEXT columns storing JSON.

        Generic databases may not have native JSON types, so serialize to string.

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
            # Validate it's valid JSON or wrap it
            try:
                json.loads(value)
                return value
            except json.JSONDecodeError:
                return json.dumps({"value": value})

        return json.dumps({"value": value})

    def coerce_array(self, value: Any) -> Optional[str]:
        """Coerce array for TEXT columns (JSON-serialized).

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
