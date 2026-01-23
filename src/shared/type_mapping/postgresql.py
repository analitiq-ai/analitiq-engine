"""PostgreSQL-specific type mapper.

Handles PostgreSQL's native types including JSONB, TIMESTAMPTZ, native arrays,
and other PostgreSQL-specific features.
"""

import json
import logging
from datetime import datetime, date, time, timezone
from typing import Any, Dict, List, Optional

from .base import BaseTypeMapper

logger = logging.getLogger(__name__)


class PostgreSQLTypeMapper(BaseTypeMapper):
    """Type mapper for PostgreSQL databases.

    PostgreSQL features handled:
    - JSONB for complex objects (asyncpg handles serialization)
    - TIMESTAMPTZ for timezone-aware datetimes
    - Native arrays (TEXT[], BIGINT[], etc.)
    - DECIMAL with precision/scale
    - UUID type
    """

    @property
    def dialect(self) -> str:
        return "postgresql"

    def json_schema_to_native(self, field_def: Dict[str, Any]) -> str:
        """Convert JSON Schema to PostgreSQL-specific types.

        Args:
            field_def: JSON Schema field definition

        Returns:
            PostgreSQL type string
        """
        # Check for explicit database_type first
        if "database_type" in field_def:
            return field_def["database_type"]

        field_type = field_def.get("type", "string")
        field_format = field_def.get("format")

        # Handle nullable types (JSON Schema union with null)
        if isinstance(field_type, list) and "null" in field_type:
            field_type = [t for t in field_type if t != "null"][0]

        # Type mapping with PostgreSQL specifics
        if field_type == "string":
            if field_format == "date-time":
                return "TIMESTAMPTZ"
            elif field_format == "date":
                return "DATE"
            elif field_format == "time":
                return "TIME"
            elif field_format == "uuid":
                return "UUID"
            else:
                max_length = field_def.get("maxLength", 255)
                return f"VARCHAR({max_length})" if max_length <= 10485760 else "TEXT"

        elif field_type == "integer":
            return "BIGINT"

        elif field_type == "number":
            # Use precision and scale if available
            precision = field_def.get("precision", 15)
            scale = field_def.get("scale", 2)
            return f"DECIMAL({precision},{scale})"

        elif field_type == "boolean":
            return "BOOLEAN"

        elif field_type == "object":
            return "JSONB"

        elif field_type == "array":
            # Determine array element type
            items_type = field_def.get("items", {}).get("type", "string")
            if items_type == "string":
                return "TEXT[]"
            elif items_type == "integer":
                return "BIGINT[]"
            elif items_type == "number":
                return "DOUBLE PRECISION[]"
            elif items_type == "boolean":
                return "BOOLEAN[]"
            else:
                # Complex arrays -> JSONB
                return "JSONB"

        return "TEXT"

    def json_schema_to_sqlalchemy(self, field_def: Dict[str, Any]) -> Any:
        """Convert JSON Schema to SQLAlchemy type for PostgreSQL.

        Args:
            field_def: JSON Schema field definition

        Returns:
            SQLAlchemy type object
        """
        from sqlalchemy import BigInteger, Boolean, DateTime, Float, String, Text
        from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID

        if "database_type" in field_def:
            db_type = field_def["database_type"].upper()
            if db_type == "JSONB":
                return JSONB()
            if db_type == "UUID":
                return UUID(as_uuid=False)
            if "[]" in db_type:
                return JSONB()  # Fallback for arrays

        field_type = field_def.get("type", "string")
        field_format = field_def.get("format")

        if isinstance(field_type, list) and "null" in field_type:
            field_type = [t for t in field_type if t != "null"][0]

        if field_type == "string":
            if field_format == "date-time":
                return DateTime(timezone=True)
            elif field_format == "date":
                return DateTime()
            elif field_format == "uuid":
                return UUID(as_uuid=False)
            max_length = field_def.get("maxLength", 255)
            return String(max_length) if max_length <= 255 else Text()

        elif field_type == "integer":
            return BigInteger()

        elif field_type == "number":
            return Float()

        elif field_type == "boolean":
            return Boolean()

        elif field_type in ("object", "array"):
            return JSONB()

        return Text()

    def coerce_datetime(self, value: Any) -> Optional[datetime]:
        """Coerce to datetime for PostgreSQL TIMESTAMP/TIMESTAMPTZ.

        asyncpg requires actual datetime objects, not strings.

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
            # Convert date to datetime at midnight
            return datetime.combine(value, time.min)

        if isinstance(value, str):
            parsed = self._parse_datetime_string(value)
            if parsed:
                return parsed
            logger.warning(f"PostgreSQL: Failed to parse datetime '{value}'")
            return None

        # Try timestamp conversion
        try:
            return datetime.fromtimestamp(float(value))
        except (ValueError, TypeError, OSError) as e:
            logger.warning(f"PostgreSQL: Cannot coerce '{value}' to datetime: {e}")
            return None

    def coerce_datetime_with_timezone(self, value: Any) -> Optional[datetime]:
        """Coerce to timezone-aware datetime for TIMESTAMPTZ columns.

        If the parsed datetime is naive, assumes UTC.

        Args:
            value: Value to coerce

        Returns:
            Timezone-aware datetime or None
        """
        dt = self.coerce_datetime(value)
        if dt is None:
            return None

        # Make timezone-aware if naive
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)

        return dt

    def coerce_json(self, value: Any) -> Any:
        """Coerce value for PostgreSQL JSONB columns.

        asyncpg handles JSONB serialization, so we can pass dicts directly.

        Args:
            value: Value to coerce

        Returns:
            Dict, list, or JSON-compatible value
        """
        if value is None:
            return None

        # If already a dict or list, return as-is (asyncpg handles JSONB)
        if isinstance(value, (dict, list)):
            return value

        if isinstance(value, str):
            # Try to parse as JSON
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return {"value": value}

        return {"value": value}

    def coerce_array(self, value: Any) -> Optional[List]:
        """Coerce array for PostgreSQL ARRAY columns.

        PostgreSQL with asyncpg can handle native Python lists for ARRAY types.

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
            # Try to parse JSON array
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    return parsed
            except json.JSONDecodeError:
                pass

        # Wrap single value in list
        return [value]

    def coerce_uuid(self, value: Any) -> Optional[str]:
        """Coerce UUID for PostgreSQL UUID columns.

        asyncpg accepts UUID strings directly.

        Args:
            value: Value to coerce

        Returns:
            UUID string or None
        """
        if value is None:
            return None

        if isinstance(value, str):
            # Validate UUID format
            import re
            uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            if re.match(uuid_pattern, value.lower()):
                return value.lower()
            logger.warning(f"Invalid UUID format: '{value}'")
            return None

        return str(value)
