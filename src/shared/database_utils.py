"""Shared database helpers used by source and destination components.

The hard-coded engine factory, dialect map, and SSL canonicalization that
used to live here have been replaced by the connector-driven transport
factory at :mod:`src.shared.transport_factory`. What remains is purely
SQL helpers (identifier validation, fully-qualified names, DEFAULT
clauses), connection acquisition, and read-side type conversion — none
of them know anything provider-specific.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, AsyncIterator, Dict

from sqlalchemy.ext.asyncio import AsyncEngine

logger = logging.getLogger(__name__)


def validate_sql_identifier(identifier: str) -> bool:
    """Return ``True`` when *identifier* is a safe SQL identifier.

    A safe identifier consists of alphanumeric characters, underscores,
    and hyphens, and must not start with a digit. Empty strings are
    rejected.
    """
    if not identifier:
        return False
    if not identifier.replace("_", "").replace("-", "").isalnum():
        return False
    if identifier[0].isdigit():
        return False
    return True


def get_full_table_name(schema_name: str, table_name: str) -> str:
    """Return ``schema.table`` (or just ``table`` when schema is empty)."""
    return f"{schema_name}.{table_name}" if schema_name else table_name


@asynccontextmanager
async def acquire_connection(engine: AsyncEngine) -> AsyncIterator:
    """Yield an :class:`AsyncConnection` from *engine* for the lifetime of the
    context manager. Raises :class:`RuntimeError` when the engine is ``None``.
    """
    if engine is None:
        raise RuntimeError("Engine not initialized")
    async with engine.connect() as conn:
        yield conn


def convert_db_to_python(value: Any) -> Any:
    """Convert a single database value to a JSON-friendly Python value.

    The only conversion applied is :class:`datetime` → ISO-8601 string;
    everything else is returned unchanged.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def convert_record_from_db(record: Dict[str, Any]) -> Dict[str, Any]:
    """Apply :func:`convert_db_to_python` to every value in a record dict."""
    return {key: convert_db_to_python(value) for key, value in record.items()}


def get_default_clause(field_def: Dict[str, Any]) -> str:
    """Return the SQL ``DEFAULT`` clause for a JSON-Schema field definition.

    Recognized default forms:

    * ``"NOW()"`` / ``"CURRENT_TIMESTAMP"`` — passed through verbatim.
    * Other strings — quoted as a string literal.
    * Booleans — lower-cased (``true``/``false``).
    * Numbers — passed through verbatim.

    All other types return an empty string (no default clause emitted).
    """
    default_value = field_def.get("default")
    if default_value is None:
        return ""

    if isinstance(default_value, bool):
        return f" DEFAULT {str(default_value).lower()}"
    if isinstance(default_value, str):
        if default_value.upper() in ("NOW()", "CURRENT_TIMESTAMP"):
            return f" DEFAULT {default_value}"
        return f" DEFAULT '{default_value}'"
    if isinstance(default_value, (int, float)):
        return f" DEFAULT {default_value}"

    return ""
