"""Shared database helpers used by source and destination components.

The hard-coded engine factory, dialect map, and SSL canonicalization
that used to live here have been replaced by the connector-driven
transport factory at :mod:`cdk.transport_factory`. What remains
is pure SQL helpers (identifier validation, fully-qualified names,
DEFAULT clauses) and connection acquisition — none of them know
anything provider-specific.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
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


def normalize_adbc_schema(schema: str, driver: str) -> str:
    """Normalize a schema name for an ADBC driver before it is quoted.

    Snowflake folds unquoted identifiers to upper case; its built-in
    default schema is unquoted ``PUBLIC``. The ADBC paths quote every
    identifier, so a connector declaring the conventional lowercase
    ``public`` would produce a quoted ``"public"`` that targets a
    different (usually non-existent) schema. Upcasing ``public`` ->
    ``PUBLIC`` makes the quoted form match the real schema; any other
    case-sensitive name is preserved verbatim.

    BigQuery and Postgres need no normalization (BigQuery datasets are
    case-sensitive; Postgres folds unquoted to lowercase, so quoted
    ``"public"`` already matches the conventional default). Shared by the
    source reader and the destination handler so read and write resolve
    the same physical schema.
    """
    if driver == "snowflake" and schema.lower() == "public":
        return "PUBLIC"
    return schema


@asynccontextmanager
async def acquire_connection(engine: AsyncEngine) -> AsyncIterator:
    """Yield an :class:`AsyncConnection` from *engine* for the lifetime of the
    context manager. Raises :class:`RuntimeError` when the engine is ``None``.
    """
    if engine is None:
        raise RuntimeError("Engine not initialized")
    async with engine.connect() as conn:
        yield conn


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
