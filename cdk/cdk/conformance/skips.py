"""Applicability gates: skip, with the reason spelled out, never guess.

Each check declares what it applies to; a connector outside that scope
skips loudly so the CI log records *why* nothing ran. Prerequisite
failures (a missing hook, an undeclared block) are reported once by the
declaration-consistency check — dependent tests skip instead of failing
a second time with a less actionable message.
"""

from __future__ import annotations

import pytest

from cdk.sql.capabilities import SqlCapabilities
from cdk.sql.dialects import SqlDialect

from .declaration import dialect_overrides
from .target import ConformanceTarget


def require_database(target: ConformanceTarget) -> None:
    """Skip the calling test for non-database connectors."""
    if not target.is_database:
        pytest.skip(
            f"connector kind is {target.kind!r}; this check applies to kind "
            f"'database' only"
        )


def require_write_role(target: ConformanceTarget) -> None:
    """Skip the calling test when the write-path checks do not apply."""
    require_database(target)
    if not target.has_write_map:
        pytest.skip(
            "connector ships no type-map-write.json (source-only); "
            "write-path checks do not apply"
        )


def require_dialect(target: ConformanceTarget) -> SqlDialect:
    """Return the connector's dialect, or skip naming the prerequisite."""
    require_database(target)
    dialect = target.dialect
    if dialect is None:
        pytest.skip(
            "no connector class or dialect resolved; a database connector's "
            "class carries a dialect_class (for write-capable connectors the "
            "declaration-consistency check reports this as a failure)"
        )
    return dialect


def require_stage_rendering(
    target: ConformanceTarget,
) -> tuple[SqlDialect, SqlCapabilities]:
    """Return the dialect and declared capabilities the stage cycle needs.

    Skips when a prerequisite is missing — each such gap is already a
    declaration-consistency failure, reported once with the actionable
    message; failing every rendering test again would only bury it.
    """
    require_write_role(target)
    dialect = require_dialect(target)
    caps = target.declared_capabilities
    if caps is None:
        pytest.skip(
            "sql_capabilities undeclared; reported by the "
            "declaration-consistency check"
        )
    if not dialect_overrides(type(dialect), "stage_table_sql"):
        pytest.skip(
            "stage_table_sql not implemented; reported by the "
            "declaration-consistency check"
        )
    return dialect, caps
