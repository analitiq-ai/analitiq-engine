"""Prerequisite gates: skip, with the reason spelled out, never guess.

A check whose prerequisite the connector does not carry skips loudly, so
the CI log records *why* nothing ran. Each such gap is already reported
once by the declaration-consistency check, with the actionable message;
failing every dependent check again would only bury it.

The connector *kind* is gated elsewhere: a check module states the kinds
it applies to (:mod:`cdk.conformance.applicability`), which both skips
it for every other kind and records what the run covered.
"""

from __future__ import annotations

import pytest

from cdk.sql.capabilities import SqlCapabilities
from cdk.sql.dialects import SqlDialect, dialect_overrides

from .target import ConformanceTarget

#: Kinds the live tier has no round trip for, and why not. A kind listed
#: here is *inapplicable* to tier 2, which is a different statement from
#: "the checks were never written": the kit's rule is that a kind it cannot
#: assess fails rather than passes, and an api connector fully covered at
#: tier 1 must not carry a permanent red for a tier that can never mean
#: anything for it.
NO_LIVE_TIER = {
    "api": (
        "the live tier's value is a round trip against the real provider. A "
        "public CI carries no provider credentials, and a stub HTTP server "
        "would certify the connector's own fixtures rather than the provider "
        "-- worse than nothing, because it reads green. Kind 'api' is "
        "assessed at tier 1, where the read path is executed from the "
        "endpoint documents"
    ),
}


def require_live_tier(target: ConformanceTarget) -> None:
    """Skip the calling test when the live tier cannot mean anything here.

    Stated once, at the tier's own applicability gate, rather than inside
    :func:`~cdk.conformance.applicability.check_kind_applicability`: the
    gate exists to catch a kind nothing assesses, and an exemption living
    inside it would clear tier 1 for the same kind the day its checks were
    deleted.
    """
    reason = NO_LIVE_TIER.get(target.kind)
    if reason:
        pytest.skip(f"no live tier for connector kind {target.kind!r}: {reason}")


def require_write_role(target: ConformanceTarget) -> None:
    """Skip the calling test when the write-path checks do not apply."""
    if not target.has_write_map:
        pytest.skip(
            "connector ships no type-map-write.json (source-only); "
            "write-path checks do not apply"
        )


def require_dialect(target: ConformanceTarget) -> SqlDialect:
    """Return the connector's dialect, or skip naming the prerequisite."""
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
