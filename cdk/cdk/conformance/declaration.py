"""Declared-vs-implemented consistency (ADR sql-write-path-v2 sections 5, 10).

``connector.json`` declares *whether* the system has a SQL shape; the
dialect class renders *how*. The two must agree, both ways:

* **declared-but-wrong** — a declared fact whose rendering the dialect
  does not implement (``merge_form: "merge"`` with no
  ``merge_statement_sql``, a declared bulk mechanism with no
  ``bulk_land``) would refuse or silently downgrade on the first
  customer batch; the suite fails it in CI instead.
* **used-but-undeclared** — a hook override no declaration ever routes
  a call to (``bulk_land`` under ``bulk_load: "none"``, a merge
  rendering under ``merge_form: "none"``) is dead code that reads as
  capability; the suite fails it so the declaration and the code cannot
  drift apart.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cdk.sql.dialects import SqlDialect

from .violations import Violation

if TYPE_CHECKING:
    from .target import ConformanceTarget

CHECK = "declaration-consistency"

#: Bulk mechanisms whose landing runs through the dialect's ``bulk_land``
#: hook. ``adbc_ingest`` is the ADBC backend's own native landing — no
#: dialect code involved — so it neither requires nor forbids the hook
#: (a connector may still implement ``bulk_land`` for its SQLAlchemy
#: transport alongside a declared ``adbc_ingest``).
DIALECT_IMPLEMENTED_BULK_MECHANISMS = frozenset(
    {"copy_from", "load_data_local_infile", "load_job"}
)


def dialect_overrides(dialect_cls: type, hook: str) -> bool:
    """Whether *dialect_cls* replaces the base definition of *hook*.

    The same identity test the facade's handshake gates use
    (``type(dialect).hook is not SqlDialect.hook``), so the suite and
    the runtime can never disagree about what counts as implemented.
    """
    return getattr(dialect_cls, hook) is not getattr(SqlDialect, hook)


def check_declaration_consistency(target: ConformanceTarget) -> list[Violation]:
    """Certify that ``sql_capabilities`` and the dialect agree.

    Applies to write-capable database connectors (the ones shipping
    ``type-map-write.json``); source-only and non-database connectors
    have no write declaration to hold consistent.
    """
    if not target.write_role:
        return []
    violations: list[Violation] = []
    caps = target.declared_capabilities
    if caps is None:
        violations.append(
            Violation(
                CHECK,
                "the connector ships type-map-write.json (a write-capable "
                "connector) but declares no sql_capabilities block in "
                "connector.json; every write is refused at handshake — the "
                "engine never guesses an undeclared capability. Declare the "
                "block (ADR sql-write-path-v2 section 5).",
            )
        )
    dialect = target.dialect
    if dialect is None:
        violations.append(
            Violation(
                CHECK,
                "no dialect resolved for this write-capable connector; a "
                "database connector package carries a GenericSQLConnector "
                "subclass whose dialect_class renders its system's SQL.",
            )
        )
        return violations
    dialect_cls = type(dialect)

    if not dialect_overrides(dialect_cls, "stage_table_sql"):
        violations.append(
            Violation(
                CHECK,
                f"{dialect_cls.__name__} does not implement stage_table_sql; "
                f"every SQL write lands in a stage table first, so a "
                f"write-capable connector without stage DDL cannot write at "
                f"all (ADR sql-write-path-v2 section 2).",
            )
        )

    if caps is None:
        return violations

    renders_merge = dialect_overrides(dialect_cls, "merge_statement_sql")
    if caps.merge_form != "none" and not renders_merge:
        violations.append(
            Violation(
                CHECK,
                f"connector.json declares merge_form {caps.merge_form!r} but "
                f"{dialect_cls.__name__} does not implement "
                f"merge_statement_sql; every upsert stream would fail at "
                f"handshake. Implement the hook or declare "
                f"merge_form 'none'.",
            )
        )
    if caps.merge_form == "none" and renders_merge:
        violations.append(
            Violation(
                CHECK,
                f"{dialect_cls.__name__} implements merge_statement_sql but "
                f"connector.json declares merge_form 'none'; the hook is "
                f"never called. Declare the system's merge form or delete "
                f"the override.",
            )
        )

    implements_bulk = dialect_overrides(dialect_cls, "bulk_land")
    if caps.bulk_load in DIALECT_IMPLEMENTED_BULK_MECHANISMS and not implements_bulk:
        violations.append(
            Violation(
                CHECK,
                f"connector.json declares bulk_load {caps.bulk_load!r} but "
                f"{dialect_cls.__name__} does not implement bulk_land; every "
                f"batch would fall back to executemany, silently negating "
                f"the declared mechanism. Implement the hook or declare "
                f"bulk_load 'none'.",
            )
        )
    if caps.bulk_load == "none" and implements_bulk:
        violations.append(
            Violation(
                CHECK,
                f"{dialect_cls.__name__} implements bulk_land but "
                f"connector.json declares bulk_load 'none'; an undeclared "
                f"mechanism is never called (ADR sql-write-path-v2 section "
                f"5). Declare the mechanism or delete the override.",
            )
        )
    return violations
