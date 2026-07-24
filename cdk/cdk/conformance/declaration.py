"""Declared-vs-implemented consistency (ADR sql-write-path-v2 sections 5, 10).

``connector.json`` declares *whether* the system has a SQL shape; the
dialect class renders *how*. The two must agree, both ways:

* **declared-but-wrong** — a declared fact whose rendering the dialect
  does not implement (``merge_form: "merge"`` with no
  ``merge_statement_sql``, a declared bulk mechanism with no
  ``bulk_land``) would refuse or silently downgrade on the first
  customer batch; the suite fails it in CI instead.
* **used-but-undeclared** — a hook override no declaration ever routes
  a call to (``bulk_land`` with no declared bulk mechanism, a merge
  rendering under ``merge_form: "none"``) is dead code that reads as
  capability; the suite fails it so the declaration and the code cannot
  drift apart.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cdk.sql.capabilities import DIALECT_IMPLEMENTED_BULK_MECHANISMS, SqlCapabilities
from cdk.sql.dialects import SqlDialect

from .violations import Violation

if TYPE_CHECKING:
    from .target import ConformanceTarget

CHECK = "declaration-consistency"


def declared_transport_types(target: ConformanceTarget) -> set[str]:
    """Collect the ``transport_type`` values the definition declares."""
    return {
        str(block["transport_type"])
        for block in target.declared_transports().values()
        if block.get("transport_type")
    }


def dialect_overrides(dialect_cls: type, hook: str) -> bool:
    """Whether *dialect_cls* replaces the base definition of *hook*.

    The same identity test the facade's handshake gates use
    (``type(dialect).hook is not SqlDialect.hook``), so the suite and
    the runtime can never disagree about what counts as implemented.
    """
    return getattr(dialect_cls, hook) is not getattr(SqlDialect, hook)


def _write_signals(target: ConformanceTarget) -> list[str]:
    """Name every write-capability signal the connector carries.

    Used to catch the inverse inconsistency of the write-role checks: a
    connector that declares or implements writing but ships no
    ``type-map-write.json``. The read-only capability facts (catalog,
    session targeting — and the stage sub-block the parser forces along
    with them) are deliberately not signals: a source-only connector may
    declare them for its read gates.
    """
    signals: list[str] = []
    dialect = target.dialect
    if dialect is not None:
        for hook in ("stage_table_sql", "merge_statement_sql", "bulk_land"):
            if dialect_overrides(type(dialect), hook):
                signals.append(f"the dialect implements {hook}")
    caps = target.declared_capabilities
    if caps is not None:
        if caps.merge_form != "none":
            signals.append(f"connector.json declares merge_form {caps.merge_form!r}")
        if caps.bulk_load:
            signals.append(
                f"connector.json declares bulk_load {dict(caps.bulk_load)!r}"
            )
    return signals


def check_declaration_consistency(target: ConformanceTarget) -> list[Violation]:
    """Certify that ``sql_capabilities`` and the dialect agree.

    The write-path checks apply to write-capable database connectors
    (the ones shipping ``type-map-write.json``). A connector *without* a
    write map is checked for the inverse inconsistency: declaring or
    implementing write capability it cannot use — otherwise a forgotten
    or misnamed write map would silently switch every write check off
    (the gate's input must not be supplied by the defect it gates).
    """
    if not target.is_database:
        return []
    if not target.write_role:
        signals = _write_signals(target)
        if not signals:
            return []
        return [
            Violation(
                CHECK,
                f"the connector ships no type-map-write.json (which makes it "
                f"source-only) but carries write capability: "
                f"{'; '.join(signals)}. Ship the write map, or remove the "
                f"write declarations and hooks.",
            )
        ]
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

    if caps is not None:
        violations.extend(_hook_declaration_violations(caps, dialect_cls))
        shipped = declared_transport_types(target)
        for transport_type, mechanism in caps.bulk_load.items():
            if transport_type not in shipped:
                violations.append(
                    Violation(
                        CHECK,
                        f"connector.json declares bulk_load "
                        f"{{{transport_type!r}: {mechanism!r}}} but ships no "
                        f"{transport_type} transport; the declared mechanism "
                        f"can never run. Declare the transport or drop the "
                        f"entry.",
                    )
                )
    return violations


def _hook_declaration_violations(
    caps: SqlCapabilities, dialect_cls: type
) -> list[Violation]:
    """Both directions of the merge and bulk hook/declaration pairing."""
    violations: list[Violation] = []
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
    hook_mechanisms = {
        transport_type: mechanism
        for transport_type, mechanism in caps.bulk_load.items()
        if mechanism in DIALECT_IMPLEMENTED_BULK_MECHANISMS
    }
    if hook_mechanisms and not implements_bulk:
        violations.append(
            Violation(
                CHECK,
                f"connector.json declares bulk_load {hook_mechanisms!r} but "
                f"{dialect_cls.__name__} does not implement bulk_land; every "
                f"batch on those transports would fall back to executemany, "
                f"silently negating the declared mechanism. Implement the "
                f"hook or drop the entries.",
            )
        )
    if implements_bulk and not hook_mechanisms:
        violations.append(
            Violation(
                CHECK,
                f"{dialect_cls.__name__} implements bulk_land but "
                f"connector.json declares no bulk_load mechanism that routes "
                f"through it (adbc_ingest is the ADBC backend's own "
                f"landing); the hook is never called (ADR sql-write-path-v2 "
                f"section 5). Declare the mechanism or delete the override.",
            )
        )
    return violations
