"""Deliberate breaks fail tier 1 actionably (issue #391 acceptance, part 2).

Each test breaks the reference connector the way a real regression
would — a bent hook signature, an undeclared-capability override, a
private-internal override, a declaration the dialect cannot honor — and
asserts the kit fails with a message naming the offending member.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Sequence
from typing import Any

import pytest

from cdk.conformance import (
    check_declaration_consistency,
    check_override_surface,
    check_type_map_round_trip,
    load_target,
)
from cdk.conformance import target as target_module
from cdk.conformance.target import ConformanceSetupError, ConformanceTarget
from cdk.conformance.tier1 import test_rendering as kit_rendering
from cdk.sql.capabilities import SqlCapabilities
from cdk.sql.dialects import SqlDialect, TableAddress
from cdk.sql.generic import GenericSQLConnector
from cdk.type_map.loader import build_type_mapper

from .kit_runner import REFERENCE_CLASS, REFERENCE_DIR
from .reference_connector import ReferenceConnector, ReferencePostgresDialect


@pytest.fixture()
def reference_target() -> ConformanceTarget:
    return load_target(REFERENCE_DIR, class_path=REFERENCE_CLASS)


def _with_connector(
    target: ConformanceTarget, connector_class: type
) -> ConformanceTarget:
    """The reference target with its connector class swapped."""
    return dataclasses.replace(target, connector_class=connector_class)


def _messages(violations: list[Any]) -> str:
    return "\n".join(str(v) for v in violations)


class _BrokenSignatureDialect(ReferencePostgresDialect):
    def merge_statement_sql(  # type: ignore[override]
        self, stage: TableAddress, target: TableAddress
    ) -> str:
        return "SELECT 1"


class _BrokenSignatureConnector(GenericSQLConnector):
    dialect_class = _BrokenSignatureDialect


class _UndeclaredBulkDialect(ReferencePostgresDialect):
    def bulk_land(
        self,
        conn: Any,
        stage: TableAddress,
        batch: Any,
        *,
        runtime: Any,
    ) -> bool:
        return False


class _UndeclaredBulkConnector(GenericSQLConnector):
    dialect_class = _UndeclaredBulkDialect


class _PrivateDialectOverride(ReferencePostgresDialect):
    def _check_catalog(self, catalog: str) -> None:
        return None


class _PrivateDialectOverrideConnector(GenericSQLConnector):
    dialect_class = _PrivateDialectOverride


class _PrivateFacadeOverrideConnector(ReferenceConnector):
    def _prepare_write_batch(self, state: Any, record_batch: Any) -> Any:
        return record_batch


class _ExtraMemberConnector(ReferenceConnector):
    def load_helper(self) -> None:
        return None


class _NoMergeDialect(SqlDialect):
    name = "conformance_no_merge"

    def stage_table_sql(
        self, stage: TableAddress, target: TableAddress, *, temp: bool
    ) -> str:
        keyword = "CREATE TEMPORARY TABLE" if temp else "CREATE TABLE"
        return f"{keyword} {self.quote_table(stage)} (LIKE {self.quote_table(target)})"


class _NoMergeConnector(GenericSQLConnector):
    dialect_class = _NoMergeDialect


class _WrongFormDialect(ReferencePostgresDialect):
    """Renders ON CONFLICT while the doctored declaration says MERGE."""


class _WrongFormConnector(GenericSQLConnector):
    dialect_class = _WrongFormDialect


class _RenamedKeywordDialect(ReferencePostgresDialect):
    """Renames bulk_land's keyword-only parameter (a bent call shape)."""

    def bulk_land(  # type: ignore[override]
        self,
        conn: Any,
        stage: TableAddress,
        batch: Any,
        *,
        rt: Any,
    ) -> bool:
        return False


class _RenamedKeywordConnector(GenericSQLConnector):
    dialect_class = _RenamedKeywordDialect


class _TableAddressOverrideDialect(ReferencePostgresDialect):
    """Overrides the framework-owned bind-once address factory."""

    def table_address(
        self, table: str, *, schema: str = "", catalog: str = ""
    ) -> TableAddress:
        return super().table_address(table, schema=schema, catalog=catalog)


class _TableAddressOverrideConnector(GenericSQLConnector):
    dialect_class = _TableAddressOverrideDialect


class _ExtraDefaultParamDialect(ReferencePostgresDialect):
    """Adds a defaulted parameter of its own — the documented allowance."""

    def merge_statement_sql(
        self,
        stage: TableAddress,
        target: TableAddress,
        conflict_keys: Sequence[str],
        columns: Sequence[str],
        _annotate: bool = False,
    ) -> str:
        return super().merge_statement_sql(stage, target, conflict_keys, columns)


class _ExtraDefaultParamConnector(GenericSQLConnector):
    dialect_class = _ExtraDefaultParamDialect


class _StaticHookDialect(ReferencePostgresDialect):
    """Implements a sanctioned hook as a staticmethod (allowed shape)."""

    @staticmethod
    def current_timestamp_default() -> str:
        return "CURRENT_TIMESTAMP"


class _StaticHookConnector(GenericSQLConnector):
    dialect_class = _StaticHookDialect


class _MergeFormDialect(ReferencePostgresDialect):
    """Renders the MERGE form, for the merge_form: 'merge' rendering arm."""

    def merge_statement_sql(
        self,
        stage: TableAddress,
        target: TableAddress,
        conflict_keys: Sequence[str],
        columns: Sequence[str],
    ) -> str:
        column_list = ", ".join(self.quote_ident(c) for c in columns)
        match = " AND ".join(
            f"t.{self.quote_ident(c)} = s.{self.quote_ident(c)}" for c in conflict_keys
        )
        update_columns = [c for c in columns if c not in set(conflict_keys)]
        statement = (
            f"MERGE INTO {self.quote_table(target)} t "
            f"USING {self.quote_table(stage)} s ON ({match})"
        )
        if update_columns:
            assignments = ", ".join(
                f"t.{self.quote_ident(c)} = s.{self.quote_ident(c)}"
                for c in update_columns
            )
            statement += f" WHEN MATCHED THEN UPDATE SET {assignments}"
        values = ", ".join(f"s.{self.quote_ident(c)}" for c in columns)
        return (
            statement
            + f" WHEN NOT MATCHED THEN INSERT ({column_list}) VALUES ({values})"
        )


class _MergeFormConnector(GenericSQLConnector):
    dialect_class = _MergeFormDialect


class TestOverrideSurfaceBreaks:
    def test_broken_hook_signature_fails_naming_the_hook(
        self, reference_target: ConformanceTarget
    ) -> None:
        violations = check_override_surface(
            _with_connector(reference_target, _BrokenSignatureConnector)
        )
        report = _messages(violations)
        assert violations, "a bent sanctioned-hook signature must fail tier 1"
        assert "merge_statement_sql" in report
        assert (
            "conflict_keys" in report
        ), f"the failure must name what the call shape needs: {report}"

    def test_private_dialect_override_fails(
        self, reference_target: ConformanceTarget
    ) -> None:
        violations = check_override_surface(
            _with_connector(reference_target, _PrivateDialectOverrideConnector)
        )
        assert violations
        assert "_check_catalog" in _messages(violations)

    def test_private_facade_override_fails(
        self, reference_target: ConformanceTarget
    ) -> None:
        violations = check_override_surface(
            _with_connector(reference_target, _PrivateFacadeOverrideConnector)
        )
        assert violations
        assert "_prepare_write_batch" in _messages(violations)

    def test_extra_connector_class_member_fails(
        self, reference_target: ConformanceTarget
    ) -> None:
        violations = check_override_surface(
            _with_connector(reference_target, _ExtraMemberConnector)
        )
        assert violations
        assert "load_helper" in _messages(violations)

    def test_renamed_keyword_only_parameter_fails(
        self, reference_target: ConformanceTarget
    ) -> None:
        """The keyword-only call shape (bulk_land's runtime) is enforced."""
        violations = check_override_surface(
            _with_connector(reference_target, _RenamedKeywordConnector)
        )
        report = _messages(violations)
        assert "bulk_land" in report
        assert (
            "runtime" in report
        ), f"the failure must name the renamed keyword parameter: {report}"

    def test_framework_owned_attribute_override_fails(
        self, reference_target: ConformanceTarget
    ) -> None:
        """Overriding table_address bypasses the catalog gate; refused."""
        violations = check_override_surface(
            _with_connector(reference_target, _TableAddressOverrideConnector)
        )
        report = _messages(violations)
        assert "table_address" in report
        assert "framework-owned" in report

    def test_extra_defaulted_parameter_is_allowed(
        self, reference_target: ConformanceTarget
    ) -> None:
        """The documented allowance: an override may add defaulted params."""
        assert (
            check_override_surface(
                _with_connector(reference_target, _ExtraDefaultParamConnector)
            )
            == []
        )

    def test_staticmethod_hook_is_allowed(
        self, reference_target: ConformanceTarget
    ) -> None:
        """A self-less staticmethod override of a sanctioned hook passes."""
        assert (
            check_override_surface(
                _with_connector(reference_target, _StaticHookConnector)
            )
            == []
        )


class TestDeclarationBreaks:
    def test_undeclared_bulk_override_fails(
        self, reference_target: ConformanceTarget
    ) -> None:
        doctored = dataclasses.replace(
            _with_connector(reference_target, _UndeclaredBulkConnector),
            declared_capabilities=_caps_with(reference_target, bulk_load="none"),
        )
        violations = check_declaration_consistency(doctored)
        report = _messages(violations)
        assert "bulk_land" in report
        assert "bulk_load" in report

    def test_declared_merge_without_rendering_fails(
        self, reference_target: ConformanceTarget
    ) -> None:
        violations = check_declaration_consistency(
            _with_connector(reference_target, _NoMergeConnector)
        )
        assert "merge_statement_sql" in _messages(violations)

    def test_declared_bulk_without_implementation_fails(
        self, reference_target: ConformanceTarget
    ) -> None:
        violations = check_declaration_consistency(
            _with_connector(reference_target, _NoMergeConnector)
        )
        report = _messages(violations)
        assert "copy_from" in report
        assert "bulk_land" in report

    def test_missing_stage_rendering_fails(
        self, reference_target: ConformanceTarget
    ) -> None:
        """A write-capable connector without stage DDL cannot write at all.

        The thin generic class carries the base dialect, which renders no
        stage table; the skip gates in the rendering tests rely on this
        branch reporting the defect.
        """
        violations = check_declaration_consistency(
            _with_connector(reference_target, GenericSQLConnector)
        )
        assert "stage_table_sql" in _messages(violations)

    def test_undeclared_capabilities_fail_for_a_write_connector(
        self, reference_target: ConformanceTarget
    ) -> None:
        """No sql_capabilities on a write-capable connector is a defect.

        The rendering and live tiers skip on this prerequisite, so this
        branch is what keeps the skip from reading as a pass.
        """
        doctored = dataclasses.replace(reference_target, declared_capabilities=None)
        violations = check_declaration_consistency(doctored)
        assert "sql_capabilities" in _messages(violations)

    def test_adbc_ingest_without_adbc_transport_fails(
        self, reference_target: ConformanceTarget
    ) -> None:
        """adbc_ingest on a SQLAlchemy-only connector can never run."""
        doctored = dataclasses.replace(
            reference_target,
            declared_capabilities=_caps_with(reference_target, bulk_load="adbc_ingest"),
        )
        violations = check_declaration_consistency(doctored)
        report = _messages(violations)
        assert "adbc_ingest" in report
        assert "transport" in report

    def test_merge_rendering_without_declaration_fails(
        self, reference_target: ConformanceTarget
    ) -> None:
        """The used-but-undeclared twin of the bulk case."""
        doctored = dataclasses.replace(
            reference_target,
            declared_capabilities=_caps_with(reference_target, merge_form="none"),
        )
        violations = check_declaration_consistency(doctored)
        report = _messages(violations)
        assert "merge_statement_sql" in report
        assert "'none'" in report


class TestTargetLoadingBreaks:
    def test_failing_entry_point_is_a_hard_error(
        self, reference_target: ConformanceTarget, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Unlike the engine's best-effort discovery, the suite must not
        fall back to the generic class when the connector's own entry
        point fails to load — that would pass every class check
        vacuously for exactly the defect the suite exists to surface."""

        class _FailingEntryPoint:
            name = reference_target.connector_id
            dist = None

            @staticmethod
            def load() -> type:
                raise ImportError("connector module is broken")

        monkeypatch.setattr(
            target_module.metadata,
            "entry_points",
            lambda group: [_FailingEntryPoint()],
        )
        with pytest.raises(ConformanceSetupError, match="failed to load"):
            load_target(reference_target.root)


class _LifecycleDunderConnector(ReferenceConnector):
    def __init__(self) -> None:
        super().__init__()
        self.eager_state: dict[str, str] = {}


class TestDunderBreaks:
    def test_connector_init_override_fails(
        self, reference_target: ConformanceTarget
    ) -> None:
        """An authored lifecycle dunder is facade coupling, not metadata."""
        violations = check_override_surface(
            _with_connector(reference_target, _LifecycleDunderConnector)
        )
        assert "__init__" in _messages(violations)


class TestGateInversionBreaks:
    """A defect must never disable the gate that would have caught it."""

    def test_missing_write_map_with_write_hooks_fails(
        self, reference_target: ConformanceTarget
    ) -> None:
        """A forgotten type-map-write.json must not switch write checks off.

        Without this branch, the missing file makes the target
        source-only, every write check skips, and the connector goes
        green while every customer write is refused at handshake.
        """
        read_only_mapper = build_type_mapper(
            "no-write-map",
            [{"match": "exact", "native": "TEXT", "canonical": "Utf8"}],
        )
        doctored = dataclasses.replace(reference_target, type_mapper=read_only_mapper)
        violations = check_declaration_consistency(doctored)
        report = _messages(violations)
        assert "type-map-write.json" in report
        assert "stage_table_sql" in report


class TestTypeMapBreaks:
    def test_zero_probe_coverage_fails(self) -> None:
        """A write map rendering no probe must not read as fully certified."""
        mapper = build_type_mapper(
            "zero-coverage",
            [{"match": "exact", "native": "JSONB", "canonical": "Json"}],
            [
                {
                    "match": "regex",
                    "canonical": "^(List|LargeList)<.+>$",
                    "native": "JSONB",
                }
            ],
        )
        violations = check_type_map_round_trip(mapper)
        report = _messages(violations)
        assert "type-map-coverage" in report
        assert "rendered none" in report

    def test_partial_family_regex_is_not_flagged_dead(self) -> None:
        """A rule covering part of a parameterized family is legitimate.

        The finite probe set cannot prove such a rule unreachable, so it
        must never be reported dead — only provable normalization
        defects are.
        """
        mapper = build_type_mapper(
            "partial-family",
            [
                {"match": "exact", "native": "TEXT", "canonical": "Utf8"},
                {
                    "match": "regex",
                    "native": "^NUMERIC\\((?<p>[1-5]), (?<s>\\d)\\)$",
                    "canonical": "Decimal128(${p}, ${s})",
                },
            ],
            [
                {"match": "exact", "canonical": "Utf8", "native": "TEXT"},
                # Covers only precision 1-5: matches no probe, but valid.
                {
                    "match": "regex",
                    "canonical": "^Decimal128\\((?<p>[1-5]), (?<s>\\d)\\)$",
                    "native": "NUMERIC(${p}, ${s})",
                },
            ],
        )
        violations = check_type_map_round_trip(mapper)
        assert violations == [], (
            f"a partial-family regex must not be reported dead: "
            f"{_messages(violations)}"
        )

    def test_dead_write_rule_fails(self) -> None:
        """A regex no normalized canonical can match is a dead rule."""
        mapper = build_type_mapper(
            "dead-rule",
            [{"match": "exact", "native": "TEXT", "canonical": "Utf8"}],
            [
                {"match": "exact", "canonical": "Utf8", "native": "TEXT"},
                # No space after the comma: the normalizer always emits
                # ", ", so this pattern can never match a probe.
                {
                    "match": "regex",
                    "canonical": "^Decimal128\\((?<p>\\d+),(?<s>\\d+)\\)$",
                    "native": "NUMERIC(${p}, ${s})",
                },
            ],
        )
        violations = check_type_map_round_trip(mapper)
        report = _messages(violations)
        assert "type-map-coverage" in report
        assert "Decimal128" in report

    def test_hint_requiring_rule_fails_like_production_ddl(self) -> None:
        """A template needing a hint no capture provides must fail.

        The engine's DDL path renders with no per-column hints; a rule
        like ``Utf8 -> VARCHAR(${length})`` fails on the first customer
        table, so the kit must render the same way and refuse it —
        never paper over it with fabricated hints.
        """
        mapper = build_type_mapper(
            "hint-break",
            [{"match": "exact", "native": "TEXT", "canonical": "Utf8"}],
            [{"match": "exact", "canonical": "Utf8", "native": "VARCHAR(${length})"}],
        )
        violations = check_type_map_round_trip(mapper)
        report = _messages(violations)
        assert (
            "length" in report
        ), f"the hint-requiring template must be reported: {report}"

    def test_unreadable_rendered_native_fails_read_closure(self) -> None:
        """A write rule rendering a native the read map cannot map back."""
        mapper = build_type_mapper(
            "closure-break",
            [{"match": "exact", "native": "TEXT", "canonical": "Utf8"}],
            [{"match": "exact", "canonical": "Utf8", "native": "INTERVAL"}],
        )
        violations = check_type_map_round_trip(mapper)
        assert violations, "an unreadable rendered native must fail"
        report = _messages(violations)
        assert "type-map-read-closure" in report
        assert "INTERVAL" in report

    def test_non_convergent_pair_fails(self) -> None:
        """One write/read round that never reaches a fixed point."""
        mapper = build_type_mapper(
            "convergence-break",
            [
                {"match": "exact", "native": "TEXT", "canonical": "LargeUtf8"},
                {"match": "exact", "native": "CLOB", "canonical": "LargeUtf8"},
            ],
            [
                {"match": "exact", "canonical": "Utf8", "native": "TEXT"},
                {"match": "exact", "canonical": "LargeUtf8", "native": "CLOB"},
            ],
        )
        violations = check_type_map_round_trip(mapper)
        report = _messages(violations)
        assert (
            "type-map-convergence" in report
        ), f"Utf8 -> TEXT -> LargeUtf8 -> CLOB must be reported: {report}"


class TestConformantVariantsPass:
    """The rendering arms beyond the reference's own declaration."""

    def test_merge_form_dialect_passes_the_rendering_checks(
        self, reference_target: ConformanceTarget
    ) -> None:
        """A MERGE-form connector passes the same tests the reference does."""
        doctored = dataclasses.replace(
            _with_connector(reference_target, _MergeFormConnector),
            declared_capabilities=_caps_with(reference_target, merge_form="merge"),
        )
        kit_rendering.test_merge_statement_matches_declared_form(doctored)
        kit_rendering.test_merge_statement_references_stage_target_and_keys(doctored)
        kit_rendering.test_merge_with_only_key_columns_degrades_to_insert_only(doctored)
        assert check_override_surface(doctored) == []
        assert check_declaration_consistency(doctored) == []


class TestRenderingBreaks:
    def test_declared_form_mismatch_fails_the_rendering_test(
        self, reference_target: ConformanceTarget
    ) -> None:
        """Declaring MERGE while rendering ON CONFLICT is declared-but-wrong."""
        doctored = dataclasses.replace(
            _with_connector(reference_target, _WrongFormConnector),
            declared_capabilities=_caps_with(reference_target, merge_form="merge"),
        )
        with pytest.raises(AssertionError, match="merge_form"):
            kit_rendering.test_merge_statement_matches_declared_form(doctored)


def _caps_with(target: ConformanceTarget, **facts: str) -> SqlCapabilities:
    """The reference declaration with named facts replaced."""
    caps = target.declared_capabilities
    assert caps is not None
    return dataclasses.replace(caps, **facts)
