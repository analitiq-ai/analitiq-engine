"""Deliberate breaks fail tier 1 actionably (issue #391 acceptance, part 2).

Each test breaks the reference connector the way a real regression
would — a bent hook signature, an undeclared-capability override, a
private-internal override, a declaration the dialect cannot honor — and
asserts the kit fails with a message naming the offending member.
"""

from __future__ import annotations

import dataclasses
import json
import shutil
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest
from _pytest.outcomes import Skipped

import cdk.registry
from cdk.conformance import (
    check_api_page_references,
    check_api_query_bindings,
    check_api_read_advances,
    check_api_read_compiles,
    check_api_read_stop_condition,
    check_api_record_schema,
    check_api_request_placements,
    check_declaration_consistency,
    check_override_surface,
    check_read_transport_selection,
    check_type_map_grammar,
    check_type_map_round_trip,
    load_target,
)
from cdk.conformance import target as target_module
from cdk.conformance import violation_report
from cdk.conformance.target import (
    ConformanceSetupError,
    ConformanceTarget,
    _resolve_connector_class,
)
from cdk.conformance.tier1 import test_rendering as kit_rendering
from cdk.sql.capabilities import SqlCapabilities
from cdk.sql.dialects import SqlDialect, TableAddress
from cdk.sql.generic import GenericSQLConnector
from cdk.type_map.loader import build_type_mapper

from .kit_runner import API_REFERENCE_DIR, REFERENCE_CLASS, REFERENCE_DIR
from .reference_connector import ReferenceConnector, ReferencePostgresDialect


def _report(violations: Sequence[Any]) -> str:
    """The kit's own rendering of a check's findings, for assertions."""
    assert violations, "the check reported nothing"
    return violation_report(violations)


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


class _PublicAdditionDialect(ReferencePostgresDialect):
    """Carries a stale pre-v2 flag and a public helper of its own."""

    supports_upsert_sqlalchemy = True

    def build_upsert_helper(self) -> str:
        return "SELECT 1"


class _PublicAdditionConnector(GenericSQLConnector):
    dialect_class = _PublicAdditionDialect


class _PrivateHelperDialect(ReferencePostgresDialect):
    """The same helper under a leading underscore — the sanctioned spelling."""

    def _build_upsert_helper(self) -> str:
        return "SELECT 1"


class _PrivateHelperConnector(GenericSQLConnector):
    dialect_class = _PrivateHelperDialect


class _HelperMixin:
    """A helper mixin listed after the framework base in the MRO."""

    def mixin_helper(self) -> None:
        return None


class _TrailingMixinDialect(ReferencePostgresDialect, _HelperMixin):
    """Inherits a public helper from a mixin behind the framework base."""


class _TrailingMixinDialectConnector(GenericSQLConnector):
    dialect_class = _TrailingMixinDialect


class _TrailingMixinConnector(ReferenceConnector, _HelperMixin):
    """The connector-class variant of the trailing-mixin injection."""


class _RequiredOptionalDialect(ReferencePostgresDialect):
    """Makes render_column_type's optional params keyword required."""

    def render_column_type(  # type: ignore[override]
        self, canonical: str, type_mapper: Any, *, params: Any
    ) -> str:
        return super().render_column_type(canonical, type_mapper, params=params)


class _RequiredOptionalConnector(GenericSQLConnector):
    dialect_class = _RequiredOptionalDialect


class _PositionalOnlyDialect(ReferencePostgresDialect):
    """De-keywords empty_table_sql's keyword-passable target parameter."""

    def empty_table_sql(self, target: TableAddress, /) -> str:
        return super().empty_table_sql(target)


class _PositionalOnlyConnector(GenericSQLConnector):
    dialect_class = _PositionalOnlyDialect


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


class _WrongMatchKeyDialect(ReferencePostgresDialect):
    """Names every conflict key, but matches rows on the id column.

    The keys still appear in the statement (in the inserted column list
    and the assignments), so only reading the match region catches it.
    """

    def merge_statement_sql(
        self,
        stage: TableAddress,
        target: TableAddress,
        conflict_keys: Sequence[str],
        columns: Sequence[str],
    ) -> str:
        return super().merge_statement_sql(stage, target, ["id"], columns)


class _WrongMatchKeyConnector(GenericSQLConnector):
    dialect_class = _WrongMatchKeyDialect


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

    def test_trailing_mixin_on_dialect_fails(
        self, reference_target: ConformanceTarget
    ) -> None:
        """A mixin behind the framework base still supplies attributes.

        MRO position grants no exemption: the helper is reachable on the
        dialect either way, so it must be audited either way.
        """
        violations = check_override_surface(
            _with_connector(reference_target, _TrailingMixinDialectConnector)
        )
        assert "mixin_helper" in _messages(violations)

    def test_trailing_mixin_on_connector_class_fails(
        self, reference_target: ConformanceTarget
    ) -> None:
        violations = check_override_surface(
            _with_connector(reference_target, _TrailingMixinConnector)
        )
        assert "mixin_helper" in _messages(violations)

    def test_optional_parameter_made_required_fails(
        self, reference_target: ConformanceTarget
    ) -> None:
        """The base signature admits calls that omit defaulted parameters.

        DDL calls render_column_type without params; an override that
        requires it binds the fully-populated call but breaks the first
        real DDL operation, so the omitting shape must be checked too.
        """
        violations = check_override_surface(
            _with_connector(reference_target, _RequiredOptionalConnector)
        )
        report = _messages(violations)
        assert "render_column_type" in report
        assert "params" in report
        assert "omitting" in report

    def test_keyword_parameter_made_positional_only_fails(
        self, reference_target: ConformanceTarget
    ) -> None:
        """The base signature admits keyword calls; de-keywording narrows it."""
        violations = check_override_surface(
            _with_connector(reference_target, _PositionalOnlyConnector)
        )
        report = _messages(violations)
        assert "empty_table_sql" in report
        assert "keyword" in report

    def test_public_dialect_addition_fails_naming_each_attribute(
        self, reference_target: ConformanceTarget
    ) -> None:
        """A dialect's public namespace equals the base surface exactly.

        The public-addition hole is where stale hooks from older write
        paths hide (a pre-v2 ``supports_upsert_sqlalchemy`` passes every
        other check silently); both a data attribute and a method the
        base does not define must fail, each named.
        """
        violations = check_override_surface(
            _with_connector(reference_target, _PublicAdditionConnector)
        )
        report = _messages(violations)
        assert "supports_upsert_sqlalchemy" in report
        assert "build_upsert_helper" in report
        assert (
            "underscore" in report
        ), f"the failure must say how to spell a legitimate helper: {report}"

    def test_private_dialect_helper_is_allowed(
        self, reference_target: ConformanceTarget
    ) -> None:
        """A connector's own helper lives under a leading underscore."""
        assert (
            check_override_surface(
                _with_connector(reference_target, _PrivateHelperConnector)
            )
            == []
        )

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
            declared_capabilities=_caps_with(reference_target, bulk_load={}),
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
            declared_capabilities=_caps_with(
                reference_target, bulk_load={"adbc": "adbc_ingest"}
            ),
        )
        violations = check_declaration_consistency(doctored)
        report = _messages(violations)
        assert "adbc_ingest" in report
        assert "ships no adbc transport" in report

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

    def test_case_variant_entry_point_is_matched(
        self, reference_target: ConformanceTarget, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Entry-point matching mirrors the registry's case-insensitive
        resolution: a name differing from connector_id only by case is
        the class production loads, so the suite must audit that class —
        never silently fall back to the generic one."""

        class _CaseVariantEntryPoint:
            name = reference_target.connector_id.upper()
            dist = None

            @staticmethod
            def load() -> type:
                return ReferenceConnector

        monkeypatch.setattr(
            target_module.metadata,
            "entry_points",
            lambda group: [_CaseVariantEntryPoint()],
        )
        loaded = load_target(reference_target.root)
        assert loaded.connector_class is ReferenceConnector

    def test_empty_write_map_file_is_a_setup_error(self, tmp_path: Any) -> None:
        """A shipped-but-empty type-map-write.json must not read as absent.

        Once parsed, an empty write map is indistinguishable from an
        absent one (has_write_map is rule truthiness), and absence gates
        the whole write role off — so the file the connector explicitly
        ships would silently skip every write check.
        """
        root = tmp_path / "reference"
        shutil.copytree(REFERENCE_DIR, root)
        (root / "definition" / "type-map-write.json").write_text("[]")
        with pytest.raises(ConformanceSetupError, match="no rules"):
            load_target(root, class_path=REFERENCE_CLASS)


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

    def test_async_hook_override_fails(
        self, reference_target: ConformanceTarget
    ) -> None:
        """A coroutine hook would hand the CDK an unawaited coroutine."""
        violations = check_override_surface(
            _with_connector(reference_target, _AsyncHookConnector)
        )
        report = _messages(violations)
        assert "session_init_sql" in report
        assert "async" in report


class TestTargetKindGate:
    def test_database_shaped_definition_under_wrong_kind_fails(
        self, reference_target: ConformanceTarget
    ) -> None:
        """A typo'd kind on a database-shaped definition must fail loudly.

        The kind vocabulary is owned by the published schema (and open
        to registry-discovered kinds), so the suite pins no list; the
        mismatch is decided from the definition's own evidence.
        """
        doctored = dataclasses.replace(reference_target, kind="databse")
        violations = check_declaration_consistency(doctored)
        report = _messages(violations)
        assert "databse" in report
        assert "database-shaped" in report

    def test_genuinely_new_kind_passes_through(self, tmp_path: Any) -> None:
        """A registry-discovered kind with no SQL surface is not flagged."""
        definition_dir = tmp_path / "definition"
        definition_dir.mkdir()
        (definition_dir / "connector.json").write_text(
            '{"kind": "queue", "connector_id": "conformance-queue"}'
        )
        target = load_target(tmp_path)
        assert check_declaration_consistency(target) == []


class _AsyncHookDialect(ReferencePostgresDialect):
    # skipcq: PYL-W0236 - the async-ness IS the deliberate defect this
    # fixture models; the kit must reject it, and the test below pins that.
    async def session_init_sql(self) -> list[str]:  # type: ignore[override]
        return []


class _AsyncHookConnector(GenericSQLConnector):
    dialect_class = _AsyncHookDialect


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
    def test_foreign_canonical_literal_fails_and_never_counts(self) -> None:
        """A made-up canonical must be a violation, not self-certifying.

        An exact rule pair Foo <-> TEXT round-trips with itself and
        previously counted toward probe coverage, certifying a write map
        that cannot render any canonical an endpoint document can carry.
        """
        mapper = build_type_mapper(
            "foreign-literal",
            [{"match": "exact", "native": "TEXT", "canonical": "Foo"}],
            [{"match": "exact", "canonical": "Foo", "native": "TEXT"}],
        )
        violations = check_type_map_round_trip(mapper)
        report = _messages(violations)
        assert "Foo" in report
        assert "published grammar" in report
        assert (
            "rendered none" in report
        ), f"the foreign literal must not count toward coverage: {report}"

    def test_source_only_connector_still_earns_its_read_canonicals(self) -> None:
        """No write map is not a reason to certify nothing.

        A source-only connector emits canonicals from discovery alone, so
        a foreign literal in its read map fails at runtime in
        parse_arrow_type. Gating the grammar check on a write map let
        exactly that connector pass with nothing checked.
        """
        mapper = build_type_mapper(
            "source-only",
            [{"match": "exact", "native": "TEXT", "canonical": "Bogus"}],
            None,
        )
        assert not mapper.has_write_map
        report = _messages(check_type_map_grammar(mapper))
        assert "Bogus" in report
        assert "published grammar" in report

    def test_regex_read_rule_with_a_foreign_literal_output_fails(self) -> None:
        """A regex read rule's canonical is its output, so it is checked.

        The match kind says nothing about whether the emitted canonical
        is a literal; this one interpolates no capture, so discovery
        would emit the foreign family verbatim.
        """
        mapper = build_type_mapper(
            "regex-foreign-output",
            [
                {"match": "exact", "native": "TEXT", "canonical": "Utf8"},
                {
                    "match": "regex",
                    "native": "^VARCHAR\\((?<n>\\d+)\\)$",
                    "canonical": "Bogus",
                },
            ],
            [{"match": "exact", "canonical": "Utf8", "native": "TEXT"}],
        )
        report = _messages(check_type_map_grammar(mapper))
        assert "Bogus" in report
        assert "published grammar" in report

    def test_regex_read_rule_interpolating_a_capture_is_not_flagged(self) -> None:
        """A templated canonical is not a literal family; it must pass."""
        mapper = build_type_mapper(
            "regex-templated-output",
            [
                {"match": "exact", "native": "TEXT", "canonical": "Utf8"},
                {
                    "match": "regex",
                    "native": "^NUMERIC\\((?<p>\\d+), *(?<s>\\d+)\\)$",
                    "canonical": "Decimal128(${p}, ${s})",
                },
            ],
            [{"match": "exact", "canonical": "Utf8", "native": "TEXT"}],
        )
        assert check_type_map_grammar(mapper) == []

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

    def test_case_variant_write_rule_fails_with_the_case_reason(self) -> None:
        """A lowercase canonical pattern is dead, and says why truthfully.

        Canonical matching preserves case (the Arrow vocabulary is
        mixed-case), so normalization never rewrites this spelling — the
        rule is dead for a different reason than a comma or unit defect,
        and must not be reported under that explanation.
        """
        mapper = build_type_mapper(
            "case-variant-rule",
            [{"match": "exact", "native": "TEXT", "canonical": "Utf8"}],
            [
                {"match": "exact", "canonical": "Utf8", "native": "TEXT"},
                {
                    "match": "regex",
                    "canonical": "^decimal128\\((?<p>\\d+), (?<s>\\d+)\\)$",
                    "native": "NUMERIC(${p}, ${s})",
                },
            ],
        )
        report = _messages(check_type_map_round_trip(mapper))
        assert "type-map-coverage" in report
        assert "decimal128" in report
        assert "preserves case" in report
        assert "normalization rewrites" not in report

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

    def test_keys_outside_the_match_region_fail(
        self, reference_target: ConformanceTarget
    ) -> None:
        """Naming the key somewhere is not using it as the match key.

        This renderer matches on the id column while still writing every
        declared key into the statement, so it passes a check that only
        asks whether the key appears anywhere.
        """
        doctored = _with_connector(reference_target, _WrongMatchKeyConnector)
        with pytest.raises(AssertionError, match="matched on"):
            kit_rendering.test_merge_statement_references_stage_target_and_keys(
                doctored
            )


def _caps_with(target: ConformanceTarget, **facts: Any) -> SqlCapabilities:
    """The reference declaration with named facts replaced."""
    caps = target.declared_capabilities
    assert caps is not None
    return dataclasses.replace(caps, **facts)


class TestAnApiTargetSkipsTheSqlChecks:
    """An api connector must not error the SQL half of tier 1.

    ``ConformanceTarget.dialect`` answers the SQL dialect specifically, and
    every check that asks for one is a SQL check. Once the registry started
    resolving ``GenericAPIConnector`` for kind ``api``, that class's
    ``dialect_class`` is an ``ApiDialect``, so a type guard written for the
    database family would fail the run of every api connector rather than
    skipping the checks that do not apply to it.
    """

    def _api_target(self) -> ConformanceTarget:
        from cdk.api import GenericAPIConnector

        return ConformanceTarget(
            root=Path("."),
            definition_dir=Path("."),
            definition={"kind": "api", "connector_id": "probe"},
            connector_id="probe",
            kind="api",
            declared_capabilities=None,
            type_mapper=None,
            connector_class=GenericAPIConnector,
        )

    def test_the_sql_dialect_is_absent_rather_than_an_error(self) -> None:
        assert self._api_target().dialect is None

    def test_the_sql_checks_skip_instead_of_failing(self) -> None:
        from cdk.conformance.skips import require_dialect

        # Skipped derives from BaseException, so it has to be named: a bare
        # `except Exception` would let it escape and skip this test instead.
        with pytest.raises(Skipped):
            require_dialect(self._api_target())

    def test_a_database_class_with_a_foreign_dialect_still_fails(self) -> None:
        # The guard exists for a real defect and keeps working where it
        # applies: a database connector whose dialect_class is not a
        # SqlDialect is a broken connector, not a different family.
        class Wrong:
            dialect_class = object

        target = dataclasses.replace(
            self._api_target(), kind="database", connector_class=Wrong
        )
        with pytest.raises(ConformanceSetupError, match="not a SqlDialect"):
            _ = target.dialect


class TestAKindDefaultWithoutItsExtra:
    """Every kind's default names its own extra, not just api's.

    The conformance extra deliberately pulls no transport, so a database
    connector's suite does not fail to start over one it never touches. A
    connector whose transport is genuinely absent has to say which extra it
    needs rather than resolve no class and report itself inapplicable.

    It says so on the target rather than by refusing to load one. Every
    check that reads the class is gated on the kind it applies to, so
    raising here would replace a run's real verdict -- which checks apply,
    which pass -- with one import failure at fixture setup.
    """

    @staticmethod
    def _refuse(missing: str) -> Any:
        """A stand-in for the import machinery with one package absent.

        ``ImportError.name`` is set the way the real machinery sets it,
        because that is the discriminator ``cdk._extras`` uses to tell an
        absent extra from a broken install.
        """

        def _import(module_name: str) -> Any:
            raise ModuleNotFoundError(f"No module named {missing!r}", name=missing)

        return _import

    @pytest.mark.parametrize(
        ("kind", "missing", "extra"),
        [
            ("api", "aiohttp", "api"),
            ("file", "aiofiles", "file"),
            ("s3", "aiofiles", "file"),
            ("stdout", "pyarrow", "arrow"),
        ],
    )
    def test_it_names_the_extra_instead_of_dying_on_the_import(
        self, monkeypatch: pytest.MonkeyPatch, kind: str, missing: str, extra: str
    ) -> None:
        monkeypatch.setattr(cdk.registry, "import_module", self._refuse(missing))

        cls, reason = _resolve_connector_class("not-installed", kind, None)
        assert cls is None
        assert reason is not None
        assert f"analitiq-cdk[{extra}]" in reason
        assert missing in reason

    def test_a_kind_with_no_default_reads_differently_from_one_uninstalled(
        self,
    ) -> None:
        # Two ways to have no class, and a check that needs one has to be
        # able to tell them apart: the CDK ships nothing for this kind,
        # versus it ships something this install cannot import.
        assert _resolve_connector_class("not-installed", "redis", None) == (None, None)

    def test_a_broken_install_is_not_relabelled_as_a_missing_extra(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A present-but-broken pyarrow fails on one of its own submodules.
        # Calling that "install the extra" would send the operator chasing a
        # package they already have.
        monkeypatch.setattr(cdk.registry, "import_module", self._refuse("pyarrow.lib"))

        with pytest.raises(ModuleNotFoundError) as exc:
            _resolve_connector_class("not-installed", "database", None)
        assert exc.value.name == "pyarrow.lib"


class TestApiReadPathBreaks:
    """A bent api endpoint document fails the drives that execute it.

    Each break here is one an author makes and a green suite would ship:
    a paging scheme with nowhere to go reads one page and reports success,
    a stop condition reading nothing off the page never ends a traversal,
    and a records ref addressing an undeclared field fails on the first
    response. None of them raises anywhere before the read runs, which is
    why the kit has to drive the read to find them.
    """

    @staticmethod
    def _broken(tmp_path: Path, stem: str, mutate: Any) -> ConformanceTarget:
        """The api fixture with one endpoint's read operation bent."""
        root = tmp_path / "api"
        shutil.copytree(API_REFERENCE_DIR, root)
        document = root / "definition" / "endpoints" / f"{stem}.json"
        parsed = json.loads(document.read_text())
        mutate(parsed["operations"]["read"])
        document.write_text(json.dumps(parsed))
        return load_target(root)

    def test_an_unknown_paging_scheme_names_the_contracts_union(
        self, tmp_path: Path
    ) -> None:
        target = self._broken(
            tmp_path, "widgets", lambda read: read["pagination"].update(type="seek")
        )
        report = _report(check_api_read_compiles(target))
        assert "'seek'" in report
        assert "'offset'" in report, "the message must name the union it is not in"

    def test_a_cursor_param_with_a_default_ships_on_the_first_request(
        self, tmp_path: Path
    ) -> None:
        """The first page has nothing to continue from, so it sends no token."""

        def to_cursor(read: dict[str, Any]) -> None:
            read["params"]["page_token"] = {
                "in": "query",
                "type": "string",
                "default": {"literal": "start"},
            }
            read["pagination"] = {
                "type": "cursor",
                "limit": {"param": "limit", "default": {"ref": "runtime.batch_size"}},
                "cursor": {
                    "param": "page_token",
                    "next_cursor": {"ref": "response.body.next_token"},
                },
                "stop_when": {"missing": {"ref": "response.body.next_token"}},
            }

        target = self._broken(tmp_path, "widgets", to_cursor)
        report = _report(check_api_read_compiles(target))
        assert "'page_token'" in report
        assert "controlled_by" in report

    def test_a_next_value_off_the_page_scope_reads_one_page(
        self, tmp_path: Path
    ) -> None:
        """A traversal that cannot advance is a truncated read reporting success."""

        def to_cursor(read: dict[str, Any]) -> None:
            read["pagination"] = {
                "type": "cursor",
                "limit": {"param": "limit", "default": {"ref": "runtime.batch_size"}},
                # The page scope carries the body and the record count; a
                # header is not in it, so this resolves to nothing forever.
                "cursor": {
                    "param": "page_token",
                    "next_cursor": {"ref": "response.headers.x-next"},
                },
                "stop_when": {"missing": {"ref": "response.body.next_token"}},
            }

        target = self._broken(tmp_path, "widgets", to_cursor)
        report = _report(check_api_read_advances(target))
        assert "reads its first page and reports success" in report

    def test_a_step_that_cannot_advance_is_reported_before_the_yield(
        self, tmp_path: Path
    ) -> None:
        target = self._broken(
            tmp_path,
            "widgets",
            lambda read: read["pagination"]["offset"].update(increment_by=0),
        )
        report = _report(check_api_read_advances(target))
        assert "must be positive" in report

    def test_a_keyset_ordering_field_the_schema_omits_is_not_a_finding(
        self, tmp_path: Path
    ) -> None:
        """The engine walks the provider's record, not the declared schema.

        ``extract_records`` hands the strategy the raw response objects, so
        ordering by a field the provider sends and the response schema does
        not declare reads perfectly well. A check asserting otherwise would
        fail a working connector, which is why the ordering field is planted
        rather than sampled.
        """
        target = self._broken(
            tmp_path,
            "ledger",
            lambda read: read["response"]["schema"]["properties"]["entries"]["items"][
                "properties"
            ].pop("sequence"),
        )
        assert check_api_read_advances(target) == []

    def test_a_stop_condition_reading_nothing_off_the_page(
        self, tmp_path: Path
    ) -> None:
        target = self._broken(
            tmp_path,
            "widgets",
            lambda read: read["pagination"].update(stop_when={"eq": [1, 2]}),
        )
        report = _report(check_api_read_stop_condition(target))
        assert "reads nothing under 'response'" in report

    def test_a_missing_stop_condition(self, tmp_path: Path) -> None:
        target = self._broken(
            tmp_path, "widgets", lambda read: read["pagination"].pop("stop_when")
        )
        report = _report(check_api_read_stop_condition(target))
        assert "declares no stop_when" in report

    def test_a_stop_condition_that_cannot_compare_its_operands(
        self, tmp_path: Path
    ) -> None:
        target = self._broken(
            tmp_path,
            "events",
            lambda read: read["pagination"].update(
                stop_when={"lt": [{"ref": "response.body.links.next"}, 5]}
            ),
        )
        report = _report(check_api_read_stop_condition(target))
        assert "cannot compare str with int" in report

    def test_a_records_ref_the_response_schema_does_not_declare(
        self, tmp_path: Path
    ) -> None:
        target = self._broken(
            tmp_path,
            "widgets",
            lambda read: read["response"]["records"].update(ref="response.body.items"),
        )
        report = _report(check_api_record_schema(target))
        assert "'items'" in report
        assert "not declared under properties" in report

    def test_a_json_type_the_read_map_has_no_rule_for(self, tmp_path: Path) -> None:
        target = self._broken(
            tmp_path,
            "widgets",
            lambda read: read["response"]["schema"]["properties"]["objects"]["items"][
                "properties"
            ]["id"].update(type="geometry"),
        )
        report = _report(check_api_record_schema(target))
        assert "'geometry'" in report
        assert "read type-map" in report

    def test_a_read_asking_for_a_transport_the_path_will_not_open(
        self, tmp_path: Path
    ) -> None:
        target = self._broken(
            tmp_path,
            "widgets",
            lambda read: read["request"].update(transport_ref="oauth"),
        )
        report = _report(check_read_transport_selection(target))
        assert "'oauth'" in report
        assert "default_transport" in report

    def test_a_query_key_that_is_not_its_params_name(self, tmp_path: Path) -> None:
        target = self._broken(
            tmp_path,
            "widgets",
            lambda read: read["request"].update(
                query={"page[limit]": {"from_param": "limit"}}
            ),
        )
        report = _report(check_api_query_bindings(target))
        assert "'page[limit]'" in report
        assert "never sees" in report

    def test_a_query_key_matching_its_param_is_a_no_op(self, tmp_path: Path) -> None:
        """The binding the path happens to honour is not a finding."""
        target = self._broken(
            tmp_path,
            "widgets",
            lambda read: read["request"].update(
                query={"limit": {"from_param": "limit"}}
            ),
        )
        assert check_api_query_bindings(target) == []


class TestApiPageReferenceBreaks:
    """A page value the read declares must be one a page actually carries.

    The drives next door script their page *from* these declarations, so on
    their own they can never find a reference that addresses nothing -- the
    page is built to satisfy whatever the author wrote. These are the cases
    that need something independent to check against: the scope a page has,
    and the response schema the connector published.
    """

    _broken = staticmethod(TestApiReadPathBreaks._broken)

    def test_a_typo_in_a_stop_condition_path(self, tmp_path: Path) -> None:
        """The defect the whole check exists for: a silently truncated read."""
        target = self._broken(
            tmp_path,
            "events",
            lambda read: read["pagination"].update(
                stop_when={"missing": {"ref": "response.body.lnks.next"}}
            ),
        )
        report = _report(check_api_page_references(target))
        assert "'response.body.lnks.next'" in report
        assert "does not reach" in report

    def test_a_scope_the_page_has_no_notion_of(self, tmp_path: Path) -> None:
        target = self._broken(
            tmp_path,
            "invoices",
            lambda read: read["pagination"]["cursor"].update(
                next_cursor={"ref": "response.headers.x-next-page"}
            ),
        )
        report = _report(check_api_page_references(target))
        assert "'response.headers.x-next-page'" in report
        assert "'body', 'record_count'" in report

    def test_a_template_spelled_reference_is_seen(self, tmp_path: Path) -> None:
        """``ref`` is one of two spellings that read a scope, not the only one."""
        target = self._broken(
            tmp_path,
            "events",
            lambda read: read["pagination"]["link"].update(
                next_url={"template": "${response.body.lnks.next}"}
            ),
        )
        report = _report(check_api_page_references(target))
        assert "'response.body.lnks.next'" in report

    def test_a_template_spelled_reference_that_resolves_is_clean(
        self, tmp_path: Path
    ) -> None:
        """And seeing it means not blaming the connector for the kit's blindness."""
        target = self._broken(
            tmp_path,
            "events",
            lambda read: read["pagination"]["link"].update(
                next_url={"template": "${response.body.links.next}"}
            ),
        )
        assert check_api_page_references(target) == []
        assert check_api_read_advances(target) == []


class TestApiScriptedPageTakesTheDeclaredTypes:
    """The page a drive scripts carries the types the connector declared.

    Typing a scripted operand by the paging scheme instead would make the
    verdict the kit's rather than the connector's: the same declaration
    would pass on one scheme and fail on another.
    """

    _broken = staticmethod(TestApiReadPathBreaks._broken)

    def test_a_next_link_pointing_at_its_container_is_caught(
        self, tmp_path: Path
    ) -> None:
        """The declared type wins, so the strategy is handed the real shape."""
        target = self._broken(
            tmp_path,
            "events",
            lambda read: (
                read["pagination"]["link"].update(
                    next_url={"ref": "response.body.links"}
                ),
                read["pagination"].update(
                    stop_when={"missing": {"ref": "response.body.links"}}
                ),
            ),
        )
        report = _report(check_api_read_advances(target))
        assert "not a URL" in report

    def test_a_numeric_stop_operand_on_a_cursor_scheme_is_clean(
        self, tmp_path: Path
    ) -> None:
        """ "returned < requested" is a stop condition, not a defect."""
        target = self._broken(
            tmp_path,
            "invoices",
            lambda read: read["pagination"].update(
                stop_when={
                    "lt": [
                        {"ref": "response.body.meta.returned"},
                        {"ref": "runtime.batch_size"},
                    ]
                }
            ),
        )
        assert check_api_read_stop_condition(target) == []

    def test_the_same_type_defect_is_caught_on_any_scheme(self, tmp_path: Path) -> None:
        """A declared string ordered against a number, on a numeric scheme."""

        def bend(read: dict[str, Any]) -> None:
            read["response"]["schema"]["properties"]["total"] = {"type": "string"}
            read["pagination"].update(
                stop_when={"lt": [{"ref": "response.body.total"}, 5]}
            )

        target = self._broken(tmp_path, "widgets", bend)
        report = _report(check_api_read_stop_condition(target))
        assert "cannot compare str with int" in report


class TestApiRequestPlacementBreaks:
    """A param placed where the api path cannot send it."""

    _broken = staticmethod(TestApiReadPathBreaks._broken)

    def test_a_path_placeholder_is_never_substituted(self, tmp_path: Path) -> None:
        def bend(read: dict[str, Any]) -> None:
            read["request"]["path"] = "/v1/accounts/{account_id}/widgets"
            read["request"]["path_params"] = {
                "account_id": {"from_param": "account_id"}
            }
            read["params"]["account_id"] = {
                "in": "path",
                "type": "string",
                "required": True,
            }

        target = self._broken(tmp_path, "widgets", bend)
        report = _report(check_api_request_placements(target))
        assert "path_params" in report
        assert "carrying the braces" in report

    def test_a_declared_header_is_never_sent(self, tmp_path: Path) -> None:
        def bend(read: dict[str, Any]) -> None:
            read["request"]["headers"] = {"X-Tenant": {"from_param": "tenant"}}
            read["params"]["tenant"] = {
                "in": "header",
                "type": "string",
                "required": False,
            }

        target = self._broken(tmp_path, "widgets", bend)
        report = _report(check_api_request_placements(target))
        assert "'X-Tenant'" in report
        assert "never reaches the provider" in report


class TestApiChecksSayWhenTheyDroveNothing:
    """A dependent check must not report "nothing to say" as "nothing wrong".

    Every check is exported on its own, so a repo may wire one into a
    harness of its own. A read that does not compile has no traversal to
    drive; the compile check says why, and the others say they did not
    drive it.
    """

    _broken = staticmethod(TestApiReadPathBreaks._broken)

    def test_a_read_that_does_not_compile_is_reported_by_every_check(
        self, tmp_path: Path
    ) -> None:
        target = self._broken(
            tmp_path, "widgets", lambda read: read["pagination"].update(type="seek")
        )
        for check in (
            check_api_page_references,
            check_api_read_advances,
            check_api_read_stop_condition,
        ):
            report = _report(check(target))
            assert "not driven" in report, check.__name__
            assert "api-read-compiles" in report, check.__name__
