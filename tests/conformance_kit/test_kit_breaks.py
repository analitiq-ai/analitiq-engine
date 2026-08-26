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
from urllib.parse import urljoin

import pytest
from _pytest.outcomes import Skipped
from analitiq.contracts.connector import HttpTransport
from analitiq.contracts.endpoints import ApiEndpointDoc
from pydantic import ValidationError

import cdk.registry
from cdk.api import read_setup, strategies
from cdk.conformance import (
    api_read_path,
    check_api_has_reads,
    check_api_read_advances,
    check_api_read_compiles,
    check_api_read_stop_condition,
    check_api_record_schema,
    check_declaration_consistency,
    check_override_surface,
    check_read_transport_selection,
    check_type_map_grammar,
    check_type_map_round_trip,
    load_target,
)
from cdk.conformance import target as target_module
from cdk.conformance import violation_report
from cdk.conformance.api_surface import api_origins
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

    def test_a_next_value_off_the_page_scope_is_refused_at_compile(
        self, tmp_path: Path
    ) -> None:
        """A reserved response sub-scope is contract-legal and page-absent.

        RULE-ENDP-023 resolves only `response.body` paths; `headers`,
        `status` and `metadata` are recognised, reserved sub-scopes it
        leaves to the engine -- and the read path's page scope does not
        carry them, so this next value resolves to nothing forever and the
        traversal reads one page reporting success. The compile drive is
        where the kit states that page-scope fact.
        """

        def to_cursor(read: dict[str, Any]) -> None:
            read["pagination"] = {
                "type": "cursor",
                "limit": {"param": "limit", "default": {"ref": "runtime.batch_size"}},
                "cursor": {
                    "param": "page_token",
                    "next_cursor": {"ref": "response.headers.x-next"},
                },
                "stop_when": {"missing": {"ref": "response.body.next_token"}},
            }

        target = self._broken(tmp_path, "widgets", to_cursor)
        report = _report(check_api_read_compiles(target))
        assert "'response.headers.x-next'" in report
        assert "carries only 'body', 'record_count'" in report

    def test_a_stop_condition_off_the_page_scope_is_refused_at_compile(
        self, tmp_path: Path
    ) -> None:
        """The stop-operand half of the same fact: `missing` on a header
        holds at page one and the stream stops there reporting success."""

        def bend(read: dict[str, Any]) -> None:
            read["pagination"]["stop_when"] = {
                "missing": {"ref": "response.headers.x-next"}
            }

        target = self._broken(tmp_path, "widgets", bend)
        report = _report(check_api_read_compiles(target))
        assert "'response.headers.x-next'" in report
        assert "resolves to nothing on every page" in report

    def test_pagination_params_that_reach_no_binding_read_one_page_forever(
        self, tmp_path: Path
    ) -> None:
        """The traversal advances its param table and sends the same request.

        A param reaches the wire only through a binding in ``request.query``,
        ``request.headers`` or ``request.body``. Strip the query map and the
        offset strategy still counts rows -- ``advance`` answers a
        ``PageRequest`` whose params differ from page one's -- while every
        request built from it is byte-for-byte the first one. A drive
        comparing the param tables reports nothing here and certifies a
        connector that fetches page one until the provider gets bored.
        """
        target = self._broken(
            tmp_path, "widgets", lambda read: read["request"].pop("query")
        )
        report = _report(check_api_read_advances(target))
        assert (
            "the request after the first page is the request before it again" in report
        )
        assert "request.query" in report, "the report must name the missing sink"

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

    def test_a_next_url_function_handed_the_wrong_type_is_reported(
        self, tmp_path: Path
    ) -> None:
        """The drive exists to catch exactly this, so it must not die on it.

        ``base64_encode`` answers a non-string input with ``TypeError``.
        Nothing between the strategy and the check enumerated that type, so
        the authoring mistake the module docstring says the drive exists to
        catch arrived as a raw traceback: the check ERRORed, and every probe
        queued behind it was never driven at all.
        """

        def bend(read: dict[str, Any]) -> None:
            read["pagination"]["link"]["next_url"] = {
                "function": "base64_encode",
                "input": {"ref": "response.body.links"},
            }

        target = self._broken(tmp_path, "events", bend)
        report = _report(check_api_read_advances(target))
        assert "must resolve to string or bytes" in report

    def test_a_stop_condition_function_handed_the_wrong_type_is_reported(
        self, tmp_path: Path
    ) -> None:
        """Same defect, same classification, on the other resolution site."""

        def bend(read: dict[str, Any]) -> None:
            read["pagination"]["stop_when"] = {
                "missing": {
                    "function": "base64_encode",
                    "input": {"ref": "response.body.links"},
                }
            }

        target = self._broken(tmp_path, "events", bend)
        report = _report(check_api_read_stop_condition(target))
        assert "must resolve to string or bytes" in report

    def test_a_page_size_default_reading_an_unknown_scope_is_reported(
        self, tmp_path: Path
    ) -> None:
        """A ``limit.default`` naming a scope nothing supplies is refused.

        The never-fillable walk names it at compile -- it used to escape
        ``build_read_strategy`` as a bare ``KeyError`` and take the compile
        check down with it.
        """

        def bend(read: dict[str, Any]) -> None:
            read["pagination"]["limit"]["default"] = {"ref": "nosuchscope.size"}

        target = self._broken(tmp_path, "widgets", bend)
        report = _report(check_api_read_compiles(target))
        assert "'nosuchscope.size'" in report
        assert "request-time resolution never supplies" in report

    def test_a_page_size_default_reading_the_response_is_reported(
        self, tmp_path: Path
    ) -> None:
        """The page size is resolved BEFORE the first page exists.

        ``response`` is in scope for the rest of the pagination block --
        the loop supplies it page by page -- but not for this value:
        ``resolve_page_size`` runs before anything is sent, so the read
        warns, takes the engine's batch size and pages at a size nobody
        authored, reporting success the whole way.
        """

        def bend(read: dict[str, Any]) -> None:
            read["pagination"]["limit"]["default"] = {"ref": "response.body.page_size"}

        target = self._broken(tmp_path, "widgets", bend)
        report = _report(check_api_read_compiles(target))
        assert "'response.body.page_size'" in report
        assert "request-time resolution never supplies" in report

    def test_a_stop_condition_may_still_read_the_response(self, tmp_path: Path) -> None:
        """The other side of that rule: per-page values keep their scope.

        Guarding the whole pagination block would refuse every paginated
        connector there is, so the narrowing has to be exactly the values
        resolved before page one.
        """
        target = load_target(API_REFERENCE_DIR)
        assert check_api_read_compiles(target) == []

    def test_a_param_default_reading_an_unknown_scope_is_reported(
        self, tmp_path: Path
    ) -> None:
        """A default is a declared expression, and fails the same ways.

        The param table is built before any binding map is read, so a defect
        here escaped ahead of every classified site and took the compile
        check -- and the three checks waiting on its probes -- with it.
        """

        def bend(read: dict[str, Any]) -> None:
            read.setdefault("params", {})["tag"] = {
                "in": "query",
                "type": "string",
                "required": False,
                "default": {"ref": "nosuchscope.tag"},
            }

        target = self._broken(tmp_path, "widgets", bend)
        report = _report(check_api_read_compiles(target))
        assert "Unknown resolution scope" in report

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

    def test_a_named_transport_that_cannot_be_opened_names_the_reads(
        self, tmp_path: Path
    ) -> None:
        """A read dispatches through what it names, so that block is judged too.

        And judged for the endpoints that name it: a defect in a named
        transport stops exactly those reads, while the default's stops
        every stream at connect(). A message quoting the larger
        consequence for both would send the author looking for a
        connection that never breaks.
        """
        root = tmp_path / "api"
        shutil.copytree(API_REFERENCE_DIR, root)
        connector = root / "definition" / "connector.json"
        definition = json.loads(connector.read_text())
        definition["transports"]["files"] = {
            "transport_type": "http",
            "base_url": {"literal": ""},
        }
        connector.write_text(json.dumps(definition))
        document = root / "definition" / "endpoints" / "widgets.json"
        parsed = json.loads(document.read_text())
        parsed["operations"]["read"]["request"]["transport_ref"] = "files"
        document.write_text(json.dumps(parsed))

        report = _report(check_read_transport_selection(load_target(root)))
        assert "transport 'files'" in report
        assert "no usable base_url" in report
        assert "widgets" in report, "the finding must name the reads it stops"

    def test_a_read_is_judged_against_its_own_transports_headers(
        self, tmp_path: Path
    ) -> None:
        """An endpoint can only shadow the credential of the session it opens.

        Judged against the default's names, the kit certifies a shadowing
        defect on a read that names another transport, and invents one for
        a header the transport it actually uses never sends.
        """
        root = tmp_path / "api"
        shutil.copytree(API_REFERENCE_DIR, root)
        connector = root / "definition" / "connector.json"
        definition = json.loads(connector.read_text())
        definition["transports"]["files"] = {
            "transport_type": "http",
            "base_url": "https://files.example.invalid",
            "headers": {"X-Files-Key": "k"},
        }
        connector.write_text(json.dumps(definition))
        document = root / "definition" / "endpoints" / "widgets.json"
        parsed = json.loads(document.read_text())
        read = parsed["operations"]["read"]
        read["request"]["transport_ref"] = "files"
        # Shadows the transport this read actually opens.
        read["request"]["headers"] = {"X-Files-Key": {"literal": "attacker"}}
        document.write_text(json.dumps(parsed))

        report = _report(check_api_read_compiles(load_target(root)))
        assert "X-Files-Key" in report, "the named transport's own header is reserved"

    def test_a_probe_is_armed_with_its_own_reads_origins_not_its_siblings(
        self, tmp_path: Path
    ) -> None:
        """A source run resolves one endpoint's transports, so the kit does too.

        Armed target-wide, a link onto a SIBLING endpoint's origin would
        pass here and be refused in production after page one -- the kit
        certifying an engine that does not exist.
        """
        root = tmp_path / "api"
        shutil.copytree(API_REFERENCE_DIR, root)
        connector = root / "definition" / "connector.json"
        definition = json.loads(connector.read_text())
        definition["transports"]["files"] = {
            "transport_type": "http",
            "base_url": "https://files.example.invalid",
        }
        connector.write_text(json.dumps(definition))
        document = root / "definition" / "endpoints" / "widgets.json"
        parsed = json.loads(document.read_text())
        parsed["operations"]["read"]["request"]["transport_ref"] = "files"
        document.write_text(json.dumps(parsed))
        target = load_target(root)

        # The read that names 'files' is armed with it; a sibling that
        # names nothing is not, because its own run would not resolve it.
        assert "https://files.example.invalid" in api_origins(target, "files")
        assert "https://files.example.invalid" not in api_origins(target)

    def test_a_read_naming_the_default_transport_is_not_a_finding(
        self, tmp_path: Path
    ) -> None:
        """Naming the default by name is the same transport, not a second one.

        The shape a real connector ships: every endpoint spells out the
        ``transport_ref`` it dispatches through, and it is the default.
        """
        target = self._broken(
            tmp_path,
            "widgets",
            lambda read: read["request"].update(transport_ref="api"),
        )
        assert check_read_transport_selection(target) == []

    def test_a_read_naming_an_undeclared_transport_is_left_to_the_validator(
        self, tmp_path: Path
    ) -> None:
        """Decidable from the two documents alone, so the kit does not restate it.

        ``endpoint-transport-ref`` in the package validator refuses an
        endpoint naming a transport its sibling connector.json does not
        declare. A second, differently worded verdict here would give the
        author two findings for one defect.
        """
        target = self._broken(
            tmp_path,
            "widgets",
            lambda read: read["request"].update(transport_ref="oauth"),
        )
        assert check_read_transport_selection(target) == []


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

    def test_an_operand_the_schema_declares_without_a_type_is_not_evaluated(
        self, tmp_path: Path
    ) -> None:
        """Reaching the node is not the same as knowing what it holds.

        A property node carrying no ``type`` is contract-valid -- it may
        compose its type through ``allOf``/``anyOf``/``$ref``. The kit has
        no value to script there, and a guessed string is exactly what
        decides whether the ordering comparison raises, so the condition is
        left unevaluated instead of failing a working connector.
        """

        def bend(read: dict[str, Any]) -> None:
            read["response"]["schema"]["properties"]["total"] = {
                "description": "how many widgets there are in all"
            }
            read["pagination"].update(
                stop_when={"gte": [{"ref": "response.body.total"}, 5]}
            )

        target = self._broken(tmp_path, "widgets", bend)
        assert check_api_read_stop_condition(target) == []


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
            check_api_read_advances,
            check_api_read_stop_condition,
        ):
            report = _report(check(target))
            assert "not driven" in report, check.__name__
            assert "api-read-compiles" in report, check.__name__


class TestACompileFindingBelongsToTheCompileCheck:
    """What one check finds must not turn up in the report of another.

    The compiled probes are shared between four checks, and the compile
    check has findings of its own to add to them. Sharing the mutable list
    makes those findings arrive in three other checks as "N endpoint(s)
    were not driven" -- said about endpoints that compiled and were driven
    -- and makes the same check answer differently the second time it is
    called.
    """

    _broken = staticmethod(TestApiReadPathBreaks._broken)

    @staticmethod
    def _stale_cursor_default(read: dict[str, Any]) -> None:
        """A cursor param carrying a default: a compile finding, not a crash."""
        read["params"]["page_token"].pop("controlled_by")
        read["params"]["page_token"]["default"] = {"literal": "start"}

    def test_a_read_that_compiled_is_never_reported_as_undriven(
        self, tmp_path: Path
    ) -> None:
        target = self._broken(tmp_path, "invoices", self._stale_cursor_default)
        assert "'page_token'" in _report(check_api_read_compiles(target))
        for check in (
            check_api_read_advances,
            check_api_read_stop_condition,
        ):
            assert "not driven" not in _messages(check(target)), check.__name__

    def test_a_check_answers_the_same_thing_every_time_it_is_called(
        self, tmp_path: Path
    ) -> None:
        target = self._broken(tmp_path, "invoices", self._stale_cursor_default)
        # Copied as it is answered: a check handing back the cached list
        # itself compares equal to its own later state, which is the one
        # thing this must not read as agreement.
        first = list(check_api_read_compiles(target))
        second = list(check_api_read_compiles(target))
        assert first == second


class TestApiStopConditionDecidesAboutTheRightThing:
    """A stop condition written the wrong way round stops at page one.

    Nothing else in the suite sees it: ``advance`` is driven directly and
    never consults the loop's stopping rule, so an inverted condition
    produces a perfectly good next request that production never issues.
    """

    _broken = staticmethod(TestApiReadPathBreaks._broken)

    @pytest.mark.parametrize(
        ("stem", "stop_when"),
        [
            ("invoices", {"exists": {"ref": "response.body.meta.next_token"}}),
            # The ANCESTOR of the continuation: `meta` holds `next_token`,
            # so it is populated exactly when its leaf is -- an exact-path
            # evidence match would let this inverted condition through.
            ("invoices", {"exists": {"ref": "response.body.meta"}}),
            ("events", {"exists": {"ref": "response.body.links.next"}}),
            ("widgets", {"not_empty": {"ref": "response.body.objects"}}),
            (
                "ledger",
                {
                    "gte": [
                        {"ref": "response.record_count"},
                        {"ref": "runtime.batch_size"},
                    ]
                },
            ),
        ],
    )
    def test_an_inverted_stop_condition_is_caught_on_every_scheme(
        self, tmp_path: Path, stem: str, stop_when: dict[str, Any]
    ) -> None:
        target = self._broken(
            tmp_path, stem, lambda read: read["pagination"].update(stop_when=stop_when)
        )
        report = _report(check_api_read_stop_condition(target))
        assert "holds on a full page" in report

    def test_a_condition_about_the_traversals_position_is_not_judged(
        self, tmp_path: Path
    ) -> None:
        """A page number against a page total is not the kit's call.

        The scripted page has no position to be right about, so guessing one
        would replace the connector's verdict with the kit's arithmetic. The
        page-number fixture declares exactly that condition and is clean.
        """
        assert check_api_read_stop_condition(load_target(API_REFERENCE_DIR)) == []

    def test_a_literal_is_not_a_page_lookup(self, tmp_path: Path) -> None:
        """The resolver hands a literal back untouched, so it reads nothing."""
        target = self._broken(
            tmp_path,
            "widgets",
            lambda read: read["pagination"].update(
                stop_when={"missing": {"literal": {"ref": "response.body.objects"}}}
            ),
        )
        report = _report(check_api_read_stop_condition(target))
        assert "reads nothing under 'response'" in report


class TestApiOriginGuardCoversEveryLinkDeclaration:
    """Handed an off-origin link, a link read refuses it or stays on origin.

    The invariant, not the mechanism: the drive plants the off-origin URL
    at the continuation path and lets ``follow_url`` and the declared-origin
    set -- the engine's own functions -- decide. Every declaration shape gets the
    drive, so none of them can go uncertified.
    """

    _broken = staticmethod(TestApiReadPathBreaks._broken)

    def test_a_bare_reference_link_is_refused(self) -> None:
        """The provider's value is the whole URL, so leaving the origin is fatal."""
        assert check_api_read_advances(load_target(API_REFERENCE_DIR)) == []

    def test_a_template_opening_with_the_value_is_refused(self, tmp_path: Path) -> None:
        """``${...}&limit=50`` hands the provider the origin as surely as a ref."""
        target = self._broken(
            tmp_path,
            "events",
            lambda read: read["pagination"]["link"].update(
                next_url={"template": "${response.body.links.next}&limit=50"}
            ),
        )
        assert check_api_read_advances(target) == []

    def test_a_relative_url_built_around_the_value_is_clean(
        self, tmp_path: Path
    ) -> None:
        """The author owns the path; the provider supplies a query fragment.

        Substituting an off-origin URL into that placeholder yields a
        relative URL that resolves back onto the connector's own origin, so
        demanding a refusal would fail a connector that never could send its
        credentials elsewhere. This is the proof that driving every shape
        does not start failing correct connectors.
        """
        target = self._broken(
            tmp_path,
            "events",
            lambda read: read["pagination"]["link"].update(
                next_url={"template": "/v1/events?after=${response.body.links.next}"}
            ),
        )
        assert check_api_read_advances(target) == []

    def test_a_link_the_connector_derives_is_judged_by_where_it_lands(
        self, tmp_path: Path
    ) -> None:
        """A function's result is a relative segment, so it stays on the origin.

        There is no reading of the declaration that says so: only running it
        and asking the declared-origin set about the answer. Demanding a refusal
        here -- because a function's result is "unconstrained" -- reports a
        violation against a connector whose next request provably cannot
        leave the origin.
        """
        target = self._broken(
            tmp_path,
            "events",
            lambda read: read["pagination"]["link"].update(
                next_url={
                    "function": "base64_encode",
                    "input": {"ref": "response.body.links.next"},
                }
            ),
        )
        assert check_api_read_advances(target) == []


class TestApiRefusalDrivesAreArmed:
    """A drive that certifies a refusal has to be able to report one.

    Every connector shipped here passes: the refusals are the engine's own
    (``follow_url`` for a link, ``_Keyset.advance`` for an ordering value),
    so no authorable document reaches them and answers wrongly. That is
    exactly what makes "the check returned nothing" unreadable -- it says
    the same thing whether the drive ran or was never armed. So each drive
    is pointed at a traversal whose guard has been taken out from under it,
    and required to report.

    The stand-in that replaces a guard still reads what the drive planted
    -- ``urljoin`` follows the link it was handed, and the keyset walk
    substitutes a value only where the record had none. That is what makes
    these tests say something the assertions above cannot: take the
    planting away and each one fails, because the traversal was then handed
    a page it had every reason to accept.
    """

    _broken = staticmethod(TestApiReadPathBreaks._broken)

    @staticmethod
    def _following_link(current: str, target: str, *, origins: frozenset[str]) -> str:
        """``follow_url`` with the declared-origin refusal taken out."""
        return urljoin(current, target)

    @pytest.mark.parametrize(
        ("next_url", "shape"),
        [
            ({"ref": "response.body.links.next"}, "a bare reference"),
            (
                {"template": "${response.body.links.next}&limit=50"},
                "a template opening with the value",
            ),
        ],
    )
    def test_a_link_read_that_leaves_the_origin_is_reported(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        next_url: dict[str, Any],
        shape: str,
    ) -> None:
        """Both declaration shapes are handed the off-origin link, not just one.

        The drive plants it at the continuation path; a drive that did not
        would hand the traversal the connector's own declared value, which
        resolves back onto the origin and reports nothing.
        """
        monkeypatch.setattr(read_setup, "follow_url", self._following_link)
        target = self._broken(
            tmp_path,
            "events",
            lambda read: read["pagination"]["link"].update(next_url=next_url),
        )
        report = _report(check_api_read_advances(target))
        assert "elsewhere.invalid" in report, shape
        assert "must be refused" in report, shape

    def test_a_keyset_read_that_accepts_a_page_with_no_ordering_value(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Taking the ordering value from anywhere but the record must bite.

        That is what a keyset scheme which does not refuse looks like, and
        the drive has to say so: a traversal continuing past a page it
        cannot continue from lands rows the read can never get past.

        The stand-in walks the record first and only substitutes a value
        where the record had none, so the page the drive planted is still
        what decides. That is what makes the arming testable rather than
        assumed: ``found`` carries what the real walk saw, and a page
        carrying the declared records -- one the keyset guard has every
        reason to accept -- never puts a ``None`` in it.
        """
        found: list[Any] = []
        walk = strategies.walk_path

        def planting_walk(record: dict[str, Any], path: list[str]) -> Any:
            value = walk(record, path)
            found.append(value)
            return "planted" if value is None else value

        monkeypatch.setattr(strategies, "walk_path", planting_walk)
        report = _report(check_api_read_advances(load_target(API_REFERENCE_DIR)))
        assert "instead of refusing" in report
        assert "'sequence'" in report
        assert None in found, (
            "the keyset drive never handed the traversal a record without an "
            "ordering value, so the refusal it certifies was never armed"
        )


class TestApiTransportBreaks:
    """Every read opens default_transport, so there has to be one."""

    @staticmethod
    def _bent_definition(tmp_path: Path, mutate: Any) -> ConformanceTarget:
        root = tmp_path / "api"
        shutil.copytree(API_REFERENCE_DIR, root)
        path = root / "definition" / "connector.json"
        definition = json.loads(path.read_text())
        mutate(definition)
        path.write_text(json.dumps(definition))
        return load_target(root)

    def test_a_default_transport_that_is_not_declared(self, tmp_path: Path) -> None:
        target = self._bent_definition(
            tmp_path, lambda d: d.update(default_transport="oauth")
        )
        report = _report(check_read_transport_selection(target))
        assert "'oauth'" in report
        assert "no stream on this connector reaches its first request" in report

    def test_a_default_transport_of_another_type(self, tmp_path: Path) -> None:
        target = self._bent_definition(
            tmp_path,
            lambda d: d["transports"]["api"].update(transport_type="sqlalchemy"),
        )
        report = _report(check_read_transport_selection(target))
        assert "'sqlalchemy'" in report
        assert "no session" in report


class TestApiRequestBodyBreaks:
    """The first request is query *and* body; the read builds both."""

    _broken = staticmethod(TestApiReadPathBreaks._broken)

    def test_a_body_reading_the_response_scope_is_refused(self, tmp_path: Path) -> None:
        """The request is built before any response exists."""

        def bend(read: dict[str, Any]) -> None:
            read["request"]["method"] = "POST"
            read["request"]["body"] = {"ref": "response.body.not_a_request_scope"}

        target = self._broken(tmp_path, "widgets", bend)
        report = _report(check_api_read_compiles(target))
        assert "'response.body.not_a_request_scope'" in report
        assert "request-time resolution never supplies" in report

    @pytest.mark.parametrize("subtree", ["parameters", "selections", "discovered"])
    def test_a_body_reading_the_connection_is_not_judged(
        self, tmp_path: Path, subtree: str
    ) -> None:
        """A definition-only run has no connection, and that is not a defect.

        All three subtrees ``request_resolver`` builds defer -- pinning only
        one would let the other two drop from the scope set with every test
        green while the kit falsely refuses valid connectors.
        """

        def bend(read: dict[str, Any]) -> None:
            read["request"]["method"] = "POST"
            read["request"]["body"] = {"ref": f"connection.{subtree}.filter"}

        target = self._broken(tmp_path, "widgets", bend)
        assert check_api_read_compiles(target) == []

    def test_a_root_body_binding_with_a_default_is_still_driven(
        self, tmp_path: Path
    ) -> None:
        """Deferral is about the VALUE, not the declaration.

        The param table carries every declared default, so this body binds
        here exactly as it does in production -- withholding it on the
        binding's shape alone would leave the body, and every defect in it,
        uncertified.
        """

        def bend(read: dict[str, Any]) -> None:
            read["request"]["method"] = "POST"
            read["request"]["body"] = {"from_param": "filter"}
            read["params"]["filter"] = {
                "in": "body",
                "type": "string",
                "required": False,
                "default": {"literal": "status:open"},
            }

        target = self._broken(tmp_path, "widgets", bend)
        assert check_api_read_compiles(target) == []
        probes, _ = api_read_path._probes(target)
        widgets = [p for p in probes if p.label == "widgets"]
        assert widgets and widgets[0].first_sent.body == "status:open"

    @pytest.mark.parametrize(
        ("label", "body", "refusal"),
        [
            ("a nested field", {"filter": {"literal": {"a": 1}}}, "flat name/value"),
            ("a body that is not an object", {"literal": "raw"}, "must be an object"),
        ],
    )
    def test_a_form_body_the_encoder_refuses_is_reported(
        self, tmp_path: Path, label: str, body: dict[str, Any], refusal: str
    ) -> None:
        """The kit encodes a form body, because the read encodes it too.

        A form carries flat name/value pairs, so these two raise before
        every request the stream would make -- while the request BUILD
        accepts them, which is where the drive used to stop. The encoder
        lives in ``cdk.api.body`` precisely so a transport-free install can
        reach it; leaving it undriven was the gap.
        """

        def bend(read: dict[str, Any]) -> None:
            read["request"]["method"] = "POST"
            read["request"]["content_type"] = "application/x-www-form-urlencoded"
            read["request"]["body"] = body

        target = self._broken(tmp_path, "widgets", bend)
        report = _report(check_api_read_compiles(target))
        assert refusal in report

    def test_a_flat_form_body_is_clean(self, tmp_path: Path) -> None:
        """The other side: a form endpoint the engine can send is certified."""

        def bend(read: dict[str, Any]) -> None:
            read["request"]["method"] = "POST"
            read["request"]["content_type"] = "application/x-www-form-urlencoded"
            read["request"]["body"] = {"grant_type": {"literal": "client_credentials"}}

        assert check_api_read_compiles(self._broken(tmp_path, "widgets", bend)) == []

    def test_a_root_binding_a_stream_filter_supplies_defers_through_a_function(
        self, tmp_path: Path
    ) -> None:
        """A binding is the root's own input at any depth inside it.

        Production takes ``filter`` from the stream's filters and encodes a
        real string. A definition-only run has nothing to give the
        function, so driving it reports ``base64_encode`` refusing ``None``
        -- a finding about the kit, against a connector that works.
        """

        def bend(read: dict[str, Any]) -> None:
            read["request"]["method"] = "POST"
            read["request"]["body"] = {
                "function": "base64_encode",
                "input": {"from_param": "filter"},
            }
            read["params"]["filter"] = {
                "in": "body",
                "type": "string",
                "required": False,
                "operators": ["eq"],
            }

        assert check_api_read_compiles(self._broken(tmp_path, "widgets", bend)) == []

    def test_a_root_body_binding_a_stream_filter_supplies_is_deferred(
        self, tmp_path: Path
    ) -> None:
        """A run fills this param; a definition-only run cannot.

        The whole body is one `{from_param}` binding whose param the
        stream's filters (or the param's own default) supply. Driving it
        here would refuse a connector the engine reads correctly -- the
        same reason a path placeholder bound to a declared param gets a
        stand-in segment instead of a finding.
        """

        def bend(read: dict[str, Any]) -> None:
            read["request"]["method"] = "POST"
            read["request"]["body"] = {"from_param": "filter"}
            read["params"]["filter"] = {
                "in": "body",
                "type": "string",
                "required": False,
            }

        target = self._broken(tmp_path, "widgets", bend)
        assert check_api_read_compiles(target) == []

    def test_a_body_reading_a_secret_is_not_deferred(self, tmp_path: Path) -> None:
        """Request-time resolution never sees secrets; the body is judged.

        Deferring it the way a connection read is deferred would certify a
        read whose body resolves on no run at all -- the request resolver's
        scope set excludes secrets and auth by design.
        """

        def bend(read: dict[str, Any]) -> None:
            read["request"]["method"] = "POST"
            read["request"]["body"] = {"ref": "secrets.api_key"}

        target = self._broken(tmp_path, "widgets", bend)
        report = _report(check_api_read_compiles(target))
        assert "'secrets.api_key'" in report
        assert "request-time resolution never supplies" in report

    def test_a_deferred_body_naming_an_unregistered_function_is_refused(
        self, tmp_path: Path
    ) -> None:
        """A deferred VALUE is still a judged declaration.

        The body reads a request-fillable scope, so the kit withholds it
        from the drive rather than resolving it -- and without judging the
        function name nothing would ever run this node, while production
        checks the registry before the input and refuses on the first
        request.
        """

        def bend(read: dict[str, Any]) -> None:
            read["request"]["method"] = "POST"
            read["request"]["body"] = {
                "function": "does_not_exist",
                "input": {"ref": "connection.parameters.payload"},
            }

        target = self._broken(tmp_path, "widgets", bend)
        report = _report(check_api_read_compiles(target))
        assert "unknown derived function 'does_not_exist'" in report

    def test_a_query_value_reading_the_connector_scope_is_refused(
        self, tmp_path: Path
    ) -> None:
        """`connector.*` is a definition scope, not a request-time one.

        Production's request resolver never carries it, so the key is
        dropped from every request with only a log line -- the provider
        serves the default-versioned collection while everything reports
        green.
        """

        def bend(read: dict[str, Any]) -> None:
            read["request"].setdefault("query", {})["v"] = {
                "ref": "connector.api_version"
            }

        target = self._broken(tmp_path, "widgets", bend)
        report = _report(check_api_read_compiles(target))
        assert "'connector.api_version'" in report
        assert "request-time resolution never supplies" in report

    def test_a_secret_nested_inside_a_body_is_not_silently_dropped(
        self, tmp_path: Path
    ) -> None:
        """The subtler spelling: one field of a structural body reads a secret.

        Request-time resolution omits an unresolved field rather than
        failing, so production sends this body WITHOUT the declared
        credential field, on every run -- a green pipeline and a
        credential-less request. The declaration is the only place the
        defect is visible.
        """

        def bend(read: dict[str, Any]) -> None:
            read["request"]["method"] = "POST"
            read["request"]["body"] = {
                "token": {"ref": "secrets.api_key"},
                "limit": 50,
            }

        target = self._broken(tmp_path, "widgets", bend)
        report = _report(check_api_read_compiles(target))
        assert "'secrets.api_key'" in report
        assert "dropped from every request" in report


class TestApiRunWithNothingToDrive:
    """A green tier 1 must mean the read path ran, not that there was none."""

    def test_a_connector_with_no_endpoints(self, tmp_path: Path) -> None:
        root = tmp_path / "api"
        shutil.copytree(API_REFERENCE_DIR, root)
        shutil.rmtree(root / "definition" / "endpoints")
        report = _report(check_api_has_reads(load_target(root)))
        assert "ships no endpoint documents at all" in report

    def test_a_connector_whose_endpoints_are_write_only(self, tmp_path: Path) -> None:
        root = tmp_path / "api"
        shutil.copytree(API_REFERENCE_DIR, root)
        endpoints = root / "definition" / "endpoints"
        for path in list(endpoints.glob("*.json")):
            if path.stem != "widgets":
                path.unlink()
                continue
            document = json.loads(path.read_text())
            document["operations"].pop("read")
            path.write_text(json.dumps(document))
        report = _report(check_api_has_reads(load_target(root)))
        assert "declare no operations.read" in report

    def test_a_database_connector_is_not_asked_for_reads(self) -> None:
        """The gate is the api tier's own; every other kind is untouched."""
        target = load_target(REFERENCE_DIR, class_path=REFERENCE_CLASS)
        assert check_api_has_reads(target) == []


class TestApiBaseUrlBreaks:
    """The transport build needs a base URL resolving to a non-empty string."""

    _bent_definition = staticmethod(TestApiTransportBreaks._bent_definition)

    @pytest.mark.parametrize("declared", [None, ""])
    def test_a_default_transport_with_no_usable_base_url(
        self, tmp_path: Path, declared: object
    ) -> None:
        def bend(definition: dict[str, Any]) -> None:
            block = definition["transports"]["api"]
            if declared is None:
                block.pop("base_url")
            else:
                block["base_url"] = declared

        target = self._bent_definition(tmp_path, bend)
        report = _report(check_read_transport_selection(target))
        assert "no usable base_url" in report

    def test_a_base_url_literal_that_is_empty_is_not_a_usable_one(
        self, tmp_path: Path
    ) -> None:
        """A mapping is not the same as a value: this one resolves to ''.

        Reading the declaration for truthiness certifies a connector whose
        ``connect()`` cannot open a session, because the transport build
        rejects the empty string.
        """
        target = self._bent_definition(
            tmp_path, lambda d: d["transports"]["api"].update(base_url={"literal": ""})
        )
        report = _report(check_read_transport_selection(target))
        assert "no usable base_url" in report

    def test_a_base_url_expression_that_cannot_resolve_names_why(
        self, tmp_path: Path
    ) -> None:
        """An expression reading no connection scope is resolved, not deferred."""
        target = self._bent_definition(
            tmp_path,
            lambda d: d["transports"]["api"].update(
                base_url={"ref": "runtime.no_such_value"}
            ),
        )
        report = _report(check_read_transport_selection(target))
        assert "no usable base_url" in report
        assert "no_such_value" in report

    def test_a_base_url_the_connection_supplies_is_clean(self, tmp_path: Path) -> None:
        """A definition-only run cannot say what a reference will resolve to."""
        target = self._bent_definition(
            tmp_path,
            lambda d: d["transports"]["api"].update(
                base_url={"ref": "connection.parameters.host"}
            ),
        )
        assert check_read_transport_selection(target) == []

    @pytest.mark.parametrize("declared", ["api.example.test", "ftp://api.example.test"])
    def test_a_base_url_the_http_client_cannot_open_is_refused(
        self, tmp_path: Path, declared: str
    ) -> None:
        """Non-empty is not usable: the session needs an absolute http(s) URL.

        A scheme-less or non-HTTP origin passes a bare truthiness test and
        then dies in the HTTP client on the connector's first request --
        the same one definition of "usable" the transport build now
        enforces at connect().
        """
        target = self._bent_definition(
            tmp_path,
            lambda d: d["transports"]["api"].update(base_url=declared),
        )
        report = _report(check_read_transport_selection(target))
        assert "no usable base_url" in report
        assert "absolute http(s) URL" in report

    @pytest.mark.parametrize(
        "declared",
        ["https://api.example.test/v1?tenant=a", "https://api.example.test/v1#frag"],
    )
    def test_a_base_url_carrying_a_query_or_fragment_is_refused(
        self, tmp_path: Path, declared: str
    ) -> None:
        """The endpoint path is appended to this string, so either swallows it.

        `https://h/v1?t=a` + `/items` addresses `/v1` with a `t=a/items`
        query -- every endpoint on the connector, every request.
        """
        target = self._bent_definition(
            tmp_path,
            lambda d: d["transports"]["api"].update(base_url=declared),
        )
        report = _report(check_read_transport_selection(target))
        assert "no usable base_url" in report
        assert "no query or fragment" in report

    def test_a_settled_path_declared_null_is_refused(self, tmp_path: Path) -> None:
        """A declared null resolves to nothing, which substitutes nowhere."""

        def bend(definition: dict[str, Any]) -> None:
            definition["optional_origin"] = None
            definition["transports"]["api"]["base_url"] = {
                "template": (
                    "https://${connection.parameters.host}"
                    "/${connector.optional_origin}"
                )
            }

        target = self._bent_definition(tmp_path, bend)
        report = _report(check_read_transport_selection(target))
        assert "'connector.optional_origin'" in report
        assert "declares it null" in report

    @pytest.mark.parametrize(
        ("declared", "quoted"),
        [
            # None of these parses, so none can be asked where its userinfo
            # ends. Only the one that could HAVE userinfo is withheld.
            ("https://user@", False),
            ("https://:443", True),
            ("https://api.example.test:abc", True),
            ("https://api.example.test:99999999", True),
            ("https://[abc", True),
        ],
    )
    def test_a_base_url_no_http_client_can_open_is_refused(
        self, tmp_path: Path, declared: str, quoted: bool
    ) -> None:
        """Whether the string is a URL at all is yarl's answer, not ours.

        Userinfo or a port with no host behind it, a port that is not a
        number, an unclosed IPv6 literal: yarl refuses each, and yarl is
        what aiohttp builds every request URL with -- so a value it refuses
        is one the connector could never send. Asserted on the outcome
        rather than on a phrase, because the phrasing is the library's now
        and a hand-written copy of it is what this deleted.
        """
        target = self._bent_definition(
            tmp_path,
            lambda d: d["transports"]["api"].update(base_url=declared),
        )
        report = _report(check_read_transport_selection(target))
        assert "no usable base_url" in report
        assert (repr(declared) in report) is quoted

    def test_a_base_url_carrying_credentials_is_refused(self, tmp_path: Path) -> None:
        """Basic auth comes off EACH request's URL, so page two loses it.

        A provider's absolute next link omits the userinfo, and the origin
        guard cannot notice -- ``origin()`` strips it, so both sides compare
        equal while the credentials are gone. The read 401s on page two
        looking like a provider fault.
        """
        target = self._bent_definition(
            tmp_path,
            lambda d: d["transports"]["api"].update(
                base_url="https://user:pass@api.example.test"
            ),
        )
        report = _report(check_read_transport_selection(target))
        assert "must carry no credentials" in report
        assert "pass" not in report, "the refusal must not log the password"

    def test_a_base_url_resolving_to_credentials_is_refused(
        self, tmp_path: Path
    ) -> None:
        """The half no document rule can reach.

        The literal spelling is refused by the contract
        (analitiq-ai/claude-code-plugins#175). A validator cannot resolve an
        expression, so this spelling -- credentials arriving through a
        template -- is only visible where the resolved value is, which is
        here.
        """
        target = self._bent_definition(
            tmp_path,
            lambda d: (
                d.update(cred="user:pass"),
                d["transports"]["api"].update(
                    base_url={"template": "https://${connector.cred}@api.example.test"}
                ),
            ),
        )
        report = _report(check_read_transport_selection(target))
        assert "must carry no credentials" in report

    def test_a_dead_connector_path_in_a_mixed_node_is_not_carried_past(
        self, tmp_path: Path
    ) -> None:
        """Each read is judged on its own, deferring or not.

        The connection half of this template defers; the typo'd connector
        half resolves on no connection ever -- deferring the whole node on
        the connection's account certified a connector whose connect()
        dies every time.
        """
        target = self._bent_definition(
            tmp_path,
            lambda d: d["transports"]["api"].update(
                base_url={
                    "template": (
                        "https://${connector.conector_id}"
                        ".${connection.parameters.domain}"
                    )
                }
            ),
        )
        report = _report(check_read_transport_selection(target))
        assert "'connector.conector_id'" in report
        assert "names nothing in the connector definition" in report
        assert "'connection.parameters.domain'" not in report

    def test_the_parse_stays_inside_the_guard(self, tmp_path: Path) -> None:
        """A URL the parser REJECTS must not escape as a raw exception.

        ``yarl`` raises rather than deferring on some authorities, and one
        escaping here would abandon the whole check -- including the
        ``transport_ref`` loop after it, whose finding is unrelated. Two
        violations come back, not a traceback.
        """
        target = self._bent_definition(
            tmp_path,
            lambda d: d["transports"]["api"].update(base_url="https://[abc"),
        )
        # No pytest.raises: the point is that it does NOT raise.
        report = _report(check_read_transport_selection(target))
        assert "no usable base_url" in report

    def test_a_settled_path_resolving_to_a_mapping_is_refused_in_a_mixed_node(
        self, tmp_path: Path
    ) -> None:
        """Resolving is not enough: the settled value must be substitutable.

        `connector.transports` resolves -- to the whole block -- and
        substituting a mapping into the template dies at connect() on every
        connection, while the deferred connection half would otherwise carry
        the node past the check.
        """
        target = self._bent_definition(
            tmp_path,
            lambda d: d["transports"]["api"].update(
                base_url={
                    "template": (
                        "https://${connection.parameters.host}"
                        "/${connector.transports}"
                    )
                }
            ),
        )
        report = _report(check_read_transport_selection(target))
        assert "'connector.transports'" in report
        assert "not a value" in report

    def test_an_unknown_connection_field_is_refused_not_deferred(
        self, tmp_path: Path
    ) -> None:
        """The connection scope has a fixed field vocabulary.

        Materialization exposes exactly the fields
        `_build_resolution_context` builds -- `connection.hostname` names
        none of them, so connect() fails on every connection while a bare
        `connection.` prefix match would have deferred it clean.
        """
        target = self._bent_definition(
            tmp_path,
            lambda d: d["transports"]["api"].update(
                base_url={"ref": "connection.hostname"}
            ),
        )
        report = _report(check_read_transport_selection(target))
        assert "'connection.hostname'" in report
        assert "not a scope transport materialization supplies" in report

    def test_a_whole_scope_read_is_refused_not_deferred(self, tmp_path: Path) -> None:
        """`connection.parameters` is a mapping on every connection.

        Deferring it certifies a field that can never resolve to the
        scalar the transport needs -- and production would send the dict's
        repr or die, connection after connection.
        """
        target = self._bent_definition(
            tmp_path,
            lambda d: d["transports"]["api"].update(
                base_url={"ref": "connection.parameters"}
            ),
        )
        report = _report(check_read_transport_selection(target))
        assert "'connection.parameters'" in report
        assert "whole scope" in report

    def test_a_mixed_scope_base_url_is_refused_by_the_stray_path(
        self, tmp_path: Path
    ) -> None:
        """One deferrable path must not carry an undeferrable one past.

        The connection half resolves fine in production; the ``bogus.``
        half fails ``resolve_http_spec()`` at connect() on every
        connection, so the node as a whole defers on no run -- and the
        violation names the path no phase supplies, not the half the
        connection fills.
        """
        target = self._bent_definition(
            tmp_path,
            lambda d: d["transports"]["api"].update(
                base_url={
                    "template": "https://${connection.parameters.host}/${bogus.value}"
                }
            ),
        )
        report = _report(check_read_transport_selection(target))
        assert "'bogus.value'" in report
        assert "'connection.parameters.host'" not in report

    def test_a_statically_resolvable_base_url_arms_the_real_origin(
        self, tmp_path: Path
    ) -> None:
        """A literal expression settles the same origin connect() resolves.

        Arming the link-origin guard with the stand-in instead would refuse
        an absolute same-origin link a real run follows.
        """
        target = self._bent_definition(
            tmp_path,
            lambda d: d["transports"]["api"].update(
                base_url={"literal": "https://static.example.test"}
            ),
        )
        assert check_read_transport_selection(target) == []
        probes, _ = api_read_path._probes(target)
        assert probes, "the fixture's reads must still compile"
        assert all(probe.origin == "https://static.example.test" for probe in probes)

    def test_a_connector_scoped_base_url_resolves_the_real_origin(
        self, tmp_path: Path
    ) -> None:
        """The kit holds the connector definition, so `connector.*` settles.

        Materialization supplies the definition at connect(), so a base URL
        derived from it resolves to the same origin production uses -- and
        the link guard must be armed with that origin, not the stand-in.
        """
        target = self._bent_definition(
            tmp_path,
            lambda d: d["transports"]["api"].update(
                base_url={"template": "https://${connector.connector_id}.example.test"}
            ),
        )
        assert check_read_transport_selection(target) == []
        probes, _ = api_read_path._probes(target)
        assert probes, "the fixture's reads must still compile"
        connector_id = json.loads(
            (tmp_path / "api" / "definition" / "connector.json").read_text()
        )["connector_id"]
        assert probes[0].origin == f"https://{connector_id}.example.test"

    def test_an_exact_supplied_key_is_matched_exactly(self, tmp_path: Path) -> None:
        """`runtime.connection_identifier` is a typo, not a supply.

        Materialization puts exactly `runtime.connection_id` in scope; a
        prefix match would certify a transport connect() fails on every
        connection.
        """
        target = self._bent_definition(
            tmp_path,
            lambda d: d["transports"]["api"].update(
                base_url={"ref": "runtime.connection_identifier"}
            ),
        )
        report = _report(check_read_transport_selection(target))
        assert "'runtime.connection_identifier'" in report
        assert "not a scope transport materialization supplies" in report

    def test_a_connector_path_the_definition_does_not_declare_is_refused(
        self, tmp_path: Path
    ) -> None:
        """A definition-settled read that resolves to nothing is a finding.

        `connector.*` is verified path by path against the held definition
        -- with base_url stood in by the spec drive, nothing else would
        catch it, and a mixed node's connection half must never carry a
        dead connector path past the check.
        """
        target = self._bent_definition(
            tmp_path,
            lambda d: d["transports"]["api"].update(
                base_url={"ref": "connector.no_such_key"}
            ),
        )
        report = _report(check_read_transport_selection(target))
        assert "no usable base_url" in report
        assert "names nothing in the connector definition" in report

    def test_the_exact_supplied_key_defers_clean(self, tmp_path: Path) -> None:
        """The positive half of the exact match: `runtime.connection_id`
        is per-connection, supplied at materialization, and defers."""
        target = self._bent_definition(
            tmp_path,
            lambda d: d["transports"]["api"].update(
                base_url={"template": "https://${runtime.connection_id}.example.test"}
            ),
        )
        assert check_read_transport_selection(target) == []

    def test_the_rest_of_the_transport_spec_is_driven_not_restated(
        self, tmp_path: Path
    ) -> None:
        """resolve_http_spec judges what the value ladders do not carry.

        A malformed `rate_limit` fails connect() on every connection; the
        drive hands the block to the engine's own build, so the finding
        comes from the function rather than a field-by-field restatement.
        """
        target = self._bent_definition(
            tmp_path,
            lambda d: d["transports"]["api"].update(rate_limit={"max_requests": 10}),
        )
        report = _report(check_read_transport_selection(target))
        assert "does not materialize" in report
        assert "rate_limit" in report

    def test_a_coercion_the_build_overflows_on_is_still_a_finding(
        self, tmp_path: Path
    ) -> None:
        """JSON can spell `1e999`; `int(inf)` raises OverflowError.

        An escape here would abandon the whole transport check and every
        finding after it -- the drive's catch has to hold the build's
        unwrapped coercions, arithmetic included.
        """
        target = self._bent_definition(
            tmp_path,
            lambda d: d["transports"]["api"].update(
                rate_limit={"max_requests": 1e999, "time_window_seconds": 60}
            ),
        )
        report = _report(check_read_transport_selection(target))
        assert "does not materialize" in report


class TestApiTransportHeaderBreaks:
    """connect() resolves every transport header; the check judges them all."""

    _bent_definition = staticmethod(TestApiTransportBreaks._bent_definition)

    def test_headers_that_are_not_an_object(self, tmp_path: Path) -> None:
        target = self._bent_definition(
            tmp_path, lambda d: d["transports"]["api"].update(headers=[])
        )
        report = _report(check_read_transport_selection(target))
        assert "declares headers as []" in report

    def test_a_header_value_that_cannot_resolve_names_why(self, tmp_path: Path) -> None:
        target = self._bent_definition(
            tmp_path,
            lambda d: d["transports"]["api"].update(
                headers={
                    # Registered, grammar-clean, and refused when it runs:
                    # the encoder needs text, not a number.
                    "X-Trace": {
                        "function": "base64_encode",
                        "input": {"literal": 5},
                    }
                }
            ),
        )
        report = _report(check_read_transport_selection(target))
        assert "'X-Trace'" in report
        assert "does not resolve" in report

    def test_a_header_naming_an_unregistered_function_is_refused(
        self, tmp_path: Path
    ) -> None:
        """The registry is closed, so an unknown name resolves on no run."""
        target = self._bent_definition(
            tmp_path,
            lambda d: d["transports"]["api"].update(
                headers={"X-Trace": {"function": "no_such_function", "input": "x"}}
            ),
        )
        report = _report(check_read_transport_selection(target))
        assert "'X-Trace'" in report
        assert "unknown derived function 'no_such_function'" in report

    def test_a_header_whose_name_is_not_a_token_is_refused(
        self, tmp_path: Path
    ) -> None:
        """A field NAME reaches the wire too, and the client judges it."""
        target = self._bent_definition(
            tmp_path,
            lambda d: d["transports"]["api"].update(
                headers={"Bad\nName": {"literal": "x"}}
            ),
        )
        report = _report(check_read_transport_selection(target))
        assert "does not materialize" in report
        assert "not an HTTP token" in report

    def test_a_header_value_carrying_a_line_break_is_refused(
        self, tmp_path: Path
    ) -> None:
        """A CR or LF ends the header on the wire.

        The HTTP client refuses it when the first request is written --
        after connect() reported success -- so every read fails with the
        transport already certified.
        """
        target = self._bent_definition(
            tmp_path,
            lambda d: d["transports"]["api"].update(
                headers={"X-Trace": {"literal": "one\r\nInjected: two"}}
            ),
        )
        report = _report(check_read_transport_selection(target))
        assert "does not materialize" in report
        assert "no HTTP client will send" in report

    def test_headers_the_connection_supplies_are_clean(self, tmp_path: Path) -> None:
        """Transport materialization has secrets and auth in scope; defer both."""
        target = self._bent_definition(
            tmp_path,
            lambda d: d["transports"]["api"].update(
                headers={
                    "Authorization": {"template": "Bearer ${auth.access_token}"},
                    "X-Api-Key": {"ref": "secrets.api_key"},
                    "X-Static": {"literal": "v1"},
                }
            ),
        )
        assert check_read_transport_selection(target) == []

    def test_a_timeout_no_float_can_hold_is_reported(self, tmp_path: Path) -> None:
        """A number JSON can spell and ``float()`` cannot narrow.

        ``resolve_http_spec`` coerces this field without wrapping, so the
        overflow leaves as an ``ArithmeticError`` -- classified by the
        engine's own request boundary, which is why this check catches one
        class rather than keeping a list of builtins in step by hand. An
        escaping one would abandon the transport_ref loop that runs after
        it, losing a second, unrelated finding to the first.
        """
        target = self._bent_definition(
            tmp_path,
            lambda d: d["transports"]["api"].update(timeout_seconds=10**400),
        )
        report = _report(check_read_transport_selection(target))
        assert "does not materialize" in report

    def test_a_null_inside_a_deferred_header_template_is_reported(
        self, tmp_path: Path
    ) -> None:
        """Dropping a null header is about the whole VALUE being nothing.

        A template is resolved substitution by substitution and strictly,
        so a null inside one raises at connect() rather than dropping the
        header -- on every connection, whatever the deferred half of the
        template resolves to. Excusing it because the header as a whole
        "may be dropped" certified a connector that cannot connect.
        """
        target = self._bent_definition(
            tmp_path,
            lambda d: (
                d.update(optional=None),
                d["transports"]["api"].update(
                    headers={
                        "Authorization": {
                            "template": "Bearer ${connector.optional}-"
                            "${connection.parameters.token}"
                        }
                    }
                ),
            ),
        )
        report = _report(check_read_transport_selection(target))
        assert "'connector.optional'" in report
        assert "declares it null" in report

    def test_an_optional_header_the_definition_leaves_null_is_clean(
        self, tmp_path: Path
    ) -> None:
        """``resolve_http_spec`` drops a header resolving to nothing.

        So a header pointed at a definition field declared null is a
        connector that connects and sends one header fewer -- not a
        finding. The same value in ``base_url`` IS one, which is why the
        substitutability rule has to know which field it is judging: a
        connect() that works must never fail tier 1.
        """
        target = self._bent_definition(
            tmp_path,
            lambda d: (
                d.update(optional_header=None),
                d["transports"]["api"].update(
                    headers={"X-Optional": {"ref": "connector.optional_header"}}
                ),
            ),
        )
        assert check_read_transport_selection(target) == []

    def test_a_base_url_the_definition_leaves_null_is_still_refused(
        self, tmp_path: Path
    ) -> None:
        """The other side of it: nothing to connect to is nothing to defer."""
        target = self._bent_definition(
            tmp_path,
            lambda d: (
                d.update(origin=None),
                d["transports"]["api"].update(base_url={"ref": "connector.origin"}),
            ),
        )
        report = _report(check_read_transport_selection(target))
        assert "'connector.origin'" in report
        assert "resolves to nothing" in report

    def test_a_mixed_scope_header_is_refused_by_the_stray_path(
        self, tmp_path: Path
    ) -> None:
        """A secrets read beside an unknown scope defers nothing.

        connect() resolves the whole value; the ``bogus.`` half fails it on
        every connection, so the header is a defect named by the path no
        phase supplies.
        """
        target = self._bent_definition(
            tmp_path,
            lambda d: d["transports"]["api"].update(
                headers={
                    "Authorization": {
                        "template": "Bearer ${secrets.api_key}-${bogus.value}"
                    }
                }
            ),
        )
        report = _report(check_read_transport_selection(target))
        assert "'Authorization'" in report
        assert "'bogus.value'" in report

    def test_a_base_url_scope_that_does_not_exist_does_not_stop_the_run(
        self, tmp_path: Path
    ) -> None:
        """A typo in the scope name is refused, and refused by name.

        ``"connectio."`` is not ``"connection."``, so it is a stray path --
        neither deferred nor definition-settled -- and the violation names
        it. A check that raised out instead would report neither this
        defect nor the second transport's beside it -- one authoring
        mistake hiding another, in a different block.
        """
        root = tmp_path / "api"
        shutil.copytree(API_REFERENCE_DIR, root)
        connector = root / "definition" / "connector.json"
        definition = json.loads(connector.read_text())
        definition["transports"]["api"]["base_url"] = {"ref": "connectio.base_url"}
        definition["transports"]["files"] = {
            "transport_type": "http",
            "base_url": {"ref": "nowhere.base_url"},
        }
        connector.write_text(json.dumps(definition))
        document = root / "definition" / "endpoints" / "widgets.json"
        parsed = json.loads(document.read_text())
        parsed["operations"]["read"]["request"]["transport_ref"] = "files"
        document.write_text(json.dumps(parsed))

        report = _report(check_read_transport_selection(load_target(root)))
        assert "connectio" in report
        assert "nowhere" in report, "the transport after it was still judged"

    def test_a_transport_header_the_connection_supplies_is_not_resolved(
        self, tmp_path: Path
    ) -> None:
        """Only the base URL is resolved, so an auth header is no obstacle."""
        target = self._bent_definition(
            tmp_path,
            lambda d: d["transports"]["api"]["headers"].update(
                Authorization={"template": "Bearer ${auth.access_token}"}
            ),
        )
        assert check_read_transport_selection(target) == []


class TestApiPositionlessSchemeBreaks:
    """A scheme holding only what the last page handed back has to read it.

    The defect is one page later than every other advance defect: the
    request after page one differs from the first request, so a drive that
    stops there certifies the read. It is the request after page TWO that
    comes back identical, and it does so for a cursor, a link and a keyset
    alike -- which is why the drive advances twice rather than the kit
    holding a table of which schemes keep a position of their own.
    """

    _broken = staticmethod(TestApiReadPathBreaks._broken)

    def test_a_cursor_continuing_from_a_constant(self, tmp_path: Path) -> None:
        target = self._broken(
            tmp_path,
            "invoices",
            lambda read: read["pagination"]["cursor"].update(
                next_cursor={"literal": "same"}
            ),
        )
        report = _report(check_api_read_advances(target))
        assert "the request after the second page is the request before it" in report

    def test_a_link_continuing_from_a_constant(self, tmp_path: Path) -> None:
        target = self._broken(
            tmp_path,
            "events",
            lambda read: read["pagination"]["link"].update(
                next_url={"literal": "/v1/events?after=fixed"}
            ),
        )
        report = _report(check_api_read_advances(target))
        assert "the request after the second page is the request before it" in report

    def test_a_link_moving_only_in_its_fragment(self, tmp_path: Path) -> None:
        """A fragment is not on the wire, so a link that only moves there does not.

        The scripted pages hand back ``#conformance-9901`` and
        ``#conformance-9902``, two different strings and one request
        target: the client strips the fragment building it. Comparing the
        raw URLs read that as a traversal that moved.
        """
        target = self._broken(
            tmp_path,
            "events",
            lambda read: read["pagination"]["link"].update(
                next_url={
                    "template": "/v1/events#${response.body.next_page_url}",
                }
            ),
        )
        report = _report(check_api_read_advances(target))
        assert "the request after the second page is the request before it" in report

    def test_a_link_continuing_from_the_connection(self, tmp_path: Path) -> None:
        """Nothing a definition-only run resolves, so the traversal ends."""
        target = self._broken(
            tmp_path,
            "events",
            lambda read: read["pagination"]["link"].update(
                next_url={"ref": "connection.parameters.next_page"}
            ),
        )
        report = _report(check_api_read_advances(target))
        assert "stops after the first page and reports success" in report

    def test_a_keyset_ordering_a_moving_field_is_clean(self, tmp_path: Path) -> None:
        """Keyset continues from the record, and the record moves.

        Which is why no table is needed to exempt it: the drive hands the
        second page a later ordering value, exactly as a provider does, and
        the request that follows differs on its own. Whether a *provider*
        moves that field is a fact about data, not about the declaration,
        and nothing a definition-only run can judge either way.
        """
        assert check_api_read_advances(load_target(API_REFERENCE_DIR)) == []

    def test_an_offset_stepping_by_a_constant_is_clean(self, tmp_path: Path) -> None:
        """Offset counts rows for itself, so a fixed step is exactly right."""
        target = self._broken(
            tmp_path,
            "widgets",
            lambda read: read["pagination"]["offset"].update(increment_by=50),
        )
        assert check_api_read_advances(target) == []


class TestApiWholeBodyStopConditionBreaks:
    """A full page's body is present and non-empty, so stopping on it is wrong."""

    _broken = staticmethod(TestApiReadPathBreaks._broken)

    def test_a_stop_condition_on_the_whole_payload(self, tmp_path: Path) -> None:
        target = self._broken(
            tmp_path,
            "widgets",
            lambda read: read["pagination"].update(
                stop_when={"not_empty": {"ref": "response.body"}}
            ),
        )
        report = _report(check_api_read_stop_condition(target))
        assert "holds on a full page" in report


class TestApiRequestBodyIsValidatedAroundConnectionValues:
    """One connection-scoped field must not switch off the whole body check."""

    _broken = staticmethod(TestApiReadPathBreaks._broken)

    @staticmethod
    def _post_body(spec: dict[str, Any]) -> Any:
        def bend(read: dict[str, Any]) -> None:
            read["request"]["method"] = "POST"
            read["request"]["body"] = spec

        return bend

    def test_a_malformed_branch_beside_a_connection_reference(
        self, tmp_path: Path
    ) -> None:
        target = self._broken(
            tmp_path,
            "widgets",
            self._post_body(
                {
                    "scope": {"ref": "connection.parameters.scope"},
                    "page": {"from_param": "offset"},
                    "broken": {"ref": 123},
                }
            ),
        )
        report = _report(check_api_read_compiles(target))
        assert "`ref` must be a string" in report

    def test_a_well_formed_body_reading_the_connection_is_clean(
        self, tmp_path: Path
    ) -> None:
        target = self._broken(
            tmp_path,
            "widgets",
            self._post_body(
                {
                    "scope": {"ref": "connection.parameters.scope"},
                    "page": {"from_param": "offset"},
                }
            ),
        )
        assert check_api_read_compiles(target) == []

    def test_a_body_that_is_one_connection_expression_is_clean(
        self, tmp_path: Path
    ) -> None:
        """That one really does resolve to nothing, for no fault of the connector."""
        target = self._broken(
            tmp_path,
            "widgets",
            self._post_body({"ref": "connection.parameters.filter"}),
        )
        assert check_api_read_compiles(target) == []

    def test_an_unknown_function_beside_a_connection_reference_is_caught(
        self, tmp_path: Path
    ) -> None:
        """Deferring the resolution must not defer the function name with it.

        The value this node reads is the connection's, so a definition-only
        run cannot produce it -- but the registry is engine-owned and
        closed, so the name resolves on no connection, and the engine dies
        on it the first time the stream runs.

        The node's SHAPE is not read here: the contract refuses a malformed
        expression dict in an endpoint request map and, since RULE-CTOR-065,
        in a connector field a runtime resolves. The registry is the one
        thing about an expression it cannot know.
        """
        target = self._broken(
            tmp_path,
            "widgets",
            self._post_body(
                {
                    "function": "no_such_function",
                    "input": {"ref": "connection.parameters.scope"},
                }
            ),
        )
        report = _report(check_api_read_compiles(target))
        assert "unknown derived function 'no_such_function'" in report


class TestApiRequestBlockBreaks:
    """What the request block declares has to be something the path can send."""

    _broken = staticmethod(TestApiReadPathBreaks._broken)

    def test_a_path_placeholder_the_kit_substitutes_matches_the_engine(
        self, tmp_path: Path
    ) -> None:
        """The kit compiles the URL the engine compiles, braces and all."""

        def bend(read: dict[str, Any]) -> None:
            read["request"]["path"] = "/v1/accounts/{account_id}/widgets"
            read["request"]["path_params"] = {"account_id": {"from_param": "account"}}
            read["params"]["account"] = {
                "in": "path",
                "type": "string",
                "required": True,
                "default": {"literal": "acme/eu"},
            }

        target = self._broken(tmp_path, "widgets", bend)
        assert check_api_read_compiles(target) == []
        probes, _ = api_read_path._probes(target)
        widgets = [probe for probe in probes if probe.label == "widgets"]
        assert widgets, "the bent endpoint is what this tests"
        assert "{" not in widgets[0].url
        # Percent-encoded as one segment: a value carrying '/' would
        # otherwise rewrite the URL's structure.
        assert widgets[0].url.endswith("/v1/accounts/acme%2Feu/widgets")

    @pytest.mark.parametrize(
        ("label", "bend"),
        [
            ("no binding at all", lambda request: request.update(path_params={})),
            (
                "a param the endpoint does not declare",
                lambda request: request.update(
                    path_params={"account_id": {"from_param": "acount"}}
                ),
            ),
            (
                "an expression that is not a binding",
                lambda request: request.update(
                    path_params={"account_id": {"literal": ""}}
                ),
            ),
            (
                "a binding that encodes the value a second time",
                lambda request: request.update(
                    path_params={
                        "account_id": {
                            "function": "url_encode",
                            "input": {"from_param": "account_id"},
                        }
                    }
                ),
            ),
        ],
    )
    def test_the_contract_owns_what_a_path_binding_may_say(
        self, label: str, bend: Any
    ) -> None:
        """The kit reads a binding's VALUE; the contract reads its declaration.

        These three shapes used to be kit findings, and each was a second
        answer to a question ``analitiq.contracts.endpoints`` already
        answers -- for every request map, not just this one -- before a
        document reaches the kit. The copies are gone; this pins the reason,
        so the day the contract stops refusing one, this goes red and the
        check comes back rather than the shape passing unnoticed.
        """
        document = json.loads(
            (
                API_REFERENCE_DIR / "definition" / "endpoints" / "widgets.json"
            ).read_text()
        )
        request = document["operations"]["read"]["request"]
        request["path"] = "/v1/accounts/{account_id}/widgets"
        document["operations"]["read"]["params"]["account_id"] = {
            "in": "path",
            "type": "string",
            "required": True,
            "default": {"literal": "acme"},
        }
        bend(request)
        with pytest.raises(ValidationError, match="path_params"):
            ApiEndpointDoc.model_validate(document)

    @pytest.mark.parametrize(
        "header", ["Content-Length", "content-type", "CONTENT-TYPE"]
    )
    def test_the_contract_owns_the_engine_filled_headers(self, header: str) -> None:
        """Four routes to one wire, refused in one place rather than four.

        The engine guarded an endpoint's ``request.headers`` and an
        ``idempotency.name``, and was told about a transport's headers in a
        later review round -- one route at a time, which is the argument for
        the rule living where all four are visible at once. Since contract
        1.0.0rc23 they are RULE-HTTP-002 and RULE-HTTP-003, and the engine's
        copies are gone. The media type is declared by
        ``request.content_type`` now, which :mod:`cdk.api.body` reads.
        """
        with pytest.raises(ValidationError, match="RULE-HTTP-00[23]"):
            HttpTransport.model_validate(
                {
                    "transport_type": "http",
                    "base_url": "https://api.example.test",
                    "headers": {header: "0"},
                }
            )

        document = json.loads(
            (
                API_REFERENCE_DIR / "definition" / "endpoints" / "widgets.json"
            ).read_text()
        )
        document["operations"]["read"]["request"]["headers"] = {header: "0"}
        with pytest.raises(ValidationError, match="RULE-HTTP-00[23]"):
            ApiEndpointDoc.model_validate(document)

    @pytest.mark.parametrize(
        ("label", "declared"),
        [
            ("two markers", {"ref": "secrets.a", "literal": "c"}),
            ("a stray sibling on a ref", {"ref": "secrets.api_key", "extra": 1}),
            ("a stray sibling on a function", {"function": "now", "bogus": 1}),
        ],
    )
    def test_the_contract_owns_an_expression_nodes_shape(
        self, label: str, declared: Any
    ) -> None:
        """Both places the kit used to read a node's grammar are covered.

        An endpoint request map has always been (``_validate_expression_shapes``);
        a connector field a runtime resolves is too since RULE-CTOR-065 in
        contract 1.0.0rc22, which is what let the kit's copy go. The
        registry check is what stays -- see
        :func:`~cdk.conformance.api_surface.unknown_function_problem`.
        """
        transport = {
            "transport_type": "http",
            "base_url": "https://api.example.test",
            "headers": {"X-A": declared},
        }
        with pytest.raises(ValidationError, match="RULE-CTOR-065"):
            HttpTransport.model_validate(transport)

        document = json.loads(
            (
                API_REFERENCE_DIR / "definition" / "endpoints" / "widgets.json"
            ).read_text()
        )
        document["operations"]["read"]["request"]["headers"] = {"X-A": declared}
        with pytest.raises(ValidationError, match="X-A"):
            ApiEndpointDoc.model_validate(document)

    def test_a_path_default_the_definition_settles_as_empty_is_reported(
        self, tmp_path: Path
    ) -> None:
        """An empty value is a value, so the kit judges it rather than deferring.

        Production resolves this same default to ``""`` and
        ``substitute_path`` refuses it before the first request. Standing a
        segment in certified an endpoint whose every unfiltered read fails
        on the URL it builds.
        """

        def bend(read: dict[str, Any]) -> None:
            read["request"]["path"] = "/v1/accounts/{account_id}/widgets"
            read["request"]["path_params"] = {
                "account_id": {"from_param": "account_id"}
            }
            read["params"]["account_id"] = {
                "in": "path",
                "type": "string",
                "required": True,
                "default": {"literal": ""},
            }

        target = self._broken(tmp_path, "widgets", bend)
        report = _report(check_api_read_compiles(target))
        assert "{account_id}" in report
        assert "has no value for the placeholder" in report

    def test_a_path_value_that_deletes_its_own_segment_is_reported(
        self, tmp_path: Path
    ) -> None:
        """``..`` is not encoded away -- it is resolved away.

        ``quote`` leaves a dot unchanged (it is unreserved), and the client
        then removes the dot segment AND the one before it, so
        ``/v1/accounts/../widgets`` is sent as ``/v1/widgets``. The request
        addresses another resource and succeeds there.
        """

        def bend(read: dict[str, Any]) -> None:
            read["request"]["path"] = "/v1/accounts/{account_id}/widgets"
            read["request"]["path_params"] = {
                "account_id": {"from_param": "account_id"}
            }
            read["params"]["account_id"] = {
                "in": "path",
                "type": "string",
                "required": True,
                "default": {"literal": ".."},
            }

        target = self._broken(tmp_path, "widgets", bend)
        report = _report(check_api_read_compiles(target))
        assert "'..'" in report
        assert "address a different resource" in report

    def test_a_path_placeholder_bound_to_a_secret_is_refused(
        self, tmp_path: Path
    ) -> None:
        """Secrets and auth are never request-time scopes, on any run.

        The path is substituted by ``request_resolver()``, whose scope set
        deliberately excludes them -- secret resolution happens once,
        engine-side, at transport materialization. A binding reading them
        is not a value the connection will supply later; it is dropped and
        the placeholder never substitutes.
        """

        def bend(read: dict[str, Any]) -> None:
            read["request"]["path"] = "/v1/accounts/{account_id}/widgets"
            read["request"]["path_params"] = {
                "account_id": {"ref": "secrets.account_id"}
            }

        target = self._broken(tmp_path, "widgets", bend)
        report = _report(check_api_read_compiles(target))
        assert "'secrets.account_id'" in report
        assert "request-time resolution never supplies" in report

    def test_a_connection_field_outside_the_request_subtrees_is_refused(
        self, tmp_path: Path
    ) -> None:
        """`connection.name` exists at materialization, not at request time.

        The request resolver builds exactly parameters/selections/discovered,
        so a binding reading any other connection field resolves on no run
        -- deferring it would certify a placeholder production never fills.
        """

        def bend(read: dict[str, Any]) -> None:
            read["request"]["path"] = "/v1/accounts/{account_id}/widgets"
            read["request"]["path_params"] = {"account_id": {"ref": "connection.name"}}

        target = self._broken(tmp_path, "widgets", bend)
        report = _report(check_api_read_compiles(target))
        assert "'connection.name'" in report
        assert "request-time resolution never supplies" in report

    def test_a_path_placeholder_a_run_supplies_is_not_a_finding(
        self, tmp_path: Path
    ) -> None:
        """The engine binds this one from a stream's filters or the cursor.

        A declared path param carries no value in a definition-only run and
        every value in a real one: the engine builds its table from the
        stream's filters and the replication cursor as well as the declared
        defaults, and substitutes the path only after the incremental filter
        has bound. Reporting it here fails a connector the engine reads
        correctly -- so the kit stands a segment in and drives on.
        """

        def bend(read: dict[str, Any]) -> None:
            read["request"]["path"] = "/v1/accounts/{account_id}/widgets"
            read["request"]["path_params"] = {"account_id": {"from_param": "account"}}
            read["params"]["account"] = {
                "in": "path",
                "type": "string",
                "required": True,
            }

        target = self._broken(tmp_path, "widgets", bend)
        assert check_api_read_compiles(target) == []
        probes, _ = api_read_path._probes(target)
        widgets = [probe for probe in probes if probe.label == "widgets"]
        assert widgets, "the bent endpoint is what this tests"
        assert widgets[0].url.endswith(
            f"/v1/accounts/{api_read_path._STAND_IN_PATH_SEGMENT}/widgets"
        )

    def test_a_path_placeholder_the_connection_supplies_is_not_a_finding(
        self, tmp_path: Path
    ) -> None:
        """A default reading the connection resolves to nothing here, and only here."""

        def bend(read: dict[str, Any]) -> None:
            read["request"]["path"] = "/v1/accounts/{account_id}/widgets"
            read["request"]["path_params"] = {"account_id": {"from_param": "account"}}
            read["params"]["account"] = {
                "in": "path",
                "type": "string",
                "required": True,
                "default": {"ref": "connection.parameters.account"},
            }

        target = self._broken(tmp_path, "widgets", bend)
        assert check_api_read_compiles(target) == []

    def test_a_path_placeholder_the_pagination_loop_owns_is_refused(
        self, tmp_path: Path
    ) -> None:
        """A per-page value can never reach a path substituted once per read."""

        def bend(read: dict[str, Any]) -> None:
            read["request"]["path"] = "/v1/widgets/{offset}"
            read["request"]["path_params"] = {"offset": {"from_param": "offset"}}

        target = self._broken(tmp_path, "widgets", bend)
        report = _report(check_api_read_compiles(target))
        assert "'offset'" in report
        assert "pagination loop owns" in report

    def test_a_read_removing_a_transport_header_is_refused(
        self, tmp_path: Path
    ) -> None:
        """The connection's defaults live on a shared session; nothing deletes one."""
        target = self._broken(
            tmp_path,
            "widgets",
            lambda read: read["request"].update(headers_remove=["Accept"]),
        )
        report = _report(check_api_read_compiles(target))
        assert "headers_remove" in report
        assert "shared HTTP session" in report

    def test_a_declared_header_the_connection_owns_is_refused(
        self, tmp_path: Path
    ) -> None:
        """The request build sees the session's header names, never their values."""
        target = self._broken(
            tmp_path,
            "widgets",
            lambda read: read["request"].update(headers={"Accept": "text/csv"}),
        )
        report = _report(check_api_read_compiles(target))
        assert "'Accept'" in report
        assert "cannot shadow" in report

    def test_a_header_the_transport_may_resolve_away_is_still_refused(
        self, tmp_path: Path
    ) -> None:
        """A definition cannot say which connections send an optional header.

        The engine reserves the names its session ended up carrying, so a
        transport header whose value resolves to nothing is not among them.
        A definition-only run cannot resolve it either way, and permitting
        the endpoint's copy would make the shadowing show up only for the
        connections that fill the header in.
        """
        root = tmp_path / "api"
        shutil.copytree(API_REFERENCE_DIR, root)
        path = root / "definition" / "connector.json"
        definition = json.loads(path.read_text())
        definition["transports"]["api"]["headers"]["X-Tenant"] = {
            "ref": "connection.parameters.tenant"
        }
        path.write_text(json.dumps(definition))
        document = root / "definition" / "endpoints" / "widgets.json"
        parsed = json.loads(document.read_text())
        parsed["operations"]["read"]["request"]["headers"] = {"X-Tenant": "acme"}
        document.write_text(json.dumps(parsed))

        report = _report(check_api_read_compiles(load_target(root)))
        assert "'X-Tenant'" in report
        assert "transport declares" in report

    def test_a_param_bound_under_a_transport_header_key_is_reported(
        self, tmp_path: Path
    ) -> None:
        """The binding KEY is the wire name, so the key is what is judged.

        A param is the endpoint's internal handle; it reaches the provider
        only under the ``request.headers`` key that binds it. Binding an
        innocuous param under a name the transport already sends shadows
        that header just as declaring a literal there would.
        """

        def bend(read: dict[str, Any]) -> None:
            read["params"]["media"] = {
                "in": "header",
                "type": "string",
                "required": False,
                "default": {"literal": "text/csv"},
            }
            read["request"]["headers"] = {"Accept": {"from_param": "media"}}

        target = self._broken(tmp_path, "widgets", bend)
        report = _report(check_api_read_compiles(target))
        assert "'Accept'" in report
        assert "transport declares" in report

    def test_a_param_named_after_a_transport_header_is_not_reported(
        self, tmp_path: Path
    ) -> None:
        """The mirror image: only the key goes out, so only the key is judged.

        A param CALLED ``Accept`` bound under ``X-Accept`` sends
        ``X-Accept`` and nothing else. Refusing it would fail an endpoint
        that shadows nothing, on the strength of a name the provider never
        sees.
        """

        def bend(read: dict[str, Any]) -> None:
            read["params"]["Accept"] = {
                "in": "header",
                "type": "string",
                "required": False,
                "default": {"literal": "text/csv"},
            }
            read["request"]["headers"] = {"X-Accept": {"from_param": "Accept"}}

        target = self._broken(tmp_path, "widgets", bend)
        assert check_api_read_compiles(target) == []

    def test_a_first_request_a_derived_function_refuses_is_reported(
        self, tmp_path: Path
    ) -> None:
        """The build calls the functions, so it raises what they raise.

        ``base64_encode`` answers a non-string input with ``TypeError``,
        which nothing between it and the compile wraps. Letting that escape
        turns an authoring defect into a traceback and abandons every read
        after it.
        """

        def bend(read: dict[str, Any]) -> None:
            read["request"]["method"] = "POST"
            read["request"]["body"] = {
                "function": "base64_encode",
                "input": {"literal": 5},
            }

        target = self._broken(tmp_path, "widgets", bend)
        report = _report(check_api_read_compiles(target))
        assert "must resolve to string or bytes" in report

    def test_an_unknown_scope_in_an_unchosen_map_entry_is_still_refused(
        self, tmp_path: Path
    ) -> None:
        """A scope typo resolves on no run, whichever page selects it.

        This entry is keyed by page two's offset, so a drive that waited
        for a request to reach it would need the traversal to get there
        first; the never-fillable walk refuses the declaration at compile
        instead, naming the path.
        """

        def bend(read: dict[str, Any]) -> None:
            read["request"]["method"] = "POST"
            read["request"]["body"] = {
                "function": "lookup",
                "input": {"from_param": "offset"},
                "map": {"0": "the-first-page", "37": {"ref": "connectio.token"}},
            }

        target = self._broken(tmp_path, "widgets", bend)
        report = _report(check_api_read_compiles(target))
        assert "'connectio.token'" in report
        assert "request-time resolution never supplies" in report

    def test_a_header_the_connection_does_not_send_is_clean(
        self, tmp_path: Path
    ) -> None:
        target = self._broken(
            tmp_path,
            "widgets",
            lambda read: read["request"].update(
                headers={"X-Tenant": {"from_param": "tenant"}},
                query={"page[limit]": {"from_param": "limit"}},
            ),
        )
        assert check_api_read_compiles(target) == []

    def test_a_follow_up_form_body_that_only_nests_on_page_two(
        self, tmp_path: Path
    ) -> None:
        """The first request encodes; so must the ones after it.

        A form body flat on page one can nest once the loop supplies its
        value -- the cursor is absent when the read compiles and arrives
        from the response afterwards. Production encodes the follow-up
        before sending it and refuses, having already yielded page one's
        records, and nothing that encodes only the first request sees it.
        """

        def bend(read: dict[str, Any]) -> None:
            read["request"]["method"] = "POST"
            read["request"]["content_type"] = "application/x-www-form-urlencoded"
            # The continuation the provider hands back is an OBJECT, which
            # the response schema is entitled to declare -- so the scripted
            # page carries one and page two's token is a mapping.
            meta = read["response"]["schema"]["properties"]["meta"]
            meta["properties"]["next_token"] = {
                "type": "object",
                "properties": {"value": {"type": "string"}},
            }
            # The token binds in the BODY, not the query: bound to a query
            # key the scalar rule refuses the dict one map earlier, which
            # is a different rule doing a different job. `page_token` is
            # controlled_by pagination, so it is absent when the read
            # compiles and that object once the loop has advanced.
            read["params"]["page_token"]["in"] = "body"
            read["request"]["query"].pop("pageToken", None)
            read["request"]["body"] = {"cursor": {"from_param": "page_token"}}

        target = self._broken(tmp_path, "invoices", bend)
        assert check_api_read_compiles(target) == []
        report = _report(check_api_read_advances(target))
        assert "flat name/value" in report

    def test_a_follow_up_request_whose_body_cannot_be_built(
        self, tmp_path: Path
    ) -> None:
        """Page one builds; page two, with the loop's own value, does not.

        Nothing that stops at the first request sees this: the body derives
        a field from the continuation, and the continuation does not exist
        until the traversal has already handed page one's records over.
        """

        def bend(read: dict[str, Any]) -> None:
            read["request"]["method"] = "POST"
            read["request"]["body"] = {
                "function": "lookup",
                "input": {"from_param": "offset"},
                "map": {"0": "the-first-page"},
            }

        target = self._broken(tmp_path, "widgets", bend)
        assert check_api_read_compiles(target) == []
        report = _report(check_api_read_advances(target))
        assert "the request after the first page could not be built" in report
