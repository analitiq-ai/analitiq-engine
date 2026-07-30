"""Deliberate breaks fail tier 1 actionably (issue #391 acceptance, part 2).

Each test breaks the reference connector the way a real regression
would — a bent hook signature, an undeclared-capability override, a
private-internal override, a declaration the dialect cannot honor — and
asserts the kit fails with a message naming the offending member.
"""

from __future__ import annotations

import copy
import dataclasses
import shutil
from collections.abc import Sequence
from typing import Any

import pytest

from cdk.conformance import (
    check_api_auth,
    check_api_pagination,
    check_api_request_expressions,
    check_api_response_records,
    check_api_transport,
    check_declaration_consistency,
    check_override_surface,
    check_type_map_grammar,
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

from .kit_runner import API_REFERENCE_DIR, REFERENCE_CLASS, REFERENCE_DIR
from .reference_connector import ReferenceConnector, ReferencePostgresDialect


@pytest.fixture()
def reference_target() -> ConformanceTarget:
    return load_target(REFERENCE_DIR, class_path=REFERENCE_CLASS)


@pytest.fixture()
def api_target() -> ConformanceTarget:
    return load_target(API_REFERENCE_DIR)


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


def _api_target(
    api_target: ConformanceTarget,
    *,
    definition: dict[str, Any] | None = None,
    read: dict[str, Any] | None = None,
) -> ConformanceTarget:
    """The API reference with its definition or its read operation doctored.

    ``read`` is merged into the ``items`` endpoint's read operation, so a
    break names only what it changes and the rest of the document stays
    the one the reference earns its checks with.
    """
    doctored = copy.deepcopy(api_target.definition)
    doctored.update(definition or {})
    endpoints = copy.deepcopy(api_target.endpoints)
    if read:
        endpoints["items"]["operations"]["read"].update(read)
    return dataclasses.replace(api_target, definition=doctored, endpoints=endpoints)


class TestApiTransportBreaks:
    """The connection an API definition promises, against what it reads."""

    def test_a_transport_reading_an_undeclared_secret_fails(
        self, api_target: ConformanceTarget
    ) -> None:
        """No input declares it, so no connection ever carries it."""
        transports = copy.deepcopy(api_target.definition["transports"])
        transports["api"]["headers"]["Authorization"] = {
            "template": "Bearer ${secrets.absent_token}"
        }
        doctored = _api_target(api_target, definition={"transports": transports})
        report = _messages(check_api_transport(doctored))
        assert "secrets.absent_token" in report

    def test_default_transport_naming_nothing_fails(
        self, api_target: ConformanceTarget
    ) -> None:
        """A read that names no transport_ref falls back to it."""
        doctored = _api_target(api_target, definition={"default_transport": "absent"})
        report = _messages(check_api_transport(doctored))
        assert "default_transport" in report and "'absent'" in report

    def test_an_unregistered_transport_type_fails(
        self, api_target: ConformanceTarget
    ) -> None:
        """The CDK materializes only the transport kinds it registers."""
        transports = copy.deepcopy(api_target.definition["transports"])
        transports["api"]["transport_type"] = "graphql"
        doctored = _api_target(api_target, definition={"transports": transports})
        report = _messages(check_api_transport(doctored))
        assert "graphql" in report

    def test_auth_none_while_reading_a_secret_fails(
        self, api_target: ConformanceTarget
    ) -> None:
        """A connection with no auth flow never carries credential material."""
        doctored = _api_target(api_target, definition={"auth": {"type": "none"}})
        report = _messages(check_api_auth(doctored))
        assert "secrets.api_token" in report

    def test_a_declared_auth_type_carrying_no_credential_fails(
        self, api_target: ConformanceTarget
    ) -> None:
        """Declaring api_key while sending nothing authenticates nothing."""
        transports = copy.deepcopy(api_target.definition["transports"])
        del transports["api"]["headers"]["Authorization"]
        doctored = _api_target(api_target, definition={"transports": transports})
        report = _messages(check_api_auth(doctored))
        assert "unauthenticated" in report

    def test_a_non_http_default_transport_fails(
        self, api_target: ConformanceTarget
    ) -> None:
        """Every read opens the default; another http transport does not save it.

        A registered non-http kind clears the registry loop, and the
        declares-an-http-transport check clears too, so nothing but this
        catches a connector that cannot connect at all.
        """
        transports = copy.deepcopy(api_target.definition["transports"])
        transports["db"] = {"transport_type": "adbc", "driver": "postgresql"}
        doctored = _api_target(
            api_target,
            definition={"transports": transports, "default_transport": "db"},
        )
        report = _messages(check_api_transport(doctored))
        assert "default_transport 'db'" in report and "'adbc'" in report

    def test_a_credential_on_an_unselected_transport_fails(
        self, api_target: ConformanceTarget
    ) -> None:
        """Only the transport a read opens authenticates that read.

        A credential resolved into an auth-operation or discovery transport
        is real, and reaches nothing the API path sends.
        """
        transports = copy.deepcopy(api_target.definition["transports"])
        transports["side"] = copy.deepcopy(transports["api"])
        del transports["api"]["headers"]["Authorization"]
        doctored = _api_target(api_target, definition={"transports": transports})
        report = _messages(check_api_auth(doctored))
        assert "unauthenticated" in report

    def test_a_transport_reading_a_response_fails(
        self, api_target: ConformanceTarget
    ) -> None:
        """No response exists at connect, so the scope is not merely opaque."""
        transports = copy.deepcopy(api_target.definition["transports"])
        transports["api"]["headers"]["X-Page"] = {"ref": "response.body.next"}
        doctored = _api_target(api_target, definition={"transports": transports})
        report = _messages(check_api_transport(doctored))
        assert "response.body.next" in report

    def test_a_read_naming_a_non_default_transport_fails(
        self, api_target: ConformanceTarget
    ) -> None:
        """The contract allows the selection the CDK's API path does not make.

        `operations.read.request.transport_ref` is contract-valid and
        unimplemented: the API path opens one connection at connect time.
        A read naming another transport still succeeds — against the wrong
        origin — so silence here would certify a connector whose every read
        reaches the wrong host. Naming the default explicitly is fine.
        """
        transports = copy.deepcopy(api_target.definition["transports"])
        transports["eu"] = {
            "transport_type": "http",
            "base_url": "https://eu.example.invalid",
        }
        request = copy.deepcopy(
            api_target.endpoints["items"]["operations"]["read"]["request"]
        )
        request["transport_ref"] = "eu"
        doctored = _api_target(
            api_target, definition={"transports": transports}, read={"request": request}
        )
        report = _messages(check_api_transport(doctored))
        assert "transport_ref 'eu'" in report and "default_transport" in report

        request["transport_ref"] = api_target.definition["default_transport"]
        naming_the_default = _api_target(
            api_target, definition={"transports": transports}, read={"request": request}
        )
        assert check_api_transport(naming_the_default) == []

    def test_a_non_positive_rate_limit_window_fails(
        self, api_target: ConformanceTarget
    ) -> None:
        """Resolving a spec is not evidence the transport builds from it.

        `resolve_http_spec` accepts the window; `RateLimiter` refuses it.
        A resolve-only probe would call this connection materializable and
        every connect would still fail. (`max_requests` is `ge=1` on the
        published contract, so the validator already gates that half; the
        window is typed `Any` to admit a value expression.)
        """
        transports = copy.deepcopy(api_target.definition["transports"])
        transports["api"]["rate_limit"] = {
            "max_requests": 60,
            "time_window_seconds": 0,
        }
        doctored = _api_target(api_target, definition={"transports": transports})
        report = _messages(check_api_transport(doctored))
        assert "rate_limit.time_window_seconds" in report

    def test_an_object_typed_input_in_a_url_template_fails(
        self, api_target: ConformanceTarget
    ) -> None:
        """Stand-ins carry the declared type, so type-sensitive rules apply.

        Template substitution takes scalars only. A probe standing every
        value in as a string would let an object-typed input through here
        and fail a function input that legitimately takes one.
        """
        contract = copy.deepcopy(api_target.definition["connection_contract"])
        contract["inputs"]["account_id"]["type"] = "object"
        doctored = _api_target(api_target, definition={"connection_contract": contract})
        report = _messages(check_api_transport(doctored))
        assert "only scalars" in report

    def test_an_optional_input_behind_a_strict_transport_field_fails(
        self, api_target: ConformanceTarget
    ) -> None:
        """A user may leave it blank, and base_url resolves strictly.

        A default makes it guaranteed again, so the same definition passes
        once the contract promises a value — which is the actionable fix
        the message names.
        """
        contract = copy.deepcopy(api_target.definition["connection_contract"])
        contract["inputs"]["account_id"]["required"] = False
        doctored = _api_target(api_target, definition={"connection_contract": contract})
        report = _messages(check_api_transport(doctored))
        assert "connection.parameters.account_id" in report
        assert "optional" in report

        contract["inputs"]["account_id"]["default"] = "acme"
        with_default = _api_target(
            api_target, definition={"connection_contract": contract}
        )
        assert check_api_transport(with_default) == []


class TestApiEndpointBreaks:
    """The three passes of a read, each against the scopes it actually has."""

    def test_a_param_default_reading_a_secret_fails(
        self, api_target: ConformanceTarget
    ) -> None:
        """Secrets are not in request scope, so the param is dropped silently."""
        params = copy.deepcopy(api_target.endpoints["items"]["operations"]["read"])
        params["params"]["updated_since"]["default"] = {"ref": "secrets.api_token"}
        doctored = _api_target(api_target, read={"params": params["params"]})
        report = _messages(check_api_request_expressions(doctored))
        assert "secrets.api_token" in report

    def test_a_request_body_reading_a_secret_fails(
        self, api_target: ConformanceTarget
    ) -> None:
        """The body resolves in the same phase the params do."""
        doctored = _api_target(
            api_target,
            read={
                "request": {
                    "method": "POST",
                    "path": "/items",
                    "body": {"token": {"ref": "secrets.api_token"}},
                }
            },
        )
        report = _messages(check_api_request_expressions(doctored))
        assert "secrets.api_token" in report

    def test_stop_when_on_a_scope_no_page_carries_fails(
        self, api_target: ConformanceTarget
    ) -> None:
        """An operand that never resolves makes the predicate never hold."""
        pagination = copy.deepcopy(
            api_target.endpoints["items"]["operations"]["read"]["pagination"]
        )
        pagination["stop_when"] = {"empty": {"ref": "response.headers.next"}}
        doctored = _api_target(api_target, read={"pagination": pagination})
        report = _messages(check_api_pagination(doctored))
        assert "response.headers.next" in report

    def test_a_non_positive_page_size_literal_fails(
        self, api_target: ConformanceTarget
    ) -> None:
        """The loop parses the authored literal before the first request."""
        pagination = copy.deepcopy(
            api_target.endpoints["items"]["operations"]["read"]["pagination"]
        )
        pagination["limit"]["default"] = 0
        doctored = _api_target(api_target, read={"pagination": pagination})
        report = _messages(check_api_pagination(doctored))
        assert "limit.default" in report and "positive" in report

    def test_a_pre_page_value_reading_a_response_fails(
        self, api_target: ConformanceTarget
    ) -> None:
        """The effective page size is resolved before any page comes back.

        Probing the whole block against a page would call this satisfied,
        which is why the two phases are resolved separately.
        """
        pagination = copy.deepcopy(
            api_target.endpoints["items"]["operations"]["read"]["pagination"]
        )
        pagination["limit"]["default"] = {"ref": "response.record_count"}
        doctored = _api_target(api_target, read={"pagination": pagination})
        report = _messages(check_api_pagination(doctored))
        assert "response.record_count" in report
        assert "before the first request" in report

    def test_a_non_positive_page_size_expression_literal_fails(
        self, api_target: ConformanceTarget
    ) -> None:
        """A literal node is as knowable as a bare scalar, and as rejected."""
        pagination = copy.deepcopy(
            api_target.endpoints["items"]["operations"]["read"]["pagination"]
        )
        pagination["limit"]["default"] = {"literal": 0}
        doctored = _api_target(api_target, read={"pagination": pagination})
        report = _messages(check_api_pagination(doctored))
        assert "limit.default" in report and "positive" in report

    def test_a_param_default_reading_a_response_fails(
        self, api_target: ConformanceTarget
    ) -> None:
        """A request is built before any response exists."""
        params = copy.deepcopy(
            api_target.endpoints["items"]["operations"]["read"]["params"]
        )
        params["updated_since"]["default"] = {"ref": "response.body.since"}
        doctored = _api_target(api_target, read={"params": params})
        report = _messages(check_api_request_expressions(doctored))
        assert "response.body.since" in report

    def test_a_continuation_reading_an_optional_input_fails(
        self, api_target: ConformanceTarget
    ) -> None:
        """A continuation cannot survive a value the user may leave blank.

        The engine rejects a step it cannot parse and ends the loop the
        moment a cursor resolves to nothing, so a continuation is certified
        against the guaranteed connection. Making the same input required
        clears it — the survivable parts of the block still probe the
        widest connection.
        """
        contract = copy.deepcopy(api_target.definition["connection_contract"])
        contract["inputs"]["page_step"] = {
            "source": "user",
            "phase": "pre_auth",
            "storage": "connection.parameters",
            "type": "integer",
            "required": False,
        }
        pagination = copy.deepcopy(
            api_target.endpoints["items"]["operations"]["read"]["pagination"]
        )
        pagination["offset"]["increment_by"] = {
            "ref": "connection.parameters.page_step"
        }
        doctored = _api_target(
            api_target,
            definition={"connection_contract": contract},
            read={"pagination": pagination},
        )
        report = _messages(check_api_pagination(doctored))
        assert "connection.parameters.page_step" in report

        contract["inputs"]["page_step"]["required"] = True
        required = _api_target(
            api_target,
            definition={"connection_contract": contract},
            read={"pagination": pagination},
        )
        assert check_api_pagination(required) == []

    def test_an_unparseable_explicit_arrow_type_fails(
        self, api_target: ConformanceTarget
    ) -> None:
        """A hand-annotated field skips the type map and still has to parse.

        The engine builds the record schema from the annotated result, so
        `SchemaContract` rejects the field before any request is sent.
        """
        response = copy.deepcopy(
            api_target.endpoints["items"]["operations"]["read"]["response"]
        )
        record = response["schema"]["properties"]["records"]["items"]["properties"]
        record["id"]["arrow_type"] = "Blurb"
        doctored = _api_target(api_target, read={"response": response})
        report = _messages(check_api_response_records(doctored))
        assert "Blurb" in report

    def test_a_record_field_the_read_type_map_cannot_map_fails(
        self, api_target: ConformanceTarget
    ) -> None:
        """The engine resolves every field's Arrow type before it requests."""
        response = copy.deepcopy(
            api_target.endpoints["items"]["operations"]["read"]["response"]
        )
        record = response["schema"]["properties"]["records"]["items"]["properties"]
        record["ratio"] = {"type": "quaternion"}
        doctored = _api_target(api_target, read={"response": response})
        report = _messages(check_api_response_records(doctored))
        assert "ratio" in report and "quaternion" in report

    def test_the_type_map_check_leaves_the_document_unmutated(
        self, api_target: ConformanceTarget
    ) -> None:
        """Arrow-type resolution annotates in place; the check must not.

        A mutated target would hand whatever runs next a document the
        connector never shipped.
        """
        before = copy.deepcopy(api_target.endpoints)
        assert check_api_response_records(api_target) == []
        assert api_target.endpoints == before

    def test_a_records_ref_the_schema_does_not_declare_fails(
        self, api_target: ConformanceTarget
    ) -> None:
        """The read builds its Arrow schema by walking this ref."""
        response = copy.deepcopy(
            api_target.endpoints["items"]["operations"]["read"]["response"]
        )
        response["records"] = {"ref": "response.body.data"}
        doctored = _api_target(api_target, read={"response": response})
        report = _messages(check_api_response_records(doctored))
        assert "response.body.data" in report and "records" in report
