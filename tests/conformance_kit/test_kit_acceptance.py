"""The kit accepts a conformant connector (issue #391 acceptance, part 1).

The reference connector — the in-tree ``GenericSQLConnector`` facade
with a postgres-shaped dialect on the sanctioned v2 surface — passes
every tier-1 check, both through the importable check functions and
through the shipped pytest suite exactly as a connector repo invokes it
(plugin options, and the plugin-less environment-variable path). The
suite must also actually *run*: a broken connector turns the same
invocation red, and pass-count floors keep an all-skip regression from
reading as green.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import pytest

from cdk.conformance import (
    check_declaration_consistency,
    check_override_surface,
    check_type_map_round_trip,
    load_target,
)
from cdk.conformance.roundtrip import probe_canonicals, render_probe
from cdk.conformance.target import ConformanceTarget
from cdk.type_map.exceptions import UnmappedTypeError

from .kit_runner import REFERENCE_CLASS, REFERENCE_DIR, REPO_ROOT, run_kit_suite

#: The tier-1 suite currently ships 22 tests for a full write-capable
#: target; a floor well above zero guards against the suite silently
#: collecting or skipping everything.
TIER1_MIN_PASSED = 10


def _assert_suite_passed(
    completed: subprocess.CompletedProcess[str], *, minimum: int = TIER1_MIN_PASSED
) -> None:
    """Green, above the floor, and with nothing skipped.

    Every check applies to the full write-capable reference, so a skip
    here means a gating regression quietly switched a check off — the
    exact failure shape the kit exists to prevent in connector repos.
    """
    output = completed.stdout + completed.stderr
    assert completed.returncode == 0, f"tier 1 failed against the reference:\n{output}"
    passed = re.search(r"(\d+) passed", output)
    assert (
        passed and int(passed.group(1)) >= minimum
    ), f"expected the tier-1 suite to actually run, got:\n{output}"
    assert not re.search(r"\d+ skipped", output), (
        f"no tier-1 check may skip against the full reference connector; a "
        f"skip is a gating regression:\n{output}"
    )


@pytest.fixture(scope="module")
def reference_target() -> ConformanceTarget:
    return load_target(REFERENCE_DIR, class_path=REFERENCE_CLASS)


class TestReferencePassesTier1:
    def test_override_surface_is_clean(
        self, reference_target: ConformanceTarget
    ) -> None:
        assert check_override_surface(reference_target) == []

    def test_declaration_is_consistent(
        self, reference_target: ConformanceTarget
    ) -> None:
        assert check_declaration_consistency(reference_target) == []

    def test_type_maps_round_trip(self, reference_target: ConformanceTarget) -> None:
        mapper = reference_target.type_mapper
        assert mapper is not None
        assert check_type_map_round_trip(mapper) == []

    def test_round_trip_probes_actually_render(
        self, reference_target: ConformanceTarget
    ) -> None:
        """The round-trip check must not degrade to all-probes-skipped.

        Uncovered probes are skipped by design, so a regression in the
        probe generator (or a normalization change unmatching every
        rule) would otherwise read as a clean pass — for every
        connector, with the kit's own CI green.
        """
        mapper = reference_target.type_mapper
        assert mapper is not None
        rendered = 0
        for canonical in probe_canonicals(mapper):
            try:
                render_probe(mapper, canonical, reference_target.dialect)
            except UnmappedTypeError:
                continue
            rendered += 1
        assert rendered >= 8, (
            f"only {rendered} probes reached the reference write map; the "
            f"round-trip check has gone inert"
        )


class TestThinConnectorPassesVacuously:
    """A pure-declarative source-only connector runs on the generic class."""

    def test_thin_target_raises_no_violations(self, tmp_path: Path) -> None:
        definition_dir = tmp_path / "definition"
        definition_dir.mkdir()
        (definition_dir / "connector.json").write_text(
            '{"kind": "database", "connector_id": "conformance-thin"}'
        )
        (definition_dir / "type-map-read.json").write_text(
            '[{"match": "exact", "native": "TEXT", "canonical": "Utf8"}]'
        )
        target = load_target(tmp_path)
        assert target.connector_class is not None, "thin path falls back"
        assert check_override_surface(target) == []
        assert check_declaration_consistency(target) == []


class TestPluginImportStaysLight:
    """The pytest11 entry point must not require the optional extras."""

    def test_plugin_import_pulls_no_heavy_dependencies(self) -> None:
        """Pytest imports the plugin at startup in every env with the core
        CDK installed; the import must not reach pyarrow or the SQL
        surface, or a core-only consumer's pytest runs crash before
        collection."""
        probe = (
            "import sys\n"
            "import cdk.conformance.plugin\n"
            "heavy = [m for m in ('pyarrow', 'cdk.sql.generic', "
            "'cdk.sql.dialects') if m in sys.modules]\n"
            "assert not heavy, f'plugin import loaded {heavy}'\n"
        )
        completed = subprocess.run(
            [sys.executable, "-c", probe],
            cwd=REPO_ROOT,
            env={"PYTHONPATH": str(REPO_ROOT / "cdk")},
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        assert completed.returncode == 0, (
            f"importing the pytest plugin dragged in optional dependencies:"
            f"\n{completed.stdout}{completed.stderr}"
        )


class TestSuiteInvocation:
    """The shipped pytest wiring, both consumer configuration paths."""

    def test_tier1_passes_with_plugin_options(self) -> None:
        completed = run_kit_suite(
            "cdk.conformance.tier1",
            options=[
                "--connector-dir",
                str(REFERENCE_DIR),
                "--connector-class",
                REFERENCE_CLASS,
            ],
        )
        _assert_suite_passed(completed)

    def test_tier1_passes_via_environment_only(self) -> None:
        """The plugin-less path: configuration purely through env vars."""
        completed = run_kit_suite(
            "cdk.conformance.tier1",
            load_plugin=False,
            env_extra={
                "ANALITIQ_CONNECTOR_DIR": str(REFERENCE_DIR),
                "ANALITIQ_CONNECTOR_CLASS": REFERENCE_CLASS,
            },
        )
        _assert_suite_passed(completed)

    def test_tier1_fails_red_for_a_broken_connector(self) -> None:
        """The failing direction end-to-end: a broken connector = red CI."""
        completed = run_kit_suite(
            "cdk.conformance.tier1",
            options=[
                "--connector-dir",
                str(REFERENCE_DIR),
                "--connector-class",
                "tests.conformance_kit.broken_connector:PrivateOverrideConnector",
            ],
        )
        output = completed.stdout + completed.stderr
        assert completed.returncode != 0, (
            f"a connector overriding a private facade internal must fail "
            f"tier 1, got:\n{output}"
        )
        assert (
            "_prepare_write_batch" in output
        ), f"the failure must name the offending member:\n{output}"
