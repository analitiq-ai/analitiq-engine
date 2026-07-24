"""The kit accepts a conformant connector (issue #391 acceptance, part 1).

The reference connector — the in-tree ``GenericSQLConnector`` facade
with a postgres-shaped dialect on the sanctioned v2 surface — passes
every tier-1 check, both through the importable check functions and
through the shipped pytest suite exactly as a connector repo invokes it.
"""

from __future__ import annotations

import os
import re
import subprocess  # nosec B404 - runs the suite the way a connector repo does
import sys
from pathlib import Path

import pytest

from cdk.conformance import (
    check_declaration_consistency,
    check_override_surface,
    check_type_map_round_trip,
    load_target,
)
from cdk.conformance.target import ConformanceTarget

REPO_ROOT = Path(__file__).resolve().parents[2]
REFERENCE_DIR = Path(__file__).parent / "fixtures" / "reference"
REFERENCE_CLASS = "tests.conformance_kit.reference_connector:ReferenceConnector"


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


def test_tier1_suite_passes_through_pytest(
    reference_target: ConformanceTarget,
) -> None:
    """The shipped pytest wiring works end-to-end as a connector repo runs it."""
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        [str(REPO_ROOT / "cdk"), str(REPO_ROOT), env.get("PYTHONPATH", "")]
    ).rstrip(os.pathsep)
    completed = subprocess.run(  # nosec B603 - fixed argv, no shell
        [
            sys.executable,
            "-m",
            "pytest",
            "--pyargs",
            "cdk.conformance.tier1",
            "-q",
            "--no-header",
            "-p",
            "no:cacheprovider",
            "-p",
            "cdk.conformance.plugin",
            "--connector-dir",
            str(REFERENCE_DIR),
            "--connector-class",
            REFERENCE_CLASS,
        ],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    output = completed.stdout + completed.stderr
    assert completed.returncode == 0, f"tier 1 failed against the reference:\n{output}"
    passed = re.search(r"(\d+) passed", output)
    assert (
        passed and int(passed.group(1)) >= 10
    ), f"expected the tier-1 suite to actually run, got:\n{output}"
