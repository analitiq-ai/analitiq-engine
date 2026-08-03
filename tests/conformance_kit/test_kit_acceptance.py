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

import os
import re
import subprocess
import sys
import types
from pathlib import Path

import pytest

from cdk.conformance import (
    check_declaration_consistency,
    check_kind_applicability,
    check_override_surface,
    check_type_map_round_trip,
    load_target,
)
from cdk.conformance.roundtrip import probe_canonicals, render_probe
from cdk.conformance.target import ConformanceTarget
from cdk.conformance.tier1 import test_definition as kit_definition
from cdk.type_map.exceptions import UnmappedTypeError
from src.config.schema_validator import validate_file
from src.config.utils import load_json_file

from .kit_runner import (
    API_REFERENCE_CLASS,
    API_REFERENCE_DIR,
    FIXTURES_DIR,
    REFERENCE_CLASS,
    REFERENCE_DIR,
    REPO_ROOT,
    run_kit_suite,
)

#: Every endpoint document the kit's own fixtures ship, whatever connector
#: kind they belong to.
FIXTURE_ENDPOINTS = sorted(FIXTURES_DIR.glob("*/definition/endpoints/*.json"))

#: The tier-1 suite ships 25 tests for a full write-capable target; a
#: floor well above zero guards against the suite silently collecting or
#: skipping everything.
TIER1_MIN_PASSED = 10


def _skipped_lines(output: str) -> list[str]:
    """The suite's own ``SKIPPED [n] <file>:<line>: <reason>`` lines."""
    return [line for line in output.splitlines() if line.startswith("SKIPPED ")]


def _assert_suite_passed(
    completed: subprocess.CompletedProcess[str], *, minimum: int = TIER1_MIN_PASSED
) -> None:
    """Green, above the floor, and skipping nothing but the other kind.

    Every check written for a database applies to the full write-capable
    reference, so a skip that is not the kind gate stepping aside for the
    api modules means a gating regression quietly switched a check off —
    the exact failure shape the kit exists to prevent in connector repos.
    """
    output = completed.stdout + completed.stderr
    assert completed.returncode == 0, f"tier 1 failed against the reference:\n{output}"
    passed = re.search(r"(\d+) passed", output)
    assert (
        passed and int(passed.group(1)) >= minimum
    ), f"expected the tier-1 suite to actually run, got:\n{output}"
    unexpected = [
        line
        for line in _skipped_lines(output)
        if "this check applies to kind 'api'" not in line
    ]
    assert not unexpected, (
        "no tier-1 database check may skip against the full reference "
        "connector; a skip is a gating regression:\n" + "\n".join(unexpected)
    )


#: Every tier-1 check that applies to kind 'api': five read-path drives,
#: two surface checks, and the kind-agnostic scaffolding. Pinned to the
#: exact count, not a floor: a loose floor lets a whole check module be
#: deleted without a skip line to notice, which is the same "not assessed
#: reads as passed" failure one level down.
API_TIER1_EXPECTED_PASSED = 13

#: The check modules a run against an api connector must actually execute.
API_CHECK_MODULES = ("test_api_read_path.py", "test_api_surface.py")


def _assert_api_suite_passed(completed: subprocess.CompletedProcess[str]) -> None:
    """Green, above the floor, with the api drives among what ran.

    Skips are expected here, unlike the database reference: every SQL
    check gates itself off for kind ``api``. What must not happen is an
    api check joining them — a drive that skipped certified nothing, and
    the run would still read green.
    """
    output = completed.stdout + completed.stderr
    assert (
        completed.returncode == 0
    ), f"tier 1 failed against the api fixture:\n{output}"
    passed = re.search(r"(\d+) passed", output)
    assert passed and int(passed.group(1)) == API_TIER1_EXPECTED_PASSED, (
        f"expected exactly {API_TIER1_EXPECTED_PASSED} tier-1 checks to run "
        f"for kind 'api'; a different count means a check was added or lost "
        f"without this number moving with it:\n{output}"
    )
    skipped_api = [
        line
        for line in _skipped_lines(output)
        if any(module in line for module in API_CHECK_MODULES)
    ]
    assert not skipped_api, (
        "an api check skipped; it must run for the fixture it was written "
        "for:\n" + "\n".join(skipped_api)
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


class _FakeItem:
    """A collected check, as the applicability ledger reads one."""

    def __init__(self, kinds: object) -> None:
        self.module = types.SimpleNamespace(APPLIES_TO_KINDS=kinds)


class TestUnassessableKindIsNotAPass:
    """A kind the suite carries no checks for must fail, never pass.

    Tier 1 now assesses two kinds — a database renders SQL, an api drives
    its read path — so the gate is demonstrated end to end with a kind it
    carries checks for neither of: that connector collects nothing but
    skips, and pytest exits 0 on an all-skipped run. A required status
    check that goes green for an artifact it structurally cannot evaluate
    reports "not assessed" as "passed"; the verdict has to come from the
    kit, not from a kind branch in every connector repo's CI.
    """

    def test_an_unassessed_kind_fails_tier1_naming_the_reason(
        self, tmp_path: Path
    ) -> None:
        """The end-to-end shape: a well-formed connector turns CI red."""
        definition_dir = tmp_path / "definition"
        definition_dir.mkdir()
        (definition_dir / "connector.json").write_text(
            '{"kind": "conformance-unassessed", "connector_id": "unassessed"}'
        )
        (definition_dir / "type-map-read.json").write_text(
            '[{"match": "exact", "native": "TEXT", "canonical": "Utf8"}]'
        )
        completed = run_kit_suite(
            "cdk.conformance.tier1",
            options=["--connector-dir", str(tmp_path)],
        )
        output = completed.stdout + completed.stderr
        assert completed.returncode != 0, (
            f"a connector the suite cannot assess must not exit green:\n" f"{output}"
        )
        assert "kind-applicability" in output
        assert "ungated" in output, f"the failure must name the reason:\n{output}"
        assert re.search(r"\b1 failed", output), (
            f"the applicability verdict must be the only failure; anything "
            f"else means the fixture is itself broken:\n{output}"
        )

    def test_the_verdict_names_what_the_run_does_assess(self) -> None:
        target = load_target(API_REFERENCE_DIR)
        report = "\n".join(
            str(v)
            for v in check_kind_applicability(
                target, [_FakeItem(("database",)), _FakeItem(None)]
            )
        )
        assert "'api'" in report
        assert "'database'" in report

    def test_a_check_for_the_target_kind_clears_the_verdict(self) -> None:
        """The ledger is derived, so new checks for a kind gate it.

        Nothing here names the covered kinds: a module of API checks
        stating its own scope satisfies this the day it lands, with no
        list to keep in step.
        """
        target = load_target(API_REFERENCE_DIR)
        assert check_kind_applicability(target, [_FakeItem(("api",))]) == []
        assert check_kind_applicability(target, [_FakeItem("api")]) == []

    def test_a_run_of_only_kind_agnostic_checks_fails(self) -> None:
        """Scaffolding checks certify no kind, so they satisfy nothing."""
        target = load_target(REFERENCE_DIR, class_path=REFERENCE_CLASS)
        violations = check_kind_applicability(target, [_FakeItem(None)])
        assert violations
        assert "no check collected here states a kind" in str(violations[0])

    def test_the_read_type_map_check_covers_every_kind(self, tmp_path: Path) -> None:
        """The one tier-1 check scoped by wording rather than dependency.

        Its body reads ``type_mapper``, which loads for every kind; an
        API connector without a read map cannot canonicalize the JSON
        types its endpoints declare, so it must fail here too.
        """
        definition_dir = tmp_path / "definition"
        definition_dir.mkdir()
        (definition_dir / "connector.json").write_text(
            '{"kind": "api", "connector_id": "conformance-api-no-map"}'
        )
        target = load_target(tmp_path)
        assert not target.is_database
        with pytest.raises(AssertionError, match="type-map-read.json"):
            kit_definition.test_connector_ships_a_read_type_map(target)


class TestFixtureConnectorsAreContractValid:
    """The kit's own fixtures pass the published contract (issue #433).

    Everything the kit asserts is relative to these documents, so a fixture
    the contract rejects makes a green run a statement about nothing. Five
    endpoint documents declared read params that no request binding named:
    the contract refuses that outright, and the engine sends such a param
    zero times -- so the drives were certifying a connector that asks the
    provider for page one on every request.

    The rule the checks are written against is the contract's, not the
    fixtures', so the fixtures are the side that has to be right. Validation
    goes through the engine's own ``validate_file``, so this asks the same
    models the engine loads a connector's documents with.
    """

    def test_the_fixtures_ship_endpoint_documents_to_validate(self) -> None:
        """A moved or renamed fixture tree must not read as nothing to check."""
        assert FIXTURE_ENDPOINTS, (
            f"no endpoint documents found under {FIXTURES_DIR}; the check "
            f"below would pass vacuously"
        )

    @pytest.mark.parametrize(
        "document", FIXTURE_ENDPOINTS, ids=lambda path: f"{path.parts[-4]}/{path.stem}"
    )
    def test_every_fixture_endpoint_document_validates(self, document: Path) -> None:
        kind = load_json_file(document.parents[1] / "connector.json")["kind"]
        validate_file(f"{kind}-endpoint", document)


class TestApiReferencePassesTier1:
    """The api fixture is assessed, not skipped past (issue #433).

    Its endpoint documents declare all five of the contract's paging
    schemes, so every drive has something to run on every scheme -- which
    is what makes "a correct connector is clean" an assertion rather than
    an assumption, for the schemes whose break cases live next door. A
    green run here means those checks executed; the applicability gate
    above is what keeps an all-skip regression from producing the same
    green.
    """

    def test_api_fixture_passes_tier1_on_the_generic_path(self) -> None:
        """No connector class installed: the thin path is what production loads."""
        completed = run_kit_suite(
            "cdk.conformance.tier1",
            options=["--connector-dir", str(API_REFERENCE_DIR)],
        )
        _assert_api_suite_passed(completed)

    def test_api_fixture_passes_tier1_with_a_connector_class(self) -> None:
        """A package's own class and dialect answer the same verdict.

        The override seam: the api checks read the endpoint documents and
        the CDK's read path, so resolving a connector class must change
        nothing about what they certify. A difference here would mean a
        check had started depending on the class it is not about.
        """
        completed = run_kit_suite(
            "cdk.conformance.tier1",
            options=[
                "--connector-dir",
                str(API_REFERENCE_DIR),
                "--connector-class",
                API_REFERENCE_CLASS,
            ],
        )
        _assert_api_suite_passed(completed)

    def test_tier2_reports_api_inapplicable_rather_than_failed(self) -> None:
        """No live tier for api, and that is not the same as unassessed.

        A public CI carries no provider credentials and a stub server would
        certify the connector's own fixtures, so the live tier can never
        mean anything for an api connector. Landing that in the
        "unassessable" bucket would paint a permanent red on a kind tier 1
        covers in full.
        """
        completed = run_kit_suite(
            "cdk.conformance.tier2",
            options=["--connector-dir", str(API_REFERENCE_DIR)],
        )
        output = completed.stdout + completed.stderr
        assert completed.returncode == 0, (
            f"kind 'api' is inapplicable to the live tier, not unassessed:\n"
            f"{output}"
        )
        assert "no live tier for connector kind 'api'" in output, (
            f"the skip must name why the live tier cannot mean anything "
            f"here:\n{output}"
        )
        assert "kind-applicability" not in output, (
            f"the applicability gate must not fire for an inapplicable "
            f"kind:\n{output}"
        )

    def test_tier2_still_gates_a_database_connector(self) -> None:
        """The exemption is per-kind: database tier 2 is untouched."""
        completed = run_kit_suite(
            "cdk.conformance.tier2",
            options=[
                "--connector-dir",
                str(REFERENCE_DIR),
                "--connector-class",
                REFERENCE_CLASS,
            ],
        )
        output = completed.stdout + completed.stderr
        assert completed.returncode == 0, output
        assert re.search(r"\b1 passed", output), (
            f"the database applicability gate must still run and pass:\n" f"{output}"
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


#: What a bare ``analitiq-cdk[conformance]`` install does not carry. The
#: extra pulls pytest and pyarrow and no transport, so an api connector's
#: repo runs tier 1 without an HTTP client anywhere on the machine.
ABSENT_FROM_A_CONFORMANCE_INSTALL = ("aiohttp", "aiohttp_retry", "orjson", "aiofiles")

_IMPORT_PROBE = """
import sys

class _Blocked:
    def find_module(self, name, path=None):
        return None

    def find_spec(self, name, path=None, target=None):
        if name.split(".")[0] in {absent!r}:
            raise ModuleNotFoundError("No module named " + repr(name), name=name)
        return None

sys.meta_path.insert(0, _Blocked())

from cdk.conformance import load_target
from cdk.conformance.api_read_path import (
    check_api_read_advances,
    check_api_read_compiles,
    check_api_read_stop_condition,
    check_api_record_schema,
)
from cdk.conformance.api_surface import (
    check_api_has_reads,
    check_read_transport_selection,
)

target = load_target({fixture!r})
assert target.connector_class is None, "the api class must not have imported"
findings = []
for check in (
    check_api_read_compiles,
    check_api_read_advances,
    check_api_read_stop_condition,
    check_api_record_schema,
    check_api_has_reads,
    check_read_transport_selection,
):
    findings += check(target)
assert not findings, findings
print("CHECKS RAN")
"""


class TestApiChecksRunWithoutAnHttpClient:
    """Tier 1 for an api connector needs no transport installed.

    The whole point of the ``conformance`` extra pulling no HTTP client:
    a connector repo's CI installs the kit, points it at its definition,
    and gets a verdict. If a check module reached for the connector class
    -- or for anything under ``cdk.api.http`` -- every api connector's
    suite would die at collection on a machine that has no aiohttp, and
    the only fix available to the repo would be installing a transport the
    checks never use.
    """

    def test_the_api_drives_execute_with_the_transport_absent(self) -> None:
        script = _IMPORT_PROBE.format(
            absent=set(ABSENT_FROM_A_CONFORMANCE_INSTALL),
            fixture=str(API_REFERENCE_DIR),
        )
        env = dict(os.environ)
        env["PYTHONPATH"] = os.pathsep.join(
            [
                str(REPO_ROOT / "cdk"),
                str(REPO_ROOT),
                env.get("PYTHONPATH", ""),
            ]
        ).rstrip(os.pathsep)
        completed = subprocess.run(  # nosec B603 - fixed argv, no shell
            [sys.executable, "-c", script],
            cwd=REPO_ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        output = completed.stdout + completed.stderr
        assert completed.returncode == 0, (
            f"the api tier-1 checks must run on an install carrying no HTTP "
            f"client:\n{output}"
        )
        assert "CHECKS RAN" in output
