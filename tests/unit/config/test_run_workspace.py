"""The workspace request a run is gated on, read from the on-disk layout.

The request holds the documents exactly as authored, selected by the
contract's location tables, for the pipeline to run and the packages it
references -- never a secret location, never a package it does not reference.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest

from src.config.run_workspace import (
    RunWorkspace,
    WorkspaceLayoutError,
    WorkspaceRejectedError,
    gate_run,
    read_run_workspace,
)

pytestmark = pytest.mark.unit

PIPELINE_DIR = "pipelines/p1/"


def _write(root: Path, key: str, payload: Any) -> None:
    path = root / key
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload if isinstance(payload, str) else json.dumps(payload))


def _paths(root: Path) -> dict[str, Path]:
    return {
        "root": root,
        "manifest": root / "pipelines" / "manifest.json",
        "pipelines": root / "pipelines",
        "connections": root / "connections",
        "connectors": root / "connectors",
    }


@pytest.fixture
def workspace_root(tmp_path: Path) -> Path:
    root = tmp_path
    _write(root, "pipelines/manifest.json", {"pipelines": []})
    _write(
        root,
        f"{PIPELINE_DIR}pipeline.json",
        {"connections": {"source": "src", "destinations": ["dst"]}},
    )
    _write(root, f"{PIPELINE_DIR}streams/s1.json", {"stream_id": "s1"})
    for connection_id in ("src", "dst"):
        _write(
            root,
            f"connections/{connection_id}/connection.json",
            {"connector_id": "api"},
        )
        _write(root, f"connections/{connection_id}/.secrets/credentials.json", {})
    _write(root, "connections/dst/definition/endpoints/t.json", {"endpoint_id": "t"})
    _write(root, "connections/dst/definition/type-map.json", {})
    _write(root, "connectors/api/definition/connector.json", {"connector_id": "api"})
    _write(root, "connectors/api/definition/type-map.json", {})
    _write(root, "connectors/api/definition/endpoints/e.json", {"endpoint_id": "e"})
    return root


def _read(root: Path) -> RunWorkspace:
    return read_run_workspace(_paths(root), PIPELINE_DIR)


class TestTheRequestHoldsTheRunsPackages:
    def test_every_located_document_of_every_referenced_package(
        self, workspace_root: Path
    ) -> None:
        workspace = _read(workspace_root)

        assert sorted(workspace.request.documents.root) == [
            "connections/dst/connection.json",
            "connections/dst/definition/endpoints/t.json",
            "connections/dst/definition/type-map.json",
            "connections/src/connection.json",
            "connectors/api/definition/connector.json",
            "connectors/api/definition/endpoints/e.json",
            "connectors/api/definition/type-map.json",
            "pipelines/manifest.json",
            "pipelines/p1/pipeline.json",
            "pipelines/p1/streams/s1.json",
        ]

    def test_the_texts_are_the_files_as_authored(self, workspace_root: Path) -> None:
        authored = '{ "stream_id" :"s1" }\n'
        _write(workspace_root, f"{PIPELINE_DIR}streams/s1.json", authored)

        workspace = _read(workspace_root)

        assert workspace.text(f"{PIPELINE_DIR}streams/s1.json") == authored

    def test_names_the_pipeline_to_run(self, workspace_root: Path) -> None:
        assert _read(workspace_root).request.run_pipeline == PIPELINE_DIR

    def test_never_holds_a_secret_location(self, workspace_root: Path) -> None:
        keys = _read(workspace_root).request.documents.root

        assert not [key for key in keys if "/.secrets/" in key]

    def test_leaves_out_unreferenced_packages(self, workspace_root: Path) -> None:
        _write(workspace_root, "connections/other/connection.json", "{ not json")
        _write(workspace_root, "connectors/other/definition/connector.json", "{")
        _write(workspace_root, "pipelines/p2/pipeline.json", "{")

        keys = _read(workspace_root).request.documents.root

        assert not [key for key in keys if "/other/" in key or "/p2/" in key]

    def test_leaves_out_files_no_location_names(self, workspace_root: Path) -> None:
        _write(workspace_root, "connectors/api/README.md", "readme")
        _write(workspace_root, "connectors/api/connector.py", "pass")

        keys = _read(workspace_root).request.documents.root

        assert "connectors/api/README.md" not in keys
        assert "connectors/api/connector.py" not in keys


class TestAReferenceTheRequestCannotFollow:
    """A document the engine cannot read a reference from still goes in the
    request, so the verdict reports it; the packages it would have named do
    not."""

    def test_an_unparseable_pipeline_names_no_connection(
        self, workspace_root: Path
    ) -> None:
        _write(workspace_root, f"{PIPELINE_DIR}pipeline.json", "{ not json")

        keys = _read(workspace_root).request.documents.root

        assert f"{PIPELINE_DIR}pipeline.json" in keys
        assert not [key for key in keys if key.startswith("connections/")]

    def test_an_unparseable_connection_names_no_connector(
        self, workspace_root: Path
    ) -> None:
        for connection_id in ("src", "dst"):
            _write(workspace_root, f"connections/{connection_id}/connection.json", "{")

        keys = _read(workspace_root).request.documents.root

        assert "connections/src/connection.json" in keys
        assert not [key for key in keys if key.startswith("connectors/")]

    def test_a_connection_id_that_is_not_a_directory_name_is_not_read(
        self, workspace_root: Path
    ) -> None:
        _write(
            workspace_root,
            f"{PIPELINE_DIR}pipeline.json",
            {"connections": {"source": "../../outside", "destinations": ["/abs"]}},
        )

        keys = _read(workspace_root).request.documents.root

        assert not [key for key in keys if "outside" in key or "abs" in key]

    def test_a_missing_package_is_left_for_the_verdict(
        self, workspace_root: Path
    ) -> None:
        _write(
            workspace_root,
            f"{PIPELINE_DIR}pipeline.json",
            {"connections": {"source": "absent", "destinations": ["dst"]}},
        )

        keys = _read(workspace_root).request.documents.root

        assert not [key for key in keys if "/absent/" in key]


class TestTheLayoutMustBeReadable:
    def test_a_linked_document_is_refused(
        self, workspace_root: Path, tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        outside = tmp_path_factory.mktemp("outside") / "e.json"
        outside.write_text("{}")
        linked = workspace_root / "connectors/api/definition/endpoints/linked.json"
        linked.symlink_to(outside)

        with pytest.raises(WorkspaceLayoutError, match="linked.json"):
            _read(workspace_root)

    def test_a_linked_directory_inside_a_package_is_refused(
        self, workspace_root: Path, tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        outside = tmp_path_factory.mktemp("outside")
        (outside / "e.json").write_text("{}")
        endpoints = workspace_root / "connectors/api/definition/endpoints"
        for document in endpoints.iterdir():
            document.unlink()
        endpoints.rmdir()
        endpoints.symlink_to(outside)

        with pytest.raises(WorkspaceLayoutError, match="definition/endpoints"):
            _read(workspace_root)

    def test_links_where_no_document_can_sit_are_left_alone(
        self, workspace_root: Path, tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        """A checkout carries tooling no location names -- a nested worktree's
        linked virtualenv, a linked secrets directory -- and the run reads
        past it."""
        outside = tmp_path_factory.mktemp("outside")
        venv = workspace_root / "connectors/api/.git/wt/1/.venv"
        venv.mkdir(parents=True)
        (venv / "lib64").symlink_to(outside)
        secrets = workspace_root / "connections/src/.secrets"
        (secrets / "credentials.json").unlink()
        secrets.rmdir()
        secrets.symlink_to(outside)

        keys = _read(workspace_root).request.documents.root

        assert "connectors/api/definition/connector.json" in keys
        assert not any(".secrets" in key or ".git" in key for key in keys)

    def test_a_linked_directory_no_document_sits_behind_is_left_alone(
        self, workspace_root: Path, tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        outside = tmp_path_factory.mktemp("outside")
        (outside / "a.json").write_text("{}")
        streams = workspace_root / PIPELINE_DIR / "streams"
        (streams / "notes").symlink_to(outside)
        (streams / os.fsdecode(b"x\xff")).symlink_to(outside)

        keys = _read(workspace_root).request.documents.root

        assert f"{PIPELINE_DIR}streams/s1.json" in keys
        assert not [key for key in keys if "a.json" in key]

    def test_a_linked_location_directory_is_refused_in_printable_words(
        self, workspace_root: Path, tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        streams = workspace_root / PIPELINE_DIR / "streams"
        outside = tmp_path_factory.mktemp("outside") / "streams"
        streams.rename(outside)
        streams.symlink_to(outside)

        with pytest.raises(WorkspaceLayoutError, match="p1/streams") as err:
            _read(workspace_root)

        str(err.value).encode()

    def test_a_link_back_into_its_own_package_ends(self, workspace_root: Path) -> None:
        streams = workspace_root / PIPELINE_DIR / "streams"
        (streams / "loop").symlink_to(streams)

        keys = _read(workspace_root).request.documents.root

        assert f"{PIPELINE_DIR}streams/s1.json" in keys

    def test_a_directory_whose_name_is_not_utf8_is_left_alone(
        self, workspace_root: Path
    ) -> None:
        (workspace_root / "connectors/api" / os.fsdecode(b"bad\xff")).mkdir()

        keys = _read(workspace_root).request.documents.root

        assert "connectors/api/definition/connector.json" in keys

    def test_a_document_whose_name_is_not_utf8_is_refused(
        self, workspace_root: Path
    ) -> None:
        endpoints = workspace_root / "connectors/api/definition/endpoints"
        (endpoints / os.fsdecode(b"e\xff.json")).write_text("{}")

        with pytest.raises(WorkspaceLayoutError, match="definition/endpoints/e"):
            _read(workspace_root)

    def test_a_linked_package_directory_is_read(
        self, workspace_root: Path, tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        checkout = tmp_path_factory.mktemp("checkout") / "api"
        (workspace_root / "connectors/api").rename(checkout)
        (workspace_root / "connectors/api").symlink_to(checkout)

        keys = _read(workspace_root).request.documents.root

        assert "connectors/api/definition/connector.json" in keys

    def test_a_pipeline_outside_the_pipeline_locations_is_refused(
        self, workspace_root: Path
    ) -> None:
        with pytest.raises(WorkspaceLayoutError, match="nested/p1"):
            read_run_workspace(_paths(workspace_root), "pipelines/nested/p1/")


def _finding(kind: str, severity: str | None, message: str) -> dict[str, Any]:
    finding: dict[str, Any] = {
        "message_id": "m",
        "kind": kind,
        "path": "pipelines/p1/pipeline.json#",
        "message": message,
    }
    if severity is not None:
        finding["severity"] = severity
    return finding


class TestTheRunIsGatedOnTheVerdict:
    def test_a_failed_verdict_refuses_the_run_naming_what_cost_it(
        self, workspace_root: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from src.config import run_workspace

        findings = [
            _finding("fail", "error", "stream s9 is not carried"),
            _finding("fail", "warning", "advisory only"),
        ]
        monkeypatch.setattr(
            run_workspace,
            "validate_workspace",
            lambda request: {"passed": False, "findings": findings},
        )

        with pytest.raises(WorkspaceRejectedError, match="stream s9") as err:
            gate_run(_read(workspace_root))

        assert err.value.findings == [findings[0]]
        assert "advisory only" not in str(err.value)

    def test_the_refusal_names_every_finding_that_cost_the_pass(
        self, workspace_root: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from src.config import run_workspace

        findings = [
            {**_finding("fail", "error", f"defect {n}"), "rule": f"RULE-X-{n:03}"}
            for n in range(12)
        ]
        monkeypatch.setattr(
            run_workspace,
            "validate_workspace",
            lambda request: {"passed": False, "findings": findings},
        )

        with pytest.raises(WorkspaceRejectedError) as err:
            gate_run(_read(workspace_root))

        for n in range(12):
            assert f"RULE-X-{n:03}" in str(err.value)
            assert f"defect {n}" in str(err.value)

    def test_a_passed_verdict_runs_and_logs_what_it_reported(
        self,
        workspace_root: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        from src.config import run_workspace

        monkeypatch.setattr(
            run_workspace,
            "validate_workspace",
            lambda request: {
                "passed": True,
                "findings": [_finding("fail", "warning", "advisory only")],
            },
        )

        with caplog.at_level("WARNING", logger="src.config.run_workspace"):
            gate_run(_read(workspace_root))

        assert "advisory only" in caplog.text

    @pytest.mark.parametrize(
        ("kind", "severity"),
        [("fail", None), ("informational", None), ("notApplicable", None)],
    )
    def test_the_pass_is_the_verdicts_not_a_severity_rule(
        self,
        workspace_root: Path,
        monkeypatch: pytest.MonkeyPatch,
        kind: str,
        severity: str | None,
    ) -> None:
        """#550: a finding the validator says costs no pass never blocks."""
        from src.config import run_workspace

        monkeypatch.setattr(
            run_workspace,
            "validate_workspace",
            lambda request: {
                "passed": True,
                "findings": [_finding(kind, severity, "reported")],
            },
        )

        gate_run(_read(workspace_root))
