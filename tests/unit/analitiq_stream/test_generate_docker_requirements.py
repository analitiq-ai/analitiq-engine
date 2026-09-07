"""``tools/generate_docker_requirements.py``'s render/sort/check-write logic.

The ``poetry export`` call itself needs the ``poetry-plugin-export`` plugin
and network access, so it's mocked here; what's pinned is the script's own
behaviour around that call -- sorting, the header, and ``--check``/``--write``
semantics -- the part a change to this file can actually regress.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

pytestmark = pytest.mark.unit

_SCRIPT = (
    Path(__file__).resolve().parents[3] / "tools" / "generate_docker_requirements.py"
)

_EXPORT_OUTPUT = (
    'aiohttp-retry==2.9.1 ; python_version >= "3.11"\n'
    'aiohttp==3.14.3 ; python_version >= "3.11"\n'
    'zope-interface==6.0 ; python_version >= "3.11"\n'
)


@pytest.fixture()
def gdr() -> ModuleType:
    """A fresh module instance per test, so ``REQUIREMENTS_PATH`` patches don't leak."""
    spec = importlib.util.spec_from_file_location(
        "generate_docker_requirements", _SCRIPT
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _fake_run_success(*_args: object, **_kwargs: object) -> SimpleNamespace:
    return SimpleNamespace(returncode=0, stdout=_EXPORT_OUTPUT, stderr="")


def _fake_run_failure(*_args: object, **_kwargs: object) -> SimpleNamespace:
    return SimpleNamespace(returncode=1, stdout="", stderr="plugin not installed")


class TestSortKey:
    def test_extracts_the_bare_package_name(self, gdr: ModuleType) -> None:
        line = 'aiohttp==3.14.3 ; python_version >= "3.11"'
        assert gdr._sort_key(line) == "aiohttp"

    def test_is_case_insensitive(self, gdr: ModuleType) -> None:
        assert gdr._sort_key("PyYAML==6.0.3") == "pyyaml"

    def test_a_name_that_is_a_prefix_of_another_sorts_first(
        self, gdr: ModuleType
    ) -> None:
        # requirements-txt-fixer's own rule: "aiohttp" < "aiohttp-retry"
        # because the shorter string ends first, not because '-' < '='.
        names = sorted(["aiohttp-retry==2.9.1", "aiohttp==3.14.3"], key=gdr._sort_key)
        assert names == ["aiohttp==3.14.3", "aiohttp-retry==2.9.1"]

    def test_rejects_a_line_with_no_leading_package_name(self, gdr: ModuleType) -> None:
        with pytest.raises(ValueError, match="not a requirement line"):
            gdr._sort_key("   ")


class TestRenderRequirements:
    def test_prepends_the_generated_header(
        self, gdr: ModuleType, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(subprocess, "run", _fake_run_success)
        assert gdr.render_requirements().startswith(gdr.HEADER)

    def test_sorts_by_package_name_not_raw_line(
        self, gdr: ModuleType, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(subprocess, "run", _fake_run_success)
        body = gdr.render_requirements().removeprefix(gdr.HEADER)
        lines = body.splitlines()
        assert lines == [
            'aiohttp==3.14.3 ; python_version >= "3.11"',
            'aiohttp-retry==2.9.1 ; python_version >= "3.11"',
            'zope-interface==6.0 ; python_version >= "3.11"',
        ]

    def test_raises_when_poetry_export_fails(
        self, gdr: ModuleType, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(subprocess, "run", _fake_run_failure)
        with pytest.raises(RuntimeError, match="poetry export failed"):
            gdr.render_requirements()


class TestMain:
    @pytest.fixture(autouse=True)
    def _requirements_path(
        self, gdr: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> Path:
        path = tmp_path / "requirements.txt"
        monkeypatch.setattr(gdr, "REQUIREMENTS_PATH", path)
        return path

    def test_write_creates_the_file_with_rendered_content(
        self,
        gdr: ModuleType,
        monkeypatch: pytest.MonkeyPatch,
        _requirements_path: Path,
    ) -> None:
        monkeypatch.setattr(subprocess, "run", _fake_run_success)
        assert gdr.main(["--write"]) == 0
        assert _requirements_path.read_text() == gdr.render_requirements()

    def test_check_passes_when_the_file_matches_a_fresh_render(
        self,
        gdr: ModuleType,
        monkeypatch: pytest.MonkeyPatch,
        _requirements_path: Path,
    ) -> None:
        monkeypatch.setattr(subprocess, "run", _fake_run_success)
        _requirements_path.write_text(gdr.render_requirements())
        assert gdr.main(["--check"]) == 0

    def test_check_fails_when_the_file_is_stale(
        self,
        gdr: ModuleType,
        monkeypatch: pytest.MonkeyPatch,
        _requirements_path: Path,
    ) -> None:
        monkeypatch.setattr(subprocess, "run", _fake_run_success)
        _requirements_path.write_text("stale content\n")
        assert gdr.main(["--check"]) == 1

    def test_check_fails_when_the_file_does_not_exist(
        self,
        gdr: ModuleType,
        monkeypatch: pytest.MonkeyPatch,
        _requirements_path: Path,
    ) -> None:
        monkeypatch.setattr(subprocess, "run", _fake_run_success)
        assert not _requirements_path.exists()
        assert gdr.main(["--check"]) == 1

    def test_check_and_write_are_mutually_exclusive(self, gdr: ModuleType) -> None:
        with pytest.raises(SystemExit):
            gdr.main(["--check", "--write"])

    def test_one_of_check_or_write_is_required(self, gdr: ModuleType) -> None:
        with pytest.raises(SystemExit):
            gdr.main([])
