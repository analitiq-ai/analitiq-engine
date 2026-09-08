"""``tools/check_contract_version_pin.py``'s comparison and failure modes.

The network fetch itself needs a live ``schemas.analitiq.ai``, so it's mocked
here; what's pinned is the script's own behaviour -- reusing the pin
agreement test's manifest reader, comparing against the published fact, and
failing outright (no retry) on any fetch, parse, or divergence problem.
"""

from __future__ import annotations

import importlib.util
import io
import json
import sys
import urllib.error
from pathlib import Path
from types import ModuleType

import pytest

pytestmark = pytest.mark.unit

_SCRIPT = (
    Path(__file__).resolve().parents[3] / "tools" / "check_contract_version_pin.py"
)


@pytest.fixture()
def ccvp() -> ModuleType:
    """A fresh module instance per test, so patches on it don't leak."""
    spec = importlib.util.spec_from_file_location("check_contract_version_pin", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _fake_pin_test_module(pins: dict[str, str]) -> ModuleType:
    """A stand-in for the loaded ``test_contract_pin_agreement`` module."""
    module = ModuleType("fake_pin_test_module")
    module._application_pins = lambda package: {  # type: ignore[attr-defined]
        Path(name): version for name, version in pins.items()
    }
    return module


class TestLocalPin:
    def test_returns_the_single_agreed_version(self, ccvp: ModuleType) -> None:
        module = _fake_pin_test_module({"pyproject.toml": "1.0.0rc24"})
        assert ccvp.local_pin(module) == "1.0.0rc24"

    def test_raises_when_manifests_disagree(self, ccvp: ModuleType) -> None:
        module = _fake_pin_test_module(
            {"pyproject.toml": "1.0.0rc24", "docker/requirements.txt": "1.0.0rc1"}
        )
        with pytest.raises(RuntimeError, match="not pinned to a single version"):
            ccvp.local_pin(module)


class TestPublishedVersion:
    def test_returns_the_fact_s_package_version(
        self, ccvp: ModuleType, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        payload = json.dumps({"analitiq-contract-models": "1.0.0rc24"}).encode()

        def fake_urlopen(url, timeout=None):
            return io.BytesIO(payload)

        monkeypatch.setattr(ccvp.urllib.request, "urlopen", fake_urlopen)
        assert ccvp.published_version() == "1.0.0rc24"

    def test_raises_on_http_error(
        self, ccvp: ModuleType, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def fake_urlopen(url, timeout=None):
            raise urllib.error.HTTPError(url, 404, "Not Found", {}, None)

        monkeypatch.setattr(ccvp.urllib.request, "urlopen", fake_urlopen)
        with pytest.raises(RuntimeError, match="HTTP 404"):
            ccvp.published_version()

    def test_raises_on_connection_failure(
        self, ccvp: ModuleType, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def fake_urlopen(url, timeout=None):
            raise urllib.error.URLError("boom")

        monkeypatch.setattr(ccvp.urllib.request, "urlopen", fake_urlopen)
        with pytest.raises(RuntimeError, match="failed to fetch"):
            ccvp.published_version()

    def test_raises_on_malformed_json(
        self, ccvp: ModuleType, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def fake_urlopen(url, timeout=None):
            return io.BytesIO(b"not json")

        monkeypatch.setattr(ccvp.urllib.request, "urlopen", fake_urlopen)
        with pytest.raises(RuntimeError, match="did not return valid JSON"):
            ccvp.published_version()

    def test_raises_when_package_key_is_missing(
        self, ccvp: ModuleType, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def fake_urlopen(url, timeout=None):
            return io.BytesIO(json.dumps({"tree_sha256": "abc"}).encode())

        monkeypatch.setattr(ccvp.urllib.request, "urlopen", fake_urlopen)
        with pytest.raises(RuntimeError, match="has no 'analitiq-contract-models' key"):
            ccvp.published_version()


class TestMain:
    def test_passes_when_local_pin_matches_published(
        self, ccvp: ModuleType, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            ccvp, "_load_pin_test_module", lambda: _fake_pin_test_module({})
        )
        monkeypatch.setattr(ccvp, "local_pin", lambda module: "1.0.0rc24")
        monkeypatch.setattr(ccvp, "published_version", lambda: "1.0.0rc24")
        assert ccvp.main() == 0

    def test_fails_when_local_pin_diverges_from_published(
        self, ccvp: ModuleType, monkeypatch: pytest.MonkeyPatch, capsys
    ) -> None:
        monkeypatch.setattr(
            ccvp, "_load_pin_test_module", lambda: _fake_pin_test_module({})
        )
        monkeypatch.setattr(ccvp, "local_pin", lambda module: "1.0.0rc1")
        monkeypatch.setattr(ccvp, "published_version", lambda: "1.0.0rc24")
        assert ccvp.main() == 1
        assert "pin mismatch" in capsys.readouterr().err
