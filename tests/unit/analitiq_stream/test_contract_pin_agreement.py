"""Every manifest that pins a contract package names the same version.

The engine validates a connector against ``analitiq-contract-models``
before its own parsers read the same document, so the contract version is
part of the engine's behavior, not a build detail. That version is
declared in more than one manifest — Poetry resolves ``pyproject.toml``,
the runtime image installs ``docker/requirements.txt``, and the mypy hook
installs its own isolated environment — and each carried a comment
promising it matched the others while two of the three had silently
drifted (rc13 and rc16 against a pyproject pin of rc17).

A drifted pin is not a lint issue: the image would validate a connector
against one contract revision and parse it against another, accepting
documents the running engine refuses and refusing documents it requires.
This test turns the promise those comments make into something that
fails when it stops being true.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]

#: The packages whose version is a behavioral fact for the engine.
CONTRACT_PACKAGES = ("analitiq-contract-models", "analitiq-validator")

#: Manifest kinds that install dependencies. Globbed rather than listed
#: file-by-file so a new manifest of an existing kind is covered the day
#: it is added, without anyone remembering to extend this test.
MANIFEST_GLOBS = ("pyproject.toml", "requirements*.txt", ".pre-commit-config.yaml")

#: Directories that never describe what this engine installs.
EXCLUDED_PARTS = frozenset({".venv", ".git", "node_modules", ".mypy_cache"})


def _manifests() -> list[Path]:
    found: list[Path] = []
    for pattern in MANIFEST_GLOBS:
        for path in REPO_ROOT.rglob(pattern):
            # Relative to the root, never absolute: a checkout may itself
            # live under a directory named like one of the exclusions
            # (a git worktree under .git/ does), and excluding on the
            # absolute path would silently find no manifests at all.
            if EXCLUDED_PARTS.isdisjoint(path.relative_to(REPO_ROOT).parts):
                found.append(path)
    return sorted(found)


def _pins(package: str) -> dict[Path, str]:
    """Map each manifest that pins *package* to the version it names."""
    # Covers both spellings a manifest uses: the requirements/pre-commit
    # `pkg==X` and Poetry's `pkg = {version = "X"}` / `pkg = "X"`.
    pattern = re.compile(
        rf"^\s*-?\s*{re.escape(package)}\s*"
        rf"(?:==\s*(?P<pinned>[\w.]+)"
        rf"|=\s*\{{[^}}]*version\s*=\s*\"(?P<poetry_table>[^\"]+)\""
        rf"|=\s*\"(?P<poetry_scalar>[^\"]+)\")",
        re.MULTILINE,
    )
    pins: dict[Path, str] = {}
    for manifest in _manifests():
        match = pattern.search(manifest.read_text(encoding="utf-8"))
        if match is None:
            continue
        version = (
            match.group("pinned")
            or match.group("poetry_table")
            or match.group("poetry_scalar")
        )
        pins[manifest.relative_to(REPO_ROOT)] = version
    return pins


@pytest.mark.parametrize("package", CONTRACT_PACKAGES)
def test_every_manifest_pins_the_same_contract_version(package: str) -> None:
    pins = _pins(package)
    assert pins, (
        f"no manifest pins {package}; the engine validates artifacts "
        f"against it, so some manifest must name a version"
    )
    versions = set(pins.values())
    assert len(versions) == 1, (
        f"{package} is pinned to {len(versions)} different versions: "
        + ", ".join(f"{path} -> {version}" for path, version in sorted(pins.items()))
        + ". The runtime image, the Poetry environment, and the type-check "
        "environment must validate against one contract revision; bump them "
        "together."
    )


def test_contract_packages_are_pinned_together() -> None:
    """The validator pins the models exactly, so they move as a pair."""
    versions = {package: set(_pins(package).values()) for package in CONTRACT_PACKAGES}
    assert len(set().union(*versions.values())) == 1, (
        f"contract packages disagree: {versions}. analitiq-validator declares "
        f"an exact dependency on analitiq-contract-models, so a split pin "
        f"cannot resolve."
    )
