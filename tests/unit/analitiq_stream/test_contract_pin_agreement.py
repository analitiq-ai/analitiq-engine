"""Every manifest that installs a contract package agrees on the version.

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

The CDK is the one manifest here that is not the application. It ships as
a published library, so it declares a compatible RANGE: an exact pin in a
library is that library imposing one revision on every connector that
installs it. What must hold is weaker and still sufficient -- the
application manifests agree on one revision, and that revision is one the
library's range admits.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.version import InvalidVersion, Version

REPO_ROOT = Path(__file__).resolve().parents[3]

#: The packages whose version is a behavioral fact for the engine.
CONTRACT_PACKAGES = ("analitiq-contract-models", "analitiq-validator")

#: Packages whose *floor* is a behavioral fact, held to the rule below.
#:
#: pyarrow is one because the engine publishes documents describing what it
#: can do with Arrow data -- arrow_type_grammar.json says which JSON types
#: each family carries, conversion_matrix.json which casts are permitted --
#: and those are properties of the linked Arrow build, not of the contract.
#: Below pyarrow 21 a Float16 column cannot be built from a Python float at
#: all, so a manifest on an older floor installs a runtime the published
#: artifacts are not about (#484). The engine and the CDK declared ^12.0.0
#: and >=14.0.0, ranges with no version in common, and nothing noticed
#: because nothing compared the manifests.
CAPABILITY_FLOOR_PACKAGES = ("pyarrow",)

#: The library manifest, held to a different rule than the rest: see the
#: module docstring. Every other manifest describes what this application
#: installs and must name one revision.
LIBRARY_MANIFEST = Path("cdk/pyproject.toml")

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
    """Map each manifest that declares *package* to the requirement it names.

    The value is the requirement verbatim -- an exact version from an
    application manifest, a range from the library one -- because telling
    the two apart is what the tests below are for.
    """
    # Covers every spelling a manifest uses: the requirements/pre-commit
    # `pkg==X` and `pkg>=X`, and Poetry's `pkg = {version = "X"}` /
    # `pkg = "X"`. The bare-requirement branch keeps the operator, because a
    # floor and an exact pin are different claims and the rules below tell
    # them apart.
    pattern = re.compile(
        rf"^\s*-?\s*{re.escape(package)}\s*"
        rf"(?:(?P<requirement>(?:==|>=|<=|~=|!=|[<>])\s*[\w.*+!-]+)"
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
            match.group("requirement")
            or match.group("poetry_table")
            or match.group("poetry_scalar")
        )
        version = version.replace(" ", "")
        pins[manifest.relative_to(REPO_ROOT)] = version
    return pins


def _specifier(requirement: str) -> SpecifierSet:
    """Read one declared requirement as a specifier set.

    A manifest spells the same claim three ways -- ``pkg==1.2.3``,
    ``pkg = "1.2.3"`` and ``pkg = {version = "1.2.3"}`` -- and Poetry's bare
    version means ``==``. Normalising here is what lets the rules below
    compare declarations across manifest kinds instead of comparing the
    strings people happened to type. An unparseable requirement raises
    :class:`InvalidSpecifier` rather than comparing unequal to everything,
    so a spelling this cannot read fails loudly instead of passing.
    """
    return SpecifierSet(
        requirement if requirement[0] in "=<>~!" else f"=={requirement}"
    )


def _sole_pinned_version(package: str) -> Version:
    """The single revision every application manifest names for *package*."""
    specifiers = {str(_specifier(r)) for r in _application_pins(package).values()}
    (declared,) = specifiers
    (clause,) = SpecifierSet(declared)
    return Version(clause.version)


def _application_pins(package: str) -> dict[Path, str]:
    """What each manifest that is not the library declares for *package*."""
    return {
        path: requirement
        for path, requirement in _pins(package).items()
        if path != LIBRARY_MANIFEST
    }


@pytest.mark.parametrize("package", CONTRACT_PACKAGES)
def test_every_application_manifest_pins_the_same_contract_version(
    package: str,
) -> None:
    pins = _application_pins(package)
    assert pins, (
        f"no application manifest pins {package}; the engine validates "
        f"artifacts against it, so some manifest must name a version"
    )
    versions = {str(_specifier(requirement)) for requirement in pins.values()}
    assert len(versions) == 1, (
        f"{package} is pinned to {len(versions)} different versions: "
        + ", ".join(f"{path} -> {version}" for path, version in sorted(pins.items()))
        + ". The runtime image, the Poetry environment, and the type-check "
        "environment must validate against one contract revision; bump them "
        "together."
    )
    (declared,) = versions
    clauses = list(SpecifierSet(declared))
    if len(clauses) != 1 or clauses[0].operator != "==":
        pytest.fail(
            f"{package} is declared as {declared!r} in {sorted(pins)}; an "
            f"application manifest names one revision, never a range -- a "
            f"range lets the image and the type-check environment resolve "
            f"to different contracts."
        )
    try:
        Version(clauses[0].version)
    except InvalidVersion:
        pytest.fail(f"{package} is declared as {declared!r} in {sorted(pins)}")


def test_contract_packages_are_pinned_together() -> None:
    """The validator pins the models exactly, so they move as a pair."""
    versions = {
        package: {str(_specifier(r)) for r in _application_pins(package).values()}
        for package in CONTRACT_PACKAGES
    }
    assert len(set().union(*versions.values())) == 1, (
        f"contract packages disagree: {versions}. analitiq-validator declares "
        f"an exact dependency on analitiq-contract-models, so a split pin "
        f"cannot resolve."
    )


def test_the_library_range_admits_the_application_pin() -> None:
    """The CDK's range must contain the revision the application installs.

    The CDK and the engine run in the same process, so a range that has
    drifted off the application's pin resolves to two contract revisions
    in one environment -- or, in a connector's environment, to none.
    """
    package = "analitiq-contract-models"
    declared = _pins(package).get(LIBRARY_MANIFEST)
    assert declared is not None, (
        f"{LIBRARY_MANIFEST} declares no {package}; the CDK reads endpoint "
        f"documents as contract models, so it must depend on them"
    )
    pinned = _sole_pinned_version(package)
    try:
        admitted = SpecifierSet(declared)
    except InvalidSpecifier:
        pytest.fail(f"{LIBRARY_MANIFEST} declares {package} as {declared!r}")
    # `contains` honours the prerelease the specifier itself names, so a
    # range written from an rc admits a later rc of the same release --
    # which is exactly the case the application pin is in today.
    assert admitted.contains(pinned), (
        f"{LIBRARY_MANIFEST} declares {package}{declared}, which does not "
        f"admit the {pinned} the application pins; move the range with the pin"
    )


@pytest.mark.parametrize("package", CAPABILITY_FLOOR_PACKAGES)
def test_every_manifest_declares_the_same_capability_floor(package: str) -> None:
    """Every manifest naming *package* declares the same floor.

    Unlike a contract package this is a range, and the library manifest is
    held to it too: the point is precisely that a connector resolving the
    published CDK must not land on a runtime older than the one the
    published artifacts describe. A range rather than an exact pin is
    deliberate -- a ceiling in a library propagates into every connector's
    resolution -- so what has to agree is the floor.
    """
    declared = _pins(package)
    assert declared, (
        f"no manifest declares {package}; the engine publishes artifacts "
        f"describing what it can do with it, so its floor must be stated"
    )
    floors = {str(_specifier(requirement)) for requirement in declared.values()}
    assert len(floors) == 1, (
        f"{package} is declared {len(floors)} different ways: "
        + ", ".join(f"{path} -> {req}" for path, req in sorted(declared.items()))
        + f". Every environment that installs {package} -- the Poetry "
        "environment, the runtime image, the published library -- must agree, "
        "or the engine tests one version and something else runs another."
    )
