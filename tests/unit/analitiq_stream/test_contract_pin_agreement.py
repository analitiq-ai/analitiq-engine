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

import tomllib
from collections.abc import Callable
from pathlib import Path
from typing import Any, NamedTuple

import pytest
import yaml
from packaging.requirements import InvalidRequirement, Requirement
from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.utils import canonicalize_name
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

#: Every package this module compares, canonicalised once. A requirements
#: line this module cannot parse is safe to skip only when it is not one of
#: these -- see _requirements_declarations.
_GOVERNED_PACKAGES = frozenset(
    canonicalize_name(p) for p in CONTRACT_PACKAGES + CAPABILITY_FLOOR_PACKAGES
)

#: The library manifest, held to a different rule than the rest: see the
#: module docstring. Every other manifest describes what this application
#: installs and must name one revision.
LIBRARY_MANIFEST = Path("cdk/pyproject.toml")

#: Manifest kinds that install dependencies. Globbed rather than listed
#: file-by-file so a new manifest of an existing kind is covered the day
#: it is added, without anyone remembering to extend this test.
MANIFEST_GLOBS = ("pyproject.toml", "requirements*.txt", ".pre-commit-config.yaml")

#: Directories that never describe what this engine installs.
#:
#: "connectors" is attached third-party code (.gitignore:176, the connector-
#: builder plugin's output), not something this repo controls or ships. It
#: does not have to be Poetry -- or use Python packaging at all -- so a
#: manifest under it fails _poetry_declarations's own-format assertion for a
#: reason that has nothing to do with pin drift, and a connector that IS
#: Poetry and happens to declare pyarrow describes that connector's own
#: environment, not this engine's.
EXCLUDED_PARTS = frozenset(
    {".venv", ".git", "node_modules", ".mypy_cache", "connectors"}
)


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


class Declaration(NamedTuple):
    """One place one manifest constrains one distribution.

    A manifest can constrain the same distribution more than once -- two
    pre-commit hooks each installing it, a requirements file naming it twice --
    and those declarations can disagree with each other. ``where`` names the
    site inside the manifest so a disagreement points at the line to fix, and
    keeping them as separate entries is what stops the second one being lost.
    """

    manifest: Path
    where: str
    specifier: SpecifierSet


def _poetry_declarations(manifest: Path) -> list[tuple[str, str, str]]:
    """Every dependency a Poetry manifest declares, across all its groups."""
    document = tomllib.loads(manifest.read_text(encoding="utf-8"))
    poetry = document.get("tool", {}).get("poetry")
    if poetry is None:
        # Not a Poetry manifest (a PEP 621 [project] table, say). It declares
        # nothing this module knows how to read, which is not the same as
        # declaring nothing -- so it must not silently read as empty.
        raise AssertionError(
            f"{manifest} has no [tool.poetry] table; this module reads Poetry "
            f"manifests and cannot grade a dependency declared some other way"
        )
    tables: list[tuple[str, dict[str, Any]]] = [
        ("[tool.poetry.dependencies]", poetry.get("dependencies", {}))
    ]
    tables += [
        (f"[tool.poetry.group.{name}.dependencies]", group.get("dependencies", {}))
        for name, group in poetry.get("group", {}).items()
        if isinstance(group, dict)
    ]
    found: list[tuple[str, str, str]] = []
    for where, table in tables:
        for name, spec in table.items():
            # A path or git dependency constrains no version and is skipped
            # rather than compared as an empty string.
            if isinstance(spec, str):
                found.append((name, where, spec))
            elif isinstance(spec, dict) and "version" in spec:
                found.append((name, where, spec["version"]))
    return found


def _logical_lines(text: str) -> list[tuple[int, str]]:
    """Join a requirements file's backslash line continuations.

    A hash-pinned requirement spans several physical lines: ``pkg==1.0``,
    a backslash, then one or more ``--hash=sha256:...`` continuation lines.
    A per-physical-line reader sees only the fragment ``pkg==1.0`` with a
    trailing backslash (which packaging.Requirement rejects) and then lines
    that look like bare options and start with ``-``, which get read as
    not-a-requirement. The package pinned across every line is invisible
    either way. Numbered by the first physical line, so a failure message
    points at where the
    requirement starts.
    """
    joined: list[tuple[int, str]] = []
    start = 0
    buffer: list[str] = []
    for number, raw in enumerate(text.splitlines(), start=1):
        stripped = raw.rstrip()
        if not buffer:
            start = number
        continues = stripped.endswith("\\")
        buffer.append(stripped[:-1] if continues else stripped)
        if not continues:
            joined.append((start, " ".join(buffer)))
            buffer = []
    if buffer:
        joined.append((start, " ".join(buffer)))
    return joined


def _requirement_text(logical_line: str) -> str:
    """The PEP 508 requirement a logical line names, pip's per-requirement
    options (``--hash=...``, ``--global-option=...``) stripped.

    Those options are pip's own extension, spelled with a leading ``--``
    that never appears inside a PEP 440 specifier, so splitting on the
    first one is exact rather than a heuristic.
    """
    return logical_line.split(" --", 1)[0].strip()


def _fail_if_governed(entry: str, line: str, manifest: Path, where: str) -> None:
    """Fail loud if an unreadable requirement's leading token is governed.

    Shared by every parser's ``except InvalidRequirement`` branch: a line or
    entry this module cannot read is safe to drop only when it does not name
    a package this module compares. A parser that silently continued on
    every unreadable line would let that package quietly drop out of the
    comparison -- and a manifest this cannot see cannot disagree with
    anything.
    """
    candidate = line.split()[0] if line.split() else ""
    for char in "=<>~!;[":
        candidate = candidate.split(char, 1)[0]
    if candidate and canonicalize_name(candidate) in _GOVERNED_PACKAGES:
        pytest.fail(
            f"{manifest} {where} names a package this module governs but is "
            f"not a readable requirement: {entry!r}"
        )


def _requirements_declarations(manifest: Path) -> list[tuple[str, str, str]]:
    """Every dependency a pip requirements file declares."""
    found: list[tuple[str, str, str]] = []
    for number, joined in _logical_lines(manifest.read_text(encoding="utf-8")):
        line = _requirement_text(joined.split("#", 1)[0].strip())
        if not line or line.startswith("-"):
            continue
        try:
            requirement = Requirement(line)
        except InvalidRequirement:
            # Not a PEP 508 requirement (a URL, an editable install) in the
            # common case; _fail_if_governed only raises when it is not.
            _fail_if_governed(joined, line, manifest, f"line {number}")
            continue
        found.append((requirement.name, f"line {number}", str(requirement.specifier)))
    return found


def _pre_commit_declarations(manifest: Path) -> list[tuple[str, str, str]]:
    """Every dependency a pre-commit config installs into a hook env."""
    document = yaml.safe_load(manifest.read_text(encoding="utf-8")) or {}
    found: list[tuple[str, str, str]] = []
    for repo in document.get("repos", []) or []:
        for hook in repo.get("hooks", []) or []:
            where = f"hook {hook.get('id', '?')!r}"
            for entry in hook.get("additional_dependencies", []) or []:
                line = _requirement_text(str(entry))
                try:
                    requirement = Requirement(line)
                except InvalidRequirement:
                    _fail_if_governed(str(entry), line, manifest, where)
                    continue
                found.append((requirement.name, where, str(requirement.specifier)))
    return found


def _reader(manifest: Path) -> Callable[[Path], list[tuple[str, str, str]]]:
    if manifest.name == "pyproject.toml":
        return _poetry_declarations
    if manifest.name == ".pre-commit-config.yaml":
        return _pre_commit_declarations
    return _requirements_declarations


def _pins(package: str) -> list[Declaration]:
    """Every declaration of *package*, from every manifest and every site.

    Names are matched on their PEP 503 canonical form, so ``PyArrow``,
    ``pyarrow`` and ``analitiq_contract_models`` are the same distribution
    here as they are to pip. Matching the literal spelling would let a
    manifest drop out of the comparison entirely by capitalising a letter --
    and a manifest this cannot see cannot disagree with anything.
    """
    wanted = canonicalize_name(package)
    pins: list[Declaration] = []
    for manifest in _manifests():
        relative = manifest.relative_to(REPO_ROOT)
        for name, where, raw in _reader(manifest)(manifest):
            if canonicalize_name(name) != wanted:
                continue
            pins.append(
                Declaration(relative, where, _specifier(raw, relative, package))
            )
    return pins


def _specifier(declared: str, manifest: Path, package: str) -> SpecifierSet:
    """Read one declared constraint as a specifier set.

    Poetry's bare version means ``==``; everything else already carries its
    operators.

    Poetry's ``^`` and ``~`` shorthands are refused rather than expanded.
    Their meaning depends on where the left-most non-zero digit falls
    (``^1.2.3`` admits ``<2.0.0``, ``^0.2.3`` only ``<0.3.0``), so expanding
    them here would be a second implementation of Poetry's rules that nothing
    grades -- and Poetry itself is not importable from the test environment.
    A package whose version is a behavioural fact is worth the explicit range
    at its two or three declaration sites; the shorthand stays fine for every
    dependency this module does not govern, which it never reads.
    """
    declared = declared.strip()
    text = declared if declared[:1] in "=<>~!" else f"=={declared}"
    try:
        return SpecifierSet(text)
    except InvalidSpecifier:
        pytest.fail(
            f"{manifest} declares {package} as {declared!r}, which is not a "
            f"PEP 440 constraint. {package} is graded by this module, so its "
            f"constraint has to be comparable across manifest kinds -- write "
            f"the range explicitly (e.g. '>=21.0.0,<22') instead of Poetry's "
            f"'^' or '~' shorthand."
        )


def _sole_pinned_version(package: str) -> Version:
    """The single revision every application manifest names for *package*."""
    specifiers = {str(d.specifier) for d in _application_pins(package)}
    (declared,) = specifiers
    (clause,) = SpecifierSet(declared)
    return Version(clause.version)


def _application_pins(package: str) -> list[Declaration]:
    """What each manifest that is not the library declares for *package*."""
    return [d for d in _pins(package) if d.manifest != LIBRARY_MANIFEST]


@pytest.mark.parametrize("package", CONTRACT_PACKAGES)
def test_every_application_manifest_pins_the_same_contract_version(
    package: str,
) -> None:
    pins = _application_pins(package)
    assert pins, (
        f"no application manifest pins {package}; the engine validates "
        f"artifacts against it, so some manifest must name a version"
    )
    sites = [f"{d.manifest} ({d.where})" for d in pins]
    versions = {str(d.specifier) for d in pins}
    assert len(versions) == 1, (
        f"{package} is pinned to {len(versions)} different versions across "
        + ", ".join(f"{d.manifest} ({d.where}) -> {d.specifier}" for d in pins)
        + ". The runtime image, the Poetry environment, and the type-check "
        "environment must validate against one contract revision; bump them "
        "together."
    )
    (declared,) = versions
    clauses = list(SpecifierSet(declared))
    if len(clauses) != 1 or clauses[0].operator != "==":
        pytest.fail(
            f"{package} is declared as {declared!r} in {sorted(sites)}; an "
            f"application manifest names one revision, never a range -- a "
            f"range lets the image and the type-check environment resolve "
            f"to different contracts."
        )
    try:
        Version(clauses[0].version)
    except InvalidVersion:
        pytest.fail(f"{package} is declared as {declared!r} in {sorted(sites)}")


def test_contract_packages_are_pinned_together() -> None:
    """The validator pins the models exactly, so they move as a pair."""
    versions = {
        package: {str(d.specifier) for d in _application_pins(package)}
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
    library_declarations = [d for d in _pins(package) if d.manifest == LIBRARY_MANIFEST]
    assert library_declarations, (
        f"{LIBRARY_MANIFEST} declares no {package}; the CDK reads endpoint "
        f"documents as contract models, so it must depend on them"
    )
    (declared,) = {d.specifier for d in library_declarations}
    pinned = _sole_pinned_version(package)
    admitted = declared
    # `contains` honours the prerelease the specifier itself names, so a
    # range written from an rc admits a later rc of the same release --
    # which is exactly the case the application pin is in today.
    assert admitted.contains(pinned), (
        f"{LIBRARY_MANIFEST} declares {package}{declared}, which does not "
        f"admit the {pinned} the application pins; move the range with the pin"
    )


def _floor(specifier: SpecifierSet) -> str:
    """The lower-bound clause(s) of a specifier set, joined into one string.

    A capability floor is what has to agree across manifests, not the whole
    declared range: a library may legitimately add its own ceiling or
    exclusion an application manifest does not need (``>=21.0.0,<22`` next to
    a plain ``>=21.0.0``), and that must not fail a test whose own docstring
    says only the floor has to match. Comparing full specifier strings would
    reject that legitimate divergence along with a real one.
    """
    clauses = sorted(str(c) for c in specifier if c.operator in (">=", ">"))
    return ",".join(clauses)


@pytest.mark.parametrize("package", CAPABILITY_FLOOR_PACKAGES)
def test_every_manifest_declares_the_same_capability_floor(package: str) -> None:
    """Every manifest naming *package* declares the same floor.

    Unlike a contract package this is a range, and the library manifest is
    held to it too: the point is precisely that a connector resolving the
    published CDK must not land on a runtime older than the one the
    published artifacts describe. A range rather than an exact pin is
    deliberate -- a ceiling in a library propagates into every connector's
    resolution -- so what has to agree is the floor, not the rest of the
    declared range: a manifest may add its own ceiling on top and still pass.
    """
    pins = _pins(package)
    assert pins, (
        f"no manifest declares {package}; the engine publishes artifacts "
        f"describing what it can do with it, so its floor must be stated"
    )
    floors = {_floor(d.specifier) for d in pins}
    assert all(floors), (
        f"{package} is declared with no lower bound at "
        + ", ".join(
            f"{d.manifest} ({d.where}) -> {d.specifier}"
            for d in pins
            if not _floor(d.specifier)
        )
        + "; a bare ceiling or exclusion states nothing about the capability "
        "floor this test exists to hold constant."
    )
    assert len(floors) == 1, (
        f"{package}'s floor is declared {len(floors)} different ways across "
        + ", ".join(f"{d.manifest} ({d.where}) -> {d.specifier}" for d in pins)
        + f". Every environment that installs {package} -- the Poetry "
        "environment, the runtime image, the published library -- must agree "
        "on the floor, or the engine tests one version and something else "
        "runs another."
    )
