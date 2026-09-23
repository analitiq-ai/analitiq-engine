"""The workspace request a run is gated on.

Before a run the engine hands the published validator one workspace request:
the pipeline to run and every package it references, each document as the
file's text, keyed by its path from the workspace root. The validator's
verdict is the one gate over those documents (shape and cross-document
references alike); the engine checks none of them itself, and builds its typed
models only from the texts the verdict passed, so what runs is what was graded.

Which files go in is the contract's call: a package's documents are the files
at its package model's locations, and a secret location is never read.
"""

from __future__ import annotations

import functools
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import re2
from analitiq.contracts.connection_package import ConnectionPackage
from analitiq.contracts.connector_package import ConnectorPackage
from analitiq.contracts.pipeline_package import PipelinePackage
from analitiq.contracts.shared.common import DocumentPackage
from analitiq.contracts.validation_requests import ValidateWorkspaceRequest
from analitiq.contracts.workspace import Workspace
from analitiq.validator import Finding, finding_costs_a_pass, validate_workspace

from src.config.exceptions import ConfigError
from src.config.utils import read_config_text

logger = logging.getLogger(__name__)


class WorkspaceLayoutError(ConfigError):
    """The on-disk layout cannot be read into a workspace request."""


class WorkspaceRejectedError(ConfigError):
    """The validator's verdict on the run's workspace did not pass.

    ``findings`` holds the findings that cost the pass.
    """

    def __init__(self, findings: list[Finding]):
        self.findings = findings
        super().__init__(
            "\n".join(
                ["the pipeline's workspace failed validation:"]
                + [f"  - {_describe(f)}" for f in findings]
            )
        )


def _describe(finding: Finding) -> str:
    rule = finding.get("rule")
    return (
        f"{finding['path']} [{finding['kind']}"
        f"{f' {rule}' if rule else ''}] {finding['message']}"
    )


@dataclass(frozen=True)
class RunWorkspace:
    """The run's workspace request, and where each package of it sits."""

    request: ValidateWorkspaceRequest
    pipeline_directory: str
    connection_directories: dict[str, str]  # connection_id -> directory
    connector_directories: dict[str, str]  # connector_id -> directory

    def text(self, key: str) -> str:
        """Return the authored text of the document at ``key``."""
        try:
            return self.request.documents.root[key]
        except KeyError:
            raise KeyError(
                f"the run's workspace holds no document at {key!r}"
            ) from None

    def texts(self, directory: str, kind: str) -> dict[str, str]:
        """Every document of ``kind`` in the package at ``directory``, by key."""
        return {
            key: text
            for key, text in sorted(self.request.documents.root.items())
            if key.startswith(directory) and Workspace.kind_at(key) == kind
        }


def read_run_workspace(paths: dict[str, Path], pipeline_directory: str) -> RunWorkspace:
    """Read the workspace request for running the pipeline at ``pipeline_directory``.

    ``paths`` is the engine's layout (``root``, ``manifest``, ``connections``,
    ``connectors``). The request holds the manifest, the pipeline's package,
    the package of every connection the pipeline names and of every connector
    those connections name -- only those, so an unrelated broken package never
    blocks a run.
    """
    root = paths["root"]
    if _package_directory(pipeline_directory, PipelinePackage) is None:
        raise WorkspaceLayoutError(
            f"{pipeline_directory!r} is not a pipeline package directory the "
            f"workspace locates"
        )
    documents = {_key(root, paths["manifest"]): read_config_text(paths["manifest"])}
    documents |= _read_package(root, pipeline_directory, PipelinePackage)

    connection_directories = _directories(
        root,
        paths["connections"],
        ConnectionPackage,
        _pipeline_connection_ids(
            documents.get(pipeline_directory + PipelinePackage.ROOT)
        ),
    )
    for directory in connection_directories.values():
        documents |= _read_package(root, directory, ConnectionPackage)

    connector_directories = _directories(
        root,
        paths["connectors"],
        ConnectorPackage,
        [
            _connector_id(documents.get(directory + ConnectionPackage.ROOT))
            for directory in connection_directories.values()
        ],
    )
    for directory in connector_directories.values():
        documents |= _read_package(root, directory, ConnectorPackage)

    return RunWorkspace(
        request=ValidateWorkspaceRequest.model_validate(
            {"documents": documents, "run_pipeline": pipeline_directory}
        ),
        pipeline_directory=pipeline_directory,
        connection_directories=connection_directories,
        connector_directories=connector_directories,
    )


def gate_run(workspace: RunWorkspace) -> None:
    """Refuse the run unless the validator's verdict on its workspace passed.

    The findings that cost no pass are logged, never dropped.
    """
    verdict = validate_workspace(workspace.request)
    # finding_costs_a_pass is typed over a plain dict, not the validator's own
    # Finding TypedDict, so each finding is passed as one.
    costs_a_pass = [finding_costs_a_pass(dict(f)) for f in verdict["findings"]]
    for finding, costs in zip(verdict["findings"], costs_a_pass):
        if not costs:
            logger.warning(
                "Workspace %s: %s", workspace.pipeline_directory, _describe(finding)
            )
    if not verdict["passed"]:
        raise WorkspaceRejectedError(
            [f for f, costs in zip(verdict["findings"], costs_a_pass) if costs]
        )
    logger.info("Workspace %s passed validation", workspace.pipeline_directory)


# ---------------------------------------------------------------------------
# Reading packages
# ---------------------------------------------------------------------------


def _key(root: Path, path: Path) -> str:
    return path.relative_to(root).as_posix()


def _package_directory(directory: str, model: type[DocumentPackage]) -> str | None:
    """``directory`` when the workspace locates a ``model`` package there."""
    located = Workspace.package_at(directory + model.ROOT)
    if located is None or located[0] != directory or located[1] is not model:
        return None
    return directory


def _directories(
    root: Path, family: Path, model: type[DocumentPackage], ids: list[str | None]
) -> dict[str, str]:
    """Map each id the workspace can locate to its package directory.

    An id that names no package directory (not a path segment, say) is left
    out: the document naming it is in the request, so the verdict reports the
    reference as unresolved.
    """
    directories: dict[str, str] = {}
    for package_id in ids:
        if package_id is None or package_id in directories:
            continue
        # Joined as text, never as a path: the id is not trusted to be one
        # segment until the workspace's own directory pattern has matched it.
        directory = _package_directory(f"{_key(root, family)}/{package_id}/", model)
        if directory is not None:
            directories[package_id] = directory
    return directories


#: How far into a key RE2's bounds are exact; past it they only stay bounds.
_MAX_KEY_LENGTH = 256


@functools.cache
def _document_key_ranges(
    model: type[DocumentPackage],
) -> tuple[tuple[bytes, bytes], ...]:
    """Bounds on the keys each of ``model``'s readable locations can match.

    RE2 computes them from the contract's own patterns, so which directories
    can hold a document is never a second, hand-kept table. A bound may admit
    a key its pattern refuses but never excludes one it accepts, which is all
    pruning needs.
    """
    return tuple(
        re2.compile(pattern).possiblematchrange(_MAX_KEY_LENGTH)
        for pattern in model.LOCATIONS.keys() - model.SECRET_LOCATIONS
    )


def _may_hold_document(model: type[DocumentPackage], directory_key: str) -> bool:
    """Whether a key under ``directory_key`` (ending in ``/``) may be located.

    Only prunes: the bound admits directories no location reaches, so it
    never decides a refusal.
    """
    # The name's bytes on disk, which os.walk decoded with surrogateescape.
    prefix = os.fsencode(directory_key)
    return any(
        low[: len(prefix)] <= prefix <= high
        for low, high in _document_key_ranges(model)
    )


def _is_utf8(name: str) -> bool:
    try:
        name.encode()
    except UnicodeEncodeError:
        return False
    return True


def _printable(name: str) -> str:
    """``name`` with the bytes ``os.walk`` could not decode shown as escapes."""
    return os.fsencode(name).decode(errors="backslashreplace")


def _read_package(
    root: Path, directory: str, model: type[DocumentPackage]
) -> dict[str, str]:
    """Every document at one of ``model``'s locations under ``directory``, by key.

    An absent directory reads as no documents; the reference to it is the
    verdict's to report. A file is a document when ``model`` locates its key
    at a readable location, and only a document can refuse the run: when it
    is a link, sits behind a linked directory, or has a name that is not
    UTF-8. The walk lists what is behind a linked directory to find out, but
    never reads through one; a link back into its own walk adds no directory
    and is not followed.
    """
    package_root = root / directory
    if not package_root.is_dir():
        return {}
    documents: dict[str, str] = {}
    # Per walked directory: the linked directory it is reached through, and
    # the real directories on its way down, which a followed link must avoid.
    linked_through: dict[str, str | None] = {str(package_root): None}
    walked_real: dict[str, frozenset[Path]] = {
        str(package_root): frozenset({package_root.resolve()})
    }
    for current, directories, names in os.walk(package_root, followlinks=True):
        walked = Path(current).relative_to(package_root)
        prefix = "" if walked == Path(".") else f"{walked.as_posix()}/"
        link = linked_through.pop(current)
        way_down = walked_real.pop(current)
        followed = []
        for name in directories:
            path = Path(current) / name
            real = path.resolve()
            if real in way_down or not _may_hold_document(model, f"{prefix}{name}/"):
                continue
            followed.append(name)
            linked_through[str(path)] = link or (
                f"{prefix}{name}" if path.is_symlink() else None
            )
            walked_real[str(path)] = way_down | {real}
        directories[:] = followed
        for name in names:
            key = f"{prefix}{name}"
            if model.kind_at(key) is None or model.secret_at(key):
                continue
            if link is not None:
                raise WorkspaceLayoutError(
                    f"{_printable(directory + link)} is a link, and "
                    f"{_printable(directory + key)} sits behind it; a package "
                    f"document must be a file inside its package"
                )
            if (Path(current) / name).is_symlink():
                raise WorkspaceLayoutError(
                    f"{_printable(directory + key)} is a link; a package "
                    f"document must be a file inside its package"
                )
            if not _is_utf8(key):
                raise WorkspaceLayoutError(
                    f"{_printable(directory + key)} is not a UTF-8 name; a "
                    f"package document's key must be text"
                )
            documents[directory + key] = read_config_text(Path(current) / name)
    return documents


# ---------------------------------------------------------------------------
# Following references before the gate
#
# Which packages the request holds is read from documents the verdict has not
# graded yet. A document whose reference cannot be read here is still in the
# request, and the verdict fails it for the same shape defect, so reading no
# reference from it never lets a run through.
# ---------------------------------------------------------------------------


def _parsed(text: str | None) -> Any:
    if text is None:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _pipeline_connection_ids(text: str | None) -> list[str | None]:
    document = _parsed(text)
    connections = document.get("connections") if isinstance(document, dict) else None
    if not isinstance(connections, dict):
        return []
    destinations = connections.get("destinations")
    ids = [connections.get("source")]
    ids.extend(destinations if isinstance(destinations, list) else [])
    return [i if isinstance(i, str) else None for i in ids]


def _connector_id(text: str | None) -> str | None:
    document = _parsed(text)
    connector_id = document.get("connector_id") if isinstance(document, dict) else None
    return connector_id if isinstance(connector_id, str) else None
