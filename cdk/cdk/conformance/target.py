"""Load the connector under test: definition, type maps, classes, dialect.

A conformance run points at one connector package checkout (the registry
repo layout: the repo root is the package, ``definition/`` holds
``connector.json`` and the type maps). Loading is fail-loud: a missing
definition, a malformed ``sql_capabilities`` block, or an unloadable
class is a :class:`ConformanceSetupError` naming the file or entry point
to fix — the suite never runs against a half-loaded target.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from functools import cached_property
from importlib import import_module, metadata
from pathlib import Path
from typing import Any

from cdk.registry import DESTINATION_GROUP, SOURCE_GROUP
from cdk.sql.capabilities import (
    SqlCapabilities,
    SqlCapabilitiesError,
    parse_declared_capabilities,
)
from cdk.sql.dialects import SqlDialect
from cdk.sql.generic import GenericSQLConnector
from cdk.type_map.exceptions import InvalidTypeMapError
from cdk.type_map.loader import build_type_mapper, read_raw_type_maps
from cdk.type_map.mapper import TypeMapper

CONNECTOR_DEFINITION_FILENAME = "connector.json"


class ConformanceSetupError(Exception):
    """The suite cannot load the connector under test.

    Not a conformance finding: the target itself (its path, definition
    file, or class reference) is unusable, so no check can run. The
    message names what to fix.
    """


@dataclass(frozen=True)
class ConformanceTarget:
    """Everything the suite knows about the connector under test.

    ``connector_class`` is the class the registry would resolve: the
    package's own entry-point class when one is installed, else the
    CDK's generic fallback for ``kind: database`` (the thin path), else
    ``None`` for kinds whose generic classes live outside the CDK.
    """

    root: Path
    definition_dir: Path
    definition: dict[str, Any]
    connector_id: str
    kind: str
    declared_capabilities: SqlCapabilities | None
    type_mapper: TypeMapper | None
    connector_class: type | None

    def declared_transports(self) -> dict[str, dict[str, Any]]:
        """Return transport blocks with ``transport_defaults`` merged.

        The engine merges defaults before resolving a transport, so every
        kit site reading ``transport_type``/``driver`` consumes this view;
        reading raw blocks would false-fail a connector supplying those
        fields through defaults. Shallow merge: the fields the kit reads
        are top-level scalars.
        """
        transports = self.definition.get("transports") or {}
        defaults = self.definition.get("transport_defaults") or {}
        merged: dict[str, dict[str, Any]] = {}
        for ref, block in transports.items():
            if not isinstance(block, dict):
                continue
            if isinstance(defaults, dict) and defaults:
                merged[ref] = {**defaults, **block}
            else:
                merged[ref] = dict(block)
        return merged

    @property
    def has_write_map(self) -> bool:
        """Whether the connector ships ``type-map-write.json``."""
        return self.type_mapper is not None and self.type_mapper.has_write_map

    @property
    def is_database(self) -> bool:
        """Whether the connector targets a SQL database."""
        return self.kind == "database"

    @property
    def write_role(self) -> bool:
        """Whether the write-path checks apply to this connector.

        The write-direction type vocabulary lives entirely in
        ``type-map-write.json``, so shipping one is the connector's own
        statement that it writes; source-only connectors ship none and
        the write-path checks skip.
        """
        return self.is_database and self.has_write_map

    @cached_property
    def dialect(self) -> SqlDialect | None:
        """A fresh dialect instance from the connector class, capabilities bound.

        ``None`` when no class resolved or the class carries no dialect.
        The declared capabilities are attached exactly as the facade
        attaches them on connect, so every gate the dialect owns (the
        catalog door) sees the connector's real declaration.
        """
        cls = self.connector_class
        if cls is None:
            return None
        dialect_class = getattr(cls, "dialect_class", None)
        if dialect_class is None:
            return None
        if not (
            isinstance(dialect_class, type) and issubclass(dialect_class, SqlDialect)
        ):
            raise ConformanceSetupError(
                f"{cls.__name__}.dialect_class is {dialect_class!r}, not a "
                f"SqlDialect subclass"
            )
        dialect = dialect_class()
        dialect.capabilities = self.declared_capabilities
        return dialect


def _load_definition(definition_dir: Path) -> dict[str, Any]:
    """Read and parse ``connector.json`` from *definition_dir*."""
    path = definition_dir / CONNECTOR_DEFINITION_FILENAME
    try:
        raw = path.read_text()
    except OSError as err:
        raise ConformanceSetupError(
            f"cannot read connector definition {path}: {err}"
        ) from err
    try:
        document = json.loads(raw)
    except json.JSONDecodeError as err:
        raise ConformanceSetupError(
            f"connector definition {path} is not valid JSON: {err}"
        ) from err
    if not isinstance(document, dict):
        raise ConformanceSetupError(
            f"connector definition {path} must be a JSON object, got "
            f"{type(document).__name__}"
        )
    return document


def _resolve_definition_dir(root: Path) -> Path:
    """Locate the definition directory under *root*.

    The registry layout keeps it at ``<root>/definition``; a bare
    checkout with ``connector.json`` at the root is accepted too.
    """
    for candidate in (root / "definition", root):
        if (candidate / CONNECTOR_DEFINITION_FILENAME).is_file():
            return candidate
    raise ConformanceSetupError(
        f"no {CONNECTOR_DEFINITION_FILENAME} under {root} (looked in "
        f"{root / 'definition'} and {root}); pass --connector-dir pointing "
        f"at the connector package checkout"
    )


def _load_class(class_path: str) -> type:
    """Import a ``module:Class`` reference."""
    module_name, sep, attr = class_path.partition(":")
    if not sep or not module_name or not attr:
        raise ConformanceSetupError(
            f"connector class reference {class_path!r} must be "
            f"'package.module:ClassName'"
        )
    try:
        module = import_module(module_name)
    except ImportError as err:
        raise ConformanceSetupError(
            f"cannot import {module_name!r} for connector class "
            f"{class_path!r}: {err}"
        ) from err
    try:
        cls = getattr(module, attr)
    except AttributeError as err:
        raise ConformanceSetupError(
            f"module {module_name!r} has no attribute {attr!r} "
            f"(from connector class reference {class_path!r})"
        ) from err
    if not isinstance(cls, type):
        raise ConformanceSetupError(
            f"connector class reference {class_path!r} resolves to "
            f"{cls!r}, which is not a class"
        )
    return cls


def _entry_point_class(group: str, connector_id: str) -> type | None:
    """Load the installed entry point named *connector_id* in *group*.

    Unlike the engine registry's best-effort discovery, a matching entry
    point that fails to load is a hard error here: the suite exists to
    surface exactly that defect in the connector's own CI.
    """
    matches = [
        entry
        for entry in metadata.entry_points(group=group)
        if entry.name == connector_id
    ]
    if not matches:
        return None
    if len(matches) > 1:
        dists = sorted(
            str(getattr(entry.dist, "name", "<unknown>")) for entry in matches
        )
        raise ConformanceSetupError(
            f"{len(matches)} installed packages register the {group!r} entry "
            f"point {connector_id!r} ({', '.join(dists)}); exactly one "
            f"connector package may claim a connector_id"
        )
    try:
        cls = matches[0].load()
    except Exception as err:
        raise ConformanceSetupError(
            f"entry point {connector_id!r} in group {group!r} failed to " f"load: {err}"
        ) from err
    if not isinstance(cls, type):
        raise ConformanceSetupError(
            f"entry point {connector_id!r} in group {group!r} resolves to "
            f"{cls!r}, which is not a class"
        )
    return cls


def _resolve_connector_class(
    connector_id: str, kind: str, class_path: str | None
) -> type | None:
    """Resolve the class the way the engine registry would.

    Explicit ``class_path`` wins (for running the suite before the
    package is installed); then the installed entry points; then the
    CDK's generic database fallback. Non-database kinds without an
    entry point resolve to ``None``: their generic classes live in the
    engine, and the class-level checks skip.

    Both entry-point groups are always loaded: the engine keeps
    separate source and destination registries, and a
    registered-but-broken source entry point would otherwise fall back
    silently to the generic class in production while the suite went
    green against the destination class. For ``kind: database`` the two
    groups must agree — one unified SQL class serves both roles, and a
    split would let them drift. Other kinds may legitimately register
    different classes per role (the engine's api family does); the
    destination class is used there, matching the write-focused checks.
    """
    if class_path:
        return _load_class(class_path)
    loaded: dict[str, type] = {}
    for group in (DESTINATION_GROUP, SOURCE_GROUP):
        cls = _entry_point_class(group, connector_id)
        if cls is not None:
            loaded[group] = cls
    if loaded:
        classes = set(loaded.values())
        if len(classes) > 1 and kind == "database":
            names = {group: cls.__name__ for group, cls in loaded.items()}
            raise ConformanceSetupError(
                f"database connector {connector_id!r} registers different "
                f"classes per entry-point group ({names}); one unified SQL "
                f"class serves both roles"
            )
        return loaded.get(DESTINATION_GROUP) or next(iter(loaded.values()))
    if kind == "database":
        return GenericSQLConnector
    return None


def _load_type_mapper(definition_dir: Path, connector_id: str) -> TypeMapper | None:
    """Build the connector's type mapper from its definition files."""
    try:
        raw = read_raw_type_maps(definition_dir, f"connector {connector_id!r}")
    except InvalidTypeMapError as err:
        raise ConformanceSetupError(str(err)) from err
    if raw is None:
        return None
    try:
        return build_type_mapper(
            f"connector {connector_id!r}",
            raw["rules"] or [],
            raw["write_rules"],
        )
    except InvalidTypeMapError as err:
        raise ConformanceSetupError(str(err)) from err


def load_target(
    root: Path | str, *, class_path: str | None = None
) -> ConformanceTarget:
    """Load the connector under test from its package checkout at *root*.

    ``class_path`` (``package.module:ClassName``) overrides entry-point
    resolution — the escape hatch for running the suite against a class
    that is importable but not yet installed as a package.
    """
    root = Path(root).resolve()
    if not root.is_dir():
        raise ConformanceSetupError(f"connector directory {root} does not exist")
    definition_dir = _resolve_definition_dir(root)
    definition = _load_definition(definition_dir)

    connector_id = definition.get("connector_id")
    if not isinstance(connector_id, str) or not connector_id:
        raise ConformanceSetupError(
            f"{definition_dir / CONNECTOR_DEFINITION_FILENAME} declares no "
            f"connector_id"
        )
    kind = definition.get("kind")
    if not isinstance(kind, str) or not kind:
        raise ConformanceSetupError(
            f"{definition_dir / CONNECTOR_DEFINITION_FILENAME} declares no kind"
        )

    try:
        capabilities = parse_declared_capabilities(
            definition.get("sql_capabilities"),
            source=str(definition_dir / CONNECTOR_DEFINITION_FILENAME),
        )
    except SqlCapabilitiesError as err:
        raise ConformanceSetupError(str(err)) from err

    return ConformanceTarget(
        root=root,
        definition_dir=definition_dir,
        definition=definition,
        connector_id=connector_id,
        kind=kind,
        declared_capabilities=capabilities,
        type_mapper=_load_type_mapper(definition_dir, connector_id),
        connector_class=_resolve_connector_class(connector_id, kind, class_path),
    )
