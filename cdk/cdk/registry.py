"""Connector registry — kind -> connector class, populated three ways.

The engine and the cloud control-plane both need to turn a connector ``kind``
(``database`` / ``api`` / ``file`` / ``stdout``) into a concrete class. This is
the vendor-neutral mechanism for that (ADR §7), keyed by ``kind``:

1. **Explicit registration** — built-in connectors baked into the image are
   seeded by the host at startup (``register``). This is the always-available
   path; it does not depend on package metadata, so it works for an in-tree /
   editable install and under pytest.
2. **Entry-point discovery** — externally pip-installed connector packages
   advertise themselves under a setuptools entry-point group
   (``discover_entry_points``). Best-effort: a missing/!importable entry point
   is logged and skipped, never fatal, so one broken plugin cannot take the
   engine down at startup.
3. (Mounted-dir scan for local dev is a later refinement — not implemented
   here; the two paths above cover the baked-in image and installed packages.)

Source and destination connectors are tracked in **separate** registries
(separate entry-point groups) because for some kinds the source and destination
are different classes (``api`` -> APIConnector vs ApiDestinationHandler) while a
unified SQL connector serves both roles for ``database``.
"""

from __future__ import annotations

import logging
from importlib import metadata
from typing import Dict, Iterable, List, Optional, Tuple, Type

logger = logging.getLogger(__name__)

# Entry-point groups. A connector package advertises its classes under these.
SOURCE_GROUP = "analitiq.source_connectors"
DESTINATION_GROUP = "analitiq.destination_connectors"


class ConnectorNotRegisteredError(KeyError):
    """No connector class is registered for a requested ``kind``.

    Subclasses ``KeyError`` (a lookup miss is a missing key). Callers that wrap
    a registry ``get``/``create`` in a broad ``except (KeyError, ...)`` would
    therefore swallow it — keep registry lookups out of such handlers so an
    unregistered kind surfaces loudly.
    """

    def __init__(self, kind: str, *, role: str, available: Iterable[str]) -> None:
        self.kind = kind
        self.role = role
        super().__init__(
            f"no {role} connector registered for kind {kind!r}; "
            f"available: {', '.join(sorted(available)) or '(none)'}"
        )


class ConnectorRegistry:
    """A ``kind`` -> connector-class map for one role (source or destination)."""

    def __init__(self, role: str) -> None:
        self._role = role
        self._classes: Dict[str, Type] = {}

    @property
    def role(self) -> str:
        return self._role

    def register(self, kind: str, cls: Type, *, override: bool = False) -> None:
        """Register *cls* for *kind*.

        Raises ``ValueError`` on a duplicate kind unless ``override`` is set, so
        a silent shadow (two packages claiming ``database``) fails loudly.
        """
        key = kind.lower()
        existing = self._classes.get(key)
        if existing is not None and existing is not cls and not override:
            raise ValueError(
                f"{self._role} kind {kind!r} already registered to "
                f"{existing.__name__}; refusing to shadow with {cls.__name__} "
                f"(pass override=True to replace)"
            )
        self._classes[key] = cls

    def get(self, kind: str) -> Type:
        cls = self._classes.get(kind.lower())
        if cls is None:
            raise ConnectorNotRegisteredError(
                kind, role=self._role, available=self._classes
            )
        return cls

    def create(self, kind: str):
        """Instantiate the connector for *kind* (no-arg constructor)."""
        return self.get(kind)()

    def kinds(self) -> List[str]:
        return sorted(self._classes)

    def discover_entry_points(self, group: str) -> None:
        """Register every entry point in *group* (best-effort).

        Each entry point's name is the ``kind``; its value loads the class. An
        entry point that fails to import is logged and skipped — one broken
        plugin must not abort startup.
        """
        for entry in _entry_points(group):
            try:
                cls = entry.load()
            except Exception:  # noqa: BLE001 - a bad plugin must not be fatal
                logger.warning(
                    "skipping %s connector entry point %r: failed to load",
                    self._role,
                    entry.name,
                    exc_info=True,
                )
                continue
            try:
                self.register(entry.name, cls)
            except ValueError:
                logger.warning(
                    "skipping %s connector entry point %r: %s already registered",
                    self._role,
                    entry.name,
                    entry.name,
                    exc_info=True,
                )


def _entry_points(group: str) -> Tuple[metadata.EntryPoint, ...]:
    """Return the entry points for *group* across importlib.metadata versions."""
    eps = metadata.entry_points()
    # Python 3.10+ returns a SelectableGroups supporting select(group=...);
    # older returns a dict[str, list]. Handle both without a version check.
    select = getattr(eps, "select", None)
    if select is not None:
        return tuple(select(group=group))
    return tuple(eps.get(group, ()))  # type: ignore[union-attr]


def build_registries(
    *,
    source_builtins: Optional[Dict[str, Type]] = None,
    destination_builtins: Optional[Dict[str, Type]] = None,
    discover: bool = True,
) -> Tuple[ConnectorRegistry, ConnectorRegistry]:
    """Build the (source, destination) registries.

    Built-ins are registered first (always available), then entry points are
    discovered (additive, best-effort) when *discover* is set.
    """
    source = ConnectorRegistry("source")
    destination = ConnectorRegistry("destination")
    for kind, cls in (source_builtins or {}).items():
        source.register(kind, cls)
    for kind, cls in (destination_builtins or {}).items():
        destination.register(kind, cls)
    if discover:
        source.discover_entry_points(SOURCE_GROUP)
        destination.discover_entry_points(DESTINATION_GROUP)
    return source, destination
