"""Connector registry — two-level resolution: kind -> connector_id -> class.

The engine and the cloud control-plane both need to turn a connection's
connector reference into a concrete class. Resolution happens in two steps
(ADR §7, "Resolve by kind, then connector_id"):

1. ``kind`` (``database`` / ``api`` / ``file`` / ``stdout``) selects the
   family and provides the **generic fallback** class for connectors that
   ship no code of their own (the thin path).
2. ``connector_id`` (``postgres``, ``mysql``, ``xero``, ...) selects the
   concrete class when the connector package ships one (the thick path).
   Per-system quirks live in that class — the generic fallback never
   branches on ``connector_id``.

The registry is populated two ways:

* **Kind defaults** — built-in generic classes baked into the image are
  seeded by the host at startup (``register_default``). This is the
  always-available path; it does not depend on package metadata, so it works
  for an in-tree / editable install and under pytest.
* **Connector packages** — pip-installed connector packages advertise their
  class under a setuptools entry-point group whose entry **name is the
  connector_id** (``discover_entry_points``). Best-effort: a
  missing/unimportable entry point is logged and skipped, never fatal, so one
  broken connector cannot take the engine down at startup.

Source and destination connectors are tracked in **separate** registries
(separate entry-point groups) because for some kinds the source and
destination are different classes (``api`` -> APIConnector vs
ApiDestinationHandler) while a unified SQL connector serves both roles for
``database``.
"""

from __future__ import annotations

import logging
from importlib import metadata
from typing import Dict, List, Optional, Tuple, Type

logger = logging.getLogger(__name__)

# Entry-point groups. A connector package advertises its class under these;
# the entry-point name is the connector_id.
SOURCE_GROUP = "analitiq.source_connectors"
DESTINATION_GROUP = "analitiq.destination_connectors"


class ConnectorNotRegisteredError(KeyError):
    """No class is registered for a requested (kind, connector_id) pair.

    Raised only when *both* lookups miss: no connector package registered the
    ``connector_id`` and no generic default is registered for the ``kind``.

    Subclasses ``KeyError`` (a lookup miss is a missing key). Callers that wrap
    a registry lookup in a broad ``except (KeyError, ...)`` would therefore
    swallow it — keep registry lookups out of such handlers so an unresolvable
    connector surfaces loudly.
    """

    def __init__(
        self,
        kind: str,
        connector_id: str,
        *,
        role: str,
        available_kinds: List[str],
        available_connector_ids: List[str],
    ) -> None:
        self.kind = kind
        self.connector_id = connector_id
        self.role = role
        super().__init__(
            f"no {role} connector registered for connector_id "
            f"{connector_id!r} and no generic default for kind {kind!r}; "
            f"registered connector_ids: "
            f"{', '.join(available_connector_ids) or '(none)'}; "
            f"kind defaults: {', '.join(available_kinds) or '(none)'}"
        )


class ConnectorRegistry:
    """Two-level (kind default + connector_id specific) map for one role."""

    def __init__(self, role: str) -> None:
        self._role = role
        self._defaults: Dict[str, Type] = {}
        self._specific: Dict[str, Type] = {}

    @property
    def role(self) -> str:
        return self._role

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register_default(self, kind: str, cls: Type, *, override: bool = False) -> None:
        """Register the generic fallback class for *kind*.

        Raises ``ValueError`` on a duplicate kind unless ``override`` is set,
        so a silent shadow (two classes claiming the ``database`` default)
        fails loudly.
        """
        key = kind.lower()
        existing = self._defaults.get(key)
        if existing is not None and existing is not cls and not override:
            raise ValueError(
                f"{self._role} kind default {kind!r} already registered to "
                f"{existing.__name__}; refusing to shadow with {cls.__name__} "
                f"(pass override=True to replace)"
            )
        self._defaults[key] = cls

    def register(self, connector_id: str, cls: Type, *, override: bool = False) -> None:
        """Register *cls* as the concrete class for *connector_id*.

        Raises ``ValueError`` on a duplicate connector_id unless ``override``
        is set, so two packages claiming ``postgres`` fail loudly instead of
        one silently shadowing the other.
        """
        key = connector_id.lower()
        existing = self._specific.get(key)
        if existing is not None and existing is not cls and not override:
            raise ValueError(
                f"{self._role} connector_id {connector_id!r} already registered "
                f"to {existing.__name__}; refusing to shadow with "
                f"{cls.__name__} (pass override=True to replace)"
            )
        self._specific[key] = cls

    # ------------------------------------------------------------------
    # Resolution
    # ------------------------------------------------------------------

    def resolve(self, kind: str, connector_id: str) -> Type:
        """Resolve the class for (*kind*, *connector_id*).

        The connector's own class wins when its package is installed;
        otherwise the generic default for the kind serves the connector (the
        thin path). Raises :class:`ConnectorNotRegisteredError` when both
        lookups miss.
        """
        cls = self._specific.get(connector_id.lower())
        if cls is not None:
            return cls
        default = self._defaults.get(kind.lower())
        if default is not None:
            return default
        raise ConnectorNotRegisteredError(
            kind,
            connector_id,
            role=self._role,
            available_kinds=self.kinds(),
            available_connector_ids=self.connector_ids(),
        )

    def create(self, kind: str, connector_id: str):
        """Instantiate the connector for (*kind*, *connector_id*)."""
        return self.resolve(kind, connector_id)()

    def kinds(self) -> List[str]:
        """Kinds with a registered generic default."""
        return sorted(self._defaults)

    def connector_ids(self) -> List[str]:
        """connector_ids with a registered concrete class."""
        return sorted(self._specific)

    # ------------------------------------------------------------------
    # Entry-point discovery
    # ------------------------------------------------------------------

    def discover_entry_points(self, group: str) -> None:
        """Register every entry point in *group* (best-effort).

        Each entry point's name is the **connector_id**; its value loads the
        class. An entry point that fails to import is logged and skipped — one
        broken connector package must not abort startup.
        """
        for entry in _entry_points(group):
            try:
                cls = entry.load()
            except Exception:  # noqa: BLE001 - a bad package must not be fatal
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
                    "skipping %s connector entry point %r: connector_id "
                    "already registered",
                    self._role,
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

    Built-ins are the **kind defaults** (always available), registered first;
    connector packages are then discovered from entry points (additive,
    best-effort) when *discover* is set.
    """
    source = ConnectorRegistry("source")
    destination = ConnectorRegistry("destination")
    for kind, cls in (source_builtins or {}).items():
        source.register_default(kind, cls)
    for kind, cls in (destination_builtins or {}).items():
        destination.register_default(kind, cls)
    if discover:
        source.discover_entry_points(SOURCE_GROUP)
        destination.discover_entry_points(DESTINATION_GROUP)
    return source, destination
