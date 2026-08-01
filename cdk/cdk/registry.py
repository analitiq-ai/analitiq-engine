"""Connector registry — two-level resolution: kind -> connector_id -> class.

The engine and the cloud control-plane both need to turn a connection's
connector reference into a concrete class. Resolution happens in two steps
(ADR §7, "Resolve by kind, then connector_id"):

1. ``kind`` (the keys of :data:`KIND_DEFAULTS`) selects the family and
   provides the **generic fallback** class for connectors that ship no code
   of their own (the thin path).
2. ``connector_id`` (``postgres``, ``mysql``, ``xero``, ...) selects the
   concrete class when the connector package ships one (the thick path).
   Per-system quirks live in that class — the generic fallback never
   branches on ``connector_id``.

The registry is populated two ways:

* **Kind defaults** — the CDK's own generic classes, named once in
  :data:`KIND_DEFAULTS` and registered by :func:`build_registries`. This is
  the always-available path; it does not depend on package metadata, so it
  works for an in-tree / editable install and under pytest.
* **Connector packages** — pip-installed connector packages advertise their
  class under a setuptools entry-point group whose entry **name is the
  connector_id** (``discover_entry_points``). Best-effort: a
  missing/unimportable entry point is logged and skipped, never fatal, so one
  broken connector cannot take the engine down at startup.

Source and destination connectors are tracked in **separate** registries
(separate entry-point groups) because a connector may serve one role or
both. The two populations state their roles differently, and deliberately:
a **connector package** states them through the two entry-point groups (the
package's answer to give), while a **kind default** states them through the
capability Protocols its class implements (``cdk.contract``), because the
CDK owns that class and reading the answer off it is what stops a
hand-written table from disagreeing with the code.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from importlib import import_module, metadata

from ._extras import reraise_for_missing_extra
from .contract import Readable, Writable

logger = logging.getLogger(__name__)

# Entry-point groups. A connector package advertises its class under these;
# the entry-point name is the connector_id.
SOURCE_GROUP = "analitiq.source_connectors"
DESTINATION_GROUP = "analitiq.destination_connectors"


@dataclass(frozen=True)
class KindDefault:
    """The CDK's generic connector for one kind, and what importing it needs.

    ``class_path`` is a ``module:ClassName`` string rather than a class:
    importing this module must not import a transport. ``cdk.registry`` is
    imported by ``cdk.conformance.target`` for the entry-point group
    constants, under an extra that ships no aiohttp — a table of class
    objects would make every consumer of the registry pay for every
    transport the CDK can speak.

    ``extra`` / ``modules`` are what ``cdk._extras`` needs to tell a
    genuinely-absent extra apart from a broken install, so a missing
    transport names the extra to install for every kind, not just for api.

    ``roles`` is declared rather than read off the class, because reading it
    means importing the class, and importing the class is the cost this
    whole table exists to defer. The declaration cannot drift from the code:
    :func:`load_kind_default` checks it against the class's own
    ``Readable`` / ``Writable`` Protocols the moment the class is loaded, so
    a table that says something the class does not fails there.
    """

    class_path: str
    extra: str
    modules: tuple[str, ...]
    roles: tuple[str, ...]


#: kind -> the CDK's generic connector. The single authority for what serves
#: a kind: the engine registry seeds from it and the conformance kit resolves
#: through it, so what the suite audits is what production loads.
KIND_DEFAULTS: dict[str, KindDefault] = {
    "database": KindDefault(
        "cdk.sql.generic:GenericSQLConnector",
        "arrow",
        ("pyarrow",),
        ("source", "destination"),
    ),
    "api": KindDefault(
        "cdk.api.generic:GenericAPIConnector",
        "api",
        ("aiohttp", "aiohttp_retry", "orjson", "pyarrow"),
        ("source", "destination"),
    ),
    "file": KindDefault(
        "cdk.file.generic:GenericFileConnector",
        "file",
        ("aiofiles", "pyarrow"),
        ("destination",),
    ),
    "s3": KindDefault(
        "cdk.file.generic:GenericFileConnector",
        "file",
        ("aiofiles", "pyarrow"),
        ("destination",),
    ),
    "stdout": KindDefault(
        "cdk.stdout.generic:GenericStdoutConnector",
        "arrow",
        ("pyarrow",),
        ("destination",),
    ),
}


class ConnectorClassError(ImportError):
    """A ``module:ClassName`` reference does not name a class.

    Subclasses ``ImportError`` because that is what failing to load a class
    is; the distinct type lets the conformance kit re-label it as a setup
    error without catching every import failure in the process.
    """


class UnknownConnectorKindError(KeyError):
    """The CDK ships no generic connector for a kind."""

    def __init__(self, kind: str, known: list[str]) -> None:
        self.kind = kind
        super().__init__(
            f"the CDK ships no generic connector for kind {kind!r}; "
            f"kinds with a default: {', '.join(known) or '(none)'}"
        )


def load_class(class_path: str) -> type:
    """Import the class named by a ``module:ClassName`` reference.

    The grammar is defined once here because two callers resolve it — the
    kind-default table and the conformance kit's ``--connector-class``
    override — and two parsers for one grammar is where they diverge.
    ``ImportError`` from the module import propagates untouched: only the
    caller knows whether a missing module means "install this extra" or
    "your class reference is wrong".
    """
    module_name, sep, attr = class_path.partition(":")
    if not sep or not module_name or not attr:
        raise ConnectorClassError(
            f"connector class reference {class_path!r} must be "
            f"'package.module:ClassName'"
        )
    module = import_module(module_name)
    try:
        cls = getattr(module, attr)
    except AttributeError as err:
        raise ConnectorClassError(
            f"module {module_name!r} has no attribute {attr!r} "
            f"(from connector class reference {class_path!r})"
        ) from err
    if not isinstance(cls, type):
        raise ConnectorClassError(
            f"connector class reference {class_path!r} resolves to "
            f"{cls!r}, which is not a class"
        )
    return cls


def load_kind_default(kind: str) -> type:
    """Import and return the CDK's generic connector for *kind*.

    A kind with no default fails naming every kind that has one. A default
    whose transport is not installed fails naming the extra — the answer
    ``cdk.api`` hand-wrote for one kind, now given uniformly, so no kind
    gets a worse error than another for the same intent.
    """
    entry = KIND_DEFAULTS.get(kind.lower())
    if entry is None:
        raise UnknownConnectorKindError(kind, sorted(KIND_DEFAULTS))
    try:
        cls = load_class(entry.class_path)
    except ConnectorClassError:
        raise
    except ImportError as exc:
        reraise_for_missing_extra(
            exc,
            feature=f"the {kind!r} kind default ({entry.class_path})",
            extra=entry.extra,
            modules=entry.modules,
        )
    _check_declared_roles(kind, entry, cls)
    return cls


def _check_declared_roles(kind: str, entry: KindDefault, cls: type) -> None:
    """Refuse a table entry the class does not back up.

    The roles are declared so registration costs no import, and this is
    what keeps the declaration honest: the first time the class is actually
    loaded, what it claims to serve is checked against what it implements.
    A kind declared for a role whose Protocol the class does not satisfy
    would otherwise hand that role a connector with no code path for it.
    """
    implemented = {
        role
        for role, protocol in (("source", Readable), ("destination", Writable))
        if issubclass(cls, protocol)
    }
    declared = set(entry.roles)
    if not declared:
        raise ConnectorClassError(
            f"the kind default for {kind!r} declares no roles, so no "
            f"registry can serve it"
        )
    unbacked = declared - implemented
    if unbacked:
        raise ConnectorClassError(
            f"the kind default for {kind!r} ({entry.class_path}) is declared "
            f"for {sorted(unbacked)} but implements "
            f"{sorted(implemented) or 'neither Readable nor Writable'}"
        )


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
        available_kinds: list[str],
        available_connector_ids: list[str],
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
        self._defaults: dict[str, type] = {}
        #: Kinds this role serves per :data:`KIND_DEFAULTS`, whose class is
        #: loaded on first resolve. Declaring one costs no import, which is
        #: the point: a worker that speaks one transport must not have to
        #: install every other transport the CDK can speak.
        self._declared_kinds: set[str] = set()
        self._specific: dict[str, type] = {}

    @property
    def role(self) -> str:
        return self._role

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register_default(self, kind: str, cls: type, *, override: bool = False) -> None:
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

    def declare_default(self, kind: str) -> None:
        """Note that *kind*'s CDK default serves this role, without loading it.

        The class is imported by the first :meth:`resolve` that needs it, so
        building a registry pulls in no transport at all. A default already
        registered eagerly wins and this is a no-op -- the eager one is a
        deliberate override.
        """
        self._declared_kinds.add(kind.lower())

    def register(self, connector_id: str, cls: type, *, override: bool = False) -> None:
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

    def resolve(self, kind: str, connector_id: str) -> type:
        """Resolve the class for (*kind*, *connector_id*).

        The connector's own class wins when its package is installed;
        otherwise the generic default for the kind serves the connector (the
        thin path). Raises :class:`ConnectorNotRegisteredError` when both
        lookups miss.
        """
        cls = self._specific.get(connector_id.lower())
        if cls is not None:
            return cls
        key = kind.lower()
        default = self._defaults.get(key)
        if default is not None:
            return default
        if key in self._declared_kinds:
            # First use of this kind in this process: import it now. A
            # missing transport raises here naming the extra, which is the
            # honest moment -- the run that needs it is the one that pays.
            loaded = load_kind_default(key)
            self._defaults[key] = loaded
            return loaded
        raise ConnectorNotRegisteredError(
            kind,
            connector_id,
            role=self._role,
            available_kinds=self.kinds(),
            available_connector_ids=self.connector_ids(),
        )

    def create(self, kind: str, connector_id: str) -> object:
        """Instantiate the connector for (*kind*, *connector_id*)."""
        return self.resolve(kind, connector_id)()

    def kinds(self) -> list[str]:
        """Kinds this registry can serve a generic default for.

        Declared kinds count: whether the class has been imported yet is an
        implementation detail, and a caller asking what the registry serves
        would otherwise get a different answer before and after a resolve.
        """
        return sorted(set(self._defaults) | self._declared_kinds)

    def connector_ids(self) -> list[str]:
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


def _entry_points(group: str) -> tuple[metadata.EntryPoint, ...]:
    """Return the entry points for *group* across importlib.metadata versions."""
    eps = metadata.entry_points()
    # Python 3.10+ returns a SelectableGroups supporting select(group=...);
    # older returns a dict[str, list]. Handle both without a version check.
    select = getattr(eps, "select", None)
    if select is not None:
        return tuple(select(group=group))
    return tuple(eps.get(group, ()))


def build_registries(
    *, discover: bool = True
) -> tuple[ConnectorRegistry, ConnectorRegistry]:
    """Build the (source, destination) registries.

    Every kind default is the CDK's own generic class, seeded from one
    table. Two hand-written maps that had to agree by hand were the place
    the two directions drifted apart (issue #431), and a table that seeded
    both registries blindly would hand a file *source* a class with no read
    path — a default that hides a wiring defect. One table with the roles on
    it answers both.

    Building imports nothing. The kinds are declared and each class loads on
    the first resolve that needs it, because the extras are split by family:
    an install carrying only ``[api]`` would otherwise fail here on the file
    default's ``aiofiles``, for a run that never touches file or s3. What
    keeps the declaration honest is :func:`load_kind_default`, which checks
    each class against the roles claimed for it as it loads.

    Connector packages are then discovered from entry points (additive,
    best-effort) when *discover* is set.
    """
    source = ConnectorRegistry("source")
    destination = ConnectorRegistry("destination")
    for kind, entry in KIND_DEFAULTS.items():
        if "source" in entry.roles:
            source.declare_default(kind)
        if "destination" in entry.roles:
            destination.declare_default(kind)
    if discover:
        source.discover_entry_points(SOURCE_GROUP)
        destination.discover_entry_points(DESTINATION_GROUP)
    return source, destination
