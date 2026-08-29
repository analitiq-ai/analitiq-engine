"""ConnectionRuntime — connector-driven connection materialization.

A :class:`ConnectionRuntime` ties together the validated connection document, the
connector definition that describes how to use it, and the secret store
that fills in credential values. It is the single place the engine touches
provider configuration: everything provider-specific is encoded in the
connector's ``transports`` block, resolved through the typed
:class:`~cdk.resolver.ResolutionContext`, and turned into a
concrete transport (:class:`~cdk.transport_factory.SqlAlchemyTransport`,
:class:`~cdk.transport_factory.AdbcTransport`, or
:class:`~cdk.transport_factory.HttpTransport`) by the transport
factory. The runtime never inspects host strings, header dicts, DSN
formats, or SSL flags directly.

Lifecycle:

* ``__init__`` records connection + connector + resolver references.
* ``materialize()`` resolves secrets, builds a context, materializes the
  transport, and scrubs secret values from memory.
* Reference counting (:meth:`acquire`, :meth:`close`) lets multiple
  source/destination connectors share one underlying engine/session.
* ``file``/``s3``/``stdout`` connectors expose a resolved-config dict via
  :attr:`resolved_config`; they have no shared transport to manage.
"""

from __future__ import annotations

import asyncio
import copy
import logging
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any, cast

from analitiq.contracts.connection import ConnectionInput
from analitiq.contracts.connector import Connector, DatabaseConnector
from pydantic import ValidationError
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine

if TYPE_CHECKING:
    import aiohttp

from cdk.derived_functions import DEFAULT_FUNCTIONS
from cdk.exceptions import TransportSpecError
from cdk.json_utils import authored_json
from cdk.rate_limiter import RateLimiter
from cdk.resolver import (
    REQUEST_CONNECTION_SUBTREES,
    RUNTIME_CONNECTION_ID,
    ResolutionContext,
    Resolver,
)
from cdk.secrets.exceptions import PlaceholderExpansionError, SecretNotFoundError
from cdk.secrets.protocol import SecretsResolver
from cdk.sql.exceptions import TlsVerificationError
from cdk.transport_factory import (
    HTTP_TRANSPORT_TYPE,
    AdbcTransport,
    HttpTransport,
    SqlAlchemyTransport,
    build_transport_from_spec,
    merged_transports,
    resolve_transport_specs,
)
from cdk.type_map import InvalidTypeMapError, TypeMapper, UnmappedTypeError
from cdk.types import EndpointScope

logger = logging.getLogger(__name__)

#: The connection-document fields transport materialization puts in scope --
#: the ONE statement of this fact. ``_build_resolution_context`` builds the
#: connection scope from these, and the conformance kit derives its
#: transport-phase deferral from them, so the kit cannot defer a field
#: (``connection.hostname``, say) that connect() will refuse to resolve.
#: Materialization is a superset of request time by construction: everything
#: a request may read, plus the secret pointers only the trusted side sees.
#: Derived, so a subtree added to the request scope cannot go missing here --
#: which would fail a transport expression at connect() that the identical
#: request expression resolves. Every name is a field of the connection
#: contract (``ConnectionInput``): the scope is read off the typed document,
#: so a name the contract does not declare cannot be listed here.
MATERIALIZATION_CONNECTION_SUBTREES = REQUEST_CONNECTION_SUBTREES + ("secret_refs",)

#: The non-connection scopes materialization also fills: the resolved secret
#: store (``_build_resolution_context``'s ``secrets=`` argument). Stated here
#: beside the builder so the kit's transport deferral derives from it instead
#: of restating it. The resolver's ``auth`` scope is contract-sanctioned but
#: the connection document carries no auth block, so nothing fills it here.
MATERIALIZATION_SECRET_SCOPES = ("secrets",)


def _derive_dialect(connector: Connector | None) -> str | None:
    """Return the base SQL dialect (e.g. ``postgresql``) from a definition.

    Returns ``None`` if it is not a database connector.
    Handles both ``sqlalchemy`` (``driver: 'postgresql+asyncpg'``) and
    ``adbc`` (``driver: 'snowflake'``) transports — both store the
    dialect/driver under ``transports[default].driver``; the SQLAlchemy
    flavour is composite (``base+async_driver``) so we split on ``+``.
    """
    if connector is None:
        return None
    transports = merged_transports(connector)
    default_ref = connector.default_transport
    if default_ref not in transports:
        return None
    transport = transports[default_ref]
    transport_type = transport.get("transport_type")
    if transport_type not in ("sqlalchemy", "adbc"):
        return None
    driver = transport.get("driver")
    if not isinstance(driver, str) or not driver:
        return None
    return driver.split("+", 1)[0]


class _PreResolvedSecretsResolver(SecretsResolver):
    """Placeholder resolver for worker-side runtimes built from a payload.

    The worker never touches the secret store — every value it needs arrived
    resolved in the launch bootstrap — so any resolution attempt is a contract
    violation and raises.
    """

    async def resolve(
        self, connection_id: str, secret_refs: Mapping[str, str]
    ) -> dict[str, str]:
        raise RuntimeError(
            "secret resolution attempted on a pre-resolved worker runtime; "
            "workers receive resolved values in the bootstrap and never "
            "access the secret store"
        )

    async def close(self) -> None:
        return None


class ConnectionRuntime:
    """Connector-driven connection lifecycle with shared ownership.

    Constructed by :class:`~src.engine.pipeline_config_prep.PipelineConfigPrep`
    with the validated connection document, the validated connector
    definition, and a per-connection secrets resolver. Both documents are
    the contract's own models: every connection or connector field this
    runtime reads is a typed read, which is what the contract-consumption
    census counts. ``materialize()`` is idempotent and safe to call from
    multiple consumers; the underlying transport is only disposed when the
    last reference is released.
    """

    def __init__(
        self,
        *,
        connection: ConnectionInput,
        connection_id: str,
        connector_id: str,
        connector_type: str,
        resolver: SecretsResolver,
        connector: Connector | None = None,
        driver: str | None = None,
        connector_type_mapper: TypeMapper | None = None,
        connection_type_mapper: TypeMapper | None = None,
    ) -> None:
        # Shape check only. The set of valid kinds is owned by the published
        # connector schema and by the worker registry (an unrunnable kind
        # raises ConnectorNotRegisteredError at resolution); pinning a
        # parallel frozen set here would block registry-discovered kinds.
        if not connector_type or not isinstance(connector_type, str):
            raise ValueError(
                f"connector_type must be a non-empty string, " f"got {connector_type!r}"
            )
        if not connector_id or not isinstance(connector_id, str):
            raise ValueError(
                f"connector_id must be a non-empty string, got {connector_id!r}"
            )

        self._connection = connection
        self._connection_id = connection_id
        self._connector_id = connector_id
        self._connector_type = connector_type
        self._connector = connector
        # The connector's declared ``sql_capabilities`` block (issue #390),
        # carried as the JSON its author wrote: the published contract
        # validates it engine-side, ``cdk.sql.capabilities`` parses that
        # grammar at consumption. Kept as data here so the core runtime
        # stays independent of the SQL surface (same reason ``materialize``
        # takes ``sql_dialect`` untyped). Only a database connector declares
        # it. Worker-side runtimes get it restored from the resolved payload
        # in :meth:`from_resolved_payload`.
        self._declared_sql_capabilities: dict[str, Any] | None = (
            authored_json(connector.sql_capabilities)
            if isinstance(connector, DatabaseConnector)
            else None
        )
        # Connector-level declared facts (issue #401), carried the same way:
        # ``error_map`` (the driver's failure taxonomy) and ``concurrency``
        # (the system's connection ceiling). ``cdk.declarations`` parses
        # them at consumption; absence is additive — no declared mapping /
        # no declared ceiling.
        self._declared_error_map: dict[str, Any] | None = (
            authored_json(connector.error_map) if connector is not None else None
        )
        self._declared_concurrency: dict[str, Any] | None = (
            authored_json(connector.concurrency) if connector is not None else None
        )
        self._driver_override = driver
        self._resolver = resolver
        self._connector_type_mapper = connector_type_mapper
        self._connection_type_mapper = connection_type_mapper
        self._composed_connection_mapper: TypeMapper | None = None

        # Worker-side pre-resolved payload (set by from_resolved_payload):
        # materialize() builds straight from these and never loads secrets.
        self._pre_resolved_transports: dict[str, dict[str, Any]] | None = None
        self._pre_resolved_default_ref: str | None = None
        self._pre_resolved_config: dict[str, Any] | None = None

        # Every transport this run may dispatch through, resolved to
        # JSON-safe values by materialize() and keyed by its declared ref.
        # Resolved all at once because secrets are in reach only until
        # materialization ends; opened one at a time, on first use.
        self._transport_specs: dict[str, dict[str, Any]] = {}
        self._default_transport_ref: str | None = None
        self._http_transports: dict[str, HttpTransport] = {}
        # One runtime is shared by every stream on the connection, so two
        # streams reaching a named transport for the first time do so
        # concurrently. Without this each would build a session, both would
        # write the cache, and the loser would be a live connection pool
        # nothing holds a reference to -- never closed, and pacing outside
        # the rate limiter the winner shares.
        self._http_transport_lock = asyncio.Lock()

        # Transport state — set by materialize()
        self._materialized = False
        self._engine: AsyncEngine | None = None
        # Sync SQLAlchemy engine for sync-only drivers (e.g. Redshift's
        # redshift_connector). Exactly one of _engine / _sync_engine /
        # _adbc_transport / _session is set for a transport-driven runtime.
        self._sync_engine: Engine | None = None
        self._session: aiohttp.ClientSession | None = None
        self._base_url: str | None = None
        self._rate_limiter: RateLimiter | None = None
        self._resolved_config: dict[str, Any] | None = None
        self._transport_dialect: str | None = None
        self._transport_driver: str | None = None
        # Set when materialize() built an AdbcTransport. Callers query
        # ``is_adbc`` to choose between the SA path (engine-backed) and
        # the ADBC-only path (cursor-backed); ``open_adbc_connection()``
        # hands them a fresh DBAPI connection from the closure baked at
        # materialize time.
        self._adbc_transport: AdbcTransport | None = None

        # Reference counting for shared ownership across streams
        self._ref_count = 0

        self._scrub_requests = 0

    # ------------------------------------------------------------------
    # Read-only metadata
    # ------------------------------------------------------------------

    @property
    def connector_id(self) -> str:
        """Canonical connector identifier (``postgres``, ``mysql``, ``xero``)."""
        return self._connector_id

    @property
    def connector_type(self) -> str:
        return self._connector_type

    @property
    def connection_id(self) -> str:
        return self._connection_id

    @property
    def driver(self) -> str | None:
        """Base SQL dialect (``postgresql``, ``mysql``, …) or ``None``."""
        if self._transport_dialect is not None:
            return self._transport_dialect
        if self._driver_override is not None:
            return self._driver_override
        return _derive_dialect(self._connector)

    @property
    def driver_string(self) -> str | None:
        """Driver identifier as materialised.

        SA transports return the full SQLAlchemy driver string
        (``postgresql+asyncpg``). ADBC transports return the ADBC
        driver name (``snowflake``, ``bigquery``, ``postgresql``),
        which is the closed-enum value the schema's
        ``AdbcTransport.driver`` allows.
        """
        return self._transport_driver

    @property
    def connection(self) -> ConnectionInput:
        """The validated connection document this runtime was built from.

        On a worker-side runtime it is the sanitized document that arrived
        in the resolved payload: no secret pointers.
        """
        return self._connection

    @property
    def connector(self) -> Connector | None:
        """The validated connector definition; ``None`` on a worker-side runtime."""
        return self._connector

    @property
    def declared_sql_capabilities(self) -> dict[str, Any] | None:
        """The connector's declared ``sql_capabilities`` block, verbatim.

        ``None`` means the connector declares nothing — consumers must
        refuse any needed-but-undeclared fact
        (``cdk.sql.capabilities.undeclared_capability_error``), never fill
        in a default. Trusted-side runtimes read it from the connector
        definition; worker-side runtimes get it from the resolved payload
        (the worker never reads ``connector.json``).
        """
        return (
            copy.deepcopy(self._declared_sql_capabilities)
            if self._declared_sql_capabilities is not None
            else None
        )

    @property
    def declared_error_map(self) -> dict[str, Any] | None:
        """The connector's declared ``error_map`` block, verbatim (issue #401).

        ``None`` means the connector declares no driver-error taxonomy —
        consumers keep their current heuristics (additive absence, unlike
        the sql_capabilities shape facts). Parsed at consumption via
        ``cdk.declarations.parse_declared_error_map``.
        """
        return (
            copy.deepcopy(self._declared_error_map)
            if self._declared_error_map is not None
            else None
        )

    @property
    def declared_concurrency(self) -> dict[str, Any] | None:
        """The connector's declared ``concurrency`` block, verbatim (issue #401).

        ``None`` means no declared connection ceiling — the engine's stream
        fan-out pacing keeps its current behavior. Parsed at consumption via
        ``cdk.declarations.parse_declared_concurrency``.
        """
        return (
            copy.deepcopy(self._declared_concurrency)
            if self._declared_concurrency is not None
            else None
        )

    @property
    def connector_type_mapper(self) -> TypeMapper:
        if self._connector_type_mapper is None:
            raise RuntimeError(
                f"connector_type_mapper not available for {self._connection_id!r}: "
                f"runtime was constructed without one"
            )
        return self._connector_type_mapper

    @property
    def connection_type_mapper(self) -> TypeMapper | None:
        return self._connection_type_mapper

    def type_mapper_for(self, *, scope: EndpointScope) -> TypeMapper:
        """Pick the type mapper for an endpoint of the given ``scope``.

        **Composition semantics (decision for issue #126):** connection maps
        compose with the connector map per-type. The connection's rules are
        tried first; on a miss the connector's rules are consulted — for both
        the read direction (``to_arrow_type``) and the write direction
        (``to_native_type``). A connection only needs to declare the types it
        overrides; the connector map supplies everything else.

        This means a connection endpoint that has a ``type-map-read.json`` but
        no ``type-map-write.json`` still supports DDL generation: its read
        overrides take effect and the connector's write rules cover the rest.

        For ``EndpointScope.CONNECTOR`` the connector mapper is returned
        directly; no composition takes place.

        The composed mapper is cached after the first call — both source mappers
        are immutable, so the composed result is deterministic.

        The caller passes the already-resolved :class:`~cdk.types.EndpointScope`
        (the engine maps its ``EndpointRef.scope`` to it at the boundary), so
        the CDK never imports the engine's endpoint model. Constructing the
        enum engine-side already rejects an unknown scope.
        """
        if scope == EndpointScope.CONNECTOR:
            return self.connector_type_mapper
        if scope == EndpointScope.CONNECTION:
            if self._connection_type_mapper is not None:
                if self._connector_type_mapper is None:
                    # No connector map to compose with — return connection map alone.
                    return self._connection_type_mapper
                if self._composed_connection_mapper is None:
                    self._composed_connection_mapper = TypeMapper.compose(
                        self._connection_type_mapper, self._connector_type_mapper
                    )
                return self._composed_connection_mapper
            return self.connector_type_mapper
        raise ValueError(f"type_mapper_for: unknown endpoint scope {scope!r}")

    # ------------------------------------------------------------------
    # Per-request expression resolution
    # ------------------------------------------------------------------

    def request_resolver(
        self, *, runtime_values: Mapping[str, Any] | None = None
    ) -> Resolver:
        """Resolve per-request value expressions (param defaults, bodies).

        The default derived functions are registered.
        Scopes: ``connection.{parameters,selections,discovered}`` from the
        connection config, plus ``runtime`` (``connection_id`` and any
        caller-supplied per-invocation values such as ``batch_size``).

        Secrets are intentionally absent. Per-request resolution runs
        connector-side, where the secret store is never available — secret
        resolution happens once, on the trusted side, at transport
        materialization (which uses the wider context from
        :meth:`_build_resolution_context`). Keeping this request-time scope
        set identical across the trusted engine and the sandboxed worker
        means the same expression behaves the same wherever the connector
        executes.
        """
        runtime_scope: dict[str, Any] = {RUNTIME_CONNECTION_ID: self._connection_id}
        if runtime_values:
            runtime_scope.update(runtime_values)
        context = ResolutionContext(
            connection=self._connection_subtrees(REQUEST_CONNECTION_SUBTREES),
            runtime=runtime_scope,
        )
        return Resolver(context, functions=DEFAULT_FUNCTIONS)

    def _connection_subtrees(self, names: Iterable[str]) -> dict[str, dict[str, Any]]:
        """Return the named connection-document subtrees as resolver scope mappings.

        The one dynamic read of the connection document: *names* is one of
        the scope tables declared at the top of this module, each a field of
        the connection contract, and the census claims the table's names
        through this site.
        """
        return {name: dict(getattr(self._connection, name)) for name in names}

    # ------------------------------------------------------------------
    # Materialization
    # ------------------------------------------------------------------

    async def materialize(
        self, *, sql_dialect: Any = None, transport_refs: Iterable[str] = ()
    ) -> None:
        """Resolve secrets, build the resolution context, build the transport.

        Two ways in:

        * Trusted side (engine shell, control-plane, tests): resolve secrets
          and the connector's transport spec, then build the live transport.
        * Worker side (built via :meth:`from_resolved_payload`): the resolved
          spec arrived in the launch bootstrap; build straight from it. No
          secret store is ever touched.

        ``sql_dialect`` is the connector's dialect — required whenever the
        transport declares TLS (the per-driver SSL vocabulary lives in the
        connector package's dialect), and the source of the per-connection
        session-init statements (``session_init_sql``): without a dialect
        no session state is pinned.

        A connector whose transports the factory has no kind for (the
        contract's file/s3/stdout transports are not registered there yet)
        cannot materialize; the ``resolved_config`` branch below serves a
        runtime built without a connector definition.

        ``transport_refs`` names the non-default transports this run's
        operations dispatch through (their ``request.transport_ref``).
        Their specs resolve here, with the default's, because this is the
        last moment the secrets exist; :meth:`http_transport` opens each
        one when a request first asks for it. Nothing else is resolved: a
        connector's auth or discovery transports belong to connection
        setup, and a run that never dispatches through one must not fail
        on a credential it does not use.
        """
        if self._materialized:
            return

        if (
            self._pre_resolved_transports is not None
            or self._pre_resolved_config is not None
        ):
            if self._pre_resolved_transports is not None:
                self._transport_specs = copy.deepcopy(self._pre_resolved_transports)
                self._default_transport_ref = self._pre_resolved_default_ref
                await self._open_default_transport(sql_dialect=sql_dialect)
            else:
                self._resolved_config = copy.deepcopy(self._pre_resolved_config)
            self._materialized = True
            return

        secrets = await self._load_secrets()
        self._validate_connection_contract(secrets)

        connector = self._connector
        if connector is not None and connector.transports:
            context = self._build_resolution_context(secrets)
            try:
                (
                    self._default_transport_ref,
                    self._transport_specs,
                ) = resolve_transport_specs(
                    connector,
                    transport_refs=transport_refs,
                    context=context,
                )
            finally:
                # Scrubbed the moment resolution is done, whichever way it
                # went: past this point every value a transport needs is in
                # the resolved specs, so nothing downstream has a reason to
                # read a secret again.
                self._scrub_secrets()
            await self._open_default_transport(sql_dialect=sql_dialect)
        else:
            # No transport to build: expose ``resolved_config`` directly.
            self._resolved_config = self._merge_secrets_into_config(secrets)

        self._materialized = True

    async def _open_default_transport(self, *, sql_dialect: Any = None) -> None:
        """Build the default transport's live objects from its resolved spec.

        Opened eagerly, because every consumer of a materialized runtime
        asks for the default -- the SQL engine, the base URL, the health
        check -- and an api connector with no per-operation selection asks
        for nothing else. The named ones open on first use.
        """
        ref = self._default_transport_ref
        if ref is None:  # pragma: no cover — defensive
            raise TransportSpecError(
                f"connection {self._connection_id!r} resolved no default "
                f"transport to open"
            )
        transport = await build_transport_from_spec(
            self._transport_specs[ref], sql_dialect=sql_dialect
        )
        self._apply_transport(transport)
        if isinstance(transport, HttpTransport):
            self._http_transports[ref] = transport

    async def http_transport(self, transport_ref: str | None = None) -> HttpTransport:
        """Return the HTTP transport an operation dispatches through.

        ``None`` is the default transport, which materialization already
        opened. A named one is opened here, once, on the first request
        that asks for it, and reused by every later one -- so a connector
        that names no transport opens exactly the one session it always
        opened.

        A ref this run did not resolve is refused by name. It reaches here
        only two ways, and both are defects rather than conditions to
        absorb: an operation naming a transport the connector does not
        declare (which the package validator refuses at authoring time),
        or one the bootstrap failed to collect. Dispatching through the
        default instead is the silent failure this whole path exists to
        end -- the request would go out on the wrong origin with the wrong
        headers, and the provider would answer it.
        """
        if not self._materialized:
            raise RuntimeError(
                "http_transport() called before materialize(); no transport "
                "spec has been resolved yet"
            )
        problem = self.transport_problem(transport_ref)
        if problem is not None:
            raise TransportSpecError(problem)
        # transport_problem() answered None, so the ref resolves and its
        # spec is an http one -- both narrowings below rest on that.
        ref = str(transport_ref or self._default_transport_ref)
        opened = self._http_transports.get(ref)
        if opened is not None:
            return opened
        spec = self._transport_specs[ref]
        async with self._http_transport_lock:
            # Read again under the lock: whoever held it may have been
            # opening this very ref, and building a second session for it
            # would strand the first.
            opened = self._http_transports.get(ref)
            if opened is not None:
                return opened
            transport = await build_transport_from_spec(spec)
            if not isinstance(transport, HttpTransport):  # pragma: no cover
                raise TransportSpecError(
                    f"connection {self._connection_id!r}: transport {ref!r} "
                    f"built a {type(transport).__name__}, not an HttpTransport"
                )
            self._http_transports[ref] = transport
            return transport

    def transport_problem(self, transport_ref: str | None = None) -> str | None:
        """Say why an operation cannot dispatch through *transport_ref*, or ``None``.

        The one statement of it, asked by everything that needs the
        answer: :meth:`http_transport` raises it when a request tries to
        go out, and the schema handshake returns it as the reason a stream
        is refused. A write whose transport cannot be opened must be
        refused AT the handshake -- accepting the schema and failing the
        first non-empty batch turns an authoring defect into a fatal batch
        after the engine was told the stream was ready.

        Two ways a ref fails, and both are defects rather than conditions
        to absorb: the run did not resolve it (an operation naming a
        transport the connector does not declare, which the package
        validator refuses at authoring time, or one the bootstrap failed
        to collect), or it is not HTTP, which an api operation cannot
        dispatch over at all. Falling back to the default instead is the
        silent failure this whole path exists to end -- the request would
        go out on the wrong origin with the wrong headers, and the
        provider would answer it.
        """
        ref = transport_ref or self._default_transport_ref
        if ref is None or ref not in self._transport_specs:
            return (
                f"connection {self._connection_id!r}: no resolved transport "
                f"{ref!r}; this run resolved {sorted(self._transport_specs)}. "
                f"An operation naming a transport the connector does not "
                f"declare cannot be dispatched -- it would otherwise go out "
                f"on the default transport's origin with the default "
                f"transport's headers."
            )
        declared_type = self._transport_specs[ref].get("transport_type")
        if declared_type != HTTP_TRANSPORT_TYPE:
            return (
                f"connection {self._connection_id!r}: transport {ref!r} "
                f"declares transport_type {declared_type!r}; an api "
                f"operation dispatches over HTTP and there is no session "
                f"to open from this block"
            )
        return None

    @property
    def default_transport_ref(self) -> str:
        """The ref of the transport an operation naming none dispatches through.

        Named rather than implied, because the per-operation sender cache
        keys on it: a request omitting ``transport_ref`` and one naming
        the default by name are the same transport, and two cache entries
        for it would open the connection's session twice.
        """
        if not self._materialized or self._default_transport_ref is None:
            raise RuntimeError(
                "default_transport_ref not available: call materialize() "
                "first or wrong connector_type"
            )
        return self._default_transport_ref

    def transport_header_names(self, transport_ref: str | None = None) -> set[str]:
        """Return the header names a transport sends, lowercased, without opening it.

        The names a connection owns on the transport an operation
        dispatches through -- which is what decides whether that
        operation's own ``request.headers`` may name one. Read from the
        resolved spec, not from a live session, for two reasons: the
        schema handshake that asks is deliberately await-free, and a
        connector should not open a connection pool to answer a question
        about a declaration.

        An unresolved ref answers the empty set rather than raising. The
        refusal for one belongs to the dispatch that tries to send
        through it, where it can say the request never went out; raising
        here would report a transport defect as a header defect.
        """
        ref = transport_ref or self._default_transport_ref
        spec = self._transport_specs.get(ref or "", {})
        headers = spec.get("headers")
        return {str(name).lower() for name in headers} if headers else set()

    def _apply_transport(self, transport: Any) -> None:
        """Wire a built transport's objects onto this runtime."""
        if isinstance(transport, SqlAlchemyTransport):
            # ``is_async`` is the transport's authoritative flavour flag (set
            # by build_sqlalchemy_from_spec alongside the engine); the engine
            # field's static union can't be tied to that runtime bool, so cast
            # to the slot type the flag guarantees.
            if transport.is_async:
                self._engine = cast(AsyncEngine, transport.engine)
            else:
                self._sync_engine = cast(Engine, transport.engine)
            self._transport_driver = transport.driver
            self._transport_dialect = transport.dialect
        elif isinstance(transport, AdbcTransport):
            self._adbc_transport = transport
            self._transport_driver = transport.driver
            self._transport_dialect = transport.driver
        elif isinstance(transport, HttpTransport):
            self._session = transport.session
            self._base_url = transport.base_url
            self._rate_limiter = transport.rate_limiter
        else:  # pragma: no cover — defensive
            raise NotImplementedError(
                f"Unhandled transport result type: {type(transport).__name__}"
            )

    # ------------------------------------------------------------------
    # Worker bootstrap: resolve on the trusted side, build in the worker
    # ------------------------------------------------------------------

    async def resolve_spec(
        self, *, transport_refs: Iterable[str] = ()
    ) -> dict[str, Any]:
        """Resolve this connection into a JSON-safe worker payload.

        Runs on the trusted side. Loads secrets, resolves the transports
        this run dispatches through (or the plain config for
        transport-less kinds), and returns a payload with values only — no
        constructed objects, no secret-store handle. The payload is what a
        connector worker receives in its launch bootstrap; rebuild with
        :meth:`from_resolved_payload`.

        ``transport_refs`` is the same run-scoped set
        :meth:`materialize` takes: the worker has no secret store, so a
        transport whose spec does not travel in this payload can never be
        opened there.
        """
        secrets = await self._load_secrets()
        self._validate_connection_contract(secrets)

        connector = self._connector

        payload: dict[str, Any] = {
            "connection_id": self._connection_id,
            "connector_id": self._connector_id,
            "connector_type": self._connector_type,
            "driver_hint": _derive_dialect(connector),
            # The connection document minus its secret pointers, parsed
            # again as the contract's ``ConnectionInput`` by the worker
            # runtime: connector code resolves ``connection.parameters.*``
            # refs from it at request time.
            "connection_config": self._connection.model_dump(
                mode="json", by_alias=True, exclude_unset=True, exclude={"secret_refs"}
            ),
            "transport_specs": None,
            "default_transport_ref": None,
            "resolved_config": None,
            # Declared SQL capabilities travel with the payload so the
            # worker-side facade consumes the same declaration the engine
            # validated — never a guessed default because connector.json
            # was out of reach (issue #390). The connector-level declared
            # facts (issue #401) ride the same channel.
            "sql_capabilities": self.declared_sql_capabilities,
            "error_map": self.declared_error_map,
            "concurrency": self.declared_concurrency,
        }
        if connector is not None and connector.transports:
            context = self._build_resolution_context(secrets)
            try:
                default_ref, specs = resolve_transport_specs(
                    connector, transport_refs=transport_refs, context=context
                )
            finally:
                self._scrub_secrets()
            payload["transport_specs"] = specs
            payload["default_transport_ref"] = default_ref
        else:
            payload["resolved_config"] = self._merge_secrets_into_config(secrets)
        return payload

    @classmethod
    def from_resolved_payload(
        cls,
        payload: Mapping[str, Any],
        *,
        connector_type_mapper: TypeMapper | None = None,
        connection_type_mapper: TypeMapper | None = None,
    ) -> ConnectionRuntime:
        """Rebuild a runtime in a connector worker from a resolved payload.

        The worker side of :meth:`resolve_spec`: no connector definition and
        a resolver that refuses to resolve — every value the worker may use
        arrived in the payload. ``connection`` is the payload's sanitized
        ``connection_config`` (no secret refs) parsed as the contract's
        connection document, so connector code can still resolve
        ``connection.parameters.*`` refs; a payload whose document does not
        satisfy the contract is malformed and refused here.
        """
        try:
            connection = ConnectionInput.model_validate(payload["connection_config"])
        except ValidationError as err:
            raise ValueError(
                f"worker bootstrap for connection {payload['connection_id']!r}: "
                f"connection_config does not satisfy the connection contract: {err}"
            ) from err
        runtime = cls(
            connection=connection,
            connection_id=payload["connection_id"],
            connector_id=payload["connector_id"],
            connector_type=payload["connector_type"],
            resolver=_PreResolvedSecretsResolver(),
            driver=payload.get("driver_hint"),
            connector_type_mapper=connector_type_mapper,
            connection_type_mapper=connection_type_mapper,
        )
        runtime._pre_resolved_transports = (
            {str(ref): dict(spec) for ref, spec in payload["transport_specs"].items()}
            if payload.get("transport_specs")
            else None
        )
        runtime._pre_resolved_default_ref = payload.get("default_transport_ref")
        runtime._pre_resolved_config = (
            dict(payload["resolved_config"]) if payload.get("resolved_config") else None
        )
        # deepcopy, not dict(): the blocks nest objects, and the rebuilt
        # runtime must not share mutable state with the caller's payload
        # (same isolation rule the constructor applies).
        runtime._declared_sql_capabilities = (
            copy.deepcopy(payload["sql_capabilities"])
            if payload.get("sql_capabilities") is not None
            else None
        )
        runtime._declared_error_map = (
            copy.deepcopy(payload["error_map"])
            if payload.get("error_map") is not None
            else None
        )
        runtime._declared_concurrency = (
            copy.deepcopy(payload["concurrency"])
            if payload.get("concurrency") is not None
            else None
        )
        return runtime

    # ------------------------------------------------------------------
    # Transport accessors
    # ------------------------------------------------------------------

    @property
    def engine(self) -> AsyncEngine:
        if not self._materialized:
            raise RuntimeError("engine not available: call materialize() first")
        if self._adbc_transport is not None and self._engine is None:
            raise RuntimeError(
                f"engine not available for {self._connection_id}: this runtime "
                f"was materialized with transport_type='adbc' (driver="
                f"{self._adbc_transport.driver!r}); use is_adbc / "
                f"open_adbc_connection() instead"
            )
        if self._sync_engine is not None and self._engine is None:
            raise RuntimeError(
                f"engine not available for {self._connection_id}: this runtime "
                f"was materialized with a sync-only SQLAlchemy driver "
                f"({self._transport_driver!r}); use is_sync_sqlalchemy / "
                f"sync_engine instead"
            )
        if self._engine is None:
            raise RuntimeError(
                "engine not available: wrong connector_type for SQLAlchemy"
            )
        return self._engine

    @property
    def sync_engine(self) -> Engine:
        """Sync SQLAlchemy engine for sync-only drivers.

        Callers run its operations on a worker thread
        (``asyncio.to_thread``) — mirroring the ADBC pattern — so the
        async handler interface is preserved.
        """
        if not self._materialized:
            raise RuntimeError("sync_engine not available: call materialize() first")
        if self._sync_engine is None:
            raise RuntimeError(
                f"sync_engine not available for {self._connection_id}: this "
                f"runtime was not materialized with a sync-only SQLAlchemy "
                f"driver (check is_sync_sqlalchemy / is_adbc first)"
            )
        return self._sync_engine

    @property
    def is_sync_sqlalchemy(self) -> bool:
        """True when this runtime carries a sync SQLAlchemy engine.

        Source/destination handlers branch on this to run engine
        operations through ``asyncio.to_thread`` instead of awaiting an
        :class:`AsyncEngine`.
        """
        return self._sync_engine is not None

    @property
    def is_adbc(self) -> bool:
        """True when this runtime was materialized with an AdbcTransport.

        Source/destination handlers branch on this to choose between the
        SA path (``self.engine`` + AsyncConnection) and the ADBC-only
        path (``self.open_adbc_connection()`` + DBAPI cursor).
        """
        return self._adbc_transport is not None

    def open_adbc_connection(self) -> Any:
        """Return a fresh ADBC DBAPI connection.

        ADBC drivers do not pool connections, so each caller owns the
        full lifecycle: close on disconnect, drop on ingest failure.
        Synchronous because the DBAPI itself is synchronous — callers
        wrap cursor operations in ``asyncio.to_thread`` rather than
        making this method async.
        """
        if not self._materialized:
            raise RuntimeError("open_adbc_connection() requires materialize() first")
        if self._adbc_transport is None:
            raise RuntimeError(
                f"open_adbc_connection() called on non-ADBC runtime "
                f"{self._connection_id!r} (transport is "
                f"{'SQLAlchemy' if self._engine else 'HTTP/file/stdout'})"
            )
        return self._adbc_transport.connect()

    @property
    def session(self) -> aiohttp.ClientSession:
        if not self._materialized or self._session is None:
            raise RuntimeError(
                "session not available: call materialize() first or wrong "
                "connector_type"
            )
        return self._session

    @property
    def base_url(self) -> str:
        if not self._materialized or self._base_url is None:
            raise RuntimeError(
                "base_url not available: call materialize() first or wrong "
                "connector_type"
            )
        return self._base_url

    @property
    def rate_limiter(self) -> RateLimiter | None:
        if not self._materialized:
            raise RuntimeError("rate_limiter not available: call materialize() first")
        return self._rate_limiter

    @property
    def resolved_config(self) -> dict[str, Any]:
        if not self._materialized:
            raise RuntimeError(
                "resolved_config not available: call materialize() first"
            )
        if self._resolved_config is None:
            raise RuntimeError(
                f"resolved_config for {self._connection_id} was already scrubbed "
                f"(scrub_requests={self._scrub_requests}, "
                f"ref_count={self._ref_count}). "
                f"Access resolved_config before calling scrub_resolved_config()."
            )
        return self._resolved_config

    # ------------------------------------------------------------------
    # Reference counting
    # ------------------------------------------------------------------

    def acquire(self) -> None:
        self._ref_count += 1
        logger.debug(
            f"Runtime {self._connection_id} acquired (ref_count={self._ref_count})"
        )

    async def release(self) -> None:
        await self.close()

    def scrub_resolved_config(self) -> None:
        """Signal that the caller has consumed the resolved config (file/s3/stdout)."""
        if not self._materialized:
            logger.warning(
                f"Runtime {self._connection_id}: scrub_resolved_config() "
                f"called before materialize() — ignoring"
            )
            return
        if self._resolved_config is None:
            return
        if self._ref_count == 0:
            logger.warning(
                f"Runtime {self._connection_id}: scrub_resolved_config() "
                f"called with ref_count=0 — scrubbing immediately"
            )
            self._resolved_config = None
            return
        self._scrub_requests += 1
        if self._scrub_requests >= self._ref_count:
            self._resolved_config = None
            logger.debug(
                f"Runtime {self._connection_id}: resolved config scrubbed "
                f"(all {self._ref_count} consumers signalled)"
            )

    # ------------------------------------------------------------------
    # Teardown
    # ------------------------------------------------------------------

    async def close(self) -> None:
        self._ref_count = max(0, self._ref_count - 1)
        if self._ref_count > 0:
            logger.debug(
                f"Runtime {self._connection_id} released but still in use "
                f"(ref_count={self._ref_count})"
            )
            return

        logger.debug(f"Runtime {self._connection_id} closing (last reference)")
        try:
            if self._engine is not None:
                try:
                    await self._engine.dispose()
                except Exception as e:
                    logger.error(
                        f"Failed to dispose engine for {self._connection_id}: {e}"
                    )
                self._engine = None
            if self._sync_engine is not None:
                try:
                    # Sync dispose closes pooled DBAPI connections; off the
                    # event loop like every other sync-engine operation.
                    await asyncio.to_thread(self._sync_engine.dispose)
                except Exception as e:
                    logger.error(
                        f"Failed to dispose sync engine for "
                        f"{self._connection_id}: {e}"
                    )
                self._sync_engine = None
        finally:
            # Every session this run opened, not just the default one: a
            # named transport opened on first use owns a connector pool of
            # its own, and one left behind leaks it for the rest of the
            # process.
            for ref, transport in self._http_transports.items():
                try:
                    await transport.session.close()
                except Exception as e:
                    logger.error(
                        f"Failed to close session {ref!r} for "
                        f"{self._connection_id}: {e}"
                    )
            self._http_transports.clear()
            self._transport_specs = {}
            self._default_transport_ref = None
            self._session = None
            self._base_url = None
            self._rate_limiter = None
            self._resolved_config = None
            self._scrub_requests = 0
            self._materialized = False
            self._transport_dialect = None
            self._transport_driver = None
            # AdbcTransport itself holds no shared resources (its
            # ``connect`` is a closure over the resolved spec); dropping
            # the reference is sufficient. Live DBAPI connections opened
            # via ``open_adbc_connection()`` are owned by their callers.
            self._adbc_transport = None

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    async def _load_secrets(self) -> dict[str, Any]:
        """Resolve the connection's declared ``secret_refs`` to their values.

        Each ``secret_refs.<name>`` value is a scheme-prefixed locator the
        resolver dispatches on (``env:`` / ``file:`` / ``sidecar:`` / ``s3://``).
        Connectors with no required secrets (e.g. stdout) declare no refs and
        get an empty result. An unresolvable ref fails loud from the resolver;
        there is no silent fallback to an empty secret.

        The resolver is always consulted, even for an empty ``secret_refs``: on a
        worker runtime that is the guard that trips the refusing
        :class:`_PreResolvedSecretsResolver`, so a malformed worker payload
        (no pre-resolved artifacts) cannot materialize from an empty config
        instead of failing its invalid bootstrap.
        """
        secret_refs = dict(self._connection.secret_refs)
        secrets = await self._resolver.resolve(self._connection_id, secret_refs)
        if not isinstance(secrets, Mapping):
            raise TypeError(
                f"Secrets resolver for {self._connection_id} returned "
                f"{type(secrets).__name__}, expected mapping"
            )
        # The resolver is a pluggable boundary: enforce that it returned every
        # declared ref rather than trusting it to. A conforming resolver either
        # returns all declared refs or raises; a partial result is a defect and
        # must fail loud here, not surface later as a missing binding.
        missing = [name for name in secret_refs if name not in secrets]
        if missing:
            raise SecretNotFoundError(
                connection_id=self._connection_id,
                detail=(
                    f"resolver returned no value for declared secret_refs "
                    f"{sorted(missing)!r}"
                ),
            )
        # Return only the declared refs. An adapted/legacy resolver may hand back
        # more keys than were declared; undeclared secrets must not leak into
        # ``resolved_config`` (and thence across the worker boundary).
        return {name: secrets[name] for name in secret_refs}

    def _validate_connection_contract(self, secrets: Mapping[str, Any]) -> None:
        """Enforce the connection contract before any binding is resolved.

        The published connector schema defines ``ConnectionContractInput``'s
        ``required`` as "whether resolution must produce a value" — the single
        source of truth for which connection inputs are mandatory. Checking it
        here, once, at the connection boundary, is what lets transport
        resolution treat an absent binding as a genuinely optional one (driver
        default) rather than a hard failure: by the time a binding resolves,
        every required input is guaranteed present.

        Every required input is checked, regardless of ``source`` — both
        ``user`` (operator-supplied) and ``platform`` (control-plane-supplied)
        inputs are provisioned at connection setup and stored in
        ``connection.parameters`` or ``secrets``, the scopes available here
        and the only two the contract's ``storage`` enum permits. Post-auth
        outputs (``connection.selections`` / ``connection.discovered``) are
        not contract *inputs* and so never appear in ``inputs``. A worker-side
        runtime carries no definition and is not constrained.
        """
        connector = self._connector
        if connector is None:
            return
        scopes: dict[str, Mapping[str, Any]] = {
            "connection.parameters": self._connection.parameters,
            "secrets": secrets,
        }
        missing = []
        for name, spec in connector.connection_contract.inputs.items():
            if not spec.required:
                continue
            storage = spec.storage
            if scopes[storage].get(name) is None:
                missing.append(f"{name} ({storage})")
        if missing:
            raise TransportSpecError(
                f"connection {self._connection_id!r} ({self._connector_id}) is "
                f"missing required input(s) {sorted(missing)} declared by the "
                f"connector's connection contract"
            )

    def _build_resolution_context(
        self, secrets: Mapping[str, Any]
    ) -> ResolutionContext:
        """Assemble a typed :class:`ResolutionContext` from the contract documents.

        The ``connector`` scope is the definition as its author wrote it: the
        contract lets a value expression reference any ``connector.*`` path,
        so the whole document is in scope, as JSON, for the resolver to walk.
        """
        return ResolutionContext(
            connector=authored_json(self._connector) or {},
            connection=self._connection_subtrees(MATERIALIZATION_CONNECTION_SUBTREES),
            secrets=dict(secrets),
            runtime={RUNTIME_CONNECTION_ID: self._connection_id},
        )

    def _merge_secrets_into_config(self, secrets: Mapping[str, Any]) -> dict[str, Any]:
        """Build the resolved config the transportless kinds consume.

        Serves file/s3/stdout consumers, which expose ``resolved_config``
        directly instead of a transport object: the connection's authored
        ``parameters`` (the connection subtree) and the resolved secret
        values (the ``secrets`` scope), each under its own name.
        """
        return {
            "parameters": dict(self._connection.parameters),
            "secrets": dict(secrets),
        }

    def _scrub_secrets(self) -> None:
        # For transport-driven connector types we never expose
        # ``resolved_config``; nothing to scrub beyond the in-flight dict
        # which falls out of scope when materialize() returns.
        pass

    # ------------------------------------------------------------------
    # Repr
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        status = "materialized" if self._materialized else "pending"
        return (
            f"ConnectionRuntime({self._connection_id}, "
            f"type={self._connector_type}, {status})"
        )


#: Exception types that indicate deterministic configuration problems.
#: Re-raise these unchanged in database ``connect()`` methods so callers can
#: distinguish "your type-map is missing a rule" from "the DB is unreachable".
DETERMINISTIC_CONNECT_ERRORS: tuple = (
    InvalidTypeMapError,
    UnmappedTypeError,
    PlaceholderExpansionError,
    TransportSpecError,
    # A session that fails the declared TLS mode's post-connect check
    # (SqlDialect.verify_tls_state) cannot heal by reconnecting to the
    # same endpoint; keep the type unwrapped so callers see the security
    # failure, not a generic connectivity error.
    TlsVerificationError,
)


async def materialize_runtime(
    runtime: ConnectionRuntime, *, sql_dialect: Any = None
) -> None:
    """Acquire and materialize a runtime.

    Callers are responsible for catching exceptions; use
    ``DETERMINISTIC_CONNECT_ERRORS`` to distinguish configuration errors from
    connectivity failures.

    If ``materialize()`` fails after ``acquire()``, the reference taken here is
    released before the exception propagates, so a failed connection attempt
    cannot leave a shared runtime with an elevated ref count (which would keep
    its transport/session from ever disposing). The original exception type is
    preserved for the caller's classification.
    """
    runtime.acquire()
    try:
        await runtime.materialize(sql_dialect=sql_dialect)
    except BaseException:
        await runtime.close()
        raise
