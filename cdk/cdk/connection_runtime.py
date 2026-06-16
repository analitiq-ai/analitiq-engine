"""ConnectionRuntime — connector-driven connection materialization.

A :class:`ConnectionRuntime` ties together a saved connection JSON, the
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
from typing import TYPE_CHECKING, Any, Dict, Mapping, Optional

from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine

if TYPE_CHECKING:
    import aiohttp

from cdk.exceptions import TransportSpecError
from cdk.derived_functions import DEFAULT_FUNCTIONS
from cdk.resolver import ResolutionContext, Resolver
from cdk.type_map import InvalidTypeMapError, TypeMapper, UnmappedTypeError
from cdk.types import EndpointScope
from cdk.secrets.protocol import SecretsResolver
from cdk.secrets.exceptions import (
    PlaceholderExpansionError,
    SecretNotFoundError,
    SecretResolutionError,
)
from cdk.rate_limiter import RateLimiter
from cdk.transport_factory import (
    AdbcTransport,
    HttpTransport,
    MongoDbTransport,
    SqlAlchemyTransport,
    build_transport,
    build_transport_from_spec,
    resolve_transport_spec,
)


logger = logging.getLogger(__name__)


# Connection-JSON blocks that must never cross into a worker: secret
# pointers and auth material. Everything else (parameters, selections,
# discovered, top-level settings) is non-secret by the connection contract
# and connector code resolves it at request time (``connection.parameters.*``
# refs, handler settings such as ``max_retries``).
_SECRET_BEARING_CONFIG_KEYS = frozenset({"secret_refs", "auth", "auth_state"})


def _derive_dialect(connector_definition: Optional[Mapping[str, Any]]) -> Optional[str]:
    """Return the base SQL dialect (e.g. ``postgresql``) from a connector
    definition, or ``None`` if not a database connector.

    Handles both ``sqlalchemy`` (``driver: 'postgresql+asyncpg'``) and
    ``adbc`` (``driver: 'snowflake'``) transports — both store the
    dialect/driver under ``transports[default].driver``; the SQLAlchemy
    flavour is composite (``base+async_driver``) so we split on ``+``.
    """
    if not connector_definition:
        return None
    transports = connector_definition.get("transports") or {}
    default_ref = connector_definition.get("default_transport")
    if not default_ref or default_ref not in transports:
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
    """Placeholder resolver for worker-side runtimes built from a resolved
    payload. The worker never touches the secret store — every value it
    needs arrived resolved in the launch bootstrap — so any resolution
    attempt is a contract violation and raises."""

    async def resolve(self, connection_id, *, keys=None):
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
    with the saved connection JSON, the connector definition, and a
    per-connection secrets resolver. ``materialize()`` is idempotent and
    safe to call from multiple consumers; the underlying transport is
    only disposed when the last reference is released.
    """

    def __init__(
        self,
        *,
        raw_config: Mapping[str, Any],
        connection_id: str,
        connector_id: str,
        connector_type: str,
        resolver: SecretsResolver,
        connector_definition: Optional[Mapping[str, Any]] = None,
        driver: Optional[str] = None,
        connector_type_mapper: Optional[TypeMapper] = None,
        connection_type_mapper: Optional[TypeMapper] = None,
    ) -> None:
        # Shape check only. The set of valid kinds is owned by the published
        # connector schema and by the worker registry (an unrunnable kind
        # raises ConnectorNotRegisteredError at resolution); pinning a
        # parallel frozen set here would block registry-discovered kinds.
        if not connector_type or not isinstance(connector_type, str):
            raise ValueError(
                f"connector_type must be a non-empty string, "
                f"got {connector_type!r}"
            )
        if not connector_id or not isinstance(connector_id, str):
            raise ValueError(
                f"connector_id must be a non-empty string, got {connector_id!r}"
            )

        self._raw_config: Dict[str, Any] = dict(raw_config)
        self._connection_id = connection_id
        self._connector_id = connector_id
        self._connector_type = connector_type
        self._connector_definition: Optional[Dict[str, Any]] = (
            dict(connector_definition) if connector_definition else None
        )
        self._driver_override = driver
        self._resolver = resolver
        self._connector_type_mapper = connector_type_mapper
        self._connection_type_mapper = connection_type_mapper
        self._composed_connection_mapper: Optional[TypeMapper] = None

        # Worker-side pre-resolved payload (set by from_resolved_payload):
        # materialize() builds straight from these and never loads secrets.
        self._pre_resolved_transport: Optional[Dict[str, Any]] = None
        self._pre_resolved_config: Optional[Dict[str, Any]] = None

        # Transport state — set by materialize()
        self._materialized = False
        self._engine: Optional[AsyncEngine] = None
        # Sync SQLAlchemy engine for sync-only drivers (e.g. Redshift's
        # redshift_connector). Exactly one of _engine / _sync_engine /
        # _adbc_transport / _session is set for a transport-driven runtime.
        self._sync_engine: Optional[Engine] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._base_url: Optional[str] = None
        self._rate_limiter: Optional[RateLimiter] = None
        self._resolved_config: Optional[Dict[str, Any]] = None
        self._transport_dialect: Optional[str] = None
        self._transport_driver: Optional[str] = None
        # Set when materialize() built an AdbcTransport. Callers query
        # ``is_adbc`` to choose between the SA path (engine-backed) and
        # the ADBC-only path (cursor-backed); ``open_adbc_connection()``
        # hands them a fresh DBAPI connection from the closure baked at
        # materialize time.
        self._adbc_transport: Optional[AdbcTransport] = None
        # Set when materialize() built a MongoDbTransport. Connectors
        # access the client via ``mongo_client`` and the configured
        # default database via ``mongo_default_database``.
        self._mongo_transport: Optional[MongoDbTransport] = None

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
    def driver(self) -> Optional[str]:
        """Base SQL dialect (``postgresql``, ``mysql``, …) or ``None``."""
        if self._transport_dialect is not None:
            return self._transport_dialect
        if self._driver_override is not None:
            return self._driver_override
        return _derive_dialect(self._connector_definition)

    @property
    def driver_string(self) -> Optional[str]:
        """Driver identifier as materialised.

        SA transports return the full SQLAlchemy driver string
        (``postgresql+asyncpg``). ADBC transports return the ADBC
        driver name (``snowflake``, ``bigquery``, ``postgresql``),
        which is the closed-enum value the schema's
        ``AdbcTransport.driver`` allows.
        """
        return self._transport_driver

    @property
    def raw_config(self) -> Dict[str, Any]:
        return copy.deepcopy(self._raw_config)

    @property
    def connector_definition(self) -> Optional[Dict[str, Any]]:
        return copy.deepcopy(self._connector_definition) if self._connector_definition else None

    @property
    def connector_type_mapper(self) -> TypeMapper:
        if self._connector_type_mapper is None:
            raise RuntimeError(
                f"connector_type_mapper not available for {self._connection_id!r}: "
                f"runtime was constructed without one"
            )
        return self._connector_type_mapper

    @property
    def connection_type_mapper(self) -> Optional[TypeMapper]:
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
        self, *, runtime_values: Optional[Mapping[str, Any]] = None
    ) -> Resolver:
        """Resolver for per-request value expressions (param defaults,
        request bodies), with the default derived functions registered.

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
        runtime_scope: Dict[str, Any] = {"connection_id": self._connection_id}
        if runtime_values:
            runtime_scope.update(runtime_values)
        context = ResolutionContext(
            connection={
                "parameters": dict(self._raw_config.get("parameters") or {}),
                "selections": dict(self._raw_config.get("selections") or {}),
                "discovered": dict(self._raw_config.get("discovered") or {}),
            },
            runtime=runtime_scope,
        )
        return Resolver(context, functions=DEFAULT_FUNCTIONS)

    # ------------------------------------------------------------------
    # Materialization
    # ------------------------------------------------------------------

    async def materialize(
        self, *, sql_dialect: Any = None
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
        connector package's dialect).

        Connectors without a ``transports`` block (file/s3/stdout) expose
        ``resolved_config`` directly — they have no shared transport.
        """
        if self._materialized:
            return

        if self._pre_resolved_transport is not None or self._pre_resolved_config is not None:
            if self._pre_resolved_transport is not None:
                transport = await build_transport_from_spec(
                    self._pre_resolved_transport, sql_dialect=sql_dialect
                )
                self._apply_transport(transport)
            else:
                self._resolved_config = copy.deepcopy(self._pre_resolved_config)
            self._materialized = True
            return

        secrets = await self._load_secrets()
        self._validate_secret_refs(secrets)
        self._validate_connection_contract(secrets)

        has_transports = bool(
            self._connector_definition
            and self._connector_definition.get("transports")
        )

        if has_transports:
            context = self._build_resolution_context(secrets)
            try:
                transport = await build_transport(
                    self._connector_definition,
                    context=context,
                    sql_dialect=sql_dialect,
                )
            except Exception:
                self._scrub_secrets()
                raise

            self._apply_transport(transport)
            self._scrub_secrets()
        else:
            # file/s3/stdout connectors: expose ``resolved_config``
            # directly. They have no transports block by design.
            self._resolved_config = self._merge_secrets_into_config(secrets)

        self._materialized = True

    def _apply_transport(self, transport: Any) -> None:
        """Wire a built transport's objects onto this runtime."""
        if isinstance(transport, SqlAlchemyTransport):
            if transport.is_async:
                self._engine = transport.engine
            else:
                self._sync_engine = transport.engine
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
        elif isinstance(transport, MongoDbTransport):
            self._mongo_transport = transport
        else:  # pragma: no cover — defensive
            raise NotImplementedError(
                f"Unhandled transport result type: {type(transport).__name__}"
            )

    # ------------------------------------------------------------------
    # Worker bootstrap: resolve on the trusted side, build in the worker
    # ------------------------------------------------------------------

    async def resolve_spec(self) -> Dict[str, Any]:
        """Resolve this connection into a JSON-safe worker payload.

        Runs on the trusted side. Loads secrets, resolves the connector's
        transport spec (or the plain config for transport-less kinds), and
        returns a payload with values only — no constructed objects, no
        secret-store handle. The payload is what a connector worker receives
        in its launch bootstrap; rebuild with :meth:`from_resolved_payload`.
        """
        secrets = await self._load_secrets()
        self._validate_secret_refs(secrets)
        self._validate_connection_contract(secrets)

        has_transports = bool(
            self._connector_definition
            and self._connector_definition.get("transports")
        )

        payload: Dict[str, Any] = {
            "connection_id": self._connection_id,
            "connector_id": self._connector_id,
            "connector_type": self._connector_type,
            "driver_hint": _derive_dialect(self._connector_definition),
            # Non-secret connection fields, restored as the worker
            # runtime's raw_config: connector code resolves
            # ``connection.parameters.*`` refs and reads handler settings
            # from it at request time.
            "connection_config": {
                key: value
                for key, value in self._raw_config.items()
                if key not in _SECRET_BEARING_CONFIG_KEYS
            },
            "transport_spec": None,
            "resolved_config": None,
        }
        if has_transports:
            context = self._build_resolution_context(secrets)
            try:
                payload["transport_spec"] = resolve_transport_spec(
                    self._connector_definition, context=context
                )
            finally:
                self._scrub_secrets()
        else:
            payload["resolved_config"] = self._merge_secrets_into_config(secrets)
        return payload

    @classmethod
    def from_resolved_payload(
        cls,
        payload: Mapping[str, Any],
        *,
        connector_type_mapper: Optional[TypeMapper] = None,
        connection_type_mapper: Optional[TypeMapper] = None,
    ) -> "ConnectionRuntime":
        """Rebuild a runtime in a connector worker from a resolved payload.

        The worker side of :meth:`resolve_spec`: no connector definition and
        a resolver that refuses to resolve — every value the worker may use
        arrived in the payload. ``raw_config`` is the payload's sanitized
        ``connection_config`` (no secret refs, no auth material), so
        connector code can still resolve ``connection.parameters.*`` refs.
        """
        runtime = cls(
            raw_config=dict(payload.get("connection_config") or {}),
            connection_id=payload["connection_id"],
            connector_id=payload["connector_id"],
            connector_type=payload["connector_type"],
            resolver=_PreResolvedSecretsResolver(),
            driver=payload.get("driver_hint"),
            connector_type_mapper=connector_type_mapper,
            connection_type_mapper=connection_type_mapper,
        )
        runtime._pre_resolved_transport = (
            dict(payload["transport_spec"]) if payload.get("transport_spec") else None
        )
        runtime._pre_resolved_config = (
            dict(payload["resolved_config"]) if payload.get("resolved_config") else None
        )
        return runtime

    # ------------------------------------------------------------------
    # Transport accessors
    # ------------------------------------------------------------------

    @property
    def engine(self) -> AsyncEngine:
        if not self._materialized:
            raise RuntimeError(
                "engine not available: call materialize() first"
            )
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
            raise RuntimeError(
                "sync_engine not available: call materialize() first"
            )
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
            raise RuntimeError(
                "open_adbc_connection() requires materialize() first"
            )
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
                "session not available: call materialize() first or wrong connector_type"
            )
        return self._session

    @property
    def base_url(self) -> str:
        if not self._materialized or self._base_url is None:
            raise RuntimeError(
                "base_url not available: call materialize() first or wrong connector_type"
            )
        return self._base_url

    @property
    def rate_limiter(self) -> Optional[RateLimiter]:
        if not self._materialized:
            raise RuntimeError("rate_limiter not available: call materialize() first")
        return self._rate_limiter

    @property
    def mongo_client(self) -> Any:
        """Motor ``AsyncIOMotorClient`` for nosql/MongoDB connectors."""
        if not self._materialized or self._mongo_transport is None:
            raise RuntimeError(
                "mongo_client not available: call materialize() first or "
                "wrong connector_type (expected transport_type='mongodb')"
            )
        return self._mongo_transport.client

    @property
    def mongo_default_database(self) -> Optional[str]:
        """Default database name from the connector definition, or ``None``."""
        if not self._materialized or self._mongo_transport is None:
            raise RuntimeError(
                "mongo_default_database not available: call materialize() first "
                "or wrong connector_type"
            )
        return self._mongo_transport.default_database

    @property
    def resolved_config(self) -> Dict[str, Any]:
        if not self._materialized:
            raise RuntimeError(
                "resolved_config not available: call materialize() first"
            )
        if self._resolved_config is None:
            raise RuntimeError(
                f"resolved_config for {self._connection_id} was already scrubbed "
                f"(scrub_requests={self._scrub_requests}, ref_count={self._ref_count}). "
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
                        f"Failed to dispose engine for {self._connection_id}: {e}",
                        exc_info=True,
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
                        f"{self._connection_id}: {e}",
                        exc_info=True,
                    )
                self._sync_engine = None
        finally:
            if self._session is not None:
                try:
                    await self._session.close()
                except Exception as e:
                    logger.error(
                        f"Failed to close session for {self._connection_id}: {e}",
                        exc_info=True,
                    )
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
            if self._mongo_transport is not None:
                try:
                    self._mongo_transport.client.close()
                except Exception as e:
                    logger.error(
                        f"Failed to close MongoDB client for {self._connection_id}: {e}",
                        exc_info=True,
                    )
                self._mongo_transport = None

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    async def _load_secrets(self) -> Dict[str, Any]:
        """Fetch the connection's secret store contents.

        Returns an empty dict when the connection has no secrets file —
        connectors with no required secrets (e.g. stdout) should not fail
        materialization.
        """
        try:
            secrets = await self._resolver.resolve(self._connection_id)
        except SecretNotFoundError:
            secrets = {}
        except SecretResolutionError:
            raise
        if not isinstance(secrets, Mapping):
            raise TypeError(
                f"Secrets resolver for {self._connection_id} returned "
                f"{type(secrets).__name__}, expected mapping"
            )
        return dict(secrets)

    def _validate_secret_refs(self, secrets: Mapping[str, Any]) -> None:
        """Confirm every ``secret_refs.<name>`` declared by the connection
        resolves to a value in the secret store."""
        secret_refs = self._raw_config.get("secret_refs") or {}
        if not isinstance(secret_refs, Mapping):
            raise TypeError(
                f"connection {self._connection_id!r}: `secret_refs` must be an object"
            )
        missing = [name for name in secret_refs if name not in secrets]
        if missing:
            raise SecretNotFoundError(
                connection_id=self._connection_id,
                detail=(
                    f"connection {self._connection_id!r} declares secret_refs "
                    f"{missing!r} but none were found in the secret store"
                ),
            )

    def _validate_connection_contract(self, secrets: Mapping[str, Any]) -> None:
        """Enforce the connector's connection contract before any binding is
        resolved.

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
        ``connection.parameters`` or ``secrets``, the scopes available here.
        Post-auth outputs (``connection.selections`` / ``connection.discovered``)
        are not contract *inputs* and so never appear in ``inputs``. A required
        input that declares any other storage is a malformed connector
        definition and fails loud. Connectors with no contract (or an older
        definition lacking one) are not constrained.
        """
        definition = self._connector_definition
        if not definition:
            return
        contract = definition.get("connection_contract")
        if not isinstance(contract, Mapping):
            return
        inputs = contract.get("inputs")
        if not isinstance(inputs, Mapping):
            return

        scopes: Dict[str, Mapping[str, Any]] = {
            "connection.parameters": self._raw_config.get("parameters") or {},
            "secrets": secrets,
        }
        missing = []
        for name, spec in inputs.items():
            if not isinstance(spec, Mapping):
                continue
            if not spec.get("required"):
                continue
            storage = spec.get("storage")
            scope = scopes.get(storage)
            if scope is None:
                # A required input must store its value where the connection
                # carries it (connection.parameters or secrets); the schema's
                # storage enum permits nothing else. Any other value is a
                # malformed connector definition -- fail loud, never skip.
                raise TransportSpecError(
                    f"connection {self._connection_id!r} ({self._connector_id}) "
                    f"declares required input {name!r} with unknown storage "
                    f"{storage!r}; expected one of {sorted(scopes)}"
                )
            if scope.get(name) is None:
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
        """Assemble a typed :class:`ResolutionContext` from the connection JSON."""
        connection_scope = {
            "parameters": dict(self._raw_config.get("parameters") or {}),
            "selections": dict(self._raw_config.get("selections") or {}),
            "discovered": dict(self._raw_config.get("discovered") or {}),
            "secret_refs": dict(self._raw_config.get("secret_refs") or {}),
            "auth_state": dict(self._raw_config.get("auth_state") or {}),
            # Top-level fields that connector value expressions may
            # reference directly (e.g. ``connection.name`` for logging
            # decorators). Address fields live in transports.
            "name": self._raw_config.get("name"),
            "status": self._raw_config.get("status"),
        }
        return ResolutionContext(
            connector=self._connector_definition or {},
            connection=connection_scope,
            secrets=dict(secrets),
            auth=dict(self._raw_config.get("auth") or {}),
            runtime={"connection_id": self._connection_id},
        )

    def _merge_secrets_into_config(
        self, secrets: Mapping[str, Any]
    ) -> Dict[str, Any]:
        """Merge secrets into a flat resolved-config dict for file/s3/stdout
        consumers, which expose ``resolved_config`` directly instead of
        a transport object."""
        resolved = copy.deepcopy(self._raw_config)
        resolved.setdefault("parameters", {})
        # Surface secrets at the top level for handlers that look them up
        # directly. This is intentionally generic: each secret key maps to
        # its value verbatim, no provider-specific shaping.
        for name, value in secrets.items():
            resolved.setdefault(name, value)
        return resolved

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
)


async def materialize_runtime(
    runtime: "ConnectionRuntime", *, sql_dialect: Any = None
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
