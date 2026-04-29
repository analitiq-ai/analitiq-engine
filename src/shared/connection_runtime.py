"""ConnectionRuntime — connector-driven connection materialization.

A :class:`ConnectionRuntime` ties together a saved connection JSON, the
connector definition that describes how to use it, and the secret store
that fills in credential values. It is the single place the engine touches
provider configuration: everything provider-specific is encoded in the
connector's ``transports`` block, resolved through the typed
:class:`~src.engine.resolver.ResolutionContext`, and turned into a
concrete transport (:class:`~src.shared.transport_factory.SqlAlchemyTransport`
or :class:`~src.shared.transport_factory.HttpTransport`) by the transport
factory. The runtime never inspects host strings, header dicts, DSN
formats, or SSL flags directly.

Lifecycle:

* ``__init__`` records connection + connector + resolver references.
* ``materialize()`` resolves secrets, builds a context, materializes the
  transport, and scrubs secret values from memory.
* Reference counting (:meth:`acquire`, :meth:`close`) lets multiple
  source/destination connectors share one underlying engine/session.
* ``file``/``s3``/``stdout`` connectors keep the legacy resolved-config
  passthrough until those connector families adopt ``transports`` blocks.
"""

from __future__ import annotations

import copy
import logging
from typing import Any, Dict, Mapping, Optional

import aiohttp
from sqlalchemy.ext.asyncio import AsyncEngine

from src.engine.resolver import ResolutionContext
from src.engine.type_map import TypeMapper
from src.secrets.protocol import SecretsResolver
from src.secrets.exceptions import SecretNotFoundError, SecretResolutionError
from src.shared.rate_limiter import RateLimiter
from src.shared.transport_factory import (
    HttpTransport,
    SqlAlchemyTransport,
    build_transport,
)


logger = logging.getLogger(__name__)


VALID_CONNECTOR_TYPES = frozenset({"database", "api", "file", "s3", "stdout"})


def _derive_dialect(connector_definition: Optional[Mapping[str, Any]]) -> Optional[str]:
    """Return the base SQL dialect (e.g. ``postgresql``) from a connector
    definition, or ``None`` if not a sqlalchemy connector."""
    if not connector_definition:
        return None
    transports = connector_definition.get("transports") or {}
    default_ref = connector_definition.get("default_transport")
    if not default_ref or default_ref not in transports:
        return None
    transport = transports[default_ref]
    if transport.get("kind") != "sqlalchemy":
        return None
    driver = transport.get("driver")
    if not isinstance(driver, str) or not driver:
        return None
    return driver.split("+", 1)[0]


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
        connector_type: str,
        resolver: SecretsResolver,
        connector_definition: Optional[Mapping[str, Any]] = None,
        driver: Optional[str] = None,
        connector_type_mapper: Optional[TypeMapper] = None,
        connection_type_mapper: Optional[TypeMapper] = None,
    ) -> None:
        if connector_type not in VALID_CONNECTOR_TYPES:
            raise ValueError(
                f"Invalid connector_type: {connector_type!r}. "
                f"Expected one of: {sorted(VALID_CONNECTOR_TYPES)}"
            )

        self._raw_config: Dict[str, Any] = dict(raw_config)
        self._connection_id = connection_id
        self._connector_type = connector_type
        self._connector_definition: Optional[Dict[str, Any]] = (
            dict(connector_definition) if connector_definition else None
        )
        self._driver_override = driver
        self._resolver = resolver
        self._connector_type_mapper = connector_type_mapper
        self._connection_type_mapper = connection_type_mapper

        # Transport state — set by materialize()
        self._materialized = False
        self._engine: Optional[AsyncEngine] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._base_url: Optional[str] = None
        self._rate_limiter: Optional[RateLimiter] = None
        self._resolved_config: Optional[Dict[str, Any]] = None
        self._transport_dialect: Optional[str] = None
        self._transport_driver: Optional[str] = None

        # Reference counting for shared ownership across streams
        self._ref_count = 0

        # Cooperative scrub for legacy file/s3/stdout consumers.
        self._scrub_requests = 0

    # ------------------------------------------------------------------
    # Read-only metadata
    # ------------------------------------------------------------------

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
        """Full SQLAlchemy driver string (``postgresql+asyncpg``) once the
        transport has been materialized."""
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

    def type_mapper_for(self, endpoint_ref: Any) -> TypeMapper:
        """Pick the type mapper whose scope matches ``endpoint_ref``."""
        from src.models.stream import EndpointRef

        ref = EndpointRef.from_dict(endpoint_ref)
        if ref.scope == "connector":
            return self.connector_type_mapper
        if ref.scope == "connection":
            if self._connection_type_mapper is None:
                raise RuntimeError(
                    f"endpoint {ref} is connection-scoped but connection "
                    f"{self._connection_id!r} has no type-map (expected at "
                    f"connections/{self._connection_id}/definition/type-map.json)"
                )
            return self._connection_type_mapper
        raise ValueError(f"type_mapper_for: unknown endpoint scope in {ref}")

    # ------------------------------------------------------------------
    # Materialization
    # ------------------------------------------------------------------

    async def materialize(self, *, require_port: bool = True) -> None:
        """Resolve secrets, build the resolution context, materialize the transport.

        Any connector that declares a ``transports`` block goes through the
        spec-driven transport factory regardless of its ``connector_type``.
        Connectors without a ``transports`` block fall through to a
        legacy passthrough that exposes ``resolved_config`` for handlers
        that still consume a flat dict (file/s3/stdout adapters that have
        not yet migrated to the new model).

        ``require_port`` is accepted for API compatibility with the
        previous runtime; database transport DSN templates encode whether
        a port is required, so the argument is no-op for spec-driven
        connectors.
        """
        if self._materialized:
            return

        secrets = await self._load_secrets()
        self._validate_secret_refs(secrets)

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
                )
            except Exception:
                self._scrub_secrets()
                raise

            if isinstance(transport, SqlAlchemyTransport):
                self._engine = transport.engine
                self._transport_driver = transport.driver
                self._transport_dialect = transport.dialect
            elif isinstance(transport, HttpTransport):
                self._session = transport.session
                self._base_url = transport.base_url
                self._rate_limiter = transport.rate_limiter
            else:  # pragma: no cover — defensive
                raise NotImplementedError(
                    f"Unhandled transport result type: {type(transport).__name__}"
                )

            self._scrub_secrets()
        else:
            # Legacy passthrough for connectors that have not yet declared
            # a ``transports`` block (typically file/s3/stdout). The
            # handler consumes ``resolved_config`` directly.
            self._resolved_config = self._merge_secrets_into_config(secrets)

        self._materialized = True

    # ------------------------------------------------------------------
    # Transport accessors
    # ------------------------------------------------------------------

    @property
    def engine(self) -> AsyncEngine:
        if not self._materialized or self._engine is None:
            raise RuntimeError(
                "engine not available: call materialize() first or wrong connector_type"
            )
        return self._engine

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
                        f"Failed to dispose engine for {self._connection_id}: {e}"
                    )
                self._engine = None
        finally:
            if self._session is not None:
                try:
                    await self._session.close()
                except Exception as e:
                    logger.error(
                        f"Failed to close session for {self._connection_id}: {e}"
                    )
                self._session = None
            self._base_url = None
            self._rate_limiter = None
            self._resolved_config = None
            self._scrub_requests = 0
            self._materialized = False
            self._transport_dialect = None
            self._transport_driver = None

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
            # Preserve any top-level address fields connectors may want to
            # reference (kept only as a back-compat surface; new connectors
            # encode address in transports).
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
        """For legacy file/s3/stdout connectors that still consume a
        flat resolved-config dict, expose secrets under the same keys."""
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
