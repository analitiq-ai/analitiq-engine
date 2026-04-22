"""ConnectionRuntime — unified connection materialization.

Constructed by PipelineConfigPrep with enriched config (connector_type, driver
already baked in).

Lifecycle per connector type:
- database/api: resolve secrets -> create transport -> scrub immediately.
- file/s3/stdout: resolve secrets -> retain resolved config -> handlers
  consume config during connect() -> cooperative scrub via
  scrub_resolved_config() once all consumers have signalled.

Shared ownership via reference counting: each consumer calls acquire() on
connect and close() on disconnect. Transport is disposed only when the last
consumer releases.
"""

import copy
import logging
from typing import Any, Dict, Optional

import aiohttp
from sqlalchemy.ext.asyncio import AsyncEngine

from ..engine.type_map import SSLModeMapper, TypeMapper
from ..secrets.protocol import SecretsResolver
from ..secrets.exceptions import PlaceholderExpansionError
from .database_utils import create_database_engine
from .placeholder import PLACEHOLDER_PATTERN, expand_placeholders, has_placeholders
from .rate_limiter import RateLimiter

logger = logging.getLogger(__name__)

VALID_CONNECTOR_TYPES = frozenset({"database", "api", "file", "s3", "stdout"})


async def _create_api_session(
    config: Dict[str, Any],
) -> tuple[aiohttp.ClientSession, str, Optional[RateLimiter]]:
    """
    Create an aiohttp session from resolved connection config.

    Auth headers are built from the already-resolved config, so secret values
    are baked into the session headers before the runtime scrubs the resolved
    config from memory.

    Returns:
        Tuple of (session, base_url, rate_limiter)
    """
    parameters = config.get("parameters", {})
    if "host" not in config or not config["host"]:
        raise ValueError("API connection requires 'host' in configuration")
    base_url = config["host"].rstrip("/")
    headers = dict(parameters.get("headers", {}))

    # Resolve auth into headers (operates on resolved config)
    auth_config = config.get("auth")
    if auth_config:
        auth_type = auth_config.get("type", "").lower()
        if auth_type in ("bearer_token", "bearer"):
            token = auth_config.get("token", "")
            if not token:
                raise ValueError("Auth type 'bearer' requires a non-empty 'token' field")
            headers["Authorization"] = f"Bearer {token}"
        elif auth_type == "api_key":
            header_name = auth_config.get("header_name", "X-API-Key")
            api_key = auth_config.get("api_key", "")
            if not api_key:
                raise ValueError("Auth type 'api_key' requires a non-empty 'api_key' field")
            headers[header_name] = api_key
        elif auth_type == "basic":
            import base64
            username = auth_config.get("username", "")
            password = auth_config.get("password", "")
            if not username:
                raise ValueError("Auth type 'basic' requires a non-empty 'username' field")
            credentials = base64.b64encode(
                f"{username}:{password}".encode()
            ).decode()
            headers["Authorization"] = f"Basic {credentials}"
        else:
            logger.warning(
                f"Unknown auth type '{auth_type}', skipping auth header setup"
            )

    timeout = aiohttp.ClientTimeout(total=parameters.get("timeout", 30))
    connector = aiohttp.TCPConnector(
        limit=parameters.get("max_connections", 100),
        limit_per_host=parameters.get("max_connections_per_host", 10),
    )

    session = aiohttp.ClientSession(
        timeout=timeout, connector=connector, headers=headers
    )

    rate_limiter = None
    rate_limit = parameters.get("rate_limit")
    if rate_limit:
        rate_limiter = RateLimiter(
            max_requests=rate_limit.get("max_requests", 100),
            time_window=rate_limit.get("time_window", 60),
        )

    return session, base_url, rate_limiter


class ConnectionRuntime:
    """
    Shared-ownership connection lifecycle with reference counting.

    Constructed by PipelineConfigPrep with enriched config (connector_type,
    driver already baked in). Multiple connectors may share the same instance
    when streams reference the same connection_id.

    Each consumer calls acquire() on connect and close() on disconnect.
    Transport resources are only disposed when the last consumer releases.
    """

    def __init__(
        self,
        *,
        raw_config: Dict[str, Any],
        connection_id: str,
        connector_type: str,
        driver: Optional[str],
        resolver: SecretsResolver,
        type_mapper: Optional[TypeMapper] = None,
        ssl_mapper: Optional[SSLModeMapper] = None,
    ):
        if connector_type not in VALID_CONNECTOR_TYPES:
            raise ValueError(
                f"Invalid connector_type: {connector_type!r}. "
                f"Expected one of: {sorted(VALID_CONNECTOR_TYPES)}"
            )

        self._raw_config = raw_config
        self._connection_id = connection_id
        self._connector_type = connector_type
        self._driver = driver
        self._resolver = resolver
        self._type_mapper = type_mapper
        self._ssl_mapper = ssl_mapper

        # Transport state — set by materialize()
        self._materialized = False
        self._engine: Optional[AsyncEngine] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._base_url: Optional[str] = None
        self._rate_limiter: Optional[RateLimiter] = None
        self._resolved_config: Optional[Dict] = None

        # Reference counting for shared ownership across streams
        self._ref_count = 0

        # Secret resolution internals
        self._secrets: Optional[Dict[str, str]] = None

        # Cooperative scrub: handlers call scrub_resolved_config() after
        # consuming the config.  We only actually clear when every acquirer
        # has signalled.
        self._scrub_requests: int = 0

    # --- Read-only metadata (available before materialize) ---

    @property
    def connector_type(self) -> str:
        return self._connector_type

    @property
    def driver(self) -> Optional[str]:
        return self._driver

    @property
    def connection_id(self) -> str:
        return self._connection_id

    @property
    def raw_config(self) -> Dict[str, Any]:
        return copy.deepcopy(self._raw_config)

    @property
    def type_mapper(self) -> TypeMapper:
        """Connector-owned native↔canonical type mapper.

        Always present for resolved connections because ``type-map.json`` is
        required for every connector. Raises if the runtime was constructed
        without one (bare unit tests).
        """
        if self._type_mapper is None:
            raise RuntimeError(
                f"type_mapper not available for {self._connection_id!r}: "
                f"runtime was constructed without one"
            )
        return self._type_mapper

    @property
    def ssl_mapper(self) -> Optional[SSLModeMapper]:
        """Connector-owned SSL mode mapper, or ``None`` for non-SSL connectors."""
        return self._ssl_mapper

    # --- Materialization ---

    async def materialize(self, *, require_port: bool = True) -> None:
        """
        Resolve secrets, create transport, scrub secrets.

        Args:
            require_port: For database connections, whether port is required.
                          Ignored for non-database connector types.

        Idempotent — second call is a no-op.
        """
        if self._materialized:
            return

        resolved = await self._resolve_secrets()

        try:
            if self._connector_type == "database":
                self._engine, _ = await create_database_engine(
                    resolved,
                    require_port=require_port,
                    ssl_mapper=self._ssl_mapper,
                )
            elif self._connector_type == "api":
                self._session, self._base_url, self._rate_limiter = (
                    await _create_api_session(resolved)
                )
            else:
                # file, s3, stdout — no transport, just resolved config
                self._resolved_config = resolved
        except Exception:
            # Clean up secrets on failure — don't leave them in memory
            self._scrub_secrets()
            raise

        self._scrub_secrets()
        self._materialized = True

    # --- Transport accessors (available after materialize) ---

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

    # --- Reference counting ---

    def acquire(self) -> None:
        """Register a consumer of this runtime. Call before using transport."""
        self._ref_count += 1
        logger.debug(
            f"Runtime {self._connection_id} acquired (ref_count={self._ref_count})"
        )

    async def release(self) -> None:
        """Alias for close() — decrement ref count and dispose if last."""
        await self.close()

    def scrub_resolved_config(self) -> None:
        """Signal that the caller has consumed the resolved config.

        For file/s3/stdout connections the resolved config is kept after
        materialize() so handlers can pass it to storage backends during
        connect().  Once a handler has finished consuming it, it calls this
        method.  The config is actually cleared only when every acquirer has
        signalled, so shared runtimes stay functional until the last consumer
        is done.

        For database/api connections the resolved config is already scrubbed
        by materialize(), so calling this method is a safe no-op.
        """
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

    # --- Teardown ---

    async def close(self) -> None:
        """
        Decrement ref count; dispose transport when last consumer releases.

        Safe to call multiple times. When ref count reaches zero the
        underlying engine/session is disposed and materialization state is
        reset.
        """
        self._ref_count = max(0, self._ref_count - 1)
        if self._ref_count > 0:
            logger.debug(
                f"Runtime {self._connection_id} released but still in use "
                f"(ref_count={self._ref_count})"
            )
            return

        logger.debug(f"Runtime {self._connection_id} closing (last reference)")
        try:
            if self._engine:
                try:
                    await self._engine.dispose()
                except Exception as e:
                    logger.error(f"Failed to dispose engine for {self._connection_id}: {e}")
                self._engine = None
        finally:
            if self._session:
                try:
                    await self._session.close()
                except Exception as e:
                    logger.error(f"Failed to close session for {self._connection_id}: {e}")
                self._session = None
            self._base_url = None
            self._rate_limiter = None
            self._resolved_config = None
            self._scrub_requests = 0
            self._materialized = False

    # --- Private ---

    async def _resolve_secrets(self) -> Dict[str, Any]:
        """Resolve ${placeholder} values."""
        if not has_placeholders(self._raw_config):
            logger.debug(
                f"No placeholders in connection {self._connection_id}, "
                f"skipping secret resolution"
            )
            return copy.deepcopy(self._raw_config)

        self._secrets = await self._resolver.resolve(
            self._connection_id,
        )

        try:
            resolved = expand_placeholders(self._raw_config, self._secrets)
        except KeyError as e:
            raise PlaceholderExpansionError(
                placeholder=str(e),
                connection_id=self._connection_id,
                detail=f"Secret key not found",
            ) from e
        logger.debug(f"Resolved secrets for connection: {self._connection_id}")
        return resolved

    def _scrub_secrets(self) -> None:
        self._secrets = None
        if self._connector_type in ("database", "api"):
            self._resolved_config = None

    def __repr__(self) -> str:
        status = "materialized" if self._materialized else "pending"
        return (
            f"ConnectionRuntime({self._connection_id}, "
            f"type={self._connector_type}, {status})"
        )
