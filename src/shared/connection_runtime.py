"""ConnectionRuntime — unified connection materialization.

Single owner of connection lifecycle: enrich -> resolve -> materialize -> scrub.

Created by PipelineConfigPrep with enriched config (connector_type, driver
already baked in). Passed to connectors, which call materialize() to get
a connected transport. Secrets are scrubbed from memory after materialization.

Ownership: the connector/handler that receives this runtime owns it.
It must call close() in its disconnect() method.
"""

import re
import logging
from typing import Any, Dict, Optional

import aiohttp
from sqlalchemy.ext.asyncio import AsyncEngine

from ..secrets.protocol import SecretsResolver
from ..secrets.exceptions import PlaceholderExpansionError
from .database_utils import create_database_engine
from .rate_limiter import RateLimiter

logger = logging.getLogger(__name__)

# Pattern for ${placeholder} syntax
PLACEHOLDER_PATTERN = re.compile(r"\$\{([^}]+)\}")


async def create_api_session(
    config: Dict[str, Any],
) -> tuple[aiohttp.ClientSession, str, Optional[RateLimiter]]:
    """
    Create an aiohttp session from resolved connection config.

    Auth is resolved here from the resolved config, so headers contain
    expanded secret values before the runtime scrubs the config.

    Returns:
        Tuple of (session, base_url, rate_limiter)
    """
    parameters = config.get("parameters", {})
    base_url = config["host"].rstrip("/")
    headers = dict(parameters.get("headers", {}))

    # Resolve auth into headers (operates on resolved config)
    auth_config = config.get("auth")
    if auth_config:
        auth_type = auth_config.get("type", "").lower()
        if auth_type in ("bearer_token", "bearer"):
            headers["Authorization"] = f"Bearer {auth_config.get('token', '')}"
        elif auth_type == "api_key":
            header_name = auth_config.get("header_name", "X-API-Key")
            headers[header_name] = auth_config.get("api_key", "")
        elif auth_type == "basic":
            import base64
            credentials = base64.b64encode(
                f"{auth_config.get('username', '')}:{auth_config.get('password', '')}".encode()
            ).decode()
            headers["Authorization"] = f"Basic {credentials}"

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
    Single owner of connection lifecycle.

    Created by PipelineConfigPrep with enriched config (connector_type, driver
    already baked in). Passed to connectors, which call materialize() to get
    a connected transport. Secrets are scrubbed from memory after materialization.

    Ownership: the connector/handler that receives this runtime owns it.
    It must call close() in its disconnect() method.
    """

    def __init__(
        self,
        *,
        raw_config: Dict[str, Any],
        connection_id: str,
        connector_type: str,
        driver: Optional[str],
        resolver: SecretsResolver,
        org_id: Optional[str] = None,
    ):
        self._raw_config = raw_config
        self._connection_id = connection_id
        self._connector_type = connector_type
        self._driver = driver
        self._resolver = resolver
        self._org_id = org_id

        # Transport state — set by materialize()
        self._materialized = False
        self._engine: Optional[AsyncEngine] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._base_url: Optional[str] = None
        self._rate_limiter: Optional[RateLimiter] = None
        self._resolved_config: Optional[Dict] = None

        # Secret resolution internals
        self._secrets: Optional[Dict[str, str]] = None

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
        return self._raw_config

    # --- Materialization ---

    async def materialize(self, *, require_port: bool = True) -> None:
        """
        Resolve secrets, create transport, scrub secrets.

        Args:
            require_port: For database connections, whether port is required.

        Idempotent — second call is a no-op.
        """
        if self._materialized:
            return

        resolved = await self._resolve_secrets()

        if self._connector_type == "database":
            self._engine, _ = await create_database_engine(
                resolved, require_port=require_port
            )
        elif self._connector_type == "api":
            self._session, self._base_url, self._rate_limiter = (
                await create_api_session(resolved)
            )
        else:
            # file, s3, stdout — no transport, just resolved config
            self._resolved_config = resolved

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
        if not self._materialized or self._resolved_config is None:
            raise RuntimeError(
                "resolved_config not available: call materialize() first or wrong connector_type"
            )
        return self._resolved_config

    # --- Teardown ---

    async def close(self) -> None:
        """Dispose transport resources. Safe to call multiple times."""
        if self._engine:
            await self._engine.dispose()
            self._engine = None
        if self._session:
            await self._session.close()
            self._session = None
        self._materialized = False

    # --- Private ---

    async def _resolve_secrets(self) -> Dict[str, Any]:
        """Resolve ${placeholder} values."""
        if not self._has_placeholders(self._raw_config):
            logger.debug(
                f"No placeholders in connection {self._connection_id}, "
                f"skipping secret resolution"
            )
            return self._raw_config.copy()

        self._secrets = await self._resolver.resolve(
            self._connection_id,
            org_id=self._org_id,
        )

        resolved = self._expand_placeholders(self._raw_config, self._secrets)
        logger.debug(f"Resolved secrets for connection: {self._connection_id}")
        return resolved

    def _expand_placeholders(
        self,
        value: Any,
        secrets: Dict[str, str],
    ) -> Any:
        if isinstance(value, str):
            return self._expand_string(value, secrets)
        elif isinstance(value, dict):
            return {k: self._expand_placeholders(v, secrets) for k, v in value.items()}
        elif isinstance(value, list):
            return [self._expand_placeholders(item, secrets) for item in value]
        return value

    def _expand_string(self, value: str, secrets: Dict[str, str]) -> str:
        def replace_placeholder(match: re.Match) -> str:
            key = match.group(1)
            if key in secrets:
                return str(secrets[key])
            raise PlaceholderExpansionError(
                placeholder=f"${{{key}}}",
                connection_id=self._connection_id,
                detail=f"Secret key '{key}' not found",
            )

        return PLACEHOLDER_PATTERN.sub(replace_placeholder, value)

    def _has_placeholders(self, value: Any) -> bool:
        if isinstance(value, str):
            return bool(PLACEHOLDER_PATTERN.search(value))
        elif isinstance(value, dict):
            return any(self._has_placeholders(v) for v in value.values())
        elif isinstance(value, list):
            return any(self._has_placeholders(item) for item in value)
        return False

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
