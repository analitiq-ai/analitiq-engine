"""Materialize transports from connector definitions.

The connector contract owns provider-specific transport recipes. This
module reads ``connector.transports.<ref>``, resolves expressions via a
typed :class:`~src.engine.resolver.ResolutionContext`, applies the
declared per-binding encodings to render the DSN, and produces a
concrete transport object.

Currently registered transport families:

* ``sqlalchemy`` — SQLAlchemy ``AsyncEngine`` for database connectors.
* ``http``       — ``aiohttp.ClientSession`` for API connectors.

Plugin packages call :func:`register_transport_kind` at import time to
add new families.
"""

from __future__ import annotations

import copy
import logging
import ssl as _ssl
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Mapping, Optional

import aiohttp
from sqlalchemy import text as _sa_text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from src.engine.derived_functions import DEFAULT_FUNCTIONS
from src.engine.resolver import ResolutionContext, Resolver
from src.shared.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)

TransportBuilder = Callable[[Mapping[str, Any]], Awaitable[Any]]


# ---------------------------------------------------------------------------
# SSL arg normalization
# ---------------------------------------------------------------------------


_VERIFY_MODES = {
    "CERT_NONE": _ssl.CERT_NONE,
    "CERT_OPTIONAL": _ssl.CERT_OPTIONAL,
    "CERT_REQUIRED": _ssl.CERT_REQUIRED,
}


def _ssl_dict_to_context(d: Mapping[str, Any]) -> _ssl.SSLContext:
    """Build an :class:`ssl.SSLContext` from a plain ``{verify_mode, check_hostname}`` dict.

    Connector specs declare TLS configuration as a plain object so they
    are not tied to Python's ssl module API.  This function converts the
    dict into an actual :class:`ssl.SSLContext` that drivers can consume
    directly.
    """
    check_hostname = d.get("check_hostname", False)
    if not isinstance(check_hostname, bool):
        raise TypeError(
            f"check_hostname must be a boolean, got {type(check_hostname).__name__!r}"
        )
    raw_mode = d.get("verify_mode")
    if raw_mode not in _VERIFY_MODES:
        raise ValueError(
            f"verify_mode {raw_mode!r} not recognized; "
            f"allowed: {sorted(_VERIFY_MODES)}"
        )
    verify_mode = _VERIFY_MODES[raw_mode]
    if verify_mode == _ssl.CERT_NONE and check_hostname:
        raise ValueError(
            "cannot set verify_mode=CERT_NONE with check_hostname=True; "
            "CPython forbids this combination"
        )
    ctx = _ssl.SSLContext(_ssl.PROTOCOL_TLS_CLIENT)
    # PROTOCOL_TLS_CLIENT defaults to check_hostname=True, CERT_REQUIRED.
    # Disable check_hostname first so verify_mode can be lowered freely.
    ctx.check_hostname = False
    ctx.verify_mode = verify_mode
    ctx.check_hostname = check_hostname
    return ctx


def _materialize_ssl_arg(arg: Any) -> Any:
    """Normalize a connector-declared ``ssl`` value to the form drivers accept.

    Drivers accept different shapes for the ``ssl`` connect arg:

    * ``True`` / ``False`` — enable/disable TLS, no certificate check.
    * A string like ``"prefer"`` or ``"require"`` — asyncpg canonical mode.
    * An :class:`ssl.SSLContext` — pre-built context, passed through.
    * A ``{verify_mode, check_hostname}`` dict — converted to an
      :class:`ssl.SSLContext` via :func:`_ssl_dict_to_context`.
    * ``None`` — no TLS; returned as-is so callers can skip setting ``ssl``.

    Any other type raises :class:`TypeError` immediately so connector
    authors see the bad value near the declaration site.
    """
    if arg is None or isinstance(arg, (bool, str, _ssl.SSLContext)):
        return arg
    if isinstance(arg, Mapping):
        return _ssl_dict_to_context(arg)
    raise TypeError(
        f"Unsupported ssl arg type {type(arg).__name__!r}; expected bool, str, "
        f"ssl.SSLContext, dict, or None"
    )


# ---------------------------------------------------------------------------
# Derived value materialization
# ---------------------------------------------------------------------------


def _materialize_derived(
    connector: Mapping[str, Any], ctx: ResolutionContext
) -> Dict[str, Any]:
    """Resolve all entries in ``connector.derived`` to concrete values.

    Uses a fixpoint loop: each pass tries to resolve every still-pending
    entry against the values already resolved in previous passes.  The
    loop terminates when every entry resolves or when a full pass produces
    no new resolutions (indicating a circular dependency or unresolvable
    reference), at which point a :class:`KeyError` is raised.
    """
    raw_derived = connector.get("derived")
    if raw_derived is None:
        return {}
    if not isinstance(raw_derived, Mapping):
        raise TypeError("`derived` must be an object")

    resolved: Dict[str, Any] = {}
    pending: Dict[str, Any] = dict(raw_derived)

    while pending:
        resolved_this_pass: Dict[str, Any] = {}
        current_ctx = ResolutionContext(
            connector=ctx.connector,
            connection=ctx.connection,
            secrets=ctx.secrets,
            auth=ctx.auth,
            runtime=ctx.runtime,
            state=ctx.state,
            derived=resolved,
            request=ctx.request,
            response=ctx.response,
        )
        resolver = Resolver(current_ctx, functions=DEFAULT_FUNCTIONS)
        for name, expr in list(pending.items()):
            try:
                resolved_this_pass[name] = resolver.resolve(expr)
                del pending[name]
            except (KeyError, TypeError, ValueError):
                pass
        if not resolved_this_pass:
            raise KeyError(
                f"Derived value(s) {sorted(pending)} cannot be resolved; "
                f"check for circular references or missing inputs"
            )
        resolved.update(resolved_this_pass)

    return resolved


# ---------------------------------------------------------------------------
# Spec selection helpers
# ---------------------------------------------------------------------------


def _deep_merge(base: Mapping[str, Any], override: Mapping[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {k: copy.deepcopy(v) for k, v in base.items()}
    for k, v in override.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, Mapping):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = copy.deepcopy(v)
    return out


def _select_transport(
    connector: Mapping[str, Any], transport_ref: Optional[str]
) -> tuple[str, Mapping[str, Any]]:
    """Pick the ``transports[ref]`` block, applying ``transport_defaults``."""
    transports = connector.get("transports") or {}
    if not transports:
        raise ValueError(
            f"Connector {connector.get('alias')!r} has no `transports` block; "
            f"cannot materialize transport"
        )
    ref = transport_ref or connector.get("default_transport")
    if not ref:
        raise ValueError(
            f"Connector {connector.get('alias')!r}: transport_ref not given "
            f"and `default_transport` not declared"
        )
    if ref not in transports:
        raise KeyError(
            f"Connector {connector.get('alias')!r}: transport {ref!r} not in "
            f"declared transports {sorted(transports)}"
        )
    defaults = connector.get("transport_defaults") or {}
    merged = _deep_merge(defaults, transports[ref])
    return ref, merged


# ---------------------------------------------------------------------------
# SQLAlchemy transport
# ---------------------------------------------------------------------------


@dataclass
class SqlAlchemyTransport:
    """Materialized SQLAlchemy transport."""

    engine: AsyncEngine
    driver: str
    dialect: str


async def build_sqlalchemy_transport(
    spec: Mapping[str, Any], *, resolver: Optional[Resolver] = None
) -> SqlAlchemyTransport:
    """Build a :class:`SqlAlchemyTransport` from a resolved transport spec.

    When called via :func:`build_transport`, *spec* is already fully resolved
    (all expression nodes replaced with plain values).  The *resolver*
    parameter is accepted for callers that hold an unresolved spec and want
    to resolve it inline, but it is not required.
    """
    driver = spec.get("driver")
    if resolver:
        driver = resolver.resolve(driver)
    if not isinstance(driver, str) or not driver:
        raise ValueError(
            "sqlalchemy transport requires `driver` (e.g. 'postgresql+asyncpg')"
        )

    dsn = spec.get("dsn")
    if resolver:
        dsn = resolver.resolve(dsn)
    if not isinstance(dsn, str) or not dsn:
        raise ValueError(
            "`dsn` must resolve to a non-empty connection string"
        )

    raw_connect_args = spec.get("connect_args")
    if raw_connect_args is not None and not isinstance(raw_connect_args, Mapping):
        raise TypeError("`connect_args` must be an object")
    connect_args: Dict[str, Any] = {}
    if isinstance(raw_connect_args, Mapping):
        ssl_raw = raw_connect_args.get("ssl")
        if ssl_raw is not None:
            connect_args["ssl"] = _materialize_ssl_arg(ssl_raw)
        for k, v in raw_connect_args.items():
            if k != "ssl":
                connect_args[k] = v

    options = spec.get("options") or {}
    if not isinstance(options, Mapping):
        raise TypeError("`options` must be an object")

    engine_kwargs: Dict[str, Any] = {}
    if "pool_size" in options:
        engine_kwargs["pool_size"] = int(options["pool_size"])
    if "max_overflow" in options:
        engine_kwargs["max_overflow"] = int(options["max_overflow"])
    engine_kwargs["pool_pre_ping"] = bool(options.get("pool_pre_ping", True))
    if "echo" in options:
        engine_kwargs["echo"] = bool(options["echo"])

    engine = create_async_engine(dsn, connect_args=connect_args, **engine_kwargs)
    try:
        async with engine.connect() as conn:
            await conn.execute(_sa_text("SELECT 1"))
    except Exception:
        await engine.dispose()
        raise

    base_dialect = driver.split("+", 1)[0]
    return SqlAlchemyTransport(engine=engine, driver=driver, dialect=base_dialect)


# ---------------------------------------------------------------------------
# HTTP transport
# ---------------------------------------------------------------------------


@dataclass
class HttpTransport:
    """Materialized HTTP transport ready for ``aiohttp`` requests."""

    session: aiohttp.ClientSession
    base_url: str
    headers: Dict[str, str]
    rate_limiter: Optional[RateLimiter] = None


async def build_http_transport(
    spec: Mapping[str, Any], *, resolver: Optional[Resolver] = None
) -> HttpTransport:
    raw_base = spec.get("base_url")
    base_url = resolver.resolve(raw_base) if resolver else raw_base
    if not isinstance(base_url, str) or not base_url:
        raise ValueError(
            "`base_url` must resolve to a non-empty string"
        )
    base_url = base_url.rstrip("/")

    raw_headers = spec.get("headers") or {}
    if not isinstance(raw_headers, Mapping):
        raise TypeError("`headers` must be an object")
    headers: Dict[str, str] = {}
    for name, value in raw_headers.items():
        resolved = resolver.resolve(value) if resolver else value
        if resolved is None:
            continue
        headers[str(name)] = str(resolved)

    timeout_seconds = spec.get("timeout_seconds")
    if timeout_seconds is None:
        timeout_seconds = 30
    timeout = aiohttp.ClientTimeout(total=float(timeout_seconds))

    raw_rate_limit = spec.get("rate_limit") or {}
    rate_limiter: Optional[RateLimiter] = None
    if raw_rate_limit:
        if not isinstance(raw_rate_limit, Mapping):
            raise TypeError("`rate_limit` must be an object")
        max_requests = raw_rate_limit.get("max_requests")
        time_window = raw_rate_limit.get("time_window_seconds")
        if (max_requests is None) != (time_window is None):
            raise ValueError(
                "`rate_limit` requires both `max_requests` "
                "and `time_window_seconds` (or neither)"
            )
        if max_requests is not None and time_window is not None:
            rate_limiter = RateLimiter(
                max_requests=int(max_requests), time_window=int(time_window),
            )

    connector = aiohttp.TCPConnector(limit=100, limit_per_host=10)
    session = aiohttp.ClientSession(timeout=timeout, connector=connector, headers=headers)
    return HttpTransport(
        session=session,
        base_url=base_url,
        headers=headers,
        rate_limiter=rate_limiter,
    )


# ---------------------------------------------------------------------------
# Transport-kind registry
# ---------------------------------------------------------------------------


_TRANSPORT_BUILDERS: Dict[str, TransportBuilder] = {}


def register_transport_kind(kind: str, builder: TransportBuilder) -> None:
    """Register a builder for a connector transport ``kind``."""
    if not isinstance(kind, str) or not kind:
        raise ValueError("kind must be a non-empty string")
    if not callable(builder):
        raise TypeError(
            f"transport builder for {kind!r} must be callable; "
            f"got {type(builder).__name__}"
        )
    if kind in _TRANSPORT_BUILDERS:
        raise ValueError(
            f"transport kind {kind!r} already registered; "
            f"call unregister_transport_kind first"
        )
    _TRANSPORT_BUILDERS[kind] = builder


def unregister_transport_kind(kind: str) -> None:
    if kind not in _TRANSPORT_BUILDERS:
        raise KeyError(
            f"transport kind {kind!r} not registered; "
            f"registered: {registered_transport_kinds()}"
        )
    del _TRANSPORT_BUILDERS[kind]


def registered_transport_kinds() -> list[str]:
    return sorted(_TRANSPORT_BUILDERS)


register_transport_kind("sqlalchemy", build_sqlalchemy_transport)
register_transport_kind("http", build_http_transport)


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def _resolved_context_with_derived(
    connector: Mapping[str, Any], context: ResolutionContext
) -> ResolutionContext:
    """Return a copy of *context* with ``derived`` populated from *connector*."""
    derived = _materialize_derived(connector, context)
    return ResolutionContext(
        connector=context.connector,
        connection=context.connection,
        secrets=context.secrets,
        auth=context.auth,
        runtime=context.runtime,
        state=context.state,
        derived=derived,
        request=context.request,
        response=context.response,
    )


async def build_transport(
    connector: Mapping[str, Any],
    *,
    transport_ref: Optional[str] = None,
    context: ResolutionContext,
):
    """Materialize a transport from a connector definition."""
    _ref, merged = _select_transport(connector, transport_ref)
    resolved_ctx = _resolved_context_with_derived(connector, context)
    resolver = Resolver(resolved_ctx, functions=DEFAULT_FUNCTIONS)
    spec = resolver.resolve(merged)
    kind = spec.get("kind") if isinstance(spec, Mapping) else None
    if not kind:
        raise ValueError(
            f"Resolved transport spec missing `kind`; connector "
            f"{connector.get('alias')!r}, transport {transport_ref!r}"
        )
    builder = _TRANSPORT_BUILDERS.get(kind)
    if builder is None:
        raise NotImplementedError(
            f"Unsupported transport kind: {kind!r}; "
            f"registered: {registered_transport_kinds()}"
        )
    return await builder(spec)


def resolve_transport_spec(
    connector: Mapping[str, Any],
    *,
    transport_ref: Optional[str] = None,
    context: ResolutionContext,
) -> Dict[str, Any]:
    """Resolve a transport spec into a primitives-only dict.

    Used by introspection callers that need the pre-build dict (e.g.
    tests asserting DSN rendering). Production code should call
    :func:`build_transport` instead.
    """
    _ref, merged = _select_transport(connector, transport_ref)
    resolved_ctx = _resolved_context_with_derived(connector, context)
    resolver = Resolver(resolved_ctx, functions=DEFAULT_FUNCTIONS)
    return resolver.resolve(merged)
