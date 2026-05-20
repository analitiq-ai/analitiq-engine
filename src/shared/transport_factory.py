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
import urllib.parse
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
# DSN URL-template renderer
# ---------------------------------------------------------------------------


# Encodings declared by the connector contract for each DSN binding. The
# engine never invents URL syntax — it applies the declared encoding to
# the resolved value and substitutes into the connector-authored template.
_ENCODING_QUOTES: Dict[str, str] = {
    # ``raw`` keeps numeric and pass-through values unchanged.
    "raw": "",
    # Hostnames already use a permitted character set; do not re-encode
    # dots, hyphens, or IDNA-ready labels. ``safe`` includes the entire
    # hostname character set so quote() becomes effectively a no-op.
    "host": "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~:[]",
    # URL userinfo (user:pass@host) — percent-encode every reserved
    # character. ``urllib.parse.quote`` with ``safe=""`` is correct;
    # we add the unreserved set for clarity.
    "url_userinfo": "",
    # URL path segment — `/` is the segment separator, so it is NOT safe.
    "url_path_segment": "",
    # URL query key/value — `&`, `=`, `+`, `#`, `?` must be encoded.
    "url_query_key": "",
    "url_query_value": "",
}


def _render_url_template_dsn(
    dsn_spec: Mapping[str, Any], resolver: Resolver
) -> str:
    """Render a connector-authored ``dsn.url_template`` recipe.

    The recipe has shape::

        {
          "kind": "url_template",
          "template": "postgresql+asyncpg://{username}:{password}@{host}:{port}/{database}",
          "bindings": {
            "username": {"value": {"ref": "connection.parameters.username"}, "encoding": "url_userinfo"},
            ...
          }
        }

    Each ``bindings[name].value`` is resolved through the active
    :class:`Resolver`; the result is converted to a string using the
    declared ``encoding``; the encoded values are then substituted into
    ``template`` using ``str.format_map``-style replacement. Missing
    placeholders, missing bindings, and unknown encodings are
    configuration errors, not runtime warnings.
    """
    kind = dsn_spec.get("kind")
    if kind != "url_template":
        raise ValueError(
            f"Unsupported dsn.kind {kind!r}; the connector contract currently "
            f"defines only 'url_template'"
        )
    template = dsn_spec.get("template")
    if not isinstance(template, str) or not template:
        raise ValueError("dsn.template must be a non-empty string")

    raw_bindings = dsn_spec.get("bindings") or {}
    if not isinstance(raw_bindings, Mapping):
        raise TypeError("dsn.bindings must be an object")

    rendered: Dict[str, str] = {}
    for name, entry in raw_bindings.items():
        if not isinstance(entry, Mapping):
            raise TypeError(
                f"dsn.bindings.{name} must be an object with 'value' and 'encoding'"
            )
        if "value" not in entry or "encoding" not in entry:
            raise ValueError(
                f"dsn.bindings.{name} requires both 'value' and 'encoding'"
            )
        encoding = entry["encoding"]
        if encoding not in _ENCODING_QUOTES:
            raise ValueError(
                f"dsn.bindings.{name}: unknown encoding {encoding!r}; "
                f"allowed: {sorted(_ENCODING_QUOTES)}"
            )
        value = resolver.resolve(entry["value"])
        rendered[name] = _apply_encoding(encoding, value, binding=name)

    try:
        return template.format_map(rendered)
    except KeyError as err:
        raise KeyError(
            f"dsn.template references {err.args[0]!r} but no matching binding "
            f"was declared (declared: {sorted(raw_bindings)})"
        ) from None


def _apply_encoding(encoding: str, value: Any, *, binding: str) -> str:
    """Convert a resolved value to its DSN-segment string form."""
    if value is None:
        raise ValueError(
            f"dsn.bindings.{binding}: resolved value is None; required for "
            f"DSN rendering"
        )
    if encoding == "raw":
        # ``raw`` is for numeric pass-through (port numbers, etc). Strings
        # are accepted verbatim but URL-special characters are NOT
        # encoded — the connector author is asserting the value is
        # already in protocol form.
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)
    safe = _ENCODING_QUOTES[encoding]
    return urllib.parse.quote(str(value), safe=safe)


# ---------------------------------------------------------------------------
# TLS materialization (database transports)
# ---------------------------------------------------------------------------


# Modes that skip server certificate verification entirely. Asyncpg accepts
# these as plain strings.
_PLAIN_TLS_MODES = {"disable", "allow", "prefer", "require"}
# Modes that require certificate verification — we hand asyncpg a
# pre-built SSLContext so the CA bundle declared on the connection is
# honoured.
_VERIFIED_TLS_MODES = {"verify-ca", "verify-full"}


def _resolve_tls_mode(
    tls_spec: Optional[Mapping[str, Any]], resolver: Resolver
) -> tuple[Optional[str], Optional[str]]:
    """Resolve ``tls.mode`` and (when needed) ``tls.ca_certificate``.

    Returns ``(mode, ca_pem)`` where:
    * ``mode`` is one of the canonical strings or ``None`` when no TLS
      block was declared.
    * ``ca_pem`` is the PEM bundle string for verified modes (only
      required for ``verify-ca`` / ``verify-full``).
    """
    if tls_spec is None:
        return None, None
    if not isinstance(tls_spec, Mapping):
        raise TypeError("transports.<ref>.tls must be an object")

    raw_mode = tls_spec.get("mode")
    if raw_mode is None:
        return None, None
    mode = resolver.resolve(raw_mode)
    if mode is None:
        return None, None
    if not isinstance(mode, str):
        raise TypeError(
            f"tls.mode must resolve to a string, got {type(mode).__name__}"
        )
    if mode not in _PLAIN_TLS_MODES and mode not in _VERIFIED_TLS_MODES:
        raise ValueError(
            f"tls.mode {mode!r} is not in the canonical set "
            f"{sorted(_PLAIN_TLS_MODES | _VERIFIED_TLS_MODES)}"
        )

    ca_value: Optional[str] = None
    if mode in _VERIFIED_TLS_MODES:
        raw_ca = tls_spec.get("ca_certificate")
        if raw_ca is not None:
            try:
                resolved = resolver.resolve(raw_ca)
            except KeyError:
                resolved = None
            if isinstance(resolved, str) and resolved:
                ca_value = resolved
        if ca_value is None:
            raise ValueError(
                f"tls.mode={mode!r} requires tls.ca_certificate to resolve "
                f"to a PEM certificate bundle"
            )

    return mode, ca_value


def _build_verified_ssl_context(mode: str, ca_pem: str) -> _ssl.SSLContext:
    ctx = _ssl.create_default_context(cadata=ca_pem)
    ctx.check_hostname = mode == "verify-full"
    ctx.verify_mode = _ssl.CERT_REQUIRED
    return ctx


def _materialize_tls_for_driver(
    driver: str,
    tls_spec: Optional[Mapping[str, Any]],
    resolver: Resolver,
) -> Any:
    """Dispatch TLS materialization based on the SQLAlchemy driver string.

    asyncpg accepts plain ``"disable"``/``"allow"``/``"prefer"``/``"require"``
    strings or an :class:`ssl.SSLContext`. aiomysql (and PyMySQL) only
    accept ``True`` / ``False`` / an ``SSLContext`` — string modes raise
    ``'str' object has no attribute 'wrap_bio'``. The connector contract
    keeps a single canonical vocabulary; this helper translates it to
    each driver's expected shape.
    """
    mode, ca_pem = _resolve_tls_mode(tls_spec, resolver)
    if mode is None:
        return None

    base_driver = driver.split("+", 1)[0].lower()

    if base_driver in ("postgresql", "postgres"):
        # asyncpg understands the canonical string vocabulary directly
        # and SSLContext for verified modes.
        if mode in _VERIFIED_TLS_MODES:
            return _build_verified_ssl_context(mode, ca_pem or "")
        return mode

    if base_driver in ("mysql", "mariadb"):
        # aiomysql wants an SSLContext or no ssl arg at all.
        if mode == "disable":
            return None
        if mode in _VERIFIED_TLS_MODES:
            return _build_verified_ssl_context(mode, ca_pem or "")
        # For ``allow`` / ``prefer`` / ``require`` we negotiate TLS but
        # do not verify the server certificate — the user has not
        # provided a CA bundle. ``check_hostname`` must be False for
        # ``CERT_NONE`` or CPython raises.
        ctx = _ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = _ssl.CERT_NONE
        return ctx

    # Unknown driver: pass the resolved mode through and let the
    # downstream materializer decide. SSLContext for verified modes is
    # the safest portable default.
    if mode in _VERIFIED_TLS_MODES:
        return _build_verified_ssl_context(mode, ca_pem or "")
    return mode


# ---------------------------------------------------------------------------
# Spec resolution
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
            f"Connector {connector.get('connector_id')!r} has no `transports` block; "
            f"cannot materialize transport"
        )
    ref = transport_ref or connector.get("default_transport")
    if not ref:
        raise ValueError(
            f"Connector {connector.get('connector_id')!r}: transport_ref not given "
            f"and default_transport not declared"
        )
    if ref not in transports:
        raise KeyError(
            f"Connector {connector.get('connector_id')!r}: transport {ref!r} not in "
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
    spec: Mapping[str, Any], *, resolver: Resolver
) -> SqlAlchemyTransport:
    driver = spec.get("driver")
    if not isinstance(driver, str) or not driver:
        raise ValueError(
            "sqlalchemy transport requires `driver` (e.g. 'postgresql+asyncpg')"
        )

    raw_dsn = spec.get("dsn")
    if not isinstance(raw_dsn, Mapping):
        raise TypeError(
            "sqlalchemy transport `dsn` must be the structured "
            "{kind: url_template, template, bindings} object"
        )
    dsn = _render_url_template_dsn(raw_dsn, resolver)

    connect_args: Dict[str, Any] = {}
    tls_value = _materialize_tls_for_driver(driver, spec.get("tls"), resolver)
    if tls_value is not None:
        connect_args["ssl"] = tls_value

    options = spec.get("options") or {}
    if not isinstance(options, Mapping):
        raise TypeError("sqlalchemy transport `options` must be an object")

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
    spec: Mapping[str, Any], *, resolver: Resolver
) -> HttpTransport:
    raw_base = spec.get("base_url")
    if raw_base is None:
        raise ValueError("http transport `base_url` is required")
    base_url = resolver.resolve(raw_base)
    if not isinstance(base_url, str) or not base_url:
        raise ValueError(
            "http transport `base_url` must resolve to a non-empty string"
        )
    base_url = base_url.rstrip("/")

    raw_headers = spec.get("headers") or {}
    if not isinstance(raw_headers, Mapping):
        raise TypeError("http transport `headers` must be an object")
    headers: Dict[str, str] = {}
    for name, value in raw_headers.items():
        resolved = resolver.resolve(value)
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
            raise TypeError("http transport `rate_limit` must be an object")
        max_requests = raw_rate_limit.get("max_requests")
        time_window = raw_rate_limit.get("time_window_seconds")
        if (max_requests is None) != (time_window is None):
            raise ValueError(
                "http transport `rate_limit` requires both `max_requests` "
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
# Transport-type registry (closed enum from the connector contract)
# ---------------------------------------------------------------------------


_TRANSPORT_BUILDERS: Dict[str, TransportBuilder] = {}


def register_transport_kind(transport_type: str, builder: TransportBuilder) -> None:
    """Register a builder for a connector ``transport_type``."""
    if not isinstance(transport_type, str) or not transport_type:
        raise ValueError("transport_type must be a non-empty string")
    if not callable(builder):
        raise TypeError(
            f"transport builder for {transport_type!r} must be callable; "
            f"got {type(builder).__name__}"
        )
    if transport_type in _TRANSPORT_BUILDERS:
        raise ValueError(
            f"transport_type {transport_type!r} already registered; "
            f"call unregister_transport_kind first"
        )
    _TRANSPORT_BUILDERS[transport_type] = builder


def unregister_transport_kind(transport_type: str) -> None:
    if transport_type not in _TRANSPORT_BUILDERS:
        raise KeyError(
            f"transport_type {transport_type!r} not registered; "
            f"registered: {registered_transport_kinds()}"
        )
    del _TRANSPORT_BUILDERS[transport_type]


def registered_transport_kinds() -> list[str]:
    return sorted(_TRANSPORT_BUILDERS)


register_transport_kind("sqlalchemy", build_sqlalchemy_transport)
register_transport_kind("http", build_http_transport)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


async def build_transport(
    connector: Mapping[str, Any],
    *,
    transport_ref: Optional[str] = None,
    context: ResolutionContext,
):
    """Materialize a transport from a connector definition."""
    _ref, spec = _select_transport(connector, transport_ref)
    transport_type = spec.get("transport_type")
    if not transport_type:
        raise ValueError(
            f"Resolved transport spec missing `transport_type`; connector "
            f"{connector.get('connector_id')!r}, transport {transport_ref!r}"
        )
    builder = _TRANSPORT_BUILDERS.get(transport_type)
    if builder is None:
        raise NotImplementedError(
            f"Unsupported transport_type: {transport_type!r}; "
            f"registered: {registered_transport_kinds()}"
        )
    resolver = Resolver(context, functions=DEFAULT_FUNCTIONS)
    return await builder(spec, resolver=resolver)


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
    resolver = Resolver(context, functions=DEFAULT_FUNCTIONS)
    return resolver.resolve(merged)
