"""Materialize transports from connector definitions.

The connector contract owns provider-specific transport recipes. This
module reads ``connector.transports.<ref>``, resolves expressions via a
typed :class:`~src.engine.resolver.ResolutionContext`, applies the
declared per-binding encodings to render the DSN, and produces a
concrete transport object.

Currently registered transport families:

* ``sqlalchemy`` — SQLAlchemy ``AsyncEngine`` for database connectors
  whose dialect has an async driver (Postgres asyncpg, MySQL aiomysql).
* ``adbc``       — ADBC DBAPI 2.0 connection for database connectors
  whose dialect has no async SA driver (Snowflake, BigQuery) or wants
  the Arrow-native bulk path (Postgres). Driver enum closed to the
  registry in ``_ADBC_DRIVER_MODULES``.
* ``http``       — ``aiohttp.ClientSession`` for API connectors.

Plugin packages call :func:`register_transport_kind` at import time to
add new families.
"""

from __future__ import annotations

import asyncio
import copy
import importlib
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
#
# Each driver speaks its own native SSL vocabulary. The engine resolves
# the ``tls.mode`` and ``tls.ca_certificate`` refs to whatever strings
# the connection stored and hands them to a driver-specific helper that
# knows how to apply those strings to that driver's API. No shared
# canonical set, no implicit translation.
# ---------------------------------------------------------------------------


def _resolve_tls_mode(
    tls_spec: Optional[Mapping[str, Any]], resolver: Resolver
) -> tuple[Optional[str], Optional[str]]:
    """Resolve ``tls.mode`` and ``tls.ca_certificate`` to their stored values.

    Returns ``(mode, ca_pem)``. Neither value is validated against any
    canonical vocabulary — the driver-specific materializer interprets the
    native string. ``ca_pem`` is ``None`` unless the connection provided
    a CA bundle.
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

    ca_value: Optional[str] = None
    raw_ca = tls_spec.get("ca_certificate")
    if raw_ca is not None:
        try:
            resolved = resolver.resolve(raw_ca)
        except KeyError:
            resolved = None
        if isinstance(resolved, str) and resolved:
            ca_value = resolved

    return mode, ca_value


def _ca_ssl_context(ca_pem: str, *, check_hostname: bool) -> _ssl.SSLContext:
    ctx = _ssl.create_default_context(cadata=ca_pem)
    ctx.check_hostname = check_hostname
    ctx.verify_mode = _ssl.CERT_REQUIRED
    return ctx


def _materialize_tls_postgres(mode: str, ca_pem: Optional[str]) -> Any:
    """Apply libpq-native SSL modes to asyncpg.

    asyncpg accepts ``disable``/``allow``/``prefer``/``require`` as
    strings; ``verify-ca``/``verify-full`` need an explicit SSLContext
    built from the connection's CA bundle.
    """
    if mode in ("disable", "allow", "prefer", "require"):
        return mode
    if mode == "verify-ca":
        if not ca_pem:
            raise ValueError(
                "tls.mode='verify-ca' requires tls.ca_certificate to resolve "
                "to a PEM certificate bundle"
            )
        return _ca_ssl_context(ca_pem, check_hostname=False)
    if mode == "verify-full":
        if not ca_pem:
            raise ValueError(
                "tls.mode='verify-full' requires tls.ca_certificate to resolve "
                "to a PEM certificate bundle"
            )
        return _ca_ssl_context(ca_pem, check_hostname=True)
    raise ValueError(
        f"postgresql tls.mode {mode!r} not recognized; expected one of: "
        "disable, allow, prefer, require, verify-ca, verify-full"
    )


def _materialize_tls_mysql(mode: str, ca_pem: Optional[str]) -> Any:
    """Apply MySQL-native SSL modes to aiomysql.

    aiomysql accepts ``False`` (no TLS), an SSLContext, or no argument at
    all — it does not accept native string modes. MySQL's vocabulary is
    ``DISABLED`` / ``PREFERRED`` / ``REQUIRED`` / ``VERIFY_CA`` /
    ``VERIFY_IDENTITY``; comparison is case-insensitive on the stored
    value.
    """
    canonical = mode.upper()
    if canonical == "DISABLED":
        return False
    if canonical in ("PREFERRED", "REQUIRED"):
        # Negotiate TLS without verifying the server certificate (the
        # connection didn't ship a CA bundle). ``check_hostname`` must be
        # False whenever ``verify_mode`` is ``CERT_NONE`` or CPython raises.
        ctx = _ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = _ssl.CERT_NONE
        return ctx
    if canonical == "VERIFY_CA":
        if not ca_pem:
            raise ValueError(
                "tls.mode='VERIFY_CA' requires tls.ca_certificate to resolve "
                "to a PEM certificate bundle"
            )
        return _ca_ssl_context(ca_pem, check_hostname=False)
    if canonical == "VERIFY_IDENTITY":
        if not ca_pem:
            raise ValueError(
                "tls.mode='VERIFY_IDENTITY' requires tls.ca_certificate to resolve "
                "to a PEM certificate bundle"
            )
        return _ca_ssl_context(ca_pem, check_hostname=True)
    raise ValueError(
        f"mysql tls.mode {mode!r} not recognized; expected one of: "
        "DISABLED, PREFERRED, REQUIRED, VERIFY_CA, VERIFY_IDENTITY"
    )


def _materialize_tls_for_driver(
    driver: str,
    tls_spec: Optional[Mapping[str, Any]],
    resolver: Resolver,
) -> tuple[Any, Optional[str], bool]:
    """Dispatch TLS materialization based on the SQLAlchemy driver string.

    Each branch speaks its driver's native SSL vocabulary. Connectors
    whose driver isn't listed here fall through to a portable default:
    if a CA bundle was provided, build a verifying SSLContext; otherwise
    pass the resolved mode through and let the downstream materializer
    decide.

    Returns ``(connect_args_value, raw_mode, has_ca_bundle)``. The raw
    mode and CA-presence flag travel with the transport so the ADBC
    URI builder can include ``sslmode=...`` and decide whether to
    demote (verify-ca / verify-full need a CA file path the URI can't
    inline).
    """
    mode, ca_pem = _resolve_tls_mode(tls_spec, resolver)
    if mode is None:
        return None, None, False

    base_driver = driver.split("+", 1)[0].lower()
    has_ca = ca_pem is not None

    if base_driver in ("postgresql", "postgres"):
        return _materialize_tls_postgres(mode, ca_pem), mode, has_ca

    if base_driver in ("mysql", "mariadb"):
        return _materialize_tls_mysql(mode, ca_pem), mode, has_ca

    if ca_pem:
        return _ca_ssl_context(ca_pem, check_hostname=False), mode, has_ca
    return mode, mode, has_ca


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


@dataclass(frozen=True)
class SqlAlchemyTransport:
    """Materialized SQLAlchemy transport.

    ``tls_mode`` carries the resolved ``tls.mode`` string (the
    connector's native vocabulary, e.g. PG ``"require"`` or MySQL
    ``"VERIFY_IDENTITY"``) so the ADBC URI builder can embed it as a
    libpq ``sslmode=`` parameter. ``tls_ca_bundle_present`` is True
    when a CA PEM was materialised into the SQLAlchemy SSLContext --
    the ADBC URI cannot inline raw PEM, so eligibility checks demote
    to SA when this is True.
    """

    engine: AsyncEngine
    driver: str
    dialect: str
    tls_mode: Optional[str] = None
    tls_ca_bundle_present: bool = False


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
    tls_value, tls_mode, tls_has_ca = _materialize_tls_for_driver(
        driver, spec.get("tls"), resolver
    )
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
    return SqlAlchemyTransport(
        engine=engine,
        driver=driver,
        dialect=base_dialect,
        tls_mode=tls_mode,
        tls_ca_bundle_present=tls_has_ca,
    )


# ---------------------------------------------------------------------------
# ADBC transport
#
# ADBC (Arrow Database Connectivity) drivers expose a synchronous DBAPI
# 2.0 connection that exchanges Arrow record batches with the database's
# native bulk protocol (libpq COPY BINARY for Postgres, Snowflake's
# internal stages, BigQuery's Storage Write API, …). This transport is
# the only path for drivers without an async SQLAlchemy driver
# (Snowflake, BigQuery); for Postgres it provides an alternative to the
# SQLAlchemy + asyncpg path.
#
# The closed enum for ``driver`` here mirrors the published
# ``AdbcTransport.driver`` enum in
# ``schemas.analitiq.ai/connector/latest.json``. Adding a driver here
# without first updating the schema produces connectors the validators
# will reject; adding to the schema without registering here produces
# connectors the engine cannot materialize.
# ---------------------------------------------------------------------------


# Driver -> dotted ADBC dbapi module path. Closed enum: extending this
# table requires a matching change in the published connector schema.
_ADBC_DRIVER_MODULES: Dict[str, str] = {
    "postgresql": "adbc_driver_postgresql.dbapi",
    "snowflake": "adbc_driver_snowflake.dbapi",
    "bigquery": "adbc_driver_bigquery.dbapi",
}


@dataclass(frozen=True)
class AdbcTransport:
    """Materialized ADBC transport.

    ADBC drivers are synchronous DBAPI 2.0. There is no connection pool;
    callers open a fresh connection via the ``connect`` callable and
    own its lifecycle (close on disconnect, drop on ingest failure to
    avoid reusing a poisoned handle). Driver-side concurrency is
    managed by running each cursor operation on a worker thread via
    ``asyncio.to_thread``.

    Frozen so a buggy caller cannot mutate ``driver`` after the
    transport is wired into a runtime — the driver string drives
    per-warehouse SQL dispatch in the destination handler.
    """

    connect: Callable[[], Any]
    driver: str


def _resolve_db_kwargs(
    raw: Optional[Mapping[str, Any]], resolver: Resolver
) -> Dict[str, Any]:
    """Resolve the optional ``db_kwargs`` block.

    Each value goes through the resolver so secrets and connection
    parameters flow in the same way as DSN bindings. ADBC accepts
    non-string scalar values; stringification is left to the driver.

    Keys whose values resolve to ``None`` are dropped — that matches
    the schema's treatment of optional credential fields, where an
    unset secret should not override the driver's own default. The
    drop is logged at INFO so an operator can distinguish "field was
    intentionally absent" from "the expression I authored resolved to
    None (likely an unbound placeholder or empty secret)".
    """
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        raise TypeError("adbc transport `db_kwargs` must be an object")
    out: Dict[str, Any] = {}
    for name, value in raw.items():
        resolved = resolver.resolve(value)
        if resolved is None:
            logger.info(
                "adbc db_kwargs[%s] resolved to None; passing driver default. "
                "If you intended a value, check the secret store and connection "
                "parameters.",
                name,
            )
            continue
        out[str(name)] = resolved
    return out


async def build_adbc_transport(
    spec: Mapping[str, Any], *, resolver: Resolver
) -> AdbcTransport:
    """Materialize an ADBC transport from a resolved connector spec.

    The spec shape matches ``AdbcTransport`` in the published connector
    schema: ``driver`` is required and closed to the registry,
    ``dsn`` and ``db_kwargs`` are both optional but at least one must
    be present. Drivers that accept all connection state via
    ``db_kwargs`` (Snowflake, BigQuery) typically omit ``dsn``.

    The probe ``SELECT 1`` fires at materialize time so a bad DSN,
    missing credential, or network reachability problem fails before
    the engine starts a pipeline run, not on the first batch.
    """
    driver = spec.get("driver")
    if not isinstance(driver, str) or not driver:
        raise ValueError(
            "adbc transport requires `driver` (one of "
            f"{sorted(_ADBC_DRIVER_MODULES)})"
        )
    driver = driver.lower()
    driver_module_path = _ADBC_DRIVER_MODULES.get(driver)
    if driver_module_path is None:
        raise ValueError(
            f"adbc transport driver {driver!r} is not registered; "
            f"known drivers: {sorted(_ADBC_DRIVER_MODULES)}"
        )

    # Validate the spec shape before touching the driver package so a
    # schema-violating connector still raises a useful spec error even
    # in environments where the driver wheel isn't installed.
    raw_dsn = spec.get("dsn")
    uri: Optional[str] = None
    if raw_dsn is not None:
        if not isinstance(raw_dsn, Mapping):
            raise TypeError(
                "adbc transport `dsn` must be the structured "
                "{kind: url_template, template, bindings} object"
            )
        uri = _render_url_template_dsn(raw_dsn, resolver)

    db_kwargs = _resolve_db_kwargs(spec.get("db_kwargs"), resolver)

    if uri is None and not db_kwargs:
        raise ValueError(
            "adbc transport requires at least one of `dsn` or `db_kwargs` "
            "(schema anyOf constraint)"
        )

    try:
        driver_module = importlib.import_module(driver_module_path)
    except ImportError as exc:
        raise RuntimeError(
            f"ADBC driver package not installed for driver={driver!r}: "
            f"{driver_module_path} is not importable ({exc}). Install the "
            f"matching `adbc-driver-*` extra."
        ) from exc

    def _open_connection() -> Any:
        # The driver's positional URI vs db_kwargs handling differs per
        # driver. Pass only what was provided so a Snowflake spec (db_kwargs
        # only) doesn't accidentally hand an empty string to ``connect``.
        if uri is not None and db_kwargs:
            return driver_module.connect(uri, db_kwargs=db_kwargs)
        if uri is not None:
            return driver_module.connect(uri)
        return driver_module.connect(db_kwargs=db_kwargs)

    probe_conn = await asyncio.to_thread(_open_connection)
    try:
        await asyncio.to_thread(_ping_adbc, probe_conn)
    finally:
        try:
            await asyncio.to_thread(probe_conn.close)
        except Exception:
            logger.debug("ADBC probe connection close failed", exc_info=True)

    return AdbcTransport(connect=_open_connection, driver=driver)


def _ping_adbc(conn: Any) -> None:
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT 1")
        cursor.fetchone()
    finally:
        cursor.close()


# ---------------------------------------------------------------------------
# HTTP transport
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
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
register_transport_kind("adbc", build_adbc_transport)
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
