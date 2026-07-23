"""Materialize transports from connector definitions.

The connector contract owns provider-specific transport recipes. This
module reads ``connector.transports.<ref>``, resolves expressions via a
typed :class:`~cdk.resolver.ResolutionContext`, applies the
declared per-binding encodings to render the DSN, and produces a
concrete transport object.

Currently registered transport families:

* ``sqlalchemy`` — SQLAlchemy engine for database connectors. The engine
  flavour follows the driver's own capability: an async dialect (Postgres
  asyncpg, MySQL aiomysql) gets an ``AsyncEngine``; a sync-only dialect
  (Redshift ``redshift+redshift_connector``) gets a plain ``Engine`` whose
  operations callers run via ``asyncio.to_thread``.
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
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Union

from sqlalchemy import create_engine, event
from sqlalchemy import text as _sa_text
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

if TYPE_CHECKING:
    import aiohttp

from cdk._extras import reraise_for_missing_extra
from cdk.derived_functions import DEFAULT_FUNCTIONS
from cdk.exceptions import TransportSpecError, UnresolvedValueError
from cdk.rate_limiter import RateLimiter
from cdk.resolver import ResolutionContext, Resolver
from cdk.sql.dialects import SqlDialect

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DSN URL-template renderer
# ---------------------------------------------------------------------------


# Encodings declared by the connector contract for each DSN binding. The
# engine never invents URL syntax — it applies the declared encoding to
# the resolved value and substitutes into the connector-authored template.
_ENCODING_QUOTES: dict[str, str] = {
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


def _render_url_template_dsn(dsn_spec: Mapping[str, Any], resolver: Resolver) -> str:
    """Render a connector-authored ``dsn.url_template`` recipe.

    The recipe has shape::

      {
        "kind": "url_template",
        "template":
          "postgresql+asyncpg://{username}:{password}@{host}:{port}/{database}",
        "bindings": {
          "username": {
            "value": {"ref": "connection.parameters.username"},
            "encoding": "url_userinfo"
          },
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
        raise TransportSpecError(
            f"Unsupported dsn.kind {kind!r}; the connector contract currently "
            f"defines only 'url_template'"
        )
    template = dsn_spec.get("template")
    if not isinstance(template, str) or not template:
        raise TransportSpecError("dsn.template must be a non-empty string")

    raw_bindings = dsn_spec.get("bindings") or {}
    if not isinstance(raw_bindings, Mapping):
        raise TransportSpecError("dsn.bindings must be an object")

    rendered: dict[str, str] = {}
    for name, entry in raw_bindings.items():
        if not isinstance(entry, Mapping):
            raise TransportSpecError(
                f"dsn.bindings.{name} must be an object with 'value' and 'encoding'"
            )
        if "value" not in entry or "encoding" not in entry:
            raise TransportSpecError(
                f"dsn.bindings.{name} requires both 'value' and 'encoding'"
            )
        encoding = entry["encoding"]
        if encoding not in _ENCODING_QUOTES:
            raise TransportSpecError(
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
        raise TransportSpecError(
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
# TLS resolution (database transports)
#
# Each driver speaks its own native SSL vocabulary. The RESOLUTION step
# (engine/shell side) only extracts the stored ``tls.mode`` string and the
# CA bundle PEM — both JSON-safe. The BUILD step (worker side) turns them
# into the driver's connect arguments via the connector dialect's
# ``build_tls_connect_args`` hook; the per-driver logic lives in each
# connector package, never here. ``ca_ssl_context`` is the shared helper
# those packages call to build a verifying SSLContext from a CA bundle.
# ---------------------------------------------------------------------------


def _resolve_tls_mode(
    tls_spec: Mapping[str, Any] | None, resolver: Resolver
) -> tuple[str | None, str | None]:
    """Resolve ``tls.mode`` and ``tls.ca_certificate`` to their stored values.

    Returns ``(mode, ca_pem)``. Neither value is validated against any
    canonical vocabulary — the driver-specific materializer interprets the
    native string. ``ca_pem`` is ``None`` unless the connection provided
    a CA bundle.

    The CA ref is optional at the connection level: the connector's shared
    transport recipe declares it for every connection, and one that pins
    no CA simply does not carry the secret — the resolver's typed
    missing-data signal (:class:`UnresolvedValueError`), logged at INFO
    and treated as "no CA", mirroring the optional ``db_kwargs`` policy.
    Everything else fails loud: an authoring defect (typo'd scope raises
    plain ``KeyError``) propagates, and a CA that resolves to ``None``,
    an empty string, or a non-string was provided-but-unusable — silently
    proceeding without it would hand the dialect whatever weaker trust it
    falls back to (issue #380).
    """
    if tls_spec is None:
        return None, None
    if not isinstance(tls_spec, Mapping):
        raise TransportSpecError("transports.<ref>.tls must be an object")

    raw_mode = tls_spec.get("mode")
    if raw_mode is None:
        return None, None
    mode = resolver.resolve(raw_mode)
    if mode is None:
        return None, None
    if not isinstance(mode, str):
        raise TransportSpecError(
            f"tls.mode must resolve to a string, got {type(mode).__name__}"
        )

    ca_value: str | None = None
    raw_ca = tls_spec.get("ca_certificate")
    if raw_ca is not None:
        try:
            resolved = resolver.resolve(raw_ca)
        except UnresolvedValueError:
            logger.info(
                "tls.ca_certificate omitted: optional CA ref references a "
                "value the connection does not supply; proceeding without "
                "CA pinning."
            )
        else:
            if not isinstance(resolved, str) or not resolved:
                # Type name only — the ref may be mis-wired at another
                # secret, so the resolved value must never reach the error.
                kind = "an empty string" if resolved == "" else type(resolved).__name__
                raise TransportSpecError(
                    f"tls.ca_certificate resolved to {kind}; a provided CA "
                    f"bundle must resolve to a non-empty PEM string — remove "
                    f"the value to proceed without CA pinning, or supply the "
                    f"PEM"
                )
            ca_value = resolved

    return mode, ca_value


def ca_ssl_context(ca_pem: str, *, check_hostname: bool) -> _ssl.SSLContext:
    """Build a verifying SSLContext from a PEM CA bundle.

    Shared helper for connector packages' ``build_tls_connect_arg``
    implementations (postgres verify-ca/-full, MySQL VERIFY_CA/_IDENTITY).
    """
    ctx = _ssl.create_default_context(cadata=ca_pem)
    ctx.check_hostname = check_hostname
    ctx.verify_mode = _ssl.CERT_REQUIRED
    return ctx


# ---------------------------------------------------------------------------
# Spec resolution
# ---------------------------------------------------------------------------


def _deep_merge(base: Mapping[str, Any], override: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {k: copy.deepcopy(v) for k, v in base.items()}
    for k, v in override.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, Mapping):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = copy.deepcopy(v)
    return out


def _select_transport(
    connector: Mapping[str, Any], transport_ref: str | None
) -> tuple[str, Mapping[str, Any]]:
    """Pick the ``transports[ref]`` block, applying ``transport_defaults``."""
    transports = connector.get("transports") or {}
    if not transports:
        raise TransportSpecError(
            f"Connector {connector.get('connector_id')!r} has no `transports` block; "
            f"cannot materialize transport"
        )
    ref = transport_ref or connector.get("default_transport")
    if not ref:
        raise TransportSpecError(
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

    ``is_async`` records which engine flavour ``engine`` carries: an
    :class:`AsyncEngine` for dialects with an async driver, or a plain
    sync :class:`Engine` for sync-only drivers (callers run sync-engine
    operations via ``asyncio.to_thread``).
    """

    engine: AsyncEngine | Engine
    driver: str
    dialect: str
    is_async: bool


def resolve_sqlalchemy_spec(
    spec: Mapping[str, Any], *, resolver: Resolver
) -> dict[str, Any]:
    """Resolve a sqlalchemy transport spec to JSON-safe values (no objects).

    Output carries everything the worker-side build needs: the rendered
    DSN (secrets in place), the resolved TLS inputs as plain strings, and
    the computed engine kwargs. Safe to serialize into a worker bootstrap.
    """
    driver = spec.get("driver")
    if not isinstance(driver, str) or not driver:
        raise TransportSpecError(
            "sqlalchemy transport requires `driver` (e.g. 'postgresql+asyncpg')"
        )

    raw_dsn = spec.get("dsn")
    if not isinstance(raw_dsn, Mapping):
        raise TransportSpecError(
            "sqlalchemy transport `dsn` must be the structured "
            "{kind: url_template, template, bindings} object"
        )
    dsn = _render_url_template_dsn(raw_dsn, resolver)

    mode, ca_pem = _resolve_tls_mode(spec.get("tls"), resolver)

    options = spec.get("options") or {}
    if not isinstance(options, Mapping):
        raise TransportSpecError("sqlalchemy transport `options` must be an object")
    engine_kwargs: dict[str, Any] = {}
    if "pool_size" in options:
        engine_kwargs["pool_size"] = int(options["pool_size"])
    if "max_overflow" in options:
        engine_kwargs["max_overflow"] = int(options["max_overflow"])
    engine_kwargs["pool_pre_ping"] = bool(options.get("pool_pre_ping", True))
    if "echo" in options:
        engine_kwargs["echo"] = bool(options["echo"])

    return {
        "transport_type": "sqlalchemy",
        "driver": driver,
        "dsn": dsn,
        "tls": {"mode": mode, "ca_pem": ca_pem} if mode is not None else None,
        "engine_kwargs": engine_kwargs,
    }


def _dialect_is_async(dsn: str) -> bool:
    """Whether the DSN's SQLAlchemy dialect ships an async driver.

    The dialect class itself declares the capability
    (``Dialect.is_async``), so no driver list is hardcoded here: any
    sync-only dialect (e.g. ``redshift+redshift_connector``) routes to
    the sync engine path automatically.
    """
    return bool(getattr(make_url(dsn).get_dialect(), "is_async", False))


def _attach_tls_verification(engine: Engine, sql_dialect: Any, mode: str) -> None:
    """Run the dialect's TLS probe on every new DBAPI connection.

    Registered on the pool ``connect`` event so the build-time ``SELECT 1``
    probe and every connection the pool later opens (reconnects,
    ``pool_pre_ping`` replacements, overflow) pass the same check. Connect
    arguments alone cannot guarantee encryption — a driver may silently
    skip the TLS handshake when the server does not advertise the
    capability (aiomysql does, for every mode) — so the declared mode is
    verified against the established session, not the requested
    configuration. For an async engine the listener is registered on its
    ``sync_engine`` facade; the event fires inside the greenlet bridge, so
    the dialect's DBAPI cursor calls work on the adapted connection.
    """
    if type(sql_dialect).verify_tls_state is SqlDialect.verify_tls_state:
        # Armed-but-vacuous must be greppable: with the inherited no-op the
        # declared mode is only as strong as the driver's own connect-arg
        # enforcement, which some drivers silently skip.
        logger.info(
            "TLS mode %r declared but dialect %r inherits the no-op "
            "verify_tls_state; the established session is not verified "
            "post-connect — encryption rests on the driver honoring its "
            "connect args.",
            mode,
            getattr(sql_dialect, "name", type(sql_dialect).__name__),
        )

    @event.listens_for(engine, "connect")
    def _verify_tls(dbapi_connection: Any, connection_record: Any) -> None:
        sql_dialect.verify_tls_state(dbapi_connection, mode)


def _build_and_probe_sync_engine(
    dsn: str,
    connect_args: Mapping[str, Any],
    engine_kwargs: Mapping[str, Any],
    *,
    sql_dialect: Any,
    tls_mode: str | None,
) -> Engine:
    """Build a sync engine and run the ``SELECT 1`` probe (worker thread)."""
    engine = create_engine(dsn, connect_args=dict(connect_args), **dict(engine_kwargs))
    if tls_mode is not None:
        _attach_tls_verification(engine, sql_dialect, tls_mode)
    try:
        with engine.connect() as conn:
            conn.execute(_sa_text("SELECT 1"))
    except Exception:
        engine.dispose()
        raise
    return engine


async def build_sqlalchemy_from_spec(
    resolved: Mapping[str, Any], *, sql_dialect: Any = None
) -> SqlAlchemyTransport:
    """Build the SQLAlchemy transport from a resolved spec (worker side).

    The engine flavour follows the driver's capability: an async dialect
    gets ``create_async_engine``; a sync-only dialect (no async SQLAlchemy
    driver exists, e.g. Redshift's vendor-supported ``redshift_connector``)
    gets a plain ``create_engine`` built and probed on a worker thread.

    TLS connect arguments are produced by the connector dialect's
    ``build_tls_connect_args`` hook — the per-driver SSL vocabulary lives
    in the connector package. Most drivers take a single ``ssl`` argument;
    drivers whose TLS configuration spans several connect parameters
    (``redshift_connector``: ``ssl`` + ``sslmode``) return them all. A
    declared TLS mode without a dialect that implements the hook fails
    loudly.

    A declared TLS mode also arms the dialect's ``verify_tls_state`` hook
    on the pool's connect event before the probe runs — the probe
    connection and every connection the pool ever opens are checked
    against the established session, because connect arguments only
    request TLS and a driver can silently proceed in plaintext (see
    :func:`_attach_tls_verification`).
    """
    driver = resolved["driver"]
    connect_args: dict[str, Any] = {}
    tls = resolved.get("tls")
    tls_mode = tls.get("mode") if tls is not None else None
    if tls is not None and tls_mode is not None:
        if sql_dialect is None:
            raise ValueError(
                f"transport for driver {driver!r} declares tls but no "
                f"connector dialect was supplied; the connector package's "
                f"dialect implements build_tls_connect_args"
            )
        connect_args.update(
            sql_dialect.build_tls_connect_args(tls_mode, tls.get("ca_pem"))
        )

    dsn = resolved["dsn"]
    engine_kwargs = dict(resolved.get("engine_kwargs") or {})
    is_async = _dialect_is_async(dsn)
    engine: AsyncEngine | Engine
    if is_async:
        async_engine = create_async_engine(
            dsn, connect_args=connect_args, **engine_kwargs
        )
        if tls_mode is not None:
            _attach_tls_verification(async_engine.sync_engine, sql_dialect, tls_mode)
        try:
            async with async_engine.connect() as conn:
                await conn.execute(_sa_text("SELECT 1"))
        except Exception:
            await async_engine.dispose()
            raise
        engine = async_engine
    else:
        engine = await asyncio.to_thread(
            _build_and_probe_sync_engine,
            dsn,
            connect_args,
            engine_kwargs,
            sql_dialect=sql_dialect,
            tls_mode=tls_mode,
        )

    base_dialect = driver.split("+", 1)[0]
    return SqlAlchemyTransport(
        engine=engine,
        driver=driver,
        dialect=base_dialect,
        is_async=is_async,
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


# ADBC dbapi modules follow the upstream packaging convention
# ``adbc_driver_{driver}.dbapi`` (adbc-driver-postgresql ships
# adbc_driver_postgresql, etc.). The engine derives the module from the
# driver string instead of keeping a table — the published connector
# schema's ``AdbcTransport.driver`` enum remains the validator of which
# drivers connectors may declare, and the connector package ships the
# matching wheel in its requirements.
def _adbc_dbapi_module_path(driver: str) -> str:
    return f"adbc_driver_{driver.lower()}.dbapi"


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
    raw: Mapping[str, Any] | None, resolver: Resolver
) -> dict[str, Any]:
    """Resolve the optional ``db_kwargs`` block into ADBC option strings.

    Each value goes through the resolver so secrets and connection
    parameters flow in the same way as DSN bindings, then renders to its
    ADBC option string (see :func:`_as_adbc_option_value`): ADBC database
    and connection options are string-valued, so a typed connection input
    (an integer port, a boolean flag) must reach the driver as a string —
    the same form the DSN path produces — rather than relying on the
    manager's value-type dispatch hitting a typed setter the driver may
    not implement.

    A db_kwargs entry is an independent optional driver argument (unlike a
    DSN binding, which a URL template structurally requires). An entry is
    dropped when it carries no value, in two cases:

    * the ref targets a connection input the user did not supply, so the
      resolver raises :class:`UnresolvedValueError` (the missing-data case);
    * the expression resolves to ``None`` (an input present but null).

    Both mean "no value" and let the driver apply its own default, realising
    the connector contract's ``required: false`` ("resolution need not produce
    a value"). This drop is safe because every *required* input is enforced
    up front by ``ConnectionRuntime._validate_connection_contract`` before any
    binding is resolved, so a missing value here is necessarily optional.
    Authoring defects (an unknown scope, a malformed node) raise other
    exceptions and still propagate. Each drop is logged at INFO.
    """
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        raise TransportSpecError("adbc transport `db_kwargs` must be an object")
    out: dict[str, Any] = {}
    for name, value in raw.items():
        try:
            resolved = resolver.resolve(value)
        except UnresolvedValueError:
            logger.info(
                "adbc db_kwargs[%s] omitted: optional binding references a "
                "connection input that is not set; passing driver default.",
                name,
            )
            continue
        if resolved is None:
            logger.info(
                "adbc db_kwargs[%s] resolved to None; passing driver default. "
                "If you intended a value, check the secret store and connection "
                "parameters.",
                name,
            )
            continue
        out[str(name)] = _as_adbc_option_value(name, resolved)
    return out


def _as_adbc_option_value(name: str, value: Any) -> str:
    """Render a resolved db_kwargs scalar to its ADBC option string.

    ADBC option values are strings: ``AdbcDatabaseSetOption`` takes a string,
    while the typed setters (``SetOptionInt``/``SetOptionDouble``/...) are
    optional driver extensions — passing a native ``int`` makes the manager
    dispatch to a typed setter the driver may not implement (the Snowflake
    driver rejects it outright). Booleans render lowercase to match the
    ADBC/driver convention. A non-scalar value is an authoring defect.
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float, str)):
        return str(value)
    raise TransportSpecError(
        f"adbc db_kwargs[{name}] resolved to a {type(value).__name__}; "
        f"ADBC option values must be scalars (string, number, or boolean)"
    )


def resolve_adbc_spec(spec: Mapping[str, Any], *, resolver: Resolver) -> dict[str, Any]:
    """Resolve an adbc transport spec to JSON-safe values (no objects).

    The spec shape matches ``AdbcTransport`` in the published connector
    schema: ``driver`` is required (the schema enum validates its value),
    ``dsn`` and ``db_kwargs`` are both optional but at least one must be
    present. Drivers that accept all connection state via ``db_kwargs``
    (Snowflake, BigQuery) typically omit ``dsn``.
    """
    driver = spec.get("driver")
    if not isinstance(driver, str) or not driver:
        raise TransportSpecError("adbc transport requires a non-empty `driver`")
    driver = driver.lower()

    raw_dsn = spec.get("dsn")
    uri: str | None = None
    if raw_dsn is not None:
        if not isinstance(raw_dsn, Mapping):
            raise TransportSpecError(
                "adbc transport `dsn` must be the structured "
                "{kind: url_template, template, bindings} object"
            )
        uri = _render_url_template_dsn(raw_dsn, resolver)

    db_kwargs = _resolve_db_kwargs(spec.get("db_kwargs"), resolver)

    if uri is None and not db_kwargs:
        raise TransportSpecError(
            "adbc transport requires at least one of `dsn` or `db_kwargs` "
            "(schema anyOf constraint)"
        )

    return {
        "transport_type": "adbc",
        "driver": driver,
        "uri": uri,
        "db_kwargs": db_kwargs,
    }


async def build_adbc_from_spec(
    resolved: Mapping[str, Any], *, sql_dialect: Any = None
) -> AdbcTransport:
    """Build the ADBC transport from a resolved spec (worker side).

    The probe ``SELECT 1`` fires at build time so a bad DSN, missing
    credential, or network reachability problem fails before the engine
    starts a pipeline run, not on the first batch.
    """
    driver = resolved["driver"]
    uri = resolved.get("uri")
    db_kwargs = dict(resolved.get("db_kwargs") or {})

    driver_module_path = _adbc_dbapi_module_path(driver)
    try:
        driver_module = importlib.import_module(driver_module_path)
    except ImportError as exc:
        raise RuntimeError(
            f"ADBC driver package not installed for driver={driver!r}: "
            f"{driver_module_path} is not importable ({exc}). The connector "
            f"package ships the matching adbc-driver-* wheel — install the "
            f"connector."
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
    headers: dict[str, str]
    rate_limiter: RateLimiter | None = None


def resolve_http_spec(spec: Mapping[str, Any], *, resolver: Resolver) -> dict[str, Any]:
    """Resolve an http transport spec to JSON-safe values (no objects)."""
    raw_base = spec.get("base_url")
    if raw_base is None:
        raise TransportSpecError("http transport `base_url` is required")
    base_url = resolver.resolve(raw_base)
    if not isinstance(base_url, str) or not base_url:
        raise TransportSpecError(
            "http transport `base_url` must resolve to a non-empty string"
        )
    base_url = base_url.rstrip("/")

    raw_headers = spec.get("headers") or {}
    if not isinstance(raw_headers, Mapping):
        raise TransportSpecError("http transport `headers` must be an object")
    headers: dict[str, str] = {}
    for name, value in raw_headers.items():
        resolved = resolver.resolve(value)
        if resolved is None:
            continue
        headers[str(name)] = str(resolved)

    timeout_seconds = spec.get("timeout_seconds")
    if timeout_seconds is None:
        timeout_seconds = 30

    raw_rate_limit = spec.get("rate_limit") or {}
    rate_limit: dict[str, int] | None = None
    if raw_rate_limit:
        if not isinstance(raw_rate_limit, Mapping):
            raise TransportSpecError("http transport `rate_limit` must be an object")
        max_requests = raw_rate_limit.get("max_requests")
        time_window = raw_rate_limit.get("time_window_seconds")
        if (max_requests is None) != (time_window is None):
            raise TransportSpecError(
                "http transport `rate_limit` requires both `max_requests` "
                "and `time_window_seconds` (or neither)"
            )
        if max_requests is not None and time_window is not None:
            rate_limit = {
                "max_requests": int(max_requests),
                "time_window_seconds": int(time_window),
            }

    return {
        "transport_type": "http",
        "base_url": base_url,
        "headers": headers,
        "timeout_seconds": float(timeout_seconds),
        "rate_limit": rate_limit,
    }


async def build_http_from_spec(
    resolved: Mapping[str, Any], *, sql_dialect: Any = None
) -> HttpTransport:
    """Build the HTTP transport from a resolved spec (worker side)."""
    # aiohttp is the only ``api`` extra dependency the CDK pulls; import it
    # lazily so a database-only (control-plane / SQL) install never needs it.
    try:
        import aiohttp
    except ImportError as exc:
        reraise_for_missing_extra(
            exc,
            feature="the HTTP transport (API connectors)",
            extra="api",
            modules=("aiohttp",),
        )

    headers = dict(resolved.get("headers") or {})
    timeout = aiohttp.ClientTimeout(total=float(resolved["timeout_seconds"]))

    rate_limiter: RateLimiter | None = None
    rl = resolved.get("rate_limit")
    if rl:
        rate_limiter = RateLimiter(
            max_requests=int(rl["max_requests"]),
            time_window=int(rl["time_window_seconds"]),
        )

    connector = aiohttp.TCPConnector(limit=100, limit_per_host=10)
    session = aiohttp.ClientSession(
        timeout=timeout, connector=connector, headers=headers
    )
    return HttpTransport(
        session=session,
        base_url=resolved["base_url"],
        headers=headers,
        rate_limiter=rate_limiter,
    )


# ---------------------------------------------------------------------------
# Transport-type registry (closed enum from the connector contract)
# ---------------------------------------------------------------------------


# A built transport. The three families share no base class; callers
# isinstance-narrow this union to wire the concrete objects.
Transport = Union[SqlAlchemyTransport, AdbcTransport, HttpTransport]

# Phase callables: the resolve phase is sync and yields a JSON-safe dict;
# the build phase is async and yields a live transport. Both take keyword
# arguments only beyond the leading positional, so the signature is left
# open (``...``) — the registry validates callability at registration.
ResolveSpec = Callable[..., dict[str, Any]]
BuildFromSpec = Callable[..., Awaitable[Transport]]


@dataclass(frozen=True)
class TransportKind:
    """A registered transport type: its resolve and build phases.

    ``resolve_spec(spec, resolver)`` runs on the trusted side and returns a
    JSON-safe dict (secrets in values, no constructed objects) — safe to
    hand a connector worker as part of its launch bootstrap.
    ``build_from_spec(resolved, sql_dialect=...)`` runs wherever the
    connector executes and constructs the live transport objects.
    """

    resolve_spec: ResolveSpec
    build_from_spec: BuildFromSpec


_TRANSPORT_KINDS: dict[str, TransportKind] = {}


def register_transport_kind(
    transport_type: str,
    *,
    resolve_spec: ResolveSpec,
    build_from_spec: BuildFromSpec,
) -> None:
    """Register the resolve/build pair for a connector ``transport_type``."""
    if not isinstance(transport_type, str) or not transport_type:
        raise ValueError("transport_type must be a non-empty string")
    if not callable(resolve_spec) or not callable(build_from_spec):
        raise TypeError(
            f"transport kind {transport_type!r} requires callable "
            f"resolve_spec and build_from_spec"
        )
    if transport_type in _TRANSPORT_KINDS:
        raise ValueError(
            f"transport_type {transport_type!r} already registered; "
            f"call unregister_transport_kind first"
        )
    _TRANSPORT_KINDS[transport_type] = TransportKind(
        resolve_spec=resolve_spec, build_from_spec=build_from_spec
    )


def unregister_transport_kind(transport_type: str) -> None:
    if transport_type not in _TRANSPORT_KINDS:
        raise KeyError(
            f"transport_type {transport_type!r} not registered; "
            f"registered: {registered_transport_kinds()}"
        )
    del _TRANSPORT_KINDS[transport_type]


def registered_transport_kinds() -> list[str]:
    return sorted(_TRANSPORT_KINDS)


register_transport_kind(
    "sqlalchemy",
    resolve_spec=resolve_sqlalchemy_spec,
    build_from_spec=build_sqlalchemy_from_spec,
)
register_transport_kind(
    "adbc", resolve_spec=resolve_adbc_spec, build_from_spec=build_adbc_from_spec
)
register_transport_kind(
    "http", resolve_spec=resolve_http_spec, build_from_spec=build_http_from_spec
)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def resolve_transport_spec(
    connector: Mapping[str, Any],
    *,
    transport_ref: str | None = None,
    context: ResolutionContext,
) -> dict[str, Any]:
    """Resolve a connector's transport into a JSON-safe spec (trusted side).

    The output contains rendered/resolved values only (DSN with secrets in
    place, db_kwargs, TLS mode + CA PEM, headers, engine kwargs) — no
    SQLAlchemy engines, sessions, SSLContexts, or driver closures. It is
    what a connector worker receives in its launch bootstrap.
    """
    _ref, merged = _select_transport(connector, transport_ref)
    transport_type = merged.get("transport_type")
    if not transport_type:
        raise TransportSpecError(
            f"Resolved transport spec missing `transport_type`; connector "
            f"{connector.get('connector_id')!r}, transport {transport_ref!r}"
        )
    kind = _TRANSPORT_KINDS.get(transport_type)
    if kind is None:
        raise NotImplementedError(
            f"Unsupported transport_type: {transport_type!r}; "
            f"registered: {registered_transport_kinds()}"
        )
    resolver = Resolver(context, functions=DEFAULT_FUNCTIONS)
    return kind.resolve_spec(merged, resolver=resolver)


async def build_transport_from_spec(
    resolved: Mapping[str, Any], *, sql_dialect: Any = None
) -> Transport:
    """Build a live transport from a resolved spec (connector side)."""
    transport_type = resolved.get("transport_type")
    kind = _TRANSPORT_KINDS.get(transport_type or "")
    if kind is None:
        raise NotImplementedError(
            f"Unsupported transport_type: {transport_type!r}; "
            f"registered: {registered_transport_kinds()}"
        )
    return await kind.build_from_spec(resolved, sql_dialect=sql_dialect)


async def build_transport(
    connector: Mapping[str, Any],
    *,
    transport_ref: str | None = None,
    context: ResolutionContext,
    sql_dialect: Any = None,
) -> Transport:
    """Resolve and build in one step (in-process path: control-plane, tests)."""
    resolved = resolve_transport_spec(
        connector, transport_ref=transport_ref, context=context
    )
    return await build_transport_from_spec(resolved, sql_dialect=sql_dialect)
