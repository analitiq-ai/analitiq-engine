"""Materialize transports from connector definitions.

Reads a connector's ``transports.<ref>`` block, resolves expressions
through a typed :class:`~src.engine.resolver.ResolutionContext`, and
builds a concrete transport object: a SQLAlchemy ``AsyncEngine`` for
``kind: sqlalchemy``, an ``aiohttp.ClientSession`` for ``kind: http``.

The engine never inspects connection or connector contents directly.
Provider-specific shape lives in the connector definition; this factory
simply walks the resolved JSON and feeds it to the matching transport
adapter.
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
# Spec resolution
# ---------------------------------------------------------------------------


def _deep_merge(base: Mapping[str, Any], override: Mapping[str, Any]) -> Dict[str, Any]:
    """Override-wins deep merge; non-mapping leaves are replaced wholesale."""
    out: Dict[str, Any] = {k: copy.deepcopy(v) for k, v in base.items()}
    for k, v in override.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, Mapping):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = copy.deepcopy(v)
    return out


def _materialize_derived(
    connector: Mapping[str, Any],
    context: ResolutionContext,
) -> Dict[str, Any]:
    """Eagerly evaluate every entry in ``connector.derived`` against
    *context*. The resulting dict is what ``${derived.<name>}`` template
    substitutions will see.

    Resolution iterates fixpoint-style: each pass evaluates every still-
    pending entry against the values resolved so far. The loop terminates
    cleanly only when no entries remain pending. If a pass makes no
    forward progress (a cycle, or an entry that depends on something not
    in any scope) the original :class:`KeyError` from re-resolving one of
    the stuck entries is raised, naming the missing path.
    """
    declared = connector.get("derived") or {}
    if not declared:
        return {}
    if not isinstance(declared, Mapping):
        raise TypeError(
            f"connector {connector.get('slug')!r}: `derived` must be an object"
        )

    resolved: Dict[str, Any] = {}
    pending: Dict[str, Any] = dict(declared)
    while pending:
        progress = False
        next_pending: Dict[str, Any] = {}
        scratch_ctx = ResolutionContext(
            connector=context.connector,
            connection=context.connection,
            secrets=context.secrets,
            auth=context.auth,
            runtime=context.runtime,
            state=context.state,
            derived=resolved,
            request=context.request,
            response=context.response,
        )
        scratch_resolver = Resolver(scratch_ctx, functions=DEFAULT_FUNCTIONS)
        for name, expr in pending.items():
            try:
                resolved[name] = scratch_resolver.resolve(expr)
                progress = True
            except KeyError:
                next_pending[name] = expr
        if not progress:
            # Re-resolve one of the stuck expressions so the operator
            # sees the missing reference (or cycle) by name rather than
            # an opaque "still pending" message.
            stuck_name, stuck_expr = next(iter(next_pending.items()))
            try:
                scratch_resolver.resolve(stuck_expr)
            except KeyError as err:
                raise KeyError(
                    f"connector {connector.get('slug')!r}: derived entry "
                    f"{stuck_name!r} cannot be resolved (still pending: "
                    f"{sorted(next_pending)}): {err}"
                ) from err
            # Defensive: progress was false but re-resolving did not
            # raise, which means the resolver is non-deterministic.
            raise RuntimeError(
                f"connector {connector.get('slug')!r}: derived resolution "
                f"made no progress on {sorted(next_pending)}"
            )
        pending = next_pending
    return resolved


def resolve_transport_spec(
    connector: Mapping[str, Any],
    *,
    transport_ref: Optional[str] = None,
    context: ResolutionContext,
) -> Dict[str, Any]:
    """Pick ``transports[ref]``, merge ``transport_defaults``, resolve expressions.

    The returned dict contains only resolved primitives ready for the
    transport adapters. ``connector.derived`` entries are evaluated
    first so the transport spec can reference them via
    ``${derived.<name>}``.
    """
    transports = connector.get("transports") or {}
    if not transports:
        raise ValueError(
            f"Connector {connector.get('slug')!r} has no `transports` block; "
            f"cannot materialize transport"
        )
    ref = transport_ref or connector.get("default_transport")
    if not ref:
        raise ValueError(
            f"Connector {connector.get('slug')!r}: `transport_ref` not given "
            f"and `default_transport` not declared"
        )
    if ref not in transports:
        raise KeyError(
            f"Connector {connector.get('slug')!r}: transport {ref!r} not in "
            f"declared transports {sorted(transports)}"
        )
    derived = _materialize_derived(connector, context)
    enriched = ResolutionContext(
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
    defaults = connector.get("transport_defaults") or {}
    merged = _deep_merge(defaults, transports[ref])
    resolver = Resolver(enriched, functions=DEFAULT_FUNCTIONS)
    return resolver.resolve(merged)


# ---------------------------------------------------------------------------
# SQLAlchemy transport
# ---------------------------------------------------------------------------


@dataclass
class SqlAlchemyTransport:
    """Materialized SQLAlchemy transport.

    ``driver`` is the full SQLAlchemy driver string (``postgresql+asyncpg``);
    ``dialect`` is the base dialect (``postgresql``, ``mysql``) consumed by
    SQL generation code that branches on dialect.
    """

    engine: AsyncEngine
    driver: str
    dialect: str


_VERIFY_MODES = {
    "CERT_NONE": _ssl.CERT_NONE,
    "CERT_OPTIONAL": _ssl.CERT_OPTIONAL,
    "CERT_REQUIRED": _ssl.CERT_REQUIRED,
}


def _ssl_dict_to_context(spec: Mapping[str, Any]) -> _ssl.SSLContext:
    """Translate a declarative SSL dict (from a connector's ``lookup``) into
    an :class:`ssl.SSLContext`.

    Recognized keys:

    * ``verify_mode`` — one of ``CERT_NONE``, ``CERT_OPTIONAL``, ``CERT_REQUIRED``.
    * ``check_hostname`` — boolean.
    * ``ca_data`` — PEM string passed to :meth:`SSLContext.load_verify_locations`.
    * ``ca_path`` — filesystem path passed to the same method.

    CPython raises :class:`ValueError` if ``verify_mode`` is set to
    ``CERT_NONE`` while ``check_hostname`` is still ``True``, and vice
    versa, so the assignment order is computed explicitly here regardless
    of what order the connector declared the keys in.
    """
    raw_verify = spec.get("verify_mode")
    if raw_verify is not None and raw_verify not in _VERIFY_MODES:
        raise ValueError(
            f"connect_args.ssl.verify_mode {raw_verify!r} not recognized; "
            f"expected one of {sorted(_VERIFY_MODES)}"
        )
    raw_check = spec.get("check_hostname")
    if raw_check is not None and not isinstance(raw_check, bool):
        raise TypeError("connect_args.ssl.check_hostname must be a boolean")

    ctx = _ssl.create_default_context()

    # Determine the final state, then assign in a CPython-safe order.
    final_verify = (
        _VERIFY_MODES[raw_verify] if raw_verify is not None else ctx.verify_mode
    )
    final_check = raw_check if raw_check is not None else ctx.check_hostname
    if final_check and final_verify == _ssl.CERT_NONE:
        # The connector asked for an impossible combination — surface it
        # rather than silently flipping one of the two.
        raise ValueError(
            "connect_args.ssl: cannot set verify_mode=CERT_NONE with "
            "check_hostname=True"
        )

    if final_check:
        # Going to CERT_REQUIRED with hostname check: set verify_mode first,
        # hostname check second.
        ctx.verify_mode = final_verify
        ctx.check_hostname = True
    else:
        # Going to no hostname check (often paired with CERT_NONE):
        # disable hostname check first, then assign verify_mode.
        ctx.check_hostname = False
        ctx.verify_mode = final_verify

    if spec.get("ca_data"):
        ctx.load_verify_locations(cadata=spec["ca_data"])
    if spec.get("ca_path"):
        ctx.load_verify_locations(cafile=spec["ca_path"])
    return ctx


def _materialize_ssl_arg(value: Any) -> Any:
    """Translate a resolved ``ssl`` value into a driver-acceptable form.

    The connector lookup may produce booleans, dicts, or pass-through
    strings (asyncpg accepts ``"prefer"``, ``"require"``, etc. natively).
    Anything else is rejected — silent driver-default behavior has burned
    us before.
    """
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, _ssl.SSLContext):
        return value
    if isinstance(value, Mapping):
        return _ssl_dict_to_context(value)
    raise TypeError(
        f"Unsupported ssl arg type: {type(value).__name__}; "
        f"the connector's `connect_args.ssl` lookup must produce a bool, "
        f"string, or object with verify_mode/check_hostname"
    )


async def build_sqlalchemy_transport(spec: Mapping[str, Any]) -> SqlAlchemyTransport:
    """Build a probed :class:`AsyncEngine` from a resolved transport spec."""
    driver = spec.get("driver")
    if not isinstance(driver, str) or not driver:
        raise ValueError("sqlalchemy transport requires `driver` (e.g. 'postgresql+asyncpg')")
    dsn = spec.get("dsn")
    if not isinstance(dsn, str) or not dsn:
        raise ValueError(
            "sqlalchemy transport `dsn` must resolve to a non-empty string; "
            "use `{\"template\": \"<driver>://...\"}` in the connector definition"
        )
    raw_connect_args = spec.get("connect_args") or {}
    if not isinstance(raw_connect_args, Mapping):
        raise TypeError("sqlalchemy transport `connect_args` must be an object")
    connect_args: Dict[str, Any] = {k: v for k, v in raw_connect_args.items()}
    if "ssl" in connect_args:
        materialized = _materialize_ssl_arg(connect_args["ssl"])
        if materialized is None:
            del connect_args["ssl"]
        else:
            connect_args["ssl"] = materialized

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


async def build_http_transport(spec: Mapping[str, Any]) -> HttpTransport:
    base_url = spec.get("base_url")
    if not isinstance(base_url, str) or not base_url:
        raise ValueError(
            "http transport `base_url` must resolve to a non-empty string; "
            "use `{\"template\": \"https://${connection.discovered.api_domain}.example.com\"}` "
            "or a literal URL in the connector definition"
        )
    base_url = base_url.rstrip("/")

    raw_headers = spec.get("headers") or {}
    if not isinstance(raw_headers, Mapping):
        raise TypeError("http transport `headers` must be an object")
    headers: Dict[str, str] = {}
    for name, value in raw_headers.items():
        if value is None:
            continue
        headers[str(name)] = str(value)

    timeout_seconds = spec.get("timeout_seconds")
    if timeout_seconds is None:
        timeout_seconds = 30
    timeout = aiohttp.ClientTimeout(total=float(timeout_seconds))

    raw_rate_limit = spec.get("rate_limit") or {}
    if raw_rate_limit and not isinstance(raw_rate_limit, Mapping):
        raise TypeError("http transport `rate_limit` must be an object")
    rate_limiter: Optional[RateLimiter] = None
    if raw_rate_limit:
        max_requests = raw_rate_limit.get("max_requests")
        time_window = raw_rate_limit.get("time_window_seconds")
        if (max_requests is None) != (time_window is None):
            raise ValueError(
                "http transport `rate_limit` requires both `max_requests` "
                "and `time_window_seconds` (or neither)"
            )
        if max_requests is not None and time_window is not None:
            rate_limiter = RateLimiter(
                max_requests=int(max_requests),
                time_window=int(time_window),
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
# Transport kind registry
# ---------------------------------------------------------------------------


_TRANSPORT_BUILDERS: Dict[str, TransportBuilder] = {}


def register_transport_kind(kind: str, builder: TransportBuilder) -> None:
    """Register a builder for a transport ``kind``.

    Plugin packages call this at import time to teach the engine new
    transport families (kafka, s3, mongodb, …). The builder receives the
    resolved transport spec and must return an awaitable that yields a
    materialized transport object. Re-registering an existing kind raises
    :class:`ValueError`; call :func:`unregister_transport_kind` first if
    replacement is intended.

    A non-callable ``builder`` is rejected at registration time so the
    failure surfaces near the bug rather than later inside
    :func:`build_transport` when a pipeline tries to use it.
    """
    if not isinstance(kind, str) or not kind:
        raise ValueError("transport kind must be a non-empty string")
    if not callable(builder):
        raise TypeError(
            f"transport builder for {kind!r} must be callable; got "
            f"{type(builder).__name__}"
        )
    if kind in _TRANSPORT_BUILDERS:
        raise ValueError(
            f"transport kind {kind!r} already registered; call "
            f"unregister_transport_kind first to replace it"
        )
    _TRANSPORT_BUILDERS[kind] = builder


def unregister_transport_kind(kind: str) -> None:
    """Remove a previously registered transport ``kind``.

    Raises :class:`KeyError` with the list of currently registered kinds
    if ``kind`` is not registered. Primarily useful for test isolation
    and for plugins that need to swap an implementation.
    """
    if kind not in _TRANSPORT_BUILDERS:
        raise KeyError(
            f"transport kind {kind!r} not registered; "
            f"registered: {registered_transport_kinds()}"
        )
    del _TRANSPORT_BUILDERS[kind]


def registered_transport_kinds() -> list[str]:
    """Return the sorted list of currently registered transport kinds."""
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
    """Materialize a transport from a connector definition.

    Dispatches via the transport kind registry. The two built-in families
    (``sqlalchemy``, ``http``) are registered at import time; plugins can
    add more via :func:`register_transport_kind`.
    """
    spec = resolve_transport_spec(
        connector, transport_ref=transport_ref, context=context
    )
    kind = spec.get("kind")
    if not kind:
        raise ValueError(
            f"Resolved transport spec missing `kind`; connector "
            f"{connector.get('slug')!r}, transport {transport_ref!r}"
        )
    builder = _TRANSPORT_BUILDERS.get(kind)
    if builder is None:
        raise NotImplementedError(
            f"Unsupported transport kind: {kind!r}; "
            f"registered: {registered_transport_kinds()}"
        )
    return await builder(spec)
