"""Typed transport materialization.

Bridges the resolved-runtime layer (typed
:class:`~src.engine.resolved.types.SqlAlchemyTransport` /
:class:`~src.engine.resolved.types.HttpTransport`) to the live transport
factories in :mod:`src.shared.transport_factory`. This sidesteps the
generic :class:`~src.engine.resolver.Resolver`, which conflates the
schema field name ``template`` (under ``dsn``) with the expression
marker ``{"template": "..."}`` and rejects the structured form the
schema actually uses.

Two helpers:

* :func:`materialize_typed_transport` — turn a typed transport spec plus
  the connection's resolution context into a live transport.
* :func:`resolve_template_string` — minimal ``${dotted.ref}`` substitution
  shared with the typed factories.
"""

from __future__ import annotations

from typing import Any, Mapping
from urllib.parse import quote, quote_plus

from src.engine.resolved import HttpTransport as ResolvedHttpTransport
from src.engine.resolved import SqlAlchemyTransport as ResolvedSqlAlchemyTransport
from src.engine.resolver import ResolutionContext
from src.shared.transport_factory import (
    HttpTransport,
    SqlAlchemyTransport,
    build_http_transport,
    build_sqlalchemy_transport,
)


# ---------------------------------------------------------------------------
# Reference / template helpers
# ---------------------------------------------------------------------------


def resolve_template_string(template: str, context: ResolutionContext) -> str:
    """Substitute ``${dotted.ref}`` placeholders against ``context``.

    Mirrors :meth:`~src.engine.resolver.Resolver._resolve_template` but is
    callable on a bare string without going through the full expression
    dispatcher (which doesn't know that ``template`` can be a literal
    field name inside ``dsn``).
    """
    out: list[str] = []
    i = 0
    n = len(template)
    while i < n:
        j = template.find("${", i)
        if j < 0:
            out.append(template[i:])
            break
        out.append(template[i:j])
        k = template.find("}", j + 2)
        if k < 0:
            raise ValueError(
                f"Unterminated ${{...}} placeholder in template: {template!r}"
            )
        path = template[j + 2 : k]
        value = context.lookup(path)
        if value is None:
            raise KeyError(
                f"Template substitution {path!r} resolved to None in {template!r}"
            )
        if not isinstance(value, (str, int, float, bool)):
            raise TypeError(
                f"Template substitution {path!r} in {template!r} resolved "
                f"to {type(value).__name__}; only scalars are allowed inside ${{...}}"
            )
        out.append(str(value))
        i = k + 1
    return "".join(out)


def _resolve_field(value: Any, context: ResolutionContext) -> Any:
    """Resolve a single typed-spec field that may carry a ``{ref}`` /
    ``{template}`` / literal payload."""
    if isinstance(value, Mapping):
        if "ref" in value and len(value) == 1:
            return context.lookup(value["ref"])
        if "template" in value and len(value) == 1:
            tpl = value["template"]
            if not isinstance(tpl, str):
                raise TypeError(f"`template` must be a string, got {type(tpl).__name__}")
            return resolve_template_string(tpl, context)
        if "literal" in value and len(value) == 1:
            return value["literal"]
    return value


# ---------------------------------------------------------------------------
# DSN binding encodings
# ---------------------------------------------------------------------------


def _encode_binding(value: Any, encoding: str | None) -> str:
    """Apply the per-binding URL encoding declared by the schema."""
    if value is None:
        return ""
    encoded = str(value)
    enc = (encoding or "raw").lower()
    if enc == "raw":
        return encoded
    if enc == "url_userinfo":
        return quote(encoded, safe="")
    if enc == "url_path_segment":
        return quote(encoded, safe="")
    if enc == "url_query":
        return quote_plus(encoded)
    if enc == "host":
        # IPv6 literals need bracketing; everything else passes through.
        if ":" in encoded and not encoded.startswith("["):
            return f"[{encoded}]"
        return encoded
    raise ValueError(f"Unknown DSN binding encoding: {encoding!r}")


def _resolve_dsn_bindings(
    bindings: Mapping[str, Mapping[str, Any]],
    context: ResolutionContext,
) -> dict[str, str]:
    out: dict[str, str] = {}
    for name, spec in bindings.items():
        if not isinstance(spec, Mapping):
            raise TypeError(
                f"binding {name!r} must be an object, got {type(spec).__name__}"
            )
        value = _resolve_field(spec.get("value"), context)
        encoding = spec.get("encoding")
        out[name] = _encode_binding(value, encoding)
    return out


def _format_dsn(template: str, bindings: Mapping[str, str]) -> str:
    """Substitute ``{name}`` placeholders in ``template`` from ``bindings``.

    Uses brace-style placeholders (matching the schema's ``url_template``
    syntax) and validates that every placeholder has a binding.
    """
    # str.format would also expand ``{0}``, ``{name!r}`` etc.; the schema
    # only ever uses plain ``{name}``. We hand-roll a tiny scanner to keep
    # the failure mode explicit.
    out: list[str] = []
    i = 0
    n = len(template)
    while i < n:
        j = template.find("{", i)
        if j < 0:
            out.append(template[i:])
            break
        out.append(template[i:j])
        k = template.find("}", j + 1)
        if k < 0:
            raise ValueError(f"Unterminated {{...}} placeholder in DSN template: {template!r}")
        name = template[j + 1 : k]
        if name not in bindings:
            raise KeyError(
                f"DSN template references {name!r} but no matching binding declared "
                f"(have: {sorted(bindings)})"
            )
        out.append(bindings[name])
        i = k + 1
    return "".join(out)


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


async def materialize_typed_transport(
    spec: ResolvedSqlAlchemyTransport | ResolvedHttpTransport,
    *,
    context: ResolutionContext,
) -> SqlAlchemyTransport | HttpTransport:
    """Materialize a live transport from a typed resolved-runtime spec.

    Delegates the live-engine / live-session construction to the existing
    factories in :mod:`src.shared.transport_factory` — only the
    resolution step is replaced.
    """
    if isinstance(spec, ResolvedSqlAlchemyTransport):
        return await _materialize_sqlalchemy(spec, context)
    if isinstance(spec, ResolvedHttpTransport):
        return await _materialize_http(spec, context)
    raise TypeError(
        f"materialize_typed_transport: unsupported spec type {type(spec).__name__}"
    )


async def _materialize_sqlalchemy(
    spec: ResolvedSqlAlchemyTransport, context: ResolutionContext
) -> SqlAlchemyTransport:
    bindings = _resolve_dsn_bindings(spec.dsn_bindings, context)
    dsn = _format_dsn(spec.dsn_template, bindings)
    flat_spec: dict[str, Any] = {
        "driver": spec.driver,
        "dsn": dsn,
    }
    # `tls` is currently a passthrough dict on the typed spec; if a future
    # consumer needs SSL flags they ride through unchanged.
    if spec.tls:
        flat_spec["tls"] = dict(spec.tls)
    return await build_sqlalchemy_transport(flat_spec)


async def _materialize_http(
    spec: ResolvedHttpTransport, context: ResolutionContext
) -> HttpTransport:
    base_url = _resolve_field(spec.base_url, context)
    if isinstance(base_url, str):
        # Bare literal base_url with `${...}` placeholders is legal too —
        # the typed spec stores `Any` and Wise-style connectors use a
        # plain string.
        base_url = resolve_template_string(base_url, context) if "${" in base_url else base_url

    headers: dict[str, str] = {}
    for name, value in (spec.headers or {}).items():
        resolved = _resolve_field(value, context)
        if isinstance(resolved, str) and "${" in resolved:
            resolved = resolve_template_string(resolved, context)
        if resolved is None:
            continue
        headers[str(name)] = str(resolved)

    flat_spec: dict[str, Any] = {
        "base_url": base_url,
        "headers": headers,
    }
    if spec.timeout_seconds is not None:
        flat_spec["timeout_seconds"] = spec.timeout_seconds
    return await build_http_transport(flat_spec)
