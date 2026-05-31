"""Registered derived/callable functions for connector expressions.

These functions are referenced from connector JSON via ``{"function":
"<name>", "input": ..., ...}``. They are the only escape hatch from the
declarative model — connector authors cannot embed arbitrary Python.

Adding a new function requires:

1. Implementing it here with a ``DerivedFunction`` signature.
2. Registering it in :data:`DEFAULT_FUNCTIONS`.
3. Documenting it in the spec table at the top of
   ``docs/connector-connection-parameterization.md``.
"""

from __future__ import annotations

import base64 as _base64
from typing import Any, Mapping
from urllib.parse import quote as _url_quote

from .resolver import DerivedFunction, Resolver


def _require(node: Mapping[str, Any], key: str) -> Any:
    if key not in node:
        raise ValueError(f"function {node.get('function')!r} requires `{key}`")
    return node[key]


def lookup_function(node: Mapping[str, Any], resolver: Resolver) -> Any:
    """Map a resolved input value through a connector-declared inline map.

    The ``map`` keys are JSON strings; ``input`` must resolve to a scalar
    (``bool``, ``int``, ``float``, or ``str``). Non-scalar inputs are
    rejected with :class:`TypeError` rather than stringified — a dict or
    list silently coerced to ``"{'k': 'v'}"`` produces extremely confusing
    "not present in map" errors. Unknown scalar inputs raise
    :class:`KeyError`.
    """
    raw_input = _require(node, "input")
    table = _require(node, "map")
    if not isinstance(table, Mapping):
        raise TypeError("`lookup` `map` must be an object")
    key = resolver.resolve(raw_input)
    if key is None:
        raise KeyError(
            f"`lookup` input resolved to None; known keys: {sorted(table)}"
        )
    if not isinstance(key, (bool, int, float, str)):
        raise TypeError(
            f"`lookup` input must resolve to a scalar (bool/int/float/str); "
            f"got {type(key).__name__}"
        )
    if isinstance(key, bool):
        # JSON has no boolean object keys; canonicalize to the strings the
        # author would have written in the map.
        lookup_key: str = "true" if key else "false"
    else:
        lookup_key = str(key)
    if lookup_key not in table:
        raise KeyError(
            f"`lookup` input {key!r} not present in map; known keys: "
            f"{sorted(table)}"
        )
    # Resolve the matched value so nested expressions inside the map
    # entry (templates, refs, functions) are also evaluated.
    return resolver.resolve(table[lookup_key])


def basic_auth_function(node: Mapping[str, Any], resolver: Resolver) -> str:
    """Build a Basic auth credential (``base64(username:password)``).

    Returns just the credential payload; callers wrap it as
    ``"Basic <credential>"`` themselves so this composes cleanly with
    template strings.
    """
    inputs = resolver.resolve(_require(node, "input"))
    if not isinstance(inputs, Mapping):
        raise TypeError("`basic_auth` `input` must resolve to an object")
    username = inputs.get("username", "")
    password = inputs.get("password", "")
    if username is None:
        username = ""
    if password is None:
        password = ""
    raw = f"{username}:{password}".encode("utf-8")
    return _base64.b64encode(raw).decode("ascii")


def base64_encode_function(node: Mapping[str, Any], resolver: Resolver) -> str:
    """Base64-encode a string or bytes value."""
    value = resolver.resolve(_require(node, "input"))
    if isinstance(value, str):
        data = value.encode("utf-8")
    elif isinstance(value, (bytes, bytearray)):
        data = bytes(value)
    else:
        raise TypeError(
            f"`base64_encode` input must resolve to string or bytes, got "
            f"{type(value).__name__}"
        )
    return _base64.b64encode(data).decode("ascii")


def url_encode_function(node: Mapping[str, Any], resolver: Resolver) -> str:
    """Percent-encode a value for use inside a URL component.

    The default ``safe`` characters are empty so every reserved character
    is escaped — appropriate for substituting into URL credentials and
    path segments. Connector authors can override the safe set via the
    ``safe`` field if they need a less aggressive encoding.
    """
    value = resolver.resolve(_require(node, "input"))
    if value is None:
        return ""
    if not isinstance(value, (str, int, float, bool)):
        raise TypeError(
            f"`url_encode` input must resolve to a scalar, got "
            f"{type(value).__name__}"
        )
    safe = node.get("safe", "")
    if not isinstance(safe, str):
        raise TypeError("`url_encode` `safe` must be a string")
    return _url_quote(str(value), safe=safe)


DEFAULT_FUNCTIONS: dict[str, DerivedFunction] = {
    "lookup": lookup_function,
    "basic_auth": basic_auth_function,
    "base64_encode": base64_encode_function,
    "url_encode": url_encode_function,
}
