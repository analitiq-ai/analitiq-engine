"""Shared parameter validation for encoding/encoding_write config dicts.

One set of checks for both directions -- a malformed ``pattern``, ``unit``,
or value list is refused identically whether it was declared for a decoder
or an encoder, rather than each catalog growing its own slightly different
wording.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .exceptions import InvalidTypeMapError


def require_str_param(config: Mapping[str, Any], name: str, entry: str) -> str:
    value = config.get(name)
    if not isinstance(value, str) or not value:
        raise InvalidTypeMapError(f"{entry!r} requires a non-empty string {name!r}")
    return value


def require_enum_param(
    config: Mapping[str, Any], name: str, allowed: tuple[str, ...], entry: str
) -> str:
    value = require_str_param(config, name, entry)
    if value not in allowed:
        raise InvalidTypeMapError(
            f"{entry!r} {name} must be one of {allowed}, got {value!r}"
        )
    return value


def require_list_param(config: Mapping[str, Any], name: str, entry: str) -> list[str]:
    value = config.get(name)
    if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
        raise InvalidTypeMapError(f"{entry!r} requires {name!r} as a list of strings")
    return value
