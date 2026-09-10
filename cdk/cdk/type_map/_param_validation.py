"""Shared parameter vocabulary and validation for encoding/encoding_write config dicts.

One set of checks for both directions -- a malformed ``pattern``, ``unit``,
or value list is refused identically whether it was declared for a decoder
or an encoder, rather than each catalog growing its own slightly different
wording. :class:`EncodingParam` is the one static-shape representation both
``decoders.py`` and ``encoders.py`` publish their vocabulary through, rather
than each declaring its own typed/untyped copy.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .exceptions import InvalidTypeMapError


@dataclass(frozen=True, slots=True)
class EncodingParam:
    """One parameter an ``encoding``/``encoding_write`` config may/must carry."""

    name: str
    kind: str  # "string" | "enum" | "list[string]"
    required: bool = True
    allowed: tuple[str, ...] = ()


def param_to_json(param: EncodingParam) -> dict[str, Any]:
    """Render *param* the way a published decoders/encoders catalog document does."""
    doc: dict[str, Any] = {
        "name": param.name,
        "kind": param.kind,
        "required": param.required,
    }
    if param.allowed:
        doc["allowed"] = list(param.allowed)
    return doc


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
