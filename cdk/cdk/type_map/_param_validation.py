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


def require_known_percent_directives(
    pattern: str,
    allowed: frozenset[str],
    entry: str,
    *,
    multi_char: frozenset[str] = frozenset(),
) -> None:
    """Reject a ``%``-directive pattern naming a directive outside *allowed*.

    Shared by ``strftime`` (Python's own implementation) and ``strptime``
    (pyarrow's own, a narrower vocabulary -- notably missing ``%f``/``%Z``/
    ``%G``, verified directly) so both catalogs refuse an unsupported
    directive with the same wording and the same one scanning pass, each
    against its own actual runtime's vocabulary rather than a shared
    guess. Neither implementation reliably rejects an unknown directive
    itself (Python's ``strftime`` is platform-dependent -- glibc renders
    one like ``%Q`` literally; pyarrow's ``strptime`` raises the same
    generic "failed to parse" error for an unknown directive as for an
    ordinary value mismatch, so it cannot even be probed for), so this is
    the only place either gets validated at all. ``%%`` is always
    accepted, matching the literal-percent escape both implementations
    honor. *multi_char* names directives longer than one character after
    the ``%`` (Python 3.12+'s ``%:z``, passed only when the running
    interpreter actually supports it -- on 3.11 the identical pattern
    renders the literal, useless text ``:z``, so accepting it
    unconditionally here would trade one silent-garbage runtime for
    another).
    """
    i = 0
    n = len(pattern)
    while i < n:
        if pattern[i] != "%":
            i += 1
            continue
        if i + 1 >= n:
            raise InvalidTypeMapError(
                f"{entry}: pattern {pattern!r} ends with a bare '%'"
            )
        rest = pattern[i + 1 :]
        matched = next((m for m in multi_char if rest.startswith(m)), None)
        if matched is not None:
            i += 1 + len(matched)
            continue
        directive = pattern[i + 1]
        if directive != "%" and directive not in allowed:
            raise InvalidTypeMapError(
                f"{entry}: pattern {pattern!r} names directive '%{directive}', "
                f"which is not supported here"
            )
        i += 2
