"""Deterministic matchers for type-map and ssl-mode-map.

``TypeMapper`` walks a rule list top-to-bottom and returns the canonical
type of the first matching rule. It never defaults, never invokes an LLM,
and never performs implicit coercion — a miss is a hard error so upstream
pipelines fail loudly on unmapped types.

Rule files are deliberately single-direction (native → canonical). Source
connectors author their own file (many source-natives → one canonical);
destination connectors author a separate file in the opposite direction
(one canonical → one opinionated destination-native) using the same
matcher. Inverting one side's rules at runtime would be lossy and ambiguous,
so this module does not attempt it.

``SSLModeMapper`` is a flat dictionary lookup from native driver SSL modes
(``prefer``, ``VERIFY_IDENTITY``, …) to the canonical engine vocabulary
(``none`` / ``encrypt`` / ``verify`` / ``prefer``).
"""

from __future__ import annotations

from typing import Final, Mapping, Pattern

from .exceptions import (
    InvalidSSLModeMapError,
    InvalidTypeMapError,
    UnmappedSSLModeError,
    UnmappedTypeError,
)
from .rules import (
    TypeMapRule,
    _SUBSTITUTION_TOKEN,
    normalize_native_type,
)

import re


CANONICAL_SSL_MODES: Final[frozenset[str]] = frozenset(
    {"none", "encrypt", "verify", "prefer"}
)


class TypeMapper:
    """Deterministic native → canonical matcher for a connector's type-map.

    Built from a list of :class:`TypeMapRule` instances. Rule order is
    authoritative: the author controls specificity by placing narrower
    rules above broader ones (e.g. ``TINYINT(1) → Boolean`` above
    ``^TINYINT(\\(\\d+\\))?$ → Int8``). Instances are immutable and safe
    to cache per ``(slug, version)``.
    """

    def __init__(self, connector_slug: str, rules: list[TypeMapRule]) -> None:
        if not rules:
            raise InvalidTypeMapError(
                f"connector {connector_slug!r}: type-map must contain at least one rule"
            )
        self._slug = connector_slug
        self._rules: tuple[TypeMapRule, ...] = tuple(rules)

        # Precompute one match artefact per rule: either the normalized
        # literal (exact) or the compiled pattern (regex).
        self._compiled: list[Pattern[str] | None] = []
        self._exact_native: list[str | None] = []
        for rule in self._rules:
            if rule.match == "exact":
                self._exact_native.append(normalize_native_type(rule.native))
                self._compiled.append(None)
            else:
                self._exact_native.append(None)
                self._compiled.append(rule.compile_pattern())

    @property
    def connector_slug(self) -> str:
        return self._slug

    @property
    def rules(self) -> tuple[TypeMapRule, ...]:
        return self._rules

    def to_canonical(self, native: str) -> str:
        """Map a native type string to its canonical form.

        Raises :class:`UnmappedTypeError` when no rule matches — never defaults.
        """
        normalized = normalize_native_type(native)
        for rule, compiled, exact in zip(
            self._rules, self._compiled, self._exact_native
        ):
            if rule.match == "exact":
                if exact == normalized:
                    return rule.canonical
                continue
            assert compiled is not None
            match = compiled.fullmatch(normalized)
            if match is None:
                continue
            return _substitute_tokens(rule.canonical, match.groupdict())
        raise UnmappedTypeError(self._slug, "forward", native)


def _substitute_tokens(template: str, values: Mapping[str, str]) -> str:
    """Replace every ``${name}`` in *template* with ``values[name]``.

    The rule model guarantees every token has a corresponding capture, so a
    missing key here indicates an internal bug rather than a user error.
    """

    def _replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in values:
            raise InvalidTypeMapError(
                f"internal error: template {template!r} references {name!r} "
                f"but no such capture was produced"
            )
        return values[name]

    return _SUBSTITUTION_TOKEN.sub(_replace, template)


class SSLModeMapper:
    """Translate a driver-native SSL mode to the engine's canonical vocabulary.

    Mappings are exact-match on the normalized native key (same normalization
    rules as :class:`TypeMapper` — trim, collapse whitespace, uppercase).
    Values must be drawn from :data:`CANONICAL_SSL_MODES`.

    Inputs that are *already* a canonical value (case-insensitive) are
    passed through unchanged so pipelines that bypass the form and store
    canonical values directly still work — see :meth:`to_canonical`.
    """

    def __init__(self, connector_slug: str, mapping: Mapping[str, str]) -> None:
        self._slug = connector_slug
        normalized: dict[str, str] = {}
        for native, canonical in mapping.items():
            if not isinstance(native, str) or not isinstance(canonical, str):
                raise InvalidSSLModeMapError(
                    f"connector {connector_slug!r}: ssl-mode entries must be "
                    f"string-to-string, got {native!r}: {canonical!r}"
                )
            canonical_lower = canonical.strip().lower()
            if canonical_lower not in CANONICAL_SSL_MODES:
                raise InvalidSSLModeMapError(
                    f"connector {connector_slug!r}: ssl-mode maps {native!r} "
                    f"to {canonical!r}, which is not in the canonical set "
                    f"{sorted(CANONICAL_SSL_MODES)}"
                )
            key = normalize_native_type(native)
            if key in normalized and normalized[key] != canonical_lower:
                raise InvalidSSLModeMapError(
                    f"connector {connector_slug!r}: native ssl-mode {native!r} "
                    f"maps to both {normalized[key]!r} and {canonical_lower!r}"
                )
            normalized[key] = canonical_lower
        if not normalized:
            raise InvalidSSLModeMapError(
                f"connector {connector_slug!r}: ssl-mode-map is empty"
            )
        self._mapping: Mapping[str, str] = normalized

    @property
    def connector_slug(self) -> str:
        return self._slug

    @property
    def entries(self) -> Mapping[str, str]:
        return dict(self._mapping)

    def to_canonical(self, native: str) -> str:
        """Return the canonical ssl-mode for *native*.

        Raises :class:`UnmappedSSLModeError` when the native value is not
        known. If the input is already a canonical value (``none`` /
        ``encrypt`` / ``verify`` / ``prefer``) we let it through as-is so
        pipelines that bypass the form and write canonical values directly
        still work.
        """
        trimmed = native.strip()
        if trimmed.lower() in CANONICAL_SSL_MODES:
            return trimmed.lower()
        key = normalize_native_type(native)
        if key in self._mapping:
            return self._mapping[key]
        raise UnmappedSSLModeError(self._slug, native)
