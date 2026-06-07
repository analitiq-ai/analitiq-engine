"""Deterministic matcher for type-map.

``TypeMapper`` returns the first matching rule's output and raises on a miss —
no defaults, no coercion. Each direction has its **own** rule file: the read
map (``type-map-read.json``, native → Arrow) feeds :meth:`TypeMapper.to_arrow_type`;
the optional write map (``type-map-write.json``, Arrow → native) feeds
:meth:`TypeMapper.to_native_type`. The two are independent rule sets, never one
inverted at runtime — inverting would be lossy and ambiguous.
"""

from __future__ import annotations

from typing import Any, Mapping, Pattern

from .exceptions import (
    InvalidTypeMapError,
    UnmappedTypeError,
)
from .rules import (
    TypeMapRule,
    WriteTypeMapRule,
    _SUBSTITUTION_TOKEN,
    normalize_canonical_type,
    normalize_native_type,
)

import re


class TypeMapper:
    """Deterministic native → canonical matcher for a connector's type-map.

    Built from a list of :class:`TypeMapRule` instances. Rule order is
    authoritative: the author controls specificity by placing narrower
    rules above broader ones (e.g. ``TINYINT(1) → Boolean`` above
    ``^TINYINT(\\(\\d+\\))?$ → Int8``). Instances are immutable and safe
    to cache per ``(slug, version)``.
    """

    def __init__(
        self,
        connector_slug: str,
        rules: list[TypeMapRule],
        write_rules: list[WriteTypeMapRule] | None = None,
    ) -> None:
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

        # Write direction (canonical -> native). Optional: API connectors and
        # source-only connectors have no write map. Built symmetrically to the
        # read side: exact rules keep their normalized literal, regex rules a
        # compiled pattern.
        self._write_rules: tuple[WriteTypeMapRule, ...] = tuple(write_rules or ())
        self._write_compiled: list[Pattern[str] | None] = []
        self._exact_canonical: list[str | None] = []
        for rule in self._write_rules:
            if rule.match == "exact":
                self._exact_canonical.append(normalize_canonical_type(rule.canonical))
                self._write_compiled.append(None)
            else:
                self._exact_canonical.append(None)
                self._write_compiled.append(rule.compile_pattern())

    @property
    def connector_slug(self) -> str:
        return self._slug

    @property
    def rules(self) -> tuple[TypeMapRule, ...]:
        return self._rules

    @property
    def write_rules(self) -> tuple[WriteTypeMapRule, ...]:
        return self._write_rules

    @property
    def has_write_map(self) -> bool:
        return bool(self._write_rules)

    @classmethod
    def compose(cls, primary: "TypeMapper", fallback: "TypeMapper") -> "TypeMapper":
        """Return a new mapper where *primary* rules take precedence per-type.

        Implemented by concatenating *primary*'s rules before *fallback*'s
        rules in a single new :class:`TypeMapper`; the existing first-match
        semantics then make primary rules authoritative and fallback rules fill
        the gaps. The resulting mapper carries no record of which rules
        originated where.

        This applies to both directions: read (``to_arrow_type``) and write
        (``to_native_type``). A connection mapper that only declares override
        types therefore inherits the connector mapper's rules for everything
        else — including write rules the connection map never needs to repeat.
        """
        combined_write = list(primary.write_rules) + list(fallback.write_rules)
        return cls(
            primary.connector_slug,
            list(primary.rules) + list(fallback.rules),
            combined_write or None,
        )

    def to_arrow_type(self, native: str) -> str:
        """Map a native type string to its Arrow-type-string form.

        Pair with :func:`~cdk.type_map.arrow.parse_arrow_type` to
        get a ``pa.DataType``. Raises :class:`UnmappedTypeError` on miss.
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

    def to_native_type(
        self, canonical: str, *, params: Mapping[str, Any] | None = None
    ) -> str:
        """Map an Arrow canonical type string to its native DDL type.

        The inverse of :meth:`to_arrow_type`, fed by the connector's
        ``type-map-write.json``. ``params`` supplies per-column hints (e.g.
        ``length``) that a rule's ``native`` template may reference via
        ``${name}`` alongside any named captures from the canonical regex;
        named captures take precedence on a name clash. Hint values are rendered
        via ``str()``, so numeric hints (e.g. ``length=255``) are accepted. Raises
        :class:`InvalidTypeMapError` if this connector has no write-type-map
        loaded, or if the matched template references a token that neither the
        capture groups nor ``params`` provide; raises :class:`UnmappedTypeError`
        (``direction="reverse"``) when no rule matches *canonical*.
        """
        if not self._write_rules:
            raise InvalidTypeMapError(
                f"connector {self._slug!r}: no write-type-map loaded; cannot "
                f"render a native type for canonical {canonical!r}"
            )
        normalized = normalize_canonical_type(canonical)
        # Hints may arrive as ints (e.g. a JSON length) — render them to str so
        # the substitution callback never trips. A None hint (a nullable/absent
        # metadata field) is treated as not provided, mirroring how a
        # non-participating optional capture is dropped below; otherwise it would
        # render literal "None" into the DDL.
        hints: dict[str, str] = {
            k: str(v) for k, v in (params or {}).items() if v is not None
        }
        for rule, compiled, exact in zip(
            self._write_rules, self._write_compiled, self._exact_canonical
        ):
            if rule.match == "exact":
                if exact == normalized:
                    return _substitute_tokens(rule.native, hints)
                continue
            assert compiled is not None
            match = compiled.fullmatch(normalized)
            if match is None:
                continue
            # Drop optional groups that did not participate (groupdict gives them
            # None) so an absent capture neither shadows a same-named hint nor
            # feeds None into the substitution callback.
            captures = {k: v for k, v in match.groupdict().items() if v is not None}
            values = {**hints, **captures}
            return _substitute_tokens(rule.native, values)
        raise UnmappedTypeError(self._slug, "reverse", canonical)


def _substitute_tokens(template: str, values: Mapping[str, str]) -> str:
    """Replace every ``${name}`` in *template* with ``values[name]``.

    On the read side the rule model guarantees every token has a corresponding
    capture; on the write side a token may instead be a per-column hint. Either
    way a token absent from *values* is a hard error (no silent default) — for
    a write rule it means a required hint (e.g. ``length``) was not supplied.
    """

    def _replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in values:
            raise InvalidTypeMapError(
                f"template {template!r} references {name!r} but neither a "
                f"capture group nor a render hint provided it"
            )
        return values[name]

    return _SUBSTITUTION_TOKEN.sub(_replace, template)
