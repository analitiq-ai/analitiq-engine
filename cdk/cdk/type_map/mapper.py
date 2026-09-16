"""Deterministic matcher for type-map.

``TypeMapper`` returns the first matching rule's output and raises on a miss —
no defaults, no coercion. Each direction has its **own** rule set: the read
map (the type-map document declaring ``direction: read``, native → Arrow) feeds
:meth:`TypeMapper.to_arrow_type`; the write map (``direction: write``,
Arrow → native) feeds :meth:`TypeMapper.to_native_type`. Either may be absent:
a source only reads, a destination only writes. The two are independent rule
sets, never one inverted at runtime — inverting would be lossy and ambiguous.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from re import Pattern
from typing import Any, Final

from .exceptions import InvalidTypeMapError, UnmappedTypeError
from .rules import (
    CompiledPattern,
    TypeMapReadRule,
    TypeMapWriteRule,
    compile_pattern,
    normalize_arrow_type,
    normalize_native_type,
    normalized_native,
)

# A ``${...}`` this does not match is left in the rendered type verbatim. The
# pinned contract rejects only empty or unclosed tokens, so a malformed name
# such as ``${length-p}`` reaches the DDL as literal text.
_SUBSTITUTION_TOKEN: Final[Pattern[str]] = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


class TypeMapper:
    r"""Deterministic matcher over a connector's read and write type maps.

    Built from :class:`TypeMapReadRule` and :class:`TypeMapWriteRule` lists,
    either of which may be absent. Rule order is
    authoritative: the author controls specificity by placing narrower
    rules above broader ones (e.g. ``TINYINT(1) → Boolean`` above
    ``^TINYINT(\(\d+\))?$ → Int8``). Instances are immutable and safe
    to cache per ``(slug, version)``.
    """

    def __init__(
        self,
        connector_slug: str,
        rules: list[TypeMapReadRule] | None,
        write_rules: list[TypeMapWriteRule] | None = None,
    ) -> None:
        self._slug = connector_slug
        self._rules: tuple[TypeMapReadRule, ...] = tuple(rules or ())

        # Precompute one match artefact per rule: either the normalized
        # literal (exact) or the compiled pattern (regex).
        self._compiled: list[CompiledPattern | None] = []
        self._exact_native: list[str | None] = []
        for rule in self._rules:
            if rule.match == "exact":
                self._exact_native.append(normalized_native(rule))
                self._compiled.append(None)
            else:
                self._exact_native.append(None)
                self._compiled.append(compile_pattern(rule))

        # Write direction (arrow_type -> native_type). Built symmetrically to the
        # read side: exact rules keep their normalized literal, regex rules a
        # compiled pattern.
        self._write_rules: tuple[TypeMapWriteRule, ...] = tuple(write_rules or ())
        self._write_compiled: list[CompiledPattern | None] = []
        self._exact_arrow: list[str | None] = []
        for write_rule in self._write_rules:
            if write_rule.match == "exact":
                self._exact_arrow.append(normalize_arrow_type(write_rule.arrow_type))
                self._write_compiled.append(None)
            else:
                self._exact_arrow.append(None)
                self._write_compiled.append(compile_pattern(write_rule))

    @property
    def connector_slug(self) -> str:
        return self._slug

    @property
    def rules(self) -> tuple[TypeMapReadRule, ...]:
        return self._rules

    @property
    def write_rules(self) -> tuple[TypeMapWriteRule, ...]:
        return self._write_rules

    @property
    def has_read_map(self) -> bool:
        return bool(self._rules)

    @property
    def has_write_map(self) -> bool:
        return bool(self._write_rules)

    @classmethod
    def compose(cls, primary: TypeMapper, fallback: TypeMapper) -> TypeMapper:
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
        return cls(
            primary.connector_slug,
            list(primary.rules) + list(fallback.rules),
            list(primary.write_rules) + list(fallback.write_rules),
        )

    def to_arrow_type(self, native: str) -> str:
        """Map a native type string to its Arrow-type-string form.

        Pair with :func:`~cdk.type_map.arrow.parse_arrow_type` to
        get a ``pa.DataType``. Raises :class:`InvalidTypeMapError` if no read
        type map is loaded, :class:`UnmappedTypeError` on miss.
        """
        if not self._rules:
            raise InvalidTypeMapError(
                f"connector {self._slug!r}: no read type map loaded; cannot "
                f"map native type {native!r}"
            )
        normalized = normalize_native_type(native)
        for rule, compiled, exact in zip(
            self._rules, self._compiled, self._exact_native
        ):
            if rule.match == "exact":
                if exact == normalized:
                    return rule.arrow_type
                continue
            assert compiled is not None
            # `normalized` is runtime input (whatever a driver or API schema
            # reported), not a pydantic-validated document field like the
            # rule's own pattern -- a lone surrogate in it makes re2's
            # internal UTF-8 encoding step raise UnicodeEncodeError instead
            # of matching or not matching. Treated as a miss on this rule,
            # the same verdict a value re2 cannot interpret gets everywhere
            # else it is checked (#504).
            try:
                match = compiled.fullmatch(normalized)
            except UnicodeEncodeError:
                continue
            if match is None:
                continue
            # Drop optional groups that did not participate (groupdict gives
            # them None), symmetrically with the write side below. Without
            # this, a non-participating capture used as a token reached
            # re.sub() as None: re.sub() silently treats a None return as an
            # empty string, so the token vanished from the rendered type
            # instead of raising -- filtering it here makes the lookup below
            # miss and raise the same InvalidTypeMapError as everywhere else.
            captures = {k: v for k, v in match.groupdict().items() if v is not None}
            return _substitute_tokens(rule.arrow_type, captures)
        raise UnmappedTypeError(self._slug, "forward", native)

    def to_native_type(
        self, arrow_type: str, *, params: Mapping[str, Any] | None = None
    ) -> str:
        """Map an ``arrow_type`` to its native DDL type.

        The inverse of :meth:`to_arrow_type`, fed by the connector's write
        map. ``params`` supplies per-column hints (e.g.
        ``length``) that a rule's ``native_type`` template may reference via
        ``${name}`` alongside any named captures from the arrow_type regex;
        named captures take precedence on a name clash. Hint values are rendered
        via ``str()``, so numeric hints (e.g. ``length=255``) are accepted. Raises
        :class:`InvalidTypeMapError` if no write type map is
        loaded, or if the matched template references a token that neither the
        capture groups nor ``params`` provide; raises :class:`UnmappedTypeError`
        (``direction="reverse"``) when no rule matches *arrow_type*.
        """
        if not self._write_rules:
            raise InvalidTypeMapError(
                f"connector {self._slug!r}: no write type map loaded; cannot "
                f"render a native type for arrow_type {arrow_type!r}"
            )
        normalized = normalize_arrow_type(arrow_type)
        # Hints may arrive as ints (e.g. a JSON length) — render them to str so
        # the substitution callback never trips. A None hint (a nullable/absent
        # metadata field) is treated as not provided, mirroring how a
        # non-participating optional capture is dropped below; otherwise it would
        # render literal "None" into the DDL.
        hints: dict[str, str] = {
            k: str(v) for k, v in (params or {}).items() if v is not None
        }
        for rule, compiled, exact in zip(
            self._write_rules, self._write_compiled, self._exact_arrow
        ):
            if rule.match == "exact":
                if exact == normalized:
                    return _substitute_tokens(rule.native_type, hints)
                continue
            assert compiled is not None
            # See the read-side comment in to_arrow_type: `normalized` is
            # runtime input, not a validated document field, so a lone
            # surrogate can reach re2's fullmatch here too.
            try:
                match = compiled.fullmatch(normalized)
            except UnicodeEncodeError:
                continue
            if match is None:
                continue
            # Drop optional groups that did not participate (groupdict gives them
            # None) so an absent capture neither shadows a same-named hint nor
            # feeds None into the substitution callback.
            captures = {k: v for k, v in match.groupdict().items() if v is not None}
            values = {**hints, **captures}
            return _substitute_tokens(rule.native_type, values)
        raise UnmappedTypeError(self._slug, "reverse", arrow_type)


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
