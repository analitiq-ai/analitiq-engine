"""Type-map rule model and normalization primitives.

A rule is one entry in ``type-map.json``:

    {"match": "exact", "native": "JSONB", "canonical": "Utf8"}
    {"match": "regex", "native": "^VARCHAR\\((?<n>\\d+)\\)$", "canonical": "Utf8"}

Regex rules must be written in an RE2-compatible subset so the same pattern
behaves identically across engine languages. The following Perl/Python
extensions are rejected at load time:

- lookahead ``(?=…)`` / negative lookahead ``(?!…)``
- lookbehind ``(?<=…)`` / negative lookbehind ``(?<!…)``
- atomic groups ``(?>…)``
- numeric backreferences ``\\1``..``\\9``
- named backreferences ``\\k<name>`` and Python-style ``(?P=name)``

``(?<name>…)`` is rewritten to Python's ``(?P<name>…)`` so the compiled
pattern works with ``re.fullmatch``.
"""

from __future__ import annotations

import re
from typing import Final, Iterable, Literal, Pattern

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .exceptions import InvalidTypeMapError


_NAMED_GROUP_RE2: Final[Pattern[str]] = re.compile(r"\(\?<([A-Za-z_][A-Za-z0-9_]*)>")
_SUBSTITUTION_TOKEN: Final[Pattern[str]] = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")

# RE2 excludes these Perl/Python extensions. We reject any rule that uses them
# at load time so the committed rule set stays portable.
_FORBIDDEN_CONSTRUCTS: Final[tuple[tuple[str, str], ...]] = (
    ("(?=", "lookahead"),
    ("(?!", "negative lookahead"),
    ("(?<=", "lookbehind"),
    ("(?<!", "negative lookbehind"),
    ("(?>", "atomic group"),
    ("(?P=", "Python-style named backreference"),
    (r"\k<", "named backreference"),
)
_BACKREFERENCE_DIGIT: Final[Pattern[str]] = re.compile(r"\\[1-9]")


def normalize_native_type(value: str) -> str:
    """Normalize a native type string for matching.

    - Trim leading/trailing whitespace.
    - Collapse internal whitespace runs to a single space.
    - Uppercase the whole string.

    The same normalization is applied to rule ``native`` values (for exact
    rules) and to inputs at lookup time.
    """
    if not isinstance(value, str):
        raise TypeError(
            f"native type must be a string, got {type(value).__name__}"
        )
    return re.sub(r"\s+", " ", value.strip()).upper()


def _assert_re2_subset(pattern: str) -> None:
    """Reject Perl/Python regex extensions that RE2 does not support."""
    for token, label in _FORBIDDEN_CONSTRUCTS:
        if token in pattern:
            raise InvalidTypeMapError(
                f"regex pattern uses unsupported construct ({label}): {pattern!r}"
            )
    if _BACKREFERENCE_DIGIT.search(pattern):
        raise InvalidTypeMapError(
            f"regex pattern uses numeric backreference: {pattern!r}"
        )


def _to_python_named_groups(pattern: str) -> str:
    """Translate RE2-style ``(?<name>...)`` groups to Python ``(?P<name>...)``."""
    return _NAMED_GROUP_RE2.sub(lambda m: f"(?P<{m.group(1)}>", pattern)


class TypeMapRule(BaseModel):
    """A single entry in ``type-map.json``."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    match: Literal["exact", "regex"]
    native: str = Field(min_length=1)
    canonical: str = Field(min_length=1)

    @model_validator(mode="after")
    def _validate(self) -> "TypeMapRule":
        tokens = set(_SUBSTITUTION_TOKEN.findall(self.canonical))

        if self.match == "exact":
            if tokens:
                raise InvalidTypeMapError(
                    f"exact rule for {self.native!r} has ${{...}} tokens in "
                    f"canonical {self.canonical!r}; only regex rules may use "
                    f"named-capture substitution"
                )
            return self

        _assert_re2_subset(self.native)

        translated = _to_python_named_groups(self.native)
        try:
            compiled = re.compile(translated)
        except re.error as err:
            raise InvalidTypeMapError(
                f"regex pattern {self.native!r} failed to compile: {err}"
            ) from err

        groups = set(compiled.groupindex)
        missing = tokens - groups
        if missing:
            raise InvalidTypeMapError(
                f"canonical {self.canonical!r} references unknown named "
                f"groups: {sorted(missing)}"
            )

        return self

    def normalized_native(self) -> str:
        """Normalization form of ``native`` used for exact matching."""
        if self.match != "exact":
            raise RuntimeError("normalized_native is only defined for exact rules")
        return normalize_native_type(self.native)

    def compile_pattern(self) -> Pattern[str]:
        """Compile the regex pattern (RE2-subset) for forward matching.

        Inputs are normalized to uppercase before matching, so literal
        characters in the pattern must be authored in uppercase too — we do
        NOT uppercase the pattern itself because that would turn character
        classes like ``\\d`` into ``\\D``.
        """
        if self.match != "regex":
            raise RuntimeError("compile_pattern is only defined for regex rules")
        translated = _to_python_named_groups(self.native)
        return re.compile(translated)


def parse_rules(payload: Iterable[object], *, source: str) -> list[TypeMapRule]:
    """Validate and parse a list of rule dicts loaded from JSON.

    Args:
        payload: Iterable of rule dicts, typically the top-level JSON array.
        source: Human-readable origin (e.g. file path) used in error messages.
    """
    rules: list[TypeMapRule] = []
    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            raise InvalidTypeMapError(
                f"{source}: rule #{index} is not a JSON object"
            )
        try:
            rules.append(TypeMapRule(**item))
        except InvalidTypeMapError:
            raise
        except Exception as err:
            raise InvalidTypeMapError(
                f"{source}: rule #{index} is invalid: {err}"
            ) from err
    if not rules:
        raise InvalidTypeMapError(f"{source}: rule list is empty")
    return rules