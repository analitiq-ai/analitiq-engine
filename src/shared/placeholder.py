"""Placeholder expansion for ${key} syntax."""

import re
from typing import Any, Dict

PLACEHOLDER_PATTERN = re.compile(r"\$\{([^}]+)\}")


def expand_placeholders(
    value: Any,
    lookup: Dict[str, str],
    *,
    ignore_missing: bool = False,
) -> Any:
    """
    Recursively expand ${key} placeholders in a value using the lookup dict.

    Args:
        value: String, dict, list, or scalar to expand.
        lookup: Mapping of placeholder keys to replacement values.
        ignore_missing: If True, leave unresolved placeholders as-is.
            If False, raise KeyError on missing keys.

    Returns:
        Value with all resolvable placeholders replaced.
    """
    if isinstance(value, str):
        return _expand_string(value, lookup, ignore_missing=ignore_missing)
    elif isinstance(value, dict):
        return {
            k: expand_placeholders(v, lookup, ignore_missing=ignore_missing)
            for k, v in value.items()
        }
    elif isinstance(value, list):
        return [
            expand_placeholders(item, lookup, ignore_missing=ignore_missing)
            for item in value
        ]
    return value


def has_placeholders(value: Any) -> bool:
    """Check whether a value contains any ${key} placeholders."""
    if isinstance(value, str):
        return bool(PLACEHOLDER_PATTERN.search(value))
    elif isinstance(value, dict):
        return any(has_placeholders(v) for v in value.values())
    elif isinstance(value, list):
        return any(has_placeholders(item) for item in value)
    return False


def _expand_string(
    value: str,
    lookup: Dict[str, str],
    *,
    ignore_missing: bool,
) -> str:
    def replace_match(match: re.Match) -> str:
        key = match.group(1)
        if key in lookup:
            return str(lookup[key])
        if ignore_missing:
            return match.group(0)
        raise KeyError(f"Placeholder key '{key}' not found in lookup")

    return PLACEHOLDER_PATTERN.sub(replace_match, value)