"""Errors raised by the resolved-runtime factories.

`ResolveError` carries the source file path and the JSON pointer-style
field path so failures point at `file:$.foo.bar` instead of a stack inside
the dataclass constructor.
"""

from __future__ import annotations


class ResolveError(ValueError):
    """Raised when a raw JSON artifact cannot be lifted into a resolved type."""

    def __init__(self, source: str, path: str, message: str) -> None:
        self.source = source
        self.path = path
        self.message = message
        super().__init__(f"{source}:{path}: {message}")


def require(spec: dict, key: str, *, source: str, path: str) -> object:
    if key not in spec:
        raise ResolveError(source, path, f"missing required key '{key}'")
    value = spec[key]
    if value is None:
        raise ResolveError(source, path, f"key '{key}' is null")
    return value


def require_str(spec: dict, key: str, *, source: str, path: str) -> str:
    value = require(spec, key, source=source, path=path)
    if not isinstance(value, str):
        raise ResolveError(source, path, f"key '{key}' must be a string, got {type(value).__name__}")
    return value
