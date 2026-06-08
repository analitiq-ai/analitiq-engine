"""Shared helper for nested dict traversal by a sequence of string keys."""

from __future__ import annotations

from typing import Any


def walk_path(data: Any, path: list[str]) -> Any:
    """Walk *data* by *path*, returning the value or ``None`` on any miss."""
    current = data
    for key in path:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return None
    return current
