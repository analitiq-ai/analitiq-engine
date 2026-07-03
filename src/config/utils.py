"""Shared config-layer utilities."""

import json
from pathlib import Path
from typing import Any


def load_json_file(
    path: Path, error_cls: type[Exception] = ValueError
) -> dict[str, Any]:
    """Open *path*, parse JSON, and return the result dict.

    Raises *error_cls* (default ``ValueError``) with the file path included in
    the message when the file contains invalid JSON.  Does not check whether
    the file exists — callers are responsible for that guard.
    """
    try:
        with path.open() as fh:
            data: dict[str, Any] = json.load(fh)
            return data
    except json.JSONDecodeError as err:
        raise error_cls(f"Invalid JSON in {path}: {err}") from err
