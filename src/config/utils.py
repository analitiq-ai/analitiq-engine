"""Shared config-layer utilities."""

import json
from pathlib import Path
from typing import Any

from ..state.error_classification import ErrorCode, FailureStage, tag_failure


def load_json_file(
    path: Path, error_cls: type[Exception] = ValueError
) -> dict[str, Any]:
    """Open *path*, parse JSON, and return the result dict.

    Raises *error_cls* (default ``ValueError``) with the file path included in
    the message when the file contains invalid JSON.  Does not check whether
    the file exists — callers are responsible for that guard.

    This is the config layer's only file read, which makes it the one place
    that can tell a bad config from a bad volume (issue #429). Failing to
    *read* the file -- permissions, a full or read-only mount, a vanished
    path -- is an engine/infra fault, so it is tagged INTERNAL here at the
    raise site. Failing to *parse* what was read is a genuine config defect
    and keeps the config phase's own CONFIG_INVALID. Without this split the
    runner would report an unreadable volume to the customer as "your
    pipeline configuration is invalid", which they cannot act on.
    """
    try:
        with path.open() as fh:
            data: dict[str, Any] = json.load(fh)
            return data
    except json.JSONDecodeError as err:
        raise error_cls(f"Invalid JSON in {path}: {err}") from err
    except OSError as err:
        # tag_failure is no-overwrite, so this INTERNAL tag survives the
        # config phase's coarser CONFIG_INVALID tag in the runner.
        raise tag_failure(
            err, code=ErrorCode.INTERNAL, stage=FailureStage.CONFIG
        ) from err
