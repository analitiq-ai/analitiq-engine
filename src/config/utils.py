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

    Failing to *read* the file -- permissions, a full or read-only mount, a
    vanished path -- is an engine/infra fault, so it is tagged INTERNAL here
    at the raise site (issue #429). Failing to *parse* what was read is a
    genuine config defect and keeps the config phase's own CONFIG_INVALID.
    Without the split the runner would report an unreadable volume to the
    customer as "your pipeline configuration is invalid", which they cannot
    act on.

    This is the config layer's only ``open()``, so it covers every config
    document the engine reads. It does not cover directory enumeration
    elsewhere in the config phase, which still takes the phase's own tag.
    """
    try:
        with path.open() as fh:
            data: dict[str, Any] = json.load(fh)
            return data
    except json.JSONDecodeError as err:
        raise error_cls(f"Invalid JSON in {path}: {err}") from err
    except OSError as err:
        # Tag then bare-raise: tag_failure returns the same exception, so
        # `raise tag_failure(err) from err` would make err its own __cause__
        # and hand a cycle to the chain walkers that read this tag back.
        # tag_failure is no-overwrite, so this INTERNAL verdict survives the
        # config phase's coarser CONFIG_INVALID tag in the runner.
        tag_failure(err, code=ErrorCode.INTERNAL, stage=FailureStage.CONFIG)
        raise
