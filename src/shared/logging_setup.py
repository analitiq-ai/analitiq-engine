"""Process-wide log level: the one place that translates and applies it.

``src.main`` configures logging at import from ``LOG_LEVEL`` -- that level
governs everything logged before a pipeline document exists, and stands as the
default for a pipeline that declares none. Once the pipeline's
``runtime.logging.log_level`` is known it supersedes it, in both run modes and
in every spawned connector worker. Those three sites share this module so the
same declared level cannot mean three different things in one run.
"""

from __future__ import annotations

import logging


def resolve_level(name: str) -> int:
    """Translate a level name into its :mod:`logging` constant.

    An unknown name raises rather than degrading to INFO: a silently ignored
    ``LOG_LEVEL=DEGUB`` is indistinguishable from a level that was honoured.
    """
    level = logging.getLevelNamesMapping().get(name.upper())
    if level is None:
        raise ValueError(
            f"Unknown log level {name!r}; expected one of "
            f"{sorted(logging.getLevelNamesMapping())}"
        )
    return level


def apply_log_level(name: str) -> None:
    """Set the root logger to *name*, superseding the startup ``LOG_LEVEL``."""
    logging.getLogger().setLevel(resolve_level(name))


def current_log_level_name() -> str:
    """Name of the level the root logger is currently effective at."""
    return logging.getLevelName(logging.getLogger().getEffectiveLevel())
