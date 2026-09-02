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

    ``NOTSET`` raises for the same reason, though it IS a name. On the root
    logger it is not "log everything" but ``0``, and ``isEnabledFor(0)`` is
    false -- so it silences the whole run, which is the opposite of what an
    operator typing it intends and is indistinguishable from a process that
    had nothing to say.
    """
    level = logging.getLevelNamesMapping().get(name.upper())
    if level is None or level == logging.NOTSET:
        usable = sorted(
            n
            for n, value in logging.getLevelNamesMapping().items()
            if value != logging.NOTSET
        )
        raise ValueError(f"Unusable log level {name!r}; expected one of {usable}")
    return level


def apply_log_level(name: str) -> None:
    """Set the root logger to *name*, superseding the startup ``LOG_LEVEL``."""
    logging.getLogger().setLevel(resolve_level(name))


def current_log_level() -> int:
    """Return the level the root logger is currently effective at.

    The number, not its name. A name is a lossy channel in one direction:
    ``getLevelName`` answers ``"Level 25"`` for any level the stdlib does
    not name, and the far side of the worker boundary then cannot resolve
    it -- a crash for a value that was correct where it started. The
    integer is what ``setLevel`` takes anyway, and it is JSON-native, so
    nothing has to be translated to carry it.
    """
    return logging.getLogger().getEffectiveLevel()
