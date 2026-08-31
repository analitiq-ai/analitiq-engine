"""Installing the verbosity a pipeline declared, once its config is loaded.

``src.main`` configures logging at import from ``LOG_LEVEL``, long before any
pipeline document exists. That level has to stay: it is what everything logged
during config load is emitted at, and it is the only level a process that never
reaches a pipeline -- a bad ``RUN_MODE``, a missing ``PIPELINE_ID`` -- ever has.
Once the document is loaded, its ``runtime.logging.log_level`` is the authority.

Both run modes load the same pipeline document (the destination reads it
through the same :class:`~src.engine.pipeline_config_prep.PipelineConfigPrep`),
so both install the level through this one function rather than each keeping
its own ``setLevel`` call -- two copies is how a source run and the destination
serving it come to log at different levels.
"""

import logging
from typing import Final

from analitiq.contracts.pipelines.config import Logging

from src.shared.contract_literals import contract_literals

#: The levels a run may be set to, taken from the published contract rather
#: than from ``logging``'s own attributes. One vocabulary for one env var: the
#: import-time configuration and the per-pipeline block read the same
#: ``LOG_LEVEL``, and ``getattr(logging, name, INFO)`` would accept ``WARN``,
#: ``FATAL`` and ``NOTSET`` for the first while the second refused them -- so a
#: deployment setting ``LOG_LEVEL=WARN`` would start fine and then fail every
#: pipeline at config parse, with a message about a level no document mentions.
VALID_LOG_LEVELS: Final[frozenset[str]] = contract_literals(Logging, "log_level")


def require_log_level(level: str) -> str:
    """Return *level* if the contract declares it, else refuse naming the set.

    Called at process start on the environment's value and again on a
    pipeline's declared one, so an unusable level fails where it was written
    rather than being silently read as INFO.
    """
    if level not in VALID_LOG_LEVELS:
        raise ValueError(
            f"Unknown log level {level!r}; "
            f"expected one of {sorted(VALID_LOG_LEVELS)}"
        )
    return level


def apply_log_level(level: str) -> None:
    """Set the root logger to *level* for the remainder of the process.

    ``logging.basicConfig`` configures the root logger on its first call only,
    so calling it again here would silently do nothing; the level is set on the
    root logger directly instead. The handler ``src.main`` installs carries no
    level of its own -- ``NOTSET`` passes every record its logger admits --
    which leaves the root logger's level as the whole gate, so nothing else
    needs touching for a pipeline declaring ``DEBUG`` to actually emit DEBUG.

    *level* is a name from the published contract's enum, checked against it by
    :class:`~src.models.resolved.LoggingConfig` before it gets here, so an
    unresolvable name is a defect that already failed at config parse.
    """
    logging.getLogger().setLevel(level)
