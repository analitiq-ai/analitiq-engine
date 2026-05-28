"""Shared fixtures for the database E2E matrix.

Two responsibilities:

1. Make the repo importable. pytest roots itself at this directory (the
   nearest ``pytest.ini``), so the ``tests.e2e_databases`` / ``src``
   absolute imports the framework uses need the repo root on
   ``sys.path``.
2. Load optional cloud credentials from ``.env`` and guarantee the whole
   compose project is torn down once at the end of the session, so a
   crashed run never leaves containers behind.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

import pytest

_FRAMEWORK_ROOT = Path(__file__).resolve().parent
_REPO_ROOT = _FRAMEWORK_ROOT.parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from tests.e2e_databases.databases._docker import compose_down_all  # noqa: E402

logger = logging.getLogger(__name__)


def pytest_configure(config: pytest.Config) -> None:
    """Load cloud credentials from ``.env`` if present.

    Anything missing simply leaves the corresponding cloud pair to skip;
    we never fail a run because credentials are absent.
    """
    env_file = _FRAMEWORK_ROOT / ".env"
    if not env_file.is_file():
        logger.info("No .env at %s — cloud pairs will skip", env_file)
        return
    try:
        from dotenv import load_dotenv
    except ImportError:
        logger.warning("python-dotenv not installed; skipping .env load")
        return
    load_dotenv(env_file)
    logger.info("Loaded cloud credentials from %s", env_file)


@pytest.fixture(scope="session", autouse=True)
def _compose_project_teardown():
    """Tear the whole framework compose project down at session end.

    Individual runs keep their DB containers up for reuse across the
    matrix; this is the single place that removes them all once every
    pair has run (or skipped).
    """
    yield
    compose_down_all()
