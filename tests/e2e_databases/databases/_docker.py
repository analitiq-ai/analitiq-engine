"""Thin wrapper around ``docker compose`` for the test framework.

Every test uses one compose project, rooted at ``tests/e2e_databases/``,
that defines all services across all DBs. Profiles control which
services come up — bringing up only the source DB + destination DB +
engine services for each test.
"""
from __future__ import annotations

import logging
import subprocess
from pathlib import Path
from typing import Iterable, List

logger = logging.getLogger(__name__)

_FRAMEWORK_ROOT = Path(__file__).resolve().parents[1]
_COMPOSE_FILE = _FRAMEWORK_ROOT / "docker-compose.yml"


def _compose_cmd(extra_args: Iterable[str]) -> List[str]:
    return ["docker", "compose", "-f", str(_COMPOSE_FILE), *extra_args]


def compose_up(*services: str) -> None:
    """Bring the given services up in detached mode."""
    if not services:
        return
    cmd = _compose_cmd(["up", "-d", "--wait", *services])
    logger.info("compose up: %s", " ".join(services))
    subprocess.run(cmd, check=True)


def compose_down(*services: str) -> None:
    """Stop and remove the given services. Volumes are wiped."""
    if not services:
        return
    cmd = _compose_cmd(["rm", "-f", "-s", "-v", *services])
    logger.info("compose down: %s", " ".join(services))
    subprocess.run(cmd, check=False)


def compose_run_source_engine(pipeline_id: str) -> subprocess.CompletedProcess[str]:
    """Run the one-shot source engine container against ``pipeline_id``."""
    cmd = _compose_cmd(
        [
            "run",
            "--rm",
            "-e",
            f"PIPELINE_ID={pipeline_id}",
            "source_engine",
        ]
    )
    logger.info("compose run source_engine PIPELINE_ID=%s", pipeline_id)
    return subprocess.run(cmd, capture_output=True, text=True, check=False)


def compose_down_all() -> None:
    """Stop everything and remove all volumes / networks."""
    cmd = _compose_cmd(["down", "-v", "--remove-orphans"])
    logger.info("compose down -v --remove-orphans")
    subprocess.run(cmd, check=False)
