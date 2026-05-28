"""Thin wrapper around ``docker compose`` for the test framework.

Every test uses one compose project, rooted at ``tests/e2e_databases/``,
that defines all services across all DBs. The framework brings up only the
services a given pair needs by naming them explicitly on ``compose up`` /
``compose run`` — there are no compose profiles.
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


def compose_recreate(*services: str) -> None:
    """Force-recreate services so they pick up the current environment.

    Used for the destination engine: it binds ``PIPELINE_ID`` at container
    start, so a container left over from a previous pipeline would serve
    stale config. ``--force-recreate`` guarantees a fresh binding for the
    current run regardless of whether the prior teardown succeeded;
    ``--wait`` blocks until it is healthy so a startup failure surfaces
    here rather than as a confusing gRPC error later.
    """
    if not services:
        return
    cmd = _compose_cmd(["up", "-d", "--force-recreate", "--wait", *services])
    logger.info("compose up --force-recreate: %s", " ".join(services))
    subprocess.run(cmd, check=True)


def compose_down(*services: str) -> None:
    """Stop and remove the given services (best effort).

    Anonymous volumes attached to those containers are removed too; the DB
    services here declare no named volumes, so nothing persists between
    runs. Teardown ignores failures — a container that is already gone is
    not an error.
    """
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
