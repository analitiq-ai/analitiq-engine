"""The declared log level reaches the worker on the bootstrap, not the env.

``_clean_env`` strips ``LOG_LEVEL`` from the worker's environment on purpose,
so a pipeline declaring DEBUG would otherwise run the engine at DEBUG and its
connector worker at INFO in the same run. These pin the handover: the shell
packs the level it is itself running at, and the worker applies what it was
handed.
"""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.worker import __main__ as worker_main
from src.worker.bootstrap import parse_bootstrap
from src.worker.shell import build_bootstrap
from src.worker.spawn import _clean_env


@pytest.fixture
def root_level_restored():
    """Give the test the root logger back the way it found it."""
    root = logging.getLogger()
    previous = root.level
    yield root
    root.setLevel(previous)


def _bootstrap_raw(**overrides):
    raw = {
        "role": "source",
        "kind": "database",
        "connector_id": "postgres",
        "uds_path": "/tmp/w/worker.sock",  # nosec B108
        "log_level": "DEBUG",
        "connection": {"connection_id": "my-pg"},
    }
    raw.update(overrides)
    return raw


class TestShellPacksItsLevel:
    async def test_bootstrap_carries_the_level_the_shell_runs_at(
        self, tmp_path: Path, root_level_restored
    ):
        root_level_restored.setLevel(logging.DEBUG)

        runtime = MagicMock()
        runtime.connector_type = "database"
        runtime.connector_id = "postgres"
        runtime.connection_id = "my-pg"
        runtime.resolve_spec = AsyncMock(return_value={"connection_id": "my-pg"})

        bootstrap = await build_bootstrap(
            runtime,
            role="source",
            connectors_dir=tmp_path / "connectors",
            connections_dir=tmp_path / "connections",
        )
        assert bootstrap["log_level"] == "DEBUG"

    def test_the_child_environment_still_carries_no_level(self, monkeypatch):
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")
        assert "LOG_LEVEL" not in _clean_env()


class TestWorkerAppliesTheBootstrapLevel:
    def test_main_sets_the_root_logger_from_the_bootstrap(self, root_level_restored):
        root_level_restored.setLevel(logging.INFO)
        bootstrap = parse_bootstrap(_bootstrap_raw(log_level="DEBUG"))

        applied: list[int] = []
        with (
            patch.object(
                worker_main, "read_bootstrap_from_stdin", return_value=bootstrap
            ),
            patch.object(
                worker_main.asyncio,
                "run",
                side_effect=lambda coro: (
                    coro.close(),
                    applied.append(logging.getLogger().getEffectiveLevel()),
                    0,
                )[-1],
            ),
        ):
            assert worker_main.main() == 0

        assert applied == [logging.DEBUG]
