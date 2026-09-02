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
        "log_level": logging.DEBUG,
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
        assert bootstrap["log_level"] == logging.DEBUG

    def test_the_child_environment_still_carries_no_level(self, monkeypatch):
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")
        assert "LOG_LEVEL" not in _clean_env()


class TestWorkerAppliesTheBootstrapLevel:
    def test_main_sets_the_root_logger_from_the_bootstrap(self, root_level_restored):
        root_level_restored.setLevel(logging.INFO)
        bootstrap = parse_bootstrap(_bootstrap_raw(log_level=logging.DEBUG))

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


class TestTheRelayDoesNotFilterTwice:
    """The worker already logs at the declared level; the relay must not re-filter.

    A pipeline declaring WARNING raises the shell's root logger, and a relay
    fixed at INFO would then drop every line the worker chose to emit --
    including the startup traceback that is the only explanation for
    ``worker exited before becoming ready``.
    """

    async def test_a_worker_line_survives_an_elevated_declared_level(
        self, root_level_restored, caplog
    ):
        import asyncio

        from src.worker.spawn import _forward_stderr

        root_level_restored.setLevel(logging.WARNING)
        stream = asyncio.StreamReader()
        stream.feed_data(b"Traceback (most recent call last):\n")
        stream.feed_eof()
        with caplog.at_level(logging.WARNING):
            await _forward_stderr("pg-source", stream)
        assert "Traceback" in caplog.text


class TestNotsetIsNotAnExecutableLevel:
    def test_it_is_refused_rather_than_silencing_the_run(self):
        # NOTSET is a real name, so it used to resolve to 0 -- and
        # isEnabledFor(0) is false, so the root logger set to it emits
        # nothing at all. A run that says nothing is indistinguishable from
        # a run that had nothing to say, including the worker's own stderr.
        from src.shared.logging_setup import resolve_level

        with pytest.raises(ValueError, match="NOTSET"):
            resolve_level("NOTSET")

    def test_a_usable_level_still_resolves(self):
        from src.shared.logging_setup import resolve_level

        assert resolve_level("debug") == logging.DEBUG


class TestTheLevelTravelsAsANumber:
    async def test_a_level_the_stdlib_does_not_name_still_reaches_the_worker(
        self, tmp_path: Path, root_level_restored
    ):
        # The whole reason the payload carries the integer. ``getLevelName``
        # answers "Level 25" for any level the stdlib does not name, and the
        # far side could not resolve that back -- a crash across the process
        # boundary for a value that was correct where it started.
        root_level_restored.setLevel(25)

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
        assert bootstrap["log_level"] == 25
        assert parse_bootstrap(_bootstrap_raw(log_level=25)).log_level == 25
