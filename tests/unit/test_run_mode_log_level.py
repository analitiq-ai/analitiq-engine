"""A pipeline's declared log level is applied in BOTH run modes.

``LOG_LEVEL`` governs everything logged before a pipeline document exists.
Once ``runtime.logging.log_level`` is loaded it supersedes it -- and it has to
do so identically in the engine and in the destination server, which load the
same document. A level honoured in one mode only is edge-case divergence for
the same author intent, not a mode quirk.
"""

from __future__ import annotations

import importlib.util
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.main import run_destination_mode
from src.models.resolved import LoggingConfig, RuntimeConfig
from src.runner import PipelineRunner


@pytest.fixture
def root_level_restored():
    """Give the test the root logger back the way it found it."""
    root = logging.getLogger()
    previous = root.level
    root.setLevel(logging.WARNING)
    yield root
    root.setLevel(previous)


def _config_prep_returning(log_level: str, *, destinations: list[str]) -> MagicMock:
    pipeline_config = MagicMock()
    pipeline_config.runtime = RuntimeConfig(logging=LoggingConfig(log_level=log_level))
    pipeline_config.connections.destinations = destinations
    prep = MagicMock()
    prep.create_config.return_value = (pipeline_config, [], {}, {}, [])
    return prep


class TestDeclaredLevelIsApplied:
    async def test_destination_mode(self, monkeypatch, root_level_restored):
        monkeypatch.setenv("PIPELINE_ID", "p")
        prep = _config_prep_returning("DEBUG", destinations=[])
        with patch(
            "src.engine.pipeline_config_prep.PipelineConfigPrep", return_value=prep
        ):
            # No destinations: the mode exits right after applying the level,
            # before any handler or server is built.
            with pytest.raises(SystemExit):
                await run_destination_mode()

        assert root_level_restored.level == logging.DEBUG

    async def test_source_mode(self, monkeypatch, root_level_restored):
        monkeypatch.setenv("PIPELINE_ID", "p")
        prep = _config_prep_returning("DEBUG", destinations=["d"])
        with (
            patch("src.runner.PipelineConfigPrep", return_value=prep),
            patch("src.runner._build_config_dict", side_effect=RuntimeError("stop")),
            patch("src.runner.save_pipeline_metrics"),
        ):
            assert await PipelineRunner().run() is False

        assert root_level_restored.level == logging.DEBUG


class TestDotenvAtProcessStart:
    def test_the_entry_point_loads_dotenv_before_any_env_lookup(self):
        """Both run modes share this module, so both see the same ``.env``.

        Loading it in ``PipelineRunner.__init__`` instead (its previous home)
        is reached only in source mode, which would leave the destination
        server reading a different environment from the engine beside it.

        Executed under a throwaway module name so the reload does not swap the
        ``src.main`` object other tests hold references into.
        """
        source = Path(__file__).resolve().parents[2] / "src" / "main.py"
        spec = importlib.util.spec_from_file_location("_main_probe", source)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)

        with patch("dotenv.load_dotenv") as load_dotenv:
            spec.loader.exec_module(module)

        load_dotenv.assert_called_once_with()
