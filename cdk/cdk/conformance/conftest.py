"""Fixtures for the conformance suite.

Loaded automatically when the suite is collected via
``pytest --pyargs cdk.conformance...`` (pytest picks up the package's
own conftest). The command-line options live in
:mod:`cdk.conformance.plugin` (auto-registered where ``analitiq-cdk``
is pip-installed); every option doubles as an environment variable so
the suite runs even where the plugin is not loaded:
``--connector-dir`` / ``ANALITIQ_CONNECTOR_DIR``, ``--connector-class``
/ ``ANALITIQ_CONNECTOR_CLASS``, ``--live-connection`` /
``ANALITIQ_LIVE_CONNECTION``.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from .target import ConformanceTarget, load_target


def _option_or_env(config: pytest.Config, option: str, env_var: str) -> str | None:
    value = config.getoption(option, default=None)
    if value:
        return str(value)
    return os.environ.get(env_var) or None


@pytest.fixture(scope="session")
def conformance_target(request: pytest.FixtureRequest) -> ConformanceTarget:
    """Load the connector under test once per session, fail-loud."""
    root = (
        _option_or_env(request.config, "--connector-dir", "ANALITIQ_CONNECTOR_DIR")
        or "."
    )
    class_path = _option_or_env(
        request.config, "--connector-class", "ANALITIQ_CONNECTOR_CLASS"
    )
    return load_target(Path(root), class_path=class_path)


@pytest.fixture(scope="session")
def live_connection_path(request: pytest.FixtureRequest) -> Path:
    """Return the live connection document's path, or skip the live tier.

    ``ANALITIQ_CONFORMANCE_REQUIRE_LIVE`` hardens the gate for CI jobs
    that provision a service container: with it set, a missing live
    connection is a failure, not a skip — so a typo'd env var or a
    renamed option can never silently retire the live tier while the
    job stays green.
    """
    raw = _option_or_env(
        request.config, "--live-connection", "ANALITIQ_LIVE_CONNECTION"
    )
    if not raw:
        reason = (
            "no live connection configured (--live-connection / "
            "ANALITIQ_LIVE_CONNECTION); the live tier runs only where the "
            "connector repo provides a database service container"
        )
        if os.environ.get("ANALITIQ_CONFORMANCE_REQUIRE_LIVE"):
            pytest.fail(f"ANALITIQ_CONFORMANCE_REQUIRE_LIVE is set but {reason}")
        pytest.skip(reason)
    path = Path(raw)
    if not path.is_file():
        pytest.fail(f"live connection document {path} does not exist")
    return path
