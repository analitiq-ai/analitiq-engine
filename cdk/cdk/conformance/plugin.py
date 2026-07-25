"""Pytest plugin registering the conformance suite's command-line options.

Options must exist before argument parsing, which a package conftest
loaded via ``--pyargs`` is too late for — so they live in this plugin,
loaded explicitly by the invocation that wants flags
(``pytest -p cdk.conformance.plugin --pyargs cdk.conformance.tier1``).
A run configured purely through the environment variables the fixtures
read needs no plugin at all. Deliberately not a ``pytest11`` entry
point: auto-loading into every pytest run of every environment that
merely installs the core CDK is a side effect nobody asked for.
"""

from __future__ import annotations

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    """Register the suite's target-selection options."""
    group = parser.getgroup(
        "analitiq-conformance", "Analitiq connector conformance suite"
    )
    group.addoption(
        "--connector-dir",
        default=None,
        help=(
            "Path to the connector package checkout (the directory holding "
            "definition/connector.json). Defaults to $ANALITIQ_CONNECTOR_DIR, "
            "then the current directory."
        ),
    )
    group.addoption(
        "--connector-class",
        default=None,
        help=(
            "Optional 'package.module:ClassName' overriding entry-point "
            "resolution of the connector class. Defaults to "
            "$ANALITIQ_CONNECTOR_CLASS."
        ),
    )
    group.addoption(
        "--live-connection",
        default=None,
        help=(
            "Path to the tier-2 live connection document (JSON). Defaults "
            "to $ANALITIQ_LIVE_CONNECTION. Without it the live tier skips."
        ),
    )
