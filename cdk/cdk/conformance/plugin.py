"""Pytest plugin registering the conformance suite's command-line options.

Options must exist before argument parsing, which a package conftest
loaded via ``--pyargs`` is too late for — so they live in this plugin,
auto-registered through the ``pytest11`` entry point wherever
``analitiq-cdk`` is pip-installed (every connector repo). An in-tree
consumer without the entry point loads it explicitly
(``-p cdk.conformance.plugin``) or configures the suite through the
environment variables the fixtures fall back to.
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
