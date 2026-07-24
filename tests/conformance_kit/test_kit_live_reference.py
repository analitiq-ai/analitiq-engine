"""Tier 2 passes against a real postgres (issue #391 acceptance, part 3).

Runs the shipped live suite against the reference connector and a
PostgreSQL service container, exactly as a connector repo's CI would:
suite via ``pytest --pyargs``, connection via a live-connection
document, credentials via ``env:`` secret refs. Skips (loudly) without
the ``CONFORMANCE_PG_*`` environment or the asyncpg driver — CI
provides both next to the service container.
"""

from __future__ import annotations

import importlib.util
import json
import os
import re
from pathlib import Path

import pytest

from .kit_runner import REFERENCE_CLASS, REFERENCE_DIR, run_kit_suite

_ENV_VARS = (
    "CONFORMANCE_PG_HOST",
    "CONFORMANCE_PG_PORT",
    "CONFORMANCE_PG_DATABASE",
    "CONFORMANCE_PG_USER",
    "CONFORMANCE_PG_PASSWORD",
)


def _live_settings() -> dict[str, str]:
    missing = [name for name in _ENV_VARS if not os.environ.get(name)]
    if missing:
        reason = (
            f"live reference tier needs {', '.join(missing)} (a postgres "
            f"service container); the CI conformance job provides them"
        )
        if os.environ.get("ANALITIQ_CONFORMANCE_REQUIRE_LIVE"):
            # The CI job that provisions the container sets the strict
            # flag, so a renamed or typo'd variable fails the job
            # instead of retiring the live tier while it stays green.
            pytest.fail(f"ANALITIQ_CONFORMANCE_REQUIRE_LIVE is set but {reason}")
        pytest.skip(reason)
    if importlib.util.find_spec("asyncpg") is None:
        pytest.skip(
            "live reference tier needs the asyncpg driver installed, as a "
            "connector repo installs its own driver; the CI conformance job "
            "installs it"
        )
    return {name: os.environ[name] for name in _ENV_VARS}


def test_tier2_suite_passes_against_postgres(tmp_path: Path) -> None:
    settings = _live_settings()
    document = {
        "connection_id": "conformance-live",
        "schema": "public",
        "config": {
            "parameters": {
                "host": settings["CONFORMANCE_PG_HOST"],
                "port": settings["CONFORMANCE_PG_PORT"],
                "database": settings["CONFORMANCE_PG_DATABASE"],
                "username": settings["CONFORMANCE_PG_USER"],
            },
            "secret_refs": {"password": "env:CONFORMANCE_PG_PASSWORD"},
        },
    }
    document_path = tmp_path / "live-connection.json"
    document_path.write_text(json.dumps(document, indent=2))

    completed = run_kit_suite(
        "cdk.conformance.tier2",
        options=[
            "--connector-dir",
            str(REFERENCE_DIR),
            "--connector-class",
            REFERENCE_CLASS,
            "--live-connection",
            str(document_path),
        ],
    )
    output = completed.stdout + completed.stderr
    assert completed.returncode == 0, f"tier 2 failed against postgres:\n{output}"
    passed = re.search(r"(\d+) passed", output)
    assert (
        passed and int(passed.group(1)) >= 7
    ), f"expected the live tier to actually run its scenarios, got:\n{output}"
    assert not re.search(r"\d+ skipped", output), (
        f"every live scenario applies to the reference connector; a skip is "
        f"a gating regression:\n{output}"
    )
