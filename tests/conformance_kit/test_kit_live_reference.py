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
import subprocess  # nosec B404 - runs the suite the way a connector repo does
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
REFERENCE_DIR = Path(__file__).parent / "fixtures" / "reference"
REFERENCE_CLASS = "tests.conformance_kit.reference_connector:ReferenceConnector"

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
        pytest.skip(
            f"live reference tier needs {', '.join(missing)} (a postgres "
            f"service container); the CI conformance job provides them"
        )
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

    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        [str(REPO_ROOT / "cdk"), str(REPO_ROOT), env.get("PYTHONPATH", "")]
    ).rstrip(os.pathsep)
    completed = subprocess.run(  # nosec B603 - fixed argv, no shell
        [
            sys.executable,
            "-m",
            "pytest",
            "--pyargs",
            "cdk.conformance.tier2",
            "-q",
            "--no-header",
            "-p",
            "no:cacheprovider",
            "-p",
            "cdk.conformance.plugin",
            "--connector-dir",
            str(REFERENCE_DIR),
            "--connector-class",
            REFERENCE_CLASS,
            "--live-connection",
            str(document_path),
        ],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    output = completed.stdout + completed.stderr
    assert completed.returncode == 0, f"tier 2 failed against postgres:\n{output}"
    passed = re.search(r"(\d+) passed", output)
    assert (
        passed and int(passed.group(1)) >= 7
    ), f"expected the live tier to actually run its scenarios, got:\n{output}"
