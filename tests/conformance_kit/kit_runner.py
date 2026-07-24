"""Run the shipped conformance suite exactly as a connector repo does.

One subprocess runner shared by every acceptance test that exercises the
``pytest --pyargs`` entry point, so the invocation shape (interpreter,
plugin loading, PYTHONPATH) is defined once.
"""

from __future__ import annotations

import os
import subprocess  # nosec B404 - runs the suite the way a connector repo does
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
REFERENCE_DIR = Path(__file__).parent / "fixtures" / "reference"
REFERENCE_CLASS = "tests.conformance_kit.reference_connector:ReferenceConnector"


def run_kit_suite(
    suite: str,
    *,
    options: Sequence[str] = (),
    env_extra: Mapping[str, str] | None = None,
    load_plugin: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Run one kit suite (``cdk.conformance.tier1`` / ``tier2``) end to end.

    ``load_plugin`` mirrors the two consumer situations: a connector repo
    with the package pip-installed gets the options plugin via its entry
    point (here loaded explicitly with ``-p``); a plugin-less run
    configures the suite purely through environment variables.
    """
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        [str(REPO_ROOT / "cdk"), str(REPO_ROOT), env.get("PYTHONPATH", "")]
    ).rstrip(os.pathsep)
    env.update(env_extra or {})
    argv = [
        sys.executable,
        "-m",
        "pytest",
        "--pyargs",
        suite,
        "-q",
        "--no-header",
        "-p",
        "no:cacheprovider",
    ]
    if load_plugin:
        argv += ["-p", "cdk.conformance.plugin"]
    argv += list(options)
    return subprocess.run(  # nosec B603 - fixed argv, no shell
        argv,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
