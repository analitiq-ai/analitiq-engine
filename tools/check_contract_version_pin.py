"""Fail CI when this repo's contract pin diverges from the published release.

``tests/unit/analitiq_stream/test_contract_pin_agreement.py`` already
establishes, offline, that this repo's four manifests agree on one
``analitiq-contract-models`` version (and that ``analitiq-validator`` moves
with it as a pair). This script reuses that exact logic to name "this
repo's pin" -- loading the test module rather than re-deriving the manifest
parsing -- then compares it against the provenance fact the plugins repo's
published schema tree stamps at render time.

Usage (from the repository root)::

    python tools/check_contract_version_pin.py

A fetch failure (network error, timeout, non-2xx, malformed JSON, missing
key) fails outright, as does a successful fetch that reveals a version
mismatch. Neither case retries (analitiq-ai/analitiq-engine#418): this
repo's CI is never the trigger of a contract-models release, so there is no
CDN-propagation race to wait out, and a warn-only window would let the two
drift again without a CI signal naming it.
"""

from __future__ import annotations

import importlib.util
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path
from types import ModuleType
from typing import cast

REPO_ROOT = Path(__file__).resolve().parent.parent
PIN_TEST_MODULE_PATH = (
    REPO_ROOT / "tests" / "unit" / "analitiq_stream" / "test_contract_pin_agreement.py"
)
PUBLISHED_FACT_URL = "https://schemas.analitiq.ai/contracts-version.json"
PACKAGE = "analitiq-contract-models"
FETCH_TIMEOUT_SECONDS = 10


def _load_pin_test_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "contract_pin_agreement_under_test", PIN_TEST_MODULE_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def local_pin(pin_test_module: ModuleType) -> str:
    """Return this repo's single agreed ``analitiq-contract-models`` version.

    Reuses the pin-agreement test's own manifest reader, so the two checks
    read "this repo's pin" the same way and cannot silently diverge on what
    that means.
    """
    pins = pin_test_module._application_pins(PACKAGE)
    versions = set(pins.values())
    if len(versions) != 1:
        raise RuntimeError(
            f"{PACKAGE} is not pinned to a single version across manifests: "
            f"{pins}; run `poetry run pytest "
            "tests/unit/analitiq_stream/test_contract_pin_agreement.py` for detail"
        )
    (version,) = versions
    return cast(str, version)


def published_version() -> str:
    try:
        with urllib.request.urlopen(  # noqa: S310 # nosec B310
            PUBLISHED_FACT_URL, timeout=FETCH_TIMEOUT_SECONDS
        ) as response:
            body = response.read()
    except urllib.error.HTTPError as exc:
        raise RuntimeError(
            f"fetching {PUBLISHED_FACT_URL} returned HTTP {exc.code}"
        ) from exc
    except OSError as exc:
        raise RuntimeError(f"failed to fetch {PUBLISHED_FACT_URL}: {exc}") from exc

    try:
        fact = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"{PUBLISHED_FACT_URL} did not return valid JSON: {exc}"
        ) from exc

    try:
        return cast(str, fact[PACKAGE])
    except KeyError:
        raise RuntimeError(
            f"{PUBLISHED_FACT_URL} has no {PACKAGE!r} key: {fact}"
        ) from None


def main() -> int:
    local = local_pin(_load_pin_test_module())
    published = published_version()
    if local != published:
        print(
            f"{PACKAGE} pin mismatch: this repo pins {local!r}, the published "
            f"release is {published!r}. Bump pyproject.toml, "
            "docker/requirements.txt, .pre-commit-config.yaml and cdk/pyproject.toml's "
            f"range to {published!r}.",
            file=sys.stderr,
        )
        return 1
    print(f"{PACKAGE} pin {local!r} matches the published release")
    return 0


if __name__ == "__main__":
    sys.exit(main())
