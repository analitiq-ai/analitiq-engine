"""Cloud full-refresh matrix: hub-and-spoke around Postgres.

Each cloud database is tested against a local Postgres in both directions,
for each write mode. Postgres is the hub because it is the one
always-available local database, so a cloud DB only needs its own connector
+ credentials to participate.

Every cloud pair skips today: none of Snowflake / BigQuery / Redshift has a
DIP connector in the repo yet. Once one ships (and ``.env`` carries its
credentials), its parametrizations light up automatically. Incremental
coverage for cloud pairs lives in ``test_incremental.py``.
"""
from __future__ import annotations

import itertools
from typing import List

import pytest

from tests.e2e_databases.databases import all_specs, spec_for
from tests.e2e_databases.factory import WriteMode
from tests.e2e_databases.orchestrator import E2ETestRun
from tests.e2e_databases.seeds import canonical_seed_rows

_HUB = "postgres"
_WRITE_MODES: tuple[WriteMode, ...] = ("insert", "upsert")


def _cloud_slugs() -> List[str]:
    return sorted(slug for slug, spec in all_specs().items() if spec.is_cloud)


def _params():
    for cloud in _cloud_slugs():
        directions = ((cloud, _HUB), (_HUB, cloud))
        for (source, dest), write_mode in itertools.product(directions, _WRITE_MODES):
            yield pytest.param(
                source,
                dest,
                write_mode,
                id=f"{source}_to_{dest}_{write_mode}",
                marks=[pytest.mark.cloud, pytest.mark.full_refresh],
            )


@pytest.mark.parametrize("source_slug,dest_slug,write_mode", list(_params()))
def test_cloud_full_refresh(
    source_slug: str, dest_slug: str, write_mode: WriteMode
) -> None:
    source = spec_for(source_slug)
    dest = spec_for(dest_slug)
    for spec in (source, dest):
        reason = spec.available
        if reason is not None:
            pytest.skip(reason)

    run = E2ETestRun(source, dest, "full_refresh", write_mode)
    try:
        result = run.run()
    finally:
        run.teardown(keep_databases=True)

    assert result.actual_rows == canonical_seed_rows()
