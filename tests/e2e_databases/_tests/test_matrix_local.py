"""Local database matrix: every local DB as source against every local DB
as destination, in both replication modes.

Only pairs whose DIP connectors are present actually run; the rest skip
with a clear reason. Today that means the Postgres/MySQL combinations
execute and DuckDB / SQLite / MongoDB / ClickHouse skip until their
connectors ship in the DIP registry.
"""
from __future__ import annotations

import itertools
from typing import List

import pytest

from tests.e2e_databases.databases import all_specs, spec_for
from tests.e2e_databases.factory import ReplicationMode
from tests.e2e_databases.orchestrator import E2ETestRun
from tests.e2e_databases.seeds import canonical_seed_rows

_MODES: tuple[ReplicationMode, ...] = ("full_refresh", "incremental")


def _local_slugs() -> List[str]:
    return sorted(slug for slug, spec in all_specs().items() if not spec.is_cloud)


def _params():
    for source, dest, mode in itertools.product(_local_slugs(), _local_slugs(), _MODES):
        yield pytest.param(
            source,
            dest,
            mode,
            id=f"{source}_to_{dest}_{mode}",
            marks=[pytest.mark.local, getattr(pytest.mark, mode)],
        )


@pytest.mark.parametrize("source_slug,dest_slug,mode", list(_params()))
def test_local_pair(source_slug: str, dest_slug: str, mode: ReplicationMode) -> None:
    source = spec_for(source_slug)
    dest = spec_for(dest_slug)
    for spec in (source, dest):
        reason = spec.available
        if reason is not None:
            pytest.skip(reason)

    run = E2ETestRun(source, dest, mode)
    try:
        result = run.run()
    finally:
        run.teardown(keep_databases=True)

    assert result.actual_rows == canonical_seed_rows()
