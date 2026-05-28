"""Local full-refresh matrix: every local DB as source against every local
DB as destination, for each write mode.

This is the broad "does one sync move the data correctly" check. Replication
depth (resuming from a bookmark, deltas, no-op syncs) lives in
``test_incremental.py``; this file fixes ``full_refresh`` and varies the
write mode (insert vs upsert) instead.

Only pairs whose DIP connectors are present actually run; the rest skip with
a clear reason. Today that means the Postgres/MySQL combinations execute and
DuckDB / SQLite / MongoDB / ClickHouse skip until their connectors ship.
"""
from __future__ import annotations

import itertools
from typing import List

import pytest

from tests.e2e_databases.databases import all_specs, spec_for
from tests.e2e_databases.factory import WriteMode
from tests.e2e_databases.orchestrator import E2ETestRun
from tests.e2e_databases.seeds import canonical_seed_rows

_WRITE_MODES: tuple[WriteMode, ...] = ("insert", "upsert")


def _local_slugs() -> List[str]:
    return sorted(slug for slug, spec in all_specs().items() if not spec.is_cloud)


def _params():
    for source, dest, write_mode in itertools.product(
        _local_slugs(), _local_slugs(), _WRITE_MODES
    ):
        yield pytest.param(
            source,
            dest,
            write_mode,
            id=f"{source}_to_{dest}_{write_mode}",
            marks=[pytest.mark.local, pytest.mark.full_refresh],
        )


@pytest.mark.parametrize("source_slug,dest_slug,write_mode", list(_params()))
def test_local_full_refresh(
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
