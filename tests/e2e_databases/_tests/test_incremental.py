"""True incremental replication: prove a sync resumes from its bookmark.

A single incremental sync on a freshly-seeded table is indistinguishable
from a full refresh — both move every row. What makes incremental real is
the *second* sync: it must read only the rows whose cursor advanced past the
bookmark the first sync left behind.

The sequence below proves that without trusting engine logs:

1. Seed the source, sync once -> destination matches the canonical seed.
2. Plant a sentinel directly in the destination, overwriting a row whose
   source cursor sits below the bookmark. A correct incremental sync never
   re-reads that row, so the sentinel must survive. A full re-read would
   clobber it back to the canonical value.
3. Mutate the source above the bookmark (bump one row, insert another) and
   sync again -> only the delta moves: the sentinel survives, the bumped row
   updates, the new row appears.
4. Sync a third time with no source change -> the destination is byte-for-byte
   identical (no duplicates, sentinel still intact).

Incremental needs ``upsert``: the inclusive ``>=`` cursor re-reads the
boundary row on every sync, which would raise a primary-key conflict under
plain ``insert``.
"""
from __future__ import annotations

import itertools
from typing import List

import pytest

from tests.e2e_databases.databases import all_specs, spec_for
from tests.e2e_databases.databases._base import DatabaseSpec
from tests.e2e_databases.orchestrator import E2ETestRun
from tests.e2e_databases.seeds import (
    canonical_seed_rows,
    destination_sentinel_row,
    expected_after_incremental,
    incremental_mutation_rows,
)

_HUB = "postgres"


def _local_slugs() -> List[str]:
    return sorted(slug for slug, spec in all_specs().items() if not spec.is_cloud)


def _cloud_slugs() -> List[str]:
    return sorted(slug for slug, spec in all_specs().items() if spec.is_cloud)


def _skip_if_unavailable(*specs: DatabaseSpec) -> None:
    for spec in specs:
        reason = spec.available
        if reason is not None:
            pytest.skip(reason)


def _assert_incremental_sequence(source: DatabaseSpec, dest: DatabaseSpec) -> None:
    run = E2ETestRun(source, dest, "incremental", write_mode="upsert")
    try:
        run.setup()

        # Run 1: initial load. An empty bookmark reads the whole source.
        assert run.sync() == canonical_seed_rows()

        # Plant a sentinel below the bookmark, then mutate above it.
        dest.upsert_rows("destination", [destination_sentinel_row()])
        source.upsert_rows("source", incremental_mutation_rows())

        # Run 2: only the delta moves. Sentinel survival proves below-bookmark
        # rows were not re-read; the bump and insert prove the delta applied.
        expected = expected_after_incremental()
        assert run.sync() == expected

        # Run 3: no source change -> a no-op sync leaves the destination
        # untouched (no duplicates, sentinel still intact).
        assert run.sync() == expected
    finally:
        run.teardown(keep_databases=True)


def _local_params():
    for source, dest in itertools.product(_local_slugs(), _local_slugs()):
        yield pytest.param(
            source,
            dest,
            id=f"{source}_to_{dest}",
            marks=[pytest.mark.local, pytest.mark.incremental],
        )


def _cloud_params():
    for cloud in _cloud_slugs():
        for source, dest in ((cloud, _HUB), (_HUB, cloud)):
            yield pytest.param(
                source,
                dest,
                id=f"{source}_to_{dest}",
                marks=[pytest.mark.cloud, pytest.mark.incremental],
            )


@pytest.mark.parametrize("source_slug,dest_slug", list(_local_params()))
def test_local_incremental(source_slug: str, dest_slug: str) -> None:
    source = spec_for(source_slug)
    dest = spec_for(dest_slug)
    _skip_if_unavailable(source, dest)
    _assert_incremental_sequence(source, dest)


@pytest.mark.parametrize("source_slug,dest_slug", list(_cloud_params()))
def test_cloud_incremental(source_slug: str, dest_slug: str) -> None:
    source = spec_for(source_slug)
    dest = spec_for(dest_slug)
    _skip_if_unavailable(source, dest)
    _assert_incremental_sequence(source, dest)
