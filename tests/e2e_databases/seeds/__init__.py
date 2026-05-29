"""Canonical seed data used by every database in the matrix.

All databases get the same logical rows; per-DB type translation lives
in each ``databases/{slug}.py`` module.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List


@dataclass(frozen=True)
class SeedRow:
    id: int
    name: str
    email: str
    score: int | None
    created_at: datetime
    updated_at: datetime


SEED_TABLE_NAME = "e2e_seed_data"
CURSOR_COLUMN = "updated_at"
PRIMARY_KEY_COLUMN = "id"

# Name written into the destination between incremental syncs. A correct
# incremental sync never re-reads the row it sits on (its source cursor is
# below the bookmark), so the sentinel must survive every subsequent sync. A
# full re-read would clobber it back to the canonical name.
INCREMENTAL_SENTINEL_NAME = "SENTINEL_NOT_REPLACED"


def canonical_seed_rows() -> List[SeedRow]:
    """The five rows every source DB receives before a test run.

    Deliberately constructed so the assertions can catch real bugs:
    ``created_at`` and ``updated_at`` differ on every row (a column-swap in
    the engine would otherwise be invisible), one row has a NULL ``score`` so
    the nullable path is exercised end to end, and every timestamp carries a
    distinct, non-zero microsecond component. The sub-second part is what
    catches precision-truncating destination DDL: a MySQL ``DATETIME`` without
    ``(6)`` silently drops it, so an exact-equality read-back goes red.
    """
    return [
        SeedRow(
            1,
            "Alice",
            "alice@example.com",
            95,
            datetime(2026, 1, 1, 8, 30, 0, 100001),
            datetime(2026, 2, 1, 9, 15, 0, 200002),
        ),
        SeedRow(
            2,
            "Bob",
            "bob@example.com",
            80,
            datetime(2026, 1, 2, 10, 0, 0, 300003),
            datetime(2026, 2, 2, 11, 45, 0, 400004),
        ),
        SeedRow(
            3,
            "Charlie",
            "charlie@example.com",
            None,
            datetime(2026, 1, 3, 14, 20, 0, 500005),
            datetime(2026, 2, 3, 7, 5, 0, 600006),
        ),
        SeedRow(
            4,
            "Dave",
            "dave@example.com",
            72,
            datetime(2026, 1, 4, 6, 0, 0, 700007),
            datetime(2026, 2, 4, 18, 30, 0, 800008),
        ),
        SeedRow(
            5,
            "Eve",
            "eve@example.com",
            91,
            datetime(2026, 1, 5, 23, 59, 0, 900009),
            datetime(2026, 2, 5, 0, 1, 0, 10010),
        ),
    ]


def destination_sentinel_row() -> SeedRow:
    """Canonical row 1 with its name replaced by the sentinel.

    Written directly into the destination after the initial sync. Row 1's
    source ``updated_at`` (2026-02-01) sits below every later bookmark, so an
    incremental sync must never touch it again — the sentinel surviving is the
    proof that the second and third syncs moved only the delta.
    """
    base = canonical_seed_rows()[0]
    return SeedRow(
        id=base.id,
        name=INCREMENTAL_SENTINEL_NAME,
        email=base.email,
        score=base.score,
        created_at=base.created_at,
        updated_at=base.updated_at,
    )


def incremental_mutation_rows() -> List[SeedRow]:
    """Rows upserted into the source after the initial sync.

    Both carry an ``updated_at`` above the post-initial bookmark (the maximum
    canonical ``updated_at`` is row 5's 2026-02-05 00:01), so a correct
    incremental sync picks up exactly these: one bumps an existing row (id 2,
    new score and timestamp), one is brand new (id 6).
    """
    return [
        SeedRow(
            2,
            "Bob",
            "bob@example.com",
            999,
            datetime(2026, 1, 2, 10, 0, 0, 300003),
            datetime(2026, 2, 10, 12, 0, 0, 111111),
        ),
        SeedRow(
            6,
            "Frank",
            "frank@example.com",
            60,
            datetime(2026, 1, 6, 9, 0, 0, 222222),
            datetime(2026, 2, 11, 8, 0, 0, 333333),
        ),
    ]


def expected_after_incremental() -> List[SeedRow]:
    """Destination state once the delta sync has run (stable across the no-op).

    Row 1 keeps the sentinel (never re-read), row 2 reflects the bump, rows
    3-5 stay canonical, and row 6 is the new row — six rows ordered by id.
    """
    rows = {r.id: r for r in canonical_seed_rows()}
    rows[1] = destination_sentinel_row()
    for r in incremental_mutation_rows():
        rows[r.id] = r
    return [rows[i] for i in sorted(rows)]
