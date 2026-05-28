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


def canonical_seed_rows() -> List[SeedRow]:
    """The five rows every source DB receives before a test run.

    Deliberately constructed so the assertions can catch real bugs:
    ``created_at`` and ``updated_at`` differ on every row (a column-swap in
    the engine would otherwise be invisible), and one row has a NULL
    ``score`` so the nullable path is exercised end to end. Timestamps stay
    at whole seconds — sub-second precision and timezone round-tripping are
    a separate, deeper coverage concern tracked in the follow-up issue.
    """
    return [
        SeedRow(
            1,
            "Alice",
            "alice@example.com",
            95,
            datetime(2026, 1, 1, 8, 30, 0),
            datetime(2026, 2, 1, 9, 15, 0),
        ),
        SeedRow(
            2,
            "Bob",
            "bob@example.com",
            80,
            datetime(2026, 1, 2, 10, 0, 0),
            datetime(2026, 2, 2, 11, 45, 0),
        ),
        SeedRow(
            3,
            "Charlie",
            "charlie@example.com",
            None,
            datetime(2026, 1, 3, 14, 20, 0),
            datetime(2026, 2, 3, 7, 5, 0),
        ),
        SeedRow(
            4,
            "Dave",
            "dave@example.com",
            72,
            datetime(2026, 1, 4, 6, 0, 0),
            datetime(2026, 2, 4, 18, 30, 0),
        ),
        SeedRow(
            5,
            "Eve",
            "eve@example.com",
            91,
            datetime(2026, 1, 5, 23, 59, 0),
            datetime(2026, 2, 5, 0, 1, 0),
        ),
    ]
