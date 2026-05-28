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
    """The five rows every source DB receives before a test run."""
    return [
        SeedRow(
            1,
            "Alice",
            "alice@example.com",
            95,
            datetime(2026, 1, 1),
            datetime(2026, 1, 1),
        ),
        SeedRow(
            2, "Bob", "bob@example.com", 80, datetime(2026, 1, 2), datetime(2026, 1, 2)
        ),
        SeedRow(
            3,
            "Charlie",
            "charlie@example.com",
            88,
            datetime(2026, 1, 3),
            datetime(2026, 1, 3),
        ),
        SeedRow(
            4,
            "Dave",
            "dave@example.com",
            72,
            datetime(2026, 1, 4),
            datetime(2026, 1, 4),
        ),
        SeedRow(
            5, "Eve", "eve@example.com", 91, datetime(2026, 1, 5), datetime(2026, 1, 5)
        ),
    ]
