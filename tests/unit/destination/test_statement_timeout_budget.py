"""The destination's statement bound is derived from the sender's ack budget.

``derive_statement_timeout_seconds`` is a pure function (issues #231, #234):
it must land strictly below the wait the sender applies, or the destination
keeps a statement running after its caller has already given up.
"""

from __future__ import annotations

import pytest

from src.destination.server import derive_statement_timeout_seconds

pytestmark = pytest.mark.unit


def test_derive_keeps_margin_for_large_budgets():
    assert derive_statement_timeout_seconds(30) == 25.0
    assert derive_statement_timeout_seconds(300) == 295.0


def test_derive_stays_below_small_budgets():
    # Too small to spare the full 5s margin: half the budget instead.
    assert derive_statement_timeout_seconds(8) == 4.0
    assert derive_statement_timeout_seconds(1) == 0.5
