"""One conflict-key refusal for every transport (issue #467)."""

from __future__ import annotations

import pytest

from cdk.write_keys import MissingConflictKeyError, require_conflict_key_values

pytestmark = pytest.mark.unit


def test_a_record_without_a_key_value_is_refused() -> None:
    require_conflict_key_values(
        ["id", "tenant"], [{"id": 1, "tenant": "t"}], target="t"
    )
    with pytest.raises(MissingConflictKeyError, match=r"\['tenant'\].*'t'"):
        require_conflict_key_values(
            ["id", "tenant"], [{"id": 1, "tenant": None}], target="t"
        )
    with pytest.raises(MissingConflictKeyError, match=r"\['id', 'tenant'\]"):
        require_conflict_key_values(
            ["id", "tenant"], [{"id": 1, "tenant": "t"}, {"payload": "x"}], target="t"
        )


def test_the_refusal_is_a_value_error() -> None:
    # The API write loops give a record they cannot send a ValueError verdict.
    assert issubclass(MissingConflictKeyError, ValueError)


def test_insert_records_are_never_checked() -> None:
    require_conflict_key_values([], [{}], target="t")
