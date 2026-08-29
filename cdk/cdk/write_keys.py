"""The one refusal behind every upsert whose record carries no conflict key.

An upsert matches the record it updates on the stream's conflict keys. A
record with no value for one of them (the column absent, or null) is a
record nothing can match: SQL ``ON CONFLICT`` and ``MERGE`` never match a
NULL key, so the row inserts again on every run, and an API provider either
rejects the request or creates a duplicate. The duplicate is the silent
outcome, so both transports refuse the record before anything lands, through
this one check -- the same intent must behave the same way on every
transport.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

__all__ = ["MissingConflictKeyError", "require_conflict_key_values"]


class MissingConflictKeyError(ValueError):
    """A record carries no value for an upsert conflict key.

    A data defect in one record, deterministic on every retry -- a
    ``ValueError`` like the other per-record verdicts the write paths give
    a record they cannot send.
    """


def require_conflict_key_values(
    conflict_keys: Sequence[str],
    records: Iterable[Mapping[str, Any]],
    *,
    target: str,
) -> None:
    """Refuse the first record in *records* missing a value for a conflict key.

    *target* names the destination (an endpoint path, a table address) in
    the refusal. An empty *conflict_keys* (an insert) checks nothing.
    """
    if not conflict_keys:
        return
    for record in records:
        missing = [key for key in conflict_keys if record.get(key) is None]
        if missing:
            raise MissingConflictKeyError(
                f"record carries no value for the upsert conflict key(s) "
                f"{missing} declared for {target!r}; the destination cannot "
                f"match the record without them"
            )
