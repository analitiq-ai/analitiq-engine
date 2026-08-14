"""Record identity is content-derived and position-independent (issue #282).

The id is a function of the record content alone -- never the run, the batch
sequence, or the row's offset -- so the same logical row hashes to the same id
across attempts and across an inclusive cursor re-read, which is what lets the
destination enforce idempotency on row identity instead of a positional ledger.
The whole at-least-once story rests on it, so it is pinned here rather than
left to the wire tests.
"""

from __future__ import annotations

import pytest

from src.grpc.client import generate_record_id

pytestmark = pytest.mark.unit


def test_same_record_same_id_regardless_of_position():
    """The same record content always yields the same id; no run/batch/index
    dimension can move it (positional independence is the point)."""
    record = {"id": 1, "name": "test"}
    assert generate_record_id(record) == generate_record_id(dict(record))


def test_different_content_different_id():
    """Distinct record content hashes to distinct ids."""
    assert generate_record_id({"id": 1, "name": "a"}) != generate_record_id(
        {"id": 2, "name": "b"}
    )


def test_primary_key_fields_derive_id_from_keys_only():
    """With primary_key_fields the id derives only from those fields, so two
    rows sharing the key values but differing elsewhere hash equal."""
    a = {"id": 7, "name": "first", "value": 1}
    b = {"id": 7, "name": "second", "value": 99}
    assert generate_record_id(a, primary_key_fields=["id"]) == generate_record_id(
        b, primary_key_fields=["id"]
    )
    # Different key values still diverge.
    c = {"id": 8, "name": "first", "value": 1}
    assert generate_record_id(a, primary_key_fields=["id"]) != generate_record_id(
        c, primary_key_fields=["id"]
    )


def test_missing_primary_key_fields_fall_back_to_full_record():
    """When a mapping renames/drops the configured key it is absent from the
    (transformed) record; the id falls back to the whole record so distinct
    rows keep distinct ids instead of all hashing the same missing-key value
    (issue #282 -- otherwise the keyless ``_record_hash`` dedup would silently
    drop every row but the first)."""
    # Configured PK "id" is absent (a mapping renamed it to "user_id").
    a = {"user_id": 7, "name": "first"}
    b = {"user_id": 8, "name": "second"}
    assert generate_record_id(a, primary_key_fields=["id"]) != generate_record_id(
        b, primary_key_fields=["id"]
    )
    # The fallback is exactly the keyless (no-key) hash of the same record.
    assert generate_record_id(a, primary_key_fields=["id"]) == generate_record_id(a)


def test_returned_id_is_full_sha256_hex():
    """The id is the full (untruncated) 64-char SHA-256 hex digest."""
    rid = generate_record_id({"id": 1, "name": "test"})
    assert len(rid) == 64
    assert all(c in "0123456789abcdef" for c in rid)
