"""One canonicalisation behind every row-identity digest.

Three call sites derive a digest of a row: the engine's ``record_id``, the SQL
write path's ``_record_hash`` dedup column, and the API write path's upsert
idempotency key. Three commits in five days wrote the same two lines into
three files, and nothing stopped the next one from drifting -- which is why
this file carries a structural scan as well as behavioural tests. A drifted
digest fails silently: it stops matching rows it used to match, so a replay
writes duplicates instead of deduping.

The *basis* each site hashes is deliberately its own and is not unified.
"""

from __future__ import annotations

import ast
import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from cdk.record_identity import record_digest

pytestmark = pytest.mark.unit

#: Every tree a row-identity digest could hide in.
_SOURCE_ROOTS = (Path("cdk"), Path("src"))

#: The module allowed to define the canonicalisation.
_OWNER = Path("cdk/cdk/record_identity.py")


class TestTheDigestIsStable:
    def test_key_order_does_not_change_the_digest(self) -> None:
        # A row that arrives with its columns reordered is the same row.
        assert record_digest({"a": 1, "b": 2}) == record_digest({"b": 2, "a": 1})

    def test_content_changes_the_digest(self) -> None:
        assert record_digest({"a": 1}) != record_digest({"a": 2})

    def test_the_digest_is_untruncated(self) -> None:
        # It identifies rows: a truncation that collides drops a row.
        assert len(record_digest({"a": 1})) == 64

    def test_none_is_hashed_as_json_null_not_via_str(self) -> None:
        # `default=str` never sees None, so a null column hashes as JSON
        # null rather than the string "None" -- the two must not converge.
        assert record_digest({"a": None}) != record_digest({"a": "None"})

    @pytest.mark.parametrize(
        "value",
        [
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            Decimal("1.10"),
            b"bytes",
        ],
    )
    def test_types_json_cannot_encode_do_not_raise(self, value: object) -> None:
        # Totality matters more than elegance here: raising mid-batch on the
        # first unencodable value would fail a write that has no defect.
        assert len(record_digest({"a": value})) == 64

    def test_decimal_precision_is_part_of_the_identity(self) -> None:
        # A documented consequence of `default=str`, pinned so a future
        # change to the serialisation is visible rather than silent.
        assert record_digest({"a": Decimal("1.10")}) != record_digest(
            {"a": Decimal("1.1")}
        )

    def test_the_serialisation_is_pinned(self) -> None:
        # The digest is a stability contract: changing the canonicalisation
        # re-identifies every row that was ever written.
        import hashlib

        expected = hashlib.sha256(
            json.dumps({"a": 1, "b": "x"}, sort_keys=True, default=str).encode()
        ).hexdigest()
        assert record_digest({"a": 1, "b": "x"}) == expected


class TestTheCallSitesShareIt:
    def test_the_engine_and_the_api_agree_on_a_whole_record(self) -> None:
        from src.destination.connectors.api import _content_idempotency_key
        from src.grpc.client import generate_record_id

        record = {
            "id": 7,
            "name": "x",
            "when": datetime(2026, 1, 1, tzinfo=timezone.utc),
        }
        # No primary key declared, so the engine hashes the whole record too:
        # the two must not disagree about the same bytes.
        assert generate_record_id(record) == _content_idempotency_key(record)
        assert generate_record_id(record) == record_digest(record)

    def test_a_declared_key_narrows_the_basis_deliberately(self) -> None:
        from src.grpc.client import generate_record_id

        record = {"id": 7, "name": "x"}
        # The engine's id is the row's business key when it has one, so a
        # changed non-key column is still the same row. That is a different
        # question from "has the content changed", and the difference is the
        # reason the bases are not unified.
        assert generate_record_id(record, ["id"]) == record_digest({"id": 7})
        assert generate_record_id(record, ["id"]) != record_digest(record)


class TestNoRivalImplementation:
    """The scan the behavioural tests cannot do.

    Two implementations agree until one is edited; only a structural check
    catches the copy that reintroduces the drift.
    """

    def test_only_one_module_canonicalises_a_row_digest(self) -> None:
        # The idiom is canonical JSON *feeding a hash*. Either half alone is
        # legitimate and common: `sort_keys` also pretty-prints generated
        # artifacts and builds the cursor key, and hashing also
        # content-addresses batch files and names stage tables.
        offenders: list[str] = []
        for root in _SOURCE_ROOTS:
            for path in sorted(root.rglob("*.py")):
                if path == _OWNER or "generated" in path.parts:
                    continue
                tree = ast.parse(path.read_text())
                for scope in ast.walk(tree):
                    if not isinstance(
                        scope, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Module)
                    ):
                        continue
                    body = [
                        n
                        for n in ast.walk(scope)
                        if not isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
                        or n is scope
                    ]
                    canonical = any(
                        isinstance(n, ast.Call)
                        and isinstance(n.func, ast.Attribute)
                        and n.func.attr == "dumps"
                        and any(kw.arg == "sort_keys" for kw in n.keywords)
                        for n in body
                    )
                    hashed = any(
                        isinstance(n, ast.Call)
                        and isinstance(n.func, ast.Attribute)
                        and isinstance(n.func.value, ast.Name)
                        and n.func.value.id == "hashlib"
                        for n in body
                    )
                    if canonical and hashed:
                        where = getattr(scope, "name", "<module>")
                        offenders.append(f"{path}:{where}")
        assert not offenders, (
            "a row-identity canonicalisation exists outside "
            f"{_OWNER}; it must call record_digest instead:\n  "
            + "\n  ".join(offenders)
        )
