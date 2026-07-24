"""The write primitive end-to-end against the connector's real system.

All write modes, replay, and restart, exactly as the ADR's live tier
specifies (sql-write-path-v2 section 10): every phase runs a fresh
connector instance over a fresh connection, so a passing test also
certifies restart behaviour; a replayed batch must observably match the
connector's declared retry semantics.

Tests run their async phases through ``asyncio.run`` so the suite needs
no pytest asyncio configuration in the consuming repo.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from pathlib import Path

import pytest

from cdk.conformance.fakes import MemoryCheckpointStore
from cdk.conformance.skips import require_write_role
from cdk.conformance.target import ConformanceTarget
from cdk.types import AckStatus, RetrySemantics, SchemaSpec, WriteMode

from .live import STREAM_ID, LiveHarness, by_id, expect_success, rows_batch


def _truncate_spec() -> SchemaSpec:
    """The handshake spec for a truncate_insert stream."""
    return SchemaSpec(
        stream_id=STREAM_ID,
        version=1,
        write_mode=WriteMode.WRITE_MODE_TRUNCATE_INSERT,
        ack_timeout_seconds=60,
    )


@pytest.fixture
def harness(
    conformance_target: ConformanceTarget, live_connection_path: Path
) -> Iterator[LiveHarness]:
    """A unique live table per test, dropped afterwards."""
    require_write_role(conformance_target)
    if conformance_target.declared_capabilities is None:
        pytest.skip(
            "sql_capabilities undeclared; reported by the "
            "declaration-consistency check"
        )
    live = LiveHarness.load(conformance_target, live_connection_path)
    yield live
    asyncio.run(live.drop_phase())


def _requires_merge(target: ConformanceTarget) -> None:
    caps = target.declared_capabilities
    if caps is None or caps.merge_form == "none":
        pytest.skip("connector declares merge_form 'none'; no upsert to run")


def test_insert_writes_read_back_and_replay_is_stable(
    harness: LiveHarness,
) -> None:
    """Two batches land; a replayed batch leaves the target unchanged."""

    async def scenario() -> None:
        first = rows_batch([(1, "a", 1), (2, "b", 2), (3, "c", 3)])
        second = rows_batch([(3, "c2", 4), (4, "d", 5)])
        expect_success(
            await harness.write_phase("insert", [(1, first)]),
            "insert first batch",
        )
        expect_success(
            await harness.write_phase("insert", [(2, second)]),
            "insert second batch (fresh connection: restart)",
        )
        rows = by_id(await harness.read_phase())
        assert set(rows) == {1, 2, 3, 4}
        # id 3 appeared in both batches; the anti-join keeps the
        # committed row, so the first version survives.
        assert rows[3]["val"] == "c"

        replay = await harness.write_phase("insert", [(2, second)])
        expect_success(replay, "insert replayed batch")
        rows_after = by_id(await harness.read_phase())
        assert rows_after == rows, (
            "a replayed insert batch changed the target; the declared "
            "verdict promises identity-deduplicated replays"
        )

    asyncio.run(scenario())


def test_insert_collapses_intra_batch_duplicates_first_wins(
    harness: LiveHarness,
) -> None:
    """One batch carrying an identity twice lands its first occurrence."""

    async def scenario() -> None:
        batch = rows_batch([(1, "first", 1), (1, "second", 2), (2, "b", 3)])
        expect_success(
            await harness.write_phase("insert", [(1, batch)]),
            "insert with intra-batch duplicate identity",
        )
        rows = by_id(await harness.read_phase())
        assert set(rows) == {1, 2}
        assert rows[1]["val"] == "first"

    asyncio.run(scenario())


def test_upsert_updates_matches_and_replay_is_stable(
    harness: LiveHarness,
) -> None:
    """The declared merge form lands updates; a replay changes nothing."""
    _requires_merge(harness.target)

    async def scenario() -> None:
        first = rows_batch([(1, "a", 1), (2, "b", 2), (3, "c", 3)])
        second = rows_batch([(2, "b2", 4), (3, "c2", 5), (4, "d", 6)])
        expect_success(
            await harness.write_phase("upsert", [(1, first)], conflict_keys=["id"]),
            "upsert first batch",
        )
        expect_success(
            await harness.write_phase("upsert", [(2, second)], conflict_keys=["id"]),
            "upsert second batch (fresh connection: restart)",
        )
        rows = by_id(await harness.read_phase())
        assert set(rows) == {1, 2, 3, 4}
        assert rows[2]["val"] == "b2"
        assert rows[3]["val"] == "c2"

        replay = await harness.write_phase(
            "upsert", [(2, second)], conflict_keys=["id"]
        )
        expect_success(replay, "upsert replayed batch")
        rows_after = by_id(await harness.read_phase())
        assert rows_after == rows, (
            "a replayed upsert batch changed the target; upsert's "
            "declared verdict is exactly-once on the conflict keys"
        )

    asyncio.run(scenario())


def test_upsert_without_conflict_keys_is_rejected_fatally(
    harness: LiveHarness,
) -> None:
    """The keyless-upsert refusal reaches the ack as a fatal failure."""
    _requires_merge(harness.target)

    async def scenario() -> None:
        batch = rows_batch([(1, "a", 1)])
        results = await harness.write_phase("upsert", [(1, batch)], conflict_keys=[])
        assert len(results) == 1
        assert results[0].status is AckStatus.ACK_STATUS_FATAL_FAILURE, (
            f"an upsert stream with no conflict_keys must refuse fatally "
            f"(a retryable status would make the engine replay an "
            f"unhealable stream forever); got {results[0].status!r}: "
            f"{results[0].failure_summary}"
        )

    asyncio.run(scenario())


def test_declared_bulk_and_executemany_land_identically(
    harness: LiveHarness,
) -> None:
    """Landing is a pure speed slot: bulk and executemany agree (ADR s.2).

    The batch is written twice — once through the declared bulk
    mechanism, once through a probe whose declaration is doctored to an
    empty bulk_load mapping so every backend (adbc_ingest included)
    lands via executemany — into two tables, and the resulting contents
    must be identical. This is the live form of the
    ADR's landing-equivalence assertion; native bulk protocols cannot
    execute against generic fakes, so the contract tier certifies the
    declaration/hook pairing and this scenario certifies the semantics.
    """
    caps = harness.target.declared_capabilities
    if caps is None or not caps.bulk_load:
        pytest.skip(
            "connector declares no bulk mechanism; landing is executemany "
            "by definition"
        )
    transports = harness.target.declared_transports()
    default_ref = str(harness.target.definition.get("default_transport") or "")
    default_type = str((transports.get(default_ref) or {}).get("transport_type") or "")
    if caps.bulk_mechanism(default_type) is None:
        pytest.skip(
            f"no bulk mechanism declared for the transport the live "
            f"connection materializes ({default_type or 'unknown'}); "
            f"nothing to compare on this connection"
        )

    async def scenario() -> None:
        batch = rows_batch([(i, f"v{i}", i) for i in range(1, 6)])
        expect_success(
            await harness.write_phase("insert", [(1, batch)]),
            "bulk-landed write",
        )
        bulk_rows = by_id(await harness.read_phase())

        forced = harness.executemany_forced()
        try:
            expect_success(
                await forced.write_phase("insert", [(1, batch)]),
                "executemany-landed write",
            )
            executemany_rows = by_id(await forced.read_phase())
        finally:
            await forced.drop_phase()

        assert bulk_rows == executemany_rows, (
            "the declared bulk mechanism and the executemany fallback left "
            "different target contents; bulk_land is a pure speed slot and "
            "must not change what lands (ADR sql-write-path-v2 section 2)"
        )

    asyncio.run(scenario())


def test_first_batch_truncates_then_appends(harness: LiveHarness) -> None:
    """Pre-existing rows vanish on batch 1; batch 2 appends."""

    async def scenario() -> None:
        stale = rows_batch([(90, "stale", 1), (91, "stale", 2)])
        expect_success(
            await harness.write_phase("insert", [(1, stale)]),
            "seeding stale rows",
        )
        refresh_first = rows_batch([(1, "a", 3), (2, "b", 4)])
        refresh_second = rows_batch([(3, "c", 5)])
        expect_success(
            await harness.write_phase("truncate_insert", [(1, refresh_first)]),
            "full refresh first batch",
        )
        # The second batch runs on a fresh connector and connection: the
        # truncate-once gate must key on batch_seq == 1, never on
        # in-memory state a worker restart would reset.
        expect_success(
            await harness.write_phase("truncate_insert", [(2, refresh_second)]),
            "full refresh second batch (fresh connection: restart)",
        )
        rows = by_id(await harness.read_phase())
        assert set(rows) == {1, 2, 3}, (
            "the refresh's first batch must empty the target and later "
            "batches must append"
        )

    asyncio.run(scenario())


def test_declared_retry_verdict_is_at_least_once(
    harness: LiveHarness,
) -> None:
    """The mode promises no exactly-once; the verdict must say so.

    What a replayed append does is system-dependent — it duplicates
    rows where nothing blocks it, or is rejected by the enforced
    primary key — and both outcomes are inside the at-least-once
    contract (#307). What is certified here is the declaration: a
    connector must never advertise exactly-once for this mode.
    """

    async def scenario() -> None:
        batch = rows_batch([(1, "a", 1)])
        expect_success(
            await harness.write_phase("truncate_insert", [(1, batch)]),
            "full refresh",
        )
        connector = harness.connector()
        await connector.connect(harness.runtime())
        try:
            connector.set_endpoint_refs(
                {
                    STREAM_ID: {
                        "scope": "connector",
                        "connection_id": "conformance-live",
                        "endpoint_id": harness.table,
                    }
                }
            )
            connector.set_stream_endpoints({STREAM_ID: harness.endpoint_document()})
            configured = await connector.configure_schema(_truncate_spec())
            assert configured
            verdict = connector.retry_semantics(STREAM_ID)
        finally:
            await connector.disconnect()
        assert verdict.semantics is RetrySemantics.RETRY_SEMANTICS_AT_LEAST_ONCE, (
            "truncate_insert replays observably duplicate rows, so the "
            "declared verdict must be at-least-once"
        )

    asyncio.run(scenario())


def test_cursor_read_is_monotonic_and_resumes(harness: LiveHarness) -> None:
    """Paged incremental reads advance the cursor and honor a resume."""

    async def scenario() -> None:
        batch = rows_batch([(i, f"v{i}", i) for i in range(1, 8)])
        expect_success(
            await harness.write_phase("insert", [(1, batch)]),
            "seeding read rows",
        )
        checkpoint = MemoryCheckpointStore()
        rows = await harness.read_phase(
            cursor_field="seq", checkpoint=checkpoint, batch_size=3
        )
        sequence = [int(row["seq"]) for row in rows]
        assert sequence == sorted(sequence), (
            f"incremental read pages are not ordered by the cursor "
            f"field: {sequence}"
        )
        assert {int(row["id"]) for row in rows} == set(range(1, 8))
        saved = await checkpoint.get_cursor("conformance-stream")
        assert saved, "the read persisted no cursor checkpoint"

        resumed = await harness.read_phase(
            cursor_field="seq", checkpoint=checkpoint, batch_size=3
        )
        resumed_ids = {int(row["id"]) for row in resumed}
        assert resumed_ids.issubset({7}), (
            f"a resumed read must continue from the saved cursor, not "
            f"re-read the stream; got ids {sorted(resumed_ids)}"
        )

        # A row past the checkpoint must arrive on the next resume —
        # an off-by-one that permanently loses the tail would
        # otherwise be indistinguishable from a clean exclusive
        # cursor (an empty resumed read is legal above).
        tail = rows_batch([(8, "v8", 8)])
        expect_success(
            await harness.write_phase("insert", [(2, tail)]),
            "seeding the post-checkpoint row",
        )
        after_tail = await harness.read_phase(
            cursor_field="seq", checkpoint=checkpoint, batch_size=3
        )
        assert 8 in {int(row["id"]) for row in after_tail}, (
            "a row written past the saved cursor never arrived on "
            "resume; the cursor filter is losing the tail"
        )

    asyncio.run(scenario())
