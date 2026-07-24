"""GenericSQLConnector over a sync SQLAlchemy engine (issue #224).

A sync-only driver (the production case is Redshift's vendor-supported
``redshift_connector``) materializes a plain sync ``Engine``; the handler
runs the same sync-``Connection`` stage-cycle body the async path uses,
dispatched via ``asyncio.to_thread``. SQLite's stdlib driver is the
in-process stand-in for a sync-only dialect, so these tests exercise the
real stage-then-merge machinery end to end — actual SQL against an actual
database, not mocks of the engine surface.

The write path is stage-then-merge (issue #388): every batch lands in a
per-batch stage table and exactly one mode statement applies it. The test
dialect declares its capabilities the way a connector package does and
implements the two rendering hooks the primitive needs.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow as pa
import pytest
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.pool import StaticPool

from cdk.sql.backend import SqlAlchemyBackend, StageWritePlan
from cdk.sql.capabilities import SqlCapabilities
from cdk.sql.dialects import SqlDialect, TableAddress
from cdk.sql.generic import GenericSQLConnector, _StreamState
from cdk.types import AckStatus, Cursor


def _sqlite_sync_engine():
    """One shared in-memory database visible across threads.

    The handler dispatches sync-engine work via ``asyncio.to_thread``, so
    the default per-thread SQLite memory connection would see an empty
    database; StaticPool + check_same_thread=False shares one connection.
    The isolation-level/BEGIN recipe makes pysqlite genuinely
    transactional for DDL too (by default it silently commits before
    DDL), so the declared ``transactional_ddl: true`` stage shape behaves
    here the way it does on Postgres.
    """
    from sqlalchemy import event

    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False, "isolation_level": None},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "begin")
    def _do_begin(conn):
        conn.exec_driver_sql("BEGIN")

    return engine


TARGET_DDL = "CREATE TABLE events (id INTEGER PRIMARY KEY, name TEXT)"


class _SqliteStageDialect(SqlDialect):
    """Stage-then-merge rendering for the SQLite stand-in.

    Implements exactly the two hooks a write-role connector package must
    ship: the stage DDL and the declared merge form (SQLite speaks
    ``INSERT … ON CONFLICT DO UPDATE``).
    """

    name = "sqlite"

    def stage_table_sql(
        self, stage: TableAddress, target: TableAddress, *, temp: bool
    ) -> str:
        keyword = "CREATE TEMPORARY TABLE" if temp else "CREATE TABLE"
        return (
            f"{keyword} {self.quote_table(stage)} AS "
            f"SELECT * FROM {self.quote_table(target)} WHERE 0"
        )

    def merge_statement_sql(self, stage, target, conflict_keys, columns):
        col_list = ", ".join(self.quote_ident(c) for c in columns)
        keys = ", ".join(self.quote_ident(c) for c in conflict_keys)
        update_cols = [c for c in columns if c not in set(conflict_keys)]
        sql = (
            f"INSERT INTO {self.quote_table(target)} ({col_list}) "
            f"SELECT {col_list} FROM {self.quote_table(stage)} WHERE true "
            f"ON CONFLICT ({keys}) "
        )
        if not update_cols:
            return sql + "DO NOTHING"
        sets = ", ".join(
            f"{self.quote_ident(c)} = excluded.{self.quote_ident(c)}"
            for c in update_cols
        )
        return sql + f"DO UPDATE SET {sets}"


def _declared_caps(**stage_overrides: Any) -> SqlCapabilities:
    stage = {
        "scope": "temp",
        "schema": "target",
        "transactional_ddl": True,
        **stage_overrides,
    }
    return SqlCapabilities.from_declaration(
        {
            "catalog": "none",
            "session_targeting": "per_statement",
            "merge_form": "insert_on_conflict",
            "bulk_load": "none",
            "stage": stage,
        },
        source="<test>",
    )


def _arrow_contract() -> MagicMock:
    """SchemaContract stand-in: the cast is identity (rows are pre-shaped)."""
    contract = MagicMock()
    contract.cast_arrow_batch.side_effect = lambda rb: rb
    return contract


def _wire_backend(
    handler: GenericSQLConnector,
    engine: Any,
    address: TableAddress,
    caps: SqlCapabilities,
) -> None:
    """Wire dialect, declared capabilities, and a backend with the
    reflected target — what connect() + configure_schema() produce."""
    handler.dialect = _SqliteStageDialect()
    handler.dialect.capabilities = caps
    handler._capabilities = caps
    backend = SqlAlchemyBackend(handler.dialect)
    backend._sync_engine = engine
    backend._bulk_declared = caps.bulk_load != "none"
    backend._targets[address] = Table(address.table, MetaData(), autoload_with=engine)
    handler._backend = backend


def _connected_handler(
    engine,
    write_mode: str = "insert",
    *,
    conflict_keys: list[str] | None = None,
    transactional: bool = True,
) -> GenericSQLConnector:
    """Handler wired to *engine* with the reflected target table for ``s1``.

    Content-derived idempotency (issue #282) means the destination creates
    only the target table -- there is no positional ``_batch_commits``
    ledger. The stream's ``id`` primary key is the identity the insert
    anti-join skips a re-read row on.
    """
    with engine.begin() as conn:
        conn.exec_driver_sql(TARGET_DDL)

    address = TableAddress(table="events")
    handler = GenericSQLConnector()
    handler._connected = True
    handler._sync_engine = engine
    _wire_backend(
        handler, engine, address, _declared_caps(transactional_ddl=transactional)
    )
    handler._streams["s1"] = _StreamState(
        address=address,
        write_mode=write_mode,
        primary_keys=["id"],
        conflict_keys=conflict_keys or [],
        schema_contract=_arrow_contract(),
    )
    return handler


def _batch(rows) -> pa.RecordBatch:
    return pa.RecordBatch.from_pylist(rows)


async def _write(
    handler, *, seq: int = 1, rows=None, token: bytes = b"tok", run_id: str = "r1"
):
    return await handler.write_batch(
        run_id=run_id,
        stream_id="s1",
        batch_seq=seq,
        record_batch=_batch(rows if rows is not None else [{"id": 1, "name": "a"}]),
        record_ids=[],
        cursor=Cursor(token=token),
        emitted_at=datetime(2026, 7, 21, 9, 0, 0, tzinfo=timezone.utc),
    )


def _count(engine, table: str) -> int:
    with engine.connect() as conn:
        return conn.exec_driver_sql(f"SELECT count(*) FROM {table}").scalar_one()


def _table_names(engine) -> set[str]:
    with engine.connect() as conn:
        rows = conn.exec_driver_sql(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "UNION ALL SELECT name FROM sqlite_temp_master WHERE type='table'"
        ).all()
    return {r[0] for r in rows}


class TestSyncEngineWritePath:
    @pytest.mark.asyncio
    async def test_insert_writes_rows(self):
        engine = _sqlite_sync_engine()
        try:
            handler = _connected_handler(engine)
            result = await _write(
                handler, rows=[{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
            )
            assert result.status == AckStatus.ACK_STATUS_SUCCESS
            assert result.records_written == 2
            assert _count(engine, "events") == 2
        finally:
            engine.dispose()

    @pytest.mark.asyncio
    async def test_reinsert_same_key_is_deduped(self):
        # A same-run retry re-reads the inclusive cursor boundary, so the same
        # keyed row can arrive twice. There is no positional ledger (issue
        # #282); the set-based anti-join from the stage skips the row whose
        # ``id`` already exists, so the re-insert is a no-op that still acks
        # SUCCESS -- never the removed ALREADY_COMMITTED status.
        engine = _sqlite_sync_engine()
        try:
            handler = _connected_handler(engine)
            first = await _write(handler, token=b"cur-1")
            assert first.status == AckStatus.ACK_STATUS_SUCCESS
            replay = await _write(handler, token=b"cur-1")
            assert replay.status == AckStatus.ACK_STATUS_SUCCESS
            assert replay.committed_cursor.token == b"cur-1"
            # The re-insert was deduped on the primary key -- no double row.
            assert _count(engine, "events") == 1
        finally:
            engine.dispose()

    @pytest.mark.asyncio
    async def test_intra_batch_duplicate_key_first_occurrence_wins(self):
        # The stage feeds one set-based statement, so duplicate identities
        # inside a batch are collapsed in Arrow space before landing —
        # first occurrence wins, same contract as before the stage split.
        engine = _sqlite_sync_engine()
        try:
            handler = _connected_handler(engine)
            result = await _write(
                handler,
                rows=[{"id": 1, "name": "first"}, {"id": 1, "name": "second"}],
            )
            assert result.status == AckStatus.ACK_STATUS_SUCCESS
            with engine.connect() as conn:
                rows = conn.exec_driver_sql("SELECT id, name FROM events").all()
            assert rows == [(1, "first")]
        finally:
            engine.dispose()

    @pytest.mark.asyncio
    async def test_upsert_updates_in_place(self):
        # The upsert lands in the stage and applies via the dialect's
        # declared merge form; a re-sent key updates instead of duplicating.
        engine = _sqlite_sync_engine()
        try:
            handler = _connected_handler(
                engine, write_mode="upsert", conflict_keys=["id"]
            )
            await _write(handler, rows=[{"id": 1, "name": "old"}])
            result = await _write(handler, rows=[{"id": 1, "name": "new"}])
            assert result.status == AckStatus.ACK_STATUS_SUCCESS
            with engine.connect() as conn:
                rows = conn.exec_driver_sql("SELECT id, name FROM events").all()
            assert rows == [(1, "new")]
        finally:
            engine.dispose()

    @pytest.mark.asyncio
    async def test_truncate_insert_new_run_replaces_previous_refresh(self):
        # The truncate runs once per (run, stream) — issue #307. A later
        # batch of the SAME run appends; a new run's first batch empties
        # the table.
        engine = _sqlite_sync_engine()
        try:
            handler = _connected_handler(engine, write_mode="truncate_insert")
            await _write(handler, seq=1, rows=[{"id": 1, "name": "old"}], run_id="r1")
            result = await _write(
                handler, seq=2, rows=[{"id": 7, "name": "new"}], run_id="r1"
            )
            assert result.status == AckStatus.ACK_STATUS_SUCCESS
            with engine.connect() as conn:
                rows = conn.exec_driver_sql("SELECT id, name FROM events").all()
            # Same run: batch 2 appends instead of wiping batch 1.
            assert sorted(rows) == [(1, "old"), (7, "new")]

            result = await _write(
                handler, seq=1, rows=[{"id": 9, "name": "next"}], run_id="r2"
            )
            assert result.status == AckStatus.ACK_STATUS_SUCCESS
            with engine.connect() as conn:
                rows = conn.exec_driver_sql("SELECT id, name FROM events").all()
            # New run: a fresh refresh replaces the previous one.
            assert rows == [(9, "next")]
        finally:
            engine.dispose()

    @pytest.mark.asyncio
    async def test_empty_batch_returns_success(self):
        # An empty batch writes nothing and records no marker -- the cursor
        # advances and idempotency lives in the write itself (issue #282).
        engine = _sqlite_sync_engine()
        try:
            handler = _connected_handler(engine)
            result = await _write(handler, rows=[])
            assert result.status == AckStatus.ACK_STATUS_SUCCESS
            assert result.records_written == 0
            assert _count(engine, "events") == 0
        finally:
            engine.dispose()

    @pytest.mark.asyncio
    async def test_stage_is_dropped_after_the_batch(self):
        # The per-batch stage must not outlive its cycle, success included.
        engine = _sqlite_sync_engine()
        try:
            handler = _connected_handler(engine)
            await _write(handler)
            leftovers = {n for n in _table_names(engine) if "stage" in n}
            assert leftovers == set()
        finally:
            engine.dispose()

    @pytest.mark.asyncio
    async def test_health_check_runs_on_sync_engine(self):
        engine = _sqlite_sync_engine()
        try:
            handler = GenericSQLConnector()
            handler._connected = True
            handler._sync_engine = engine
            assert await handler.health_check() is True
        finally:
            engine.dispose()


class TestNonTransactionalCycle:
    """``stage.transactional_ddl: false`` runs per-step commits with the
    pre-flight drop; the primitive's self-healing rules replace atomicity."""

    @pytest.mark.asyncio
    async def test_write_lands_and_drops_stage(self):
        engine = _sqlite_sync_engine()
        try:
            handler = _connected_handler(engine, transactional=False)
            result = await _write(
                handler, rows=[{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
            )
            assert result.status == AckStatus.ACK_STATUS_SUCCESS
            assert _count(engine, "events") == 2
            assert {n for n in _table_names(engine) if "stage" in n} == set()
        finally:
            engine.dispose()

    @pytest.mark.asyncio
    async def test_retry_after_leftover_stage_self_heals(self):
        # A crashed attempt can leave the deterministic-named stage behind
        # on the session; the retry's pre-flight DROP IF EXISTS clears
        # exactly its own leftover and the batch lands.
        engine = _sqlite_sync_engine()
        try:
            handler = _connected_handler(engine, transactional=False)
            # Simulate the prior attempt's leftover under the same
            # deterministic name the retry will compute.
            from cdk.sql.write_plan import stage_table_name

            leftover = stage_table_name(
                "events",
                run_id="r1",
                stream_id="s1",
                batch_seq=1,
                max_identifier_length=handler.dialect.max_identifier_length,
            )
            with engine.begin() as conn:
                conn.exec_driver_sql(
                    f'CREATE TEMPORARY TABLE "{leftover}" AS '
                    f"SELECT * FROM events WHERE 0"
                )
            result = await _write(handler, seq=1, run_id="r1")
            assert result.status == AckStatus.ACK_STATUS_SUCCESS
            assert _count(engine, "events") == 1
        finally:
            engine.dispose()


class TestNonTransactionalFailureRules:
    """A failed cycle fails with the step's own error; cleanup never
    replaces it, and the logs say only what actually happened."""

    @pytest.mark.asyncio
    async def test_failed_mode_statement_keeps_its_error_and_drops_the_stage(self):
        import dataclasses

        from cdk.sql import generic as generic_module

        engine = _sqlite_sync_engine()
        try:
            handler = _connected_handler(engine, transactional=False)
            original_build = generic_module.build_stage_write_plan

            def _broken_mode(*args, **kwargs):
                plan = original_build(*args, **kwargs)
                return dataclasses.replace(
                    plan, mode_sql="INSERT INTO no_such_table_xyz SELECT 1"
                )

            with patch.object(generic_module, "build_stage_write_plan", _broken_mode):
                result = await _write(handler)
            assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
            # The mode statement's error, not a cleanup error.
            assert "no_such_table_xyz" in result.failure_summary
            # Best-effort drop ran: no stage leftovers on the session.
            assert {n for n in _table_names(engine) if "stage" in n} == set()
            assert _count(engine, "events") == 0
        finally:
            engine.dispose()

    def test_drop_failure_after_failed_batch_warns_and_swallows(self, caplog):
        import logging

        backend = SqlAlchemyBackend(_SqliteStageDialect())
        plan = StageWritePlan(
            stage=TableAddress(table="stg"),
            target=TableAddress(table="events"),
            scope="real",
            transactional=False,
            create_stage_sql="",
            truncate_sql=None,
            mode_sql="",
            drop_stage_sql='DROP TABLE IF EXISTS "stg"',
            columns=("id",),
        )
        conn = MagicMock()
        conn.exec_driver_sql.side_effect = RuntimeError("connection gone")
        with caplog.at_level(logging.WARNING, logger="cdk.sql.backend"):
            backend._drop_stage_after_failure(conn, plan)
        assert any(
            "could not be dropped after a failed batch" in r.getMessage()
            for r in caplog.records
        )

    def test_rollback_failure_says_the_drop_was_never_attempted(self, caplog):
        import logging

        backend = SqlAlchemyBackend(_SqliteStageDialect())
        plan = StageWritePlan(
            stage=TableAddress(table="stg"),
            target=TableAddress(table="events"),
            scope="temp",
            transactional=False,
            create_stage_sql="",
            truncate_sql=None,
            mode_sql="",
            drop_stage_sql='DROP TABLE IF EXISTS "stg"',
            columns=("id",),
        )
        conn = MagicMock()
        conn.rollback.side_effect = RuntimeError("dead")
        with caplog.at_level(logging.WARNING, logger="cdk.sql.backend"):
            backend._drop_stage_after_failure(conn, plan)
        message = "\n".join(r.getMessage() for r in caplog.records)
        assert "was not attempted" in message
        # Scope-accurate consequence: a temp stage needs no manual drop.
        assert "manual drop" not in message
        conn.exec_driver_sql.assert_not_called()


class TestSuccessDropRules:
    """The #379 rules generalized to this backend: a committed mode
    statement stays SUCCESS whatever happens to the drop."""

    def _plan(self, scope: str = "real"):
        return StageWritePlan(
            stage=TableAddress(table="stg"),
            target=TableAddress(table="events"),
            scope=scope,  # type: ignore[arg-type]
            transactional=False,
            create_stage_sql="",
            truncate_sql=None,
            mode_sql="",
            drop_stage_sql='DROP TABLE IF EXISTS "stg"',
            columns=("id",),
        )

    def test_second_attempt_recovery_logs_the_first_cause(self, caplog):
        import logging

        backend = SqlAlchemyBackend(_SqliteStageDialect())
        conn = MagicMock()
        conn.exec_driver_sql.side_effect = [RuntimeError("lock timeout"), None]
        with caplog.at_level(logging.INFO, logger="cdk.sql.backend"):
            backend._drop_stage_after_success(conn, self._plan())
        messages = [r.getMessage() for r in caplog.records]
        assert any("attempt 1/2" in m for m in messages)
        assert any("succeeded on the second attempt" in m for m in messages)
        conn.invalidate.assert_not_called()

    def test_double_failure_warns_orphan_then_invalidates(self, caplog):
        import logging

        backend = SqlAlchemyBackend(_SqliteStageDialect())
        conn = MagicMock()
        conn.exec_driver_sql.side_effect = RuntimeError("still locked")
        with caplog.at_level(logging.WARNING, logger="cdk.sql.backend"):
            backend._drop_stage_after_success(conn, self._plan("real"))
        warnings = [
            r.getMessage()
            for r in caplog.records
            if "could not be dropped after a successful" in r.getMessage()
        ]
        assert len(warnings) == 1
        assert "orphaned" in warnings[0]
        conn.invalidate.assert_called_once()

    def test_temp_scope_double_failure_notes_session_death(self, caplog):
        import logging

        backend = SqlAlchemyBackend(_SqliteStageDialect())
        conn = MagicMock()
        conn.exec_driver_sql.side_effect = RuntimeError("still locked")
        with caplog.at_level(logging.WARNING, logger="cdk.sql.backend"):
            backend._drop_stage_after_success(conn, self._plan("temp"))
        warnings = [
            r.getMessage()
            for r in caplog.records
            if "could not be dropped after a successful" in r.getMessage()
        ]
        assert len(warnings) == 1
        assert "dies with the discarded connection" in warnings[0]
        assert "orphaned" not in warnings[0]

    @pytest.mark.asyncio
    async def test_committed_batch_acks_success_despite_double_drop_failure(
        self, tmp_path
    ):
        # The single most important rule: the mode statement committed, so
        # the ack is SUCCESS — a drop failure must never turn a committed
        # batch into a reported failure (the retry would double-append a
        # truncate_insert batch). File-backed database: the double
        # failure invalidates the connection, which would erase an
        # in-memory one.
        from sqlalchemy import event

        engine = create_engine(
            f"sqlite:///{tmp_path}/t.db",
            connect_args={"check_same_thread": False, "isolation_level": None},
            poolclass=StaticPool,
        )

        @event.listens_for(engine, "begin")
        def _do_begin(conn):
            conn.exec_driver_sql("BEGIN")

        try:
            handler = _connected_handler(engine, transactional=False)
            backend = handler._backend
            with patch.object(
                backend, "_try_drop", side_effect=lambda c, p: RuntimeError("boom")
            ):
                result = await _write(handler)
            assert result.status == AckStatus.ACK_STATUS_SUCCESS
            assert _count(engine, "events") == 1
        finally:
            engine.dispose()


class TestFatalClassification:
    @pytest.mark.asyncio
    async def test_integrity_error_is_fatal_not_retryable(self):
        # Same intent, same verdict on both transports: a deterministic
        # PEP-249 failure class out of the stage cycle is fatal — the
        # engine must not burn its retry ladder on a batch that can
        # never heal (the ADBC path already reclassifies these).
        import dataclasses

        from cdk.sql import generic as generic_module
        from cdk.types import FailureCategory

        engine = _sqlite_sync_engine()
        try:
            handler = _connected_handler(engine, transactional=False)
            original_build = generic_module.build_stage_write_plan

            def _pk_violation(*args, **kwargs):
                plan = original_build(*args, **kwargs)
                return dataclasses.replace(
                    plan,
                    mode_sql=(
                        "INSERT INTO events (id, name) " "VALUES (1, 'x'), (1, 'y')"
                    ),
                )

            with patch.object(generic_module, "build_stage_write_plan", _pk_violation):
                result = await _write(handler)
            assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
            assert "sqlalchemy: IntegrityError" in result.failure_summary
            assert (
                result.failure_category
                == FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT
            )
        finally:
            engine.dispose()


class _BulkLandingDialect(_SqliteStageDialect):
    """A dialect whose bulk mechanism actually lands the batch."""

    def __init__(self) -> None:
        self.bulk_calls = 0

    def bulk_land(self, conn, stage, batch, *, runtime):
        self.bulk_calls += 1
        for row in batch.to_pylist():
            conn.exec_driver_sql(
                f"INSERT INTO {self.quote_table(stage)} (id, name) "
                f"VALUES ({int(row['id'])}, '{row['name']}')"
            )
        return True


class TestBulkLandFallback:
    @pytest.mark.asyncio
    async def test_declined_bulk_land_falls_back_to_executemany(self, caplog):
        import logging

        engine = _sqlite_sync_engine()
        try:
            handler = _connected_handler(engine)
            # Declare a bulk mechanism; the base dialect's bulk_land
            # declines, so landing falls back to executemany with an
            # INFO log — a visible speed downgrade, never a silent one.
            handler._backend._bulk_declared = True
            with caplog.at_level(logging.INFO, logger="cdk.sql.backend"):
                # bulk_land needs the runtime it connected with.
                handler._backend._runtime = MagicMock()
                result = await _write(handler)
            assert result.status == AckStatus.ACK_STATUS_SUCCESS
            assert _count(engine, "events") == 1
            assert any("declined" in r.getMessage() for r in caplog.records)
        finally:
            engine.dispose()

    @pytest.mark.asyncio
    async def test_accepted_bulk_land_skips_executemany(self):
        # A True return means landed: executemany must not run again, or
        # the stage doubles and the mode statement doubles the target.
        engine = _sqlite_sync_engine()
        try:
            handler = _connected_handler(engine)
            dialect = _BulkLandingDialect()
            dialect.capabilities = handler.dialect.capabilities
            handler.dialect = dialect
            backend = handler._backend
            backend._dialect = dialect
            backend._bulk_declared = True
            backend._runtime = MagicMock()
            result = await _write(
                handler, rows=[{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
            )
            assert result.status == AckStatus.ACK_STATUS_SUCCESS
            assert dialect.bulk_calls == 1
            assert _count(engine, "events") == 2
        finally:
            engine.dispose()

    @pytest.mark.asyncio
    async def test_undeclared_mechanism_never_calls_the_hook(self):
        # A bare bulk_land override without a declared bulk_load
        # mechanism is never called — declaration gates the hook.
        engine = _sqlite_sync_engine()
        try:
            handler = _connected_handler(engine)
            dialect = _BulkLandingDialect()
            dialect.capabilities = handler.dialect.capabilities
            handler.dialect = dialect
            handler._backend._dialect = dialect
            assert handler._backend._bulk_declared is False
            result = await _write(handler)
            assert result.status == AckStatus.ACK_STATUS_SUCCESS
            assert dialect.bulk_calls == 0
            assert _count(engine, "events") == 1
        finally:
            engine.dispose()

    @pytest.mark.asyncio
    async def test_bulk_land_claiming_success_over_empty_stage_is_refused(self):
        # bulk_land is untrusted connector code: a True return with a
        # stage row count that is not the batch's is refused loudly
        # before the mode statement — never an empty write acked as a
        # full batch.
        engine = _sqlite_sync_engine()
        try:
            handler = _connected_handler(engine)

            class _LyingDialect(_SqliteStageDialect):
                def bulk_land(self, conn, stage, batch, *, runtime):
                    return True  # landed nothing

            dialect = _LyingDialect()
            dialect.capabilities = handler.dialect.capabilities
            handler.dialect = dialect
            backend = handler._backend
            backend._dialect = dialect
            backend._bulk_declared = True
            backend._runtime = MagicMock()
            result = await _write(handler)
            assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
            assert "bulk_land reported success" in result.failure_summary
            assert _count(engine, "events") == 0
        finally:
            engine.dispose()


class TestStageTableGuards:
    @pytest.mark.asyncio
    async def test_missing_reflection_self_heals_on_the_cycle_connection(self):
        # A reconnect rebuilds the backend with an empty reflection
        # cache while streams stay configured; the cycle re-reflects on
        # its own connection instead of wedging in a retry loop.
        engine = _sqlite_sync_engine()
        try:
            handler = _connected_handler(engine)
            handler._backend._targets.clear()
            result = await _write(handler)
            assert result.status == AckStatus.ACK_STATUS_SUCCESS
            assert _count(engine, "events") == 1
        finally:
            engine.dispose()

    def test_plan_landing_unknown_columns_fails_loud(self):
        from sqlalchemy import Column, Integer

        backend = SqlAlchemyBackend(_SqliteStageDialect())
        target_address = TableAddress(table="events")
        backend._targets[target_address] = Table(
            "events", MetaData(), Column("id", Integer)
        )
        plan = StageWritePlan(
            stage=TableAddress(table="stg"),
            target=target_address,
            scope="temp",
            transactional=True,
            create_stage_sql="",
            truncate_sql=None,
            mode_sql="",
            drop_stage_sql="",
            columns=("id", "ghost"),
        )
        with pytest.raises(RuntimeError, match="ghost"):
            backend._stage_table(MagicMock(), plan)


class TestSyncEngineDdl:
    @pytest.mark.asyncio
    async def test_run_ddl_and_reflect_target_columns(self):
        engine = _sqlite_sync_engine()
        try:
            dialect = _SqliteStageDialect()
            backend = SqlAlchemyBackend(dialect)
            backend._sync_engine = engine
            await backend.run_ddl([TARGET_DDL])
            columns = await backend.target_columns(TableAddress(table="events"))
            assert set(columns) == {"id", "name"}
        finally:
            engine.dispose()


class _SyncReadRuntime:
    """Materialized-runtime stand-in carrying a real sync engine."""

    def __init__(self, engine: Any, driver: str = "sqlite"):
        self.is_adbc = False
        self.is_sync_sqlalchemy = True
        self.sync_engine = engine
        self.driver = driver
        self.connector_id = driver
        self.declared_sql_capabilities = None
        self.close = AsyncMock()


class TestSyncEngineReadPath:
    @pytest.mark.asyncio
    async def test_read_batches_pages_over_sync_engine(self):
        engine = _sqlite_sync_engine()
        try:
            with engine.begin() as conn:
                conn.exec_driver_sql(TARGET_DDL)
                for i in range(3):
                    conn.exec_driver_sql(
                        f"INSERT INTO events (id, name) VALUES ({i}, 'n{i}')"
                    )
            runtime = _SyncReadRuntime(engine)
            checkpoint = AsyncMock()
            checkpoint.get_cursor = AsyncMock(return_value=None)
            checkpoint.save_cursor = AsyncMock()
            config = {
                "endpoint_document": {
                    "database_object": {"name": "events"},
                    "columns": [{"name": "id"}, {"name": "name"}],
                },
                "stream_source": {},
            }
            connector = GenericSQLConnector()
            out = []
            with patch("cdk.sql.generic.materialize_runtime", new=AsyncMock()), patch(
                "cdk.sql.generic.SchemaContract"
            ) as sc:
                sc.return_value.from_pylist.side_effect = lambda rows: rows
                async for batch in connector.read_batches(
                    runtime,
                    config,
                    checkpoint=checkpoint,
                    stream_name="s",
                    batch_size=2,
                ):
                    out.append(batch)
            # 3 rows at batch_size=2 -> two pages.
            assert [len(page) for page in out] == [2, 1]
            assert out[0][0] == {"id": 0, "name": "n0"}
            runtime.close.assert_awaited()
        finally:
            engine.dispose()


class _SyncWriteRuntime:
    """Materialized-runtime stand-in for the write role.

    Mirrors the real ConnectionRuntime contract: the async ``engine``
    accessor refuses on a sync-only transport, so a dispatch regression
    in connect() fails here instead of silently picking the wrong mode.
    """

    def __init__(self, engine: Any, driver: str = "sqlite"):
        self.is_adbc = False
        self.is_sync_sqlalchemy = True
        self.sync_engine = engine
        self.driver = driver
        self.connector_id = driver
        self.declared_sql_capabilities = {
            "catalog": "none",
            "session_targeting": "per_statement",
            "merge_form": "insert_on_conflict",
            "bulk_load": "none",
            "stage": {
                "scope": "temp",
                "schema": "target",
                "transactional_ddl": True,
            },
        }
        self.close = AsyncMock()

    @property
    def engine(self):
        raise RuntimeError("engine not available: sync-only transport; use sync_engine")


class _StubTypeMapper:
    """Two-type mapper, enough for the DDL builder on SQLite."""

    _to_native = {
        "Int64": "INTEGER",
        "Int32": "INTEGER",
        "Utf8": "TEXT",
        "Binary": "BLOB",
        "Timestamp(MICROSECOND)": "TIMESTAMP",
        "Timestamp(MICROSECOND, UTC)": "TIMESTAMP",
    }
    _to_arrow = {"INTEGER": "Int64", "TEXT": "Utf8"}

    def to_arrow_type(self, native: str) -> str:
        return self._to_arrow[native.upper()]

    def to_native_type(self, canonical: str, params: Any = None) -> str:
        return self._to_native[canonical]


class _SqliteConnector(GenericSQLConnector):
    dialect_class = _SqliteStageDialect


class TestSyncRuntimeWiring:
    """The dispatch between the three modes, driven through the real
    entry points (connect -> DDL -> write) instead of injected fields."""

    @pytest.mark.asyncio
    async def test_connect_ddl_and_write_through_sync_runtime(self):
        """connect -> DDL -> write flows through the sync-engine runtime."""
        engine = _sqlite_sync_engine()
        try:
            handler = _SqliteConnector()
            with patch("cdk.sql.generic.materialize_runtime", new=AsyncMock()):
                await handler.connect(_SyncWriteRuntime(engine))
            assert handler._sync_engine is engine
            assert handler._engine is None
            assert handler._adbc_only is False
            assert handler._backend is not None

            state = _StreamState(
                address=TableAddress(table="events"),
                primary_keys=["id"],
                write_mode="insert",
                endpoint_document={
                    "columns": [
                        {
                            "name": "id",
                            "native_type": "INTEGER",
                            "arrow_type": "Int64",
                            "nullable": False,
                        },
                        {"name": "name", "native_type": "TEXT", "arrow_type": "Utf8"},
                    ],
                },
            )
            await handler._ensure_tables_exist(state, _StubTypeMapper())

            state.schema_contract = _arrow_contract()
            handler._streams["s1"] = state

            result = await _write(handler, rows=[{"id": 1, "name": "a"}])
            assert result.status == AckStatus.ACK_STATUS_SUCCESS
            assert _count(engine, "events") == 1
        finally:
            engine.dispose()


class TestSyncEngineStatementTimeout:
    """The ack-budget statement timeout (issues #231/#234) cannot cancel a
    worker thread; the sync path must neither apply it nor mislabel a
    driver timeout as a cancelled statement."""

    @pytest.mark.asyncio
    async def test_budget_does_not_cancel_sync_engine_write(self):
        engine = _sqlite_sync_engine()
        try:
            handler = _connected_handler(engine)
            # A zero budget wrongly applied as asyncio.timeout would fire
            # immediately and abandon the in-flight worker-thread write.
            handler.set_statement_timeout(0.0)
            result = await _write(handler)
            assert result.status == AckStatus.ACK_STATUS_SUCCESS
        finally:
            engine.dispose()

    @pytest.mark.asyncio
    async def test_driver_timeout_keeps_generic_reason(self):
        engine = _sqlite_sync_engine()
        try:
            handler = _connected_handler(engine)
            handler.set_statement_timeout(5.0)
            with patch.object(
                handler._backend,
                "execute_write",
                new=AsyncMock(side_effect=TimeoutError()),
            ):
                result = await _write(handler)
            assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
            # Never a reason-less ack, never the cancelled-statement claim.
            assert result.failure_summary
            assert "was cancelled" not in result.failure_summary
        finally:
            engine.dispose()

    def test_unenforceable_budget_warns(self, caplog):
        import logging

        engine = _sqlite_sync_engine()
        try:
            handler = _connected_handler(engine)
            with caplog.at_level(logging.WARNING, logger="cdk.sql.generic"):
                handler.set_statement_timeout(2.0)
            assert any("cannot be enforced" in r.getMessage() for r in caplog.records)
        finally:
            engine.dispose()


class TestAsyncEngineParity:
    """The async flavour enters the same shared sync-Connection cycle body
    via run_sync; prove it against a real async driver, not a fake."""

    async def _async_handler(self):
        pytest.importorskip("aiosqlite")
        from sqlalchemy import event
        from sqlalchemy.ext.asyncio import create_async_engine

        engine = create_async_engine(
            "sqlite+aiosqlite://",
            connect_args={"check_same_thread": False, "isolation_level": None},
            poolclass=StaticPool,
        )

        # Same recipe as the sync fixture: make the sqlite driver
        # genuinely transactional for DDL so the declared
        # transactional_ddl: true shape behaves as it does on Postgres.
        @event.listens_for(engine.sync_engine, "begin")
        def _do_begin(conn):
            conn.exec_driver_sql("BEGIN")

        async with engine.begin() as conn:
            await conn.exec_driver_sql(TARGET_DDL)
        address = TableAddress(table="events")
        caps = _declared_caps()
        handler = GenericSQLConnector()
        handler._connected = True
        handler._engine = engine
        handler.dialect = _SqliteStageDialect()
        handler.dialect.capabilities = caps
        handler._capabilities = caps
        backend = SqlAlchemyBackend(handler.dialect)
        backend._engine = engine
        await backend.target_columns(address)
        handler._backend = backend
        handler._streams["s1"] = _StreamState(
            address=address,
            write_mode="insert",
            primary_keys=["id"],
            conflict_keys=[],
            schema_contract=_arrow_contract(),
        )
        return handler, engine

    async def _async_count(self, engine, table: str) -> int:
        async with engine.connect() as conn:
            result = await conn.exec_driver_sql(f"SELECT count(*) FROM {table}")
            return result.scalar_one()

    @pytest.mark.asyncio
    async def test_insert_and_reinsert_dedupes(self):
        handler, engine = await self._async_handler()
        try:
            first = await _write(handler, seq=1, rows=[{"id": 1, "name": "a"}])
            assert first.status == AckStatus.ACK_STATUS_SUCCESS
            assert await self._async_count(engine, "events") == 1

            # A re-read of the same keyed row is skipped by the insert
            # anti-join (issue #282): SUCCESS, no double row -- never the
            # removed ALREADY_COMMITTED status, never a PK-violation rollback.
            replay = await _write(handler, seq=1, rows=[{"id": 1, "name": "a"}])
            assert replay.status == AckStatus.ACK_STATUS_SUCCESS
            assert await self._async_count(engine, "events") == 1

            # Same primary key with different content is likewise deduped on
            # ``id`` -- first occurrence wins, the second never lands.
            dupe = await _write(handler, seq=2, rows=[{"id": 1, "name": "dupe"}])
            assert dupe.status == AckStatus.ACK_STATUS_SUCCESS
            assert await self._async_count(engine, "events") == 1
            # First occurrence wins: the original row content is untouched.
            rows = await self._async_rows(engine)
            assert rows == [(1, "a")]
        finally:
            await engine.dispose()

    async def _async_rows(self, engine):
        async with engine.connect() as conn:
            result = await conn.exec_driver_sql("SELECT id, name FROM events")
            return result.all()

    @pytest.mark.asyncio
    async def test_failed_mode_statement_rolls_back_through_run_sync(self):
        # The async wrapper genuinely differs (greenlet exception
        # propagation through run_sync); prove a failed transactional
        # cycle leaves neither rows nor a stage behind on this flavour.
        import dataclasses

        from cdk.sql import generic as generic_module

        handler, engine = await self._async_handler()
        try:
            original_build = generic_module.build_stage_write_plan

            def _broken_mode(*args, **kwargs):
                plan = original_build(*args, **kwargs)
                return dataclasses.replace(
                    plan, mode_sql="INSERT INTO no_such_table_xyz SELECT 1"
                )

            with patch.object(generic_module, "build_stage_write_plan", _broken_mode):
                result = await _write(handler, rows=[{"id": 5, "name": "e"}])
            assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
            assert "no_such_table_xyz" in result.failure_summary
            assert await self._async_count(engine, "events") == 0
            async with engine.connect() as conn:
                rows = await conn.exec_driver_sql(
                    "SELECT name FROM sqlite_temp_master WHERE type='table'"
                )
                leftovers = {r[0] for r in rows.all() if "stage" in r[0]}
            assert leftovers == set()
        finally:
            await engine.dispose()
