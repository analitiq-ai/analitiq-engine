"""Shared write-mode semantics across both transport backends (#389).

The engine's "same concept, same semantics" acceptance for the
TransportBackend split: identical author intent — the same
``StageWritePlan`` built from the same declaration, executed over the
same batches — must leave identical target state whichever backend runs
it. Every test here is parametrized over ``SqlAlchemyBackend`` and
``AdbcBackend`` (driving a PEP-249 adapter over sqlite3, since no ADBC
driver ships in CI) and over both declared transaction shapes, against a
real SQLite database — actual SQL, not mocks.
"""

from __future__ import annotations

import sqlite3

import pyarrow as pa
import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from cdk.sql.adbc_backend import AdbcBackend
from cdk.sql.backend import SqlAlchemyBackend
from cdk.sql.capabilities import SqlCapabilities
from cdk.sql.dialects import SqlDialect, TableAddress
from cdk.sql.write_plan import build_stage_write_plan

from .conftest import caps_block

TARGET = TableAddress(table="events")
TARGET_DDL = "CREATE TABLE events (id INTEGER PRIMARY KEY, name TEXT)"


class _SqliteParityDialect(SqlDialect):
    """SQLite renderings for the stage cycle (shared by both backends)."""

    name = "sqlite_parity"

    def stage_table_sql(self, stage, target, *, temp):
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


# ---- a PEP-249 "ADBC" adapter over sqlite3 ---------------------------------


class _SqliteAdbcCursor:
    def __init__(self, conn: sqlite3.Connection):
        self._cursor = conn.cursor()

    def execute(self, sql, params=None):
        self._cursor.execute(sql, params or ())

    def executemany(self, sql, params):
        self._cursor.executemany(sql, params)

    def adbc_ingest(self, table, batch: pa.RecordBatch, mode, **kwargs):
        # The driver-native Arrow landing, modeled faithfully: append the
        # batch's rows into *table* in batch column order. Targeting
        # kwargs are meaningless on schemaless SQLite.
        assert mode == "append"
        cols = ", ".join(f'"{c}"' for c in batch.schema.names)
        placeholders = ", ".join("?" for _ in batch.schema.names)
        rows = [tuple(row[c] for c in batch.schema.names) for row in batch.to_pylist()]
        self._cursor.executemany(
            f'INSERT INTO "{table}" ({cols}) VALUES ({placeholders})', rows
        )

    def fetchone(self):
        return self._cursor.fetchone()

    @property
    def description(self):
        return self._cursor.description

    def close(self):
        self._cursor.close()


class _SqliteAdbcConnection:
    def __init__(self):
        # Autocommit mode: the AdbcBackend's own commit calls delimit the
        # steps, exactly as an ADBC driver's connection would.
        self._conn = sqlite3.connect(
            ":memory:", check_same_thread=False, isolation_level=None
        )

    def cursor(self):
        return _SqliteAdbcCursor(self._conn)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()


# ---- per-backend harnesses --------------------------------------------------


class _SaHarness:
    name = "sqlalchemy"

    def __init__(self, caps: SqlCapabilities):
        self.dialect = _SqliteParityDialect()
        self.dialect.capabilities = caps
        self._engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False, "isolation_level": None},
            poolclass=StaticPool,
        )
        from sqlalchemy import event

        @event.listens_for(self._engine, "begin")
        def _do_begin(conn):  # pragma: no cover - passthrough
            conn.exec_driver_sql("BEGIN")

        self.backend = SqlAlchemyBackend(self.dialect)
        self.backend._sync_engine = self._engine

    async def prepare(self):
        await self.backend.run_ddl([TARGET_DDL])
        await self.backend.target_columns(TARGET)

    def rows(self) -> list[tuple]:
        with self._engine.connect() as conn:
            return sorted(conn.exec_driver_sql("SELECT id, name FROM events").all())

    def table_names(self) -> set[str]:
        with self._engine.connect() as conn:
            rows = conn.exec_driver_sql(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "UNION ALL SELECT name FROM sqlite_temp_master WHERE type='table'"
            ).all()
        return {r[0] for r in rows}

    def dispose(self):
        self._engine.dispose()


class _AdbcHarness:
    name = "adbc"

    def __init__(self, caps: SqlCapabilities):
        self.dialect = _SqliteParityDialect()
        self.dialect.capabilities = caps
        self._conn = _SqliteAdbcConnection()
        self.backend = AdbcBackend(self.dialect)
        self.backend._conn = self._conn
        self.backend._bulk_load = caps.bulk_load

    async def prepare(self):
        await self.backend.run_ddl([TARGET_DDL])
        assert await self.backend.target_columns(TARGET) == ("id", "name")

    def rows(self) -> list[tuple]:
        cursor = self._conn._conn.cursor()
        try:
            return sorted(cursor.execute("SELECT id, name FROM events").fetchall())
        finally:
            cursor.close()

    def table_names(self) -> set[str]:
        cursor = self._conn._conn.cursor()
        try:
            rows = cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "UNION ALL SELECT name FROM sqlite_temp_master WHERE type='table'"
            ).fetchall()
        finally:
            cursor.close()
        return {r[0] for r in rows}

    def dispose(self):
        self._conn.close()


HARNESSES = [_SaHarness, _AdbcHarness]
SHAPES = [True, False]


def _caps(transactional: bool) -> SqlCapabilities:
    # adbc_ingest is inert on the SQLAlchemy backend (its landing is
    # executemany; the base bulk_land declines), and the AdbcBackend's
    # declared native mechanism — one declaration, both backends.
    return SqlCapabilities.from_declaration(
        caps_block(
            bulk_load="adbc_ingest",
            stage_scope="temp",
            transactional_ddl=transactional,
        )
    )


def _plan(harness, *, write_mode, seq, conflict_keys=(), truncate_now=False):
    return build_stage_write_plan(
        harness.dialect,
        harness.dialect.capabilities,
        target=TARGET,
        columns=("id", "name"),
        write_mode=write_mode,
        conflict_keys=list(conflict_keys),
        identity=["id"],
        truncate_now=truncate_now,
        run_id="r1",
        stream_id="s1",
        batch_seq=seq,
    )


def _batch(rows) -> pa.RecordBatch:
    return pa.RecordBatch.from_pylist(rows)


async def _write(harness, *, write_mode, seq, rows, conflict_keys=(), truncate=False):
    plan = _plan(
        harness,
        write_mode=write_mode,
        seq=seq,
        conflict_keys=conflict_keys,
        truncate_now=truncate,
    )
    await harness.backend.execute_write(plan, _batch(rows))


@pytest.fixture(params=HARNESSES, ids=[h.name for h in HARNESSES])
def harness_cls(request):
    return request.param


@pytest.fixture(params=SHAPES, ids=["transactional", "stepwise"])
def transactional(request):
    return request.param


@pytest.fixture
async def harness(harness_cls, transactional):
    h = harness_cls(_caps(transactional))
    await h.prepare()
    yield h
    h.dispose()


class TestWriteModeParity:
    @pytest.mark.asyncio
    async def test_insert_lands_rows_and_a_replay_is_idempotent(self, harness):
        rows = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
        await _write(harness, write_mode="insert", seq=1, rows=rows)
        assert harness.rows() == [(1, "a"), (2, "b")]
        # A lost-ack replay re-sends the identical batch: the anti-join
        # skips every row instead of raising or duplicating.
        await _write(harness, write_mode="insert", seq=1, rows=rows)
        assert harness.rows() == [(1, "a"), (2, "b")]

    @pytest.mark.asyncio
    async def test_insert_rereading_the_boundary_row_is_deduped(self, harness):
        await _write(harness, write_mode="insert", seq=1, rows=[{"id": 1, "name": "a"}])
        # The next read page re-reads the inclusive cursor boundary.
        await _write(
            harness,
            write_mode="insert",
            seq=2,
            rows=[{"id": 1, "name": "a"}, {"id": 2, "name": "b"}],
        )
        assert harness.rows() == [(1, "a"), (2, "b")]

    @pytest.mark.asyncio
    async def test_upsert_updates_in_place_and_a_replay_is_idempotent(self, harness):
        await _write(
            harness,
            write_mode="upsert",
            seq=1,
            rows=[{"id": 1, "name": "old"}],
            conflict_keys=["id"],
        )
        updated = [{"id": 1, "name": "new"}, {"id": 2, "name": "b"}]
        await _write(
            harness, write_mode="upsert", seq=2, rows=updated, conflict_keys=["id"]
        )
        assert harness.rows() == [(1, "new"), (2, "b")]
        await _write(
            harness, write_mode="upsert", seq=2, rows=updated, conflict_keys=["id"]
        )
        assert harness.rows() == [(1, "new"), (2, "b")]

    @pytest.mark.asyncio
    async def test_truncate_insert_truncates_once_then_appends(self, harness):
        await _write(
            harness, write_mode="insert", seq=1, rows=[{"id": 9, "name": "stale"}]
        )
        await _write(
            harness,
            write_mode="truncate_insert",
            seq=1,
            rows=[{"id": 1, "name": "a"}],
            truncate=True,
        )
        await _write(
            harness,
            write_mode="truncate_insert",
            seq=2,
            rows=[{"id": 2, "name": "b"}],
        )
        assert harness.rows() == [(1, "a"), (2, "b")]

    @pytest.mark.asyncio
    async def test_no_stage_table_outlives_a_cycle(self, harness):
        await _write(harness, write_mode="insert", seq=1, rows=[{"id": 1, "name": "a"}])
        await _write(
            harness,
            write_mode="upsert",
            seq=2,
            rows=[{"id": 1, "name": "b"}],
            conflict_keys=["id"],
        )
        leftovers = {
            t for t in harness.table_names() if t.startswith("_analitiq_stage_")
        }
        assert leftovers == set()


class TestCrossBackendEquality:
    @pytest.mark.asyncio
    async def test_the_same_script_leaves_byte_identical_state(self, transactional):
        """One mixed write script, executed by each backend against its own
        database, must end in the same rows — the parity claim stated as a
        direct comparison rather than per-backend literals."""
        finals = []
        for cls in HARNESSES:
            h = cls(_caps(transactional))
            await h.prepare()
            try:
                await _write(
                    h,
                    write_mode="truncate_insert",
                    seq=1,
                    rows=[{"id": 1, "name": "a"}, {"id": 2, "name": "b"}],
                    truncate=True,
                )
                await _write(
                    h,
                    write_mode="upsert",
                    seq=2,
                    rows=[{"id": 2, "name": "b2"}, {"id": 3, "name": "c"}],
                    conflict_keys=["id"],
                )
                await _write(
                    h,
                    write_mode="insert",
                    seq=3,
                    rows=[{"id": 3, "name": "dup"}, {"id": 4, "name": "d"}],
                )
                # And a lost-ack replay of the last batch.
                await _write(
                    h,
                    write_mode="insert",
                    seq=3,
                    rows=[{"id": 3, "name": "dup"}, {"id": 4, "name": "d"}],
                )
                finals.append((cls.name, h.rows()))
            finally:
                h.dispose()
        (name_a, rows_a), (name_b, rows_b) = finals
        assert rows_a == rows_b, f"{name_a} and {name_b} diverged"
        assert rows_a == [(1, "a"), (2, "b2"), (3, "c"), (4, "d")]
