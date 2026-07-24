"""Declared error_map consumption in the SQL write path (issue #401).

Two decision points: the facade's ack ladder (declared verdict before the
PEP-249 class-name heuristic) and the ADBC backend boundary (a declared
fact suppresses the fatal reclassification so the raw exception reaches
the ladder, where the declaration derives the verdict).
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from cdk.adbc_registry import AdbcConfigurationError
from cdk.declarations import parse_declared_error_map
from cdk.sql.adbc_backend import AdbcBackend
from cdk.sql.backend import StageWritePlan
from cdk.sql.dialects import SqlDialect, TableAddress
from cdk.sql.generic import GenericSQLConnector
from cdk.types import AckStatus, FailureCategory


class ProgrammingError(Exception):
    """Bears the PEP-249 fatal name; used to pit declaration vs heuristic."""


def _error_map(block):
    parsed = parse_declared_error_map(block)
    assert parsed is not None
    return parsed


class TestAckLadderDeclaredFirst:
    def _handler(self, error_map=None) -> GenericSQLConnector:
        handler = GenericSQLConnector()
        handler._error_map = error_map
        return handler

    def test_declared_transient_overrides_the_fatal_name(self):
        handler = self._handler(
            _error_map({"exception": {"ProgrammingError": "transient"}})
        )
        result = handler._classify_unexpected_write_error(ProgrammingError("boom"))
        assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
        assert (
            result.failure_category
            == FailureCategory.FAILURE_CATEGORY_WRITE_REJECTED
        )
        assert "declared error_map" in result.failure_summary

    def test_declared_sqlstate_claims_auth(self):
        handler = self._handler(_error_map({"sqlstate": {"28": "auth"}}))
        exc = Exception("login refused")
        exc.sqlstate = "28000"
        result = handler._classify_unexpected_write_error(exc)
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert (
            result.failure_category == FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT
        )

    def test_declared_write_rejected_is_fatal_write_rejected(self):
        handler = self._handler(_error_map({"sqlstate": {"23": "write_rejected"}}))
        exc = Exception("duplicate key")
        exc.sqlstate = "23505"
        result = handler._classify_unexpected_write_error(exc)
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert (
            result.failure_category
            == FailureCategory.FAILURE_CATEGORY_WRITE_REJECTED
        )

    def test_unclaimed_exception_keeps_the_heuristic(self):
        handler = self._handler(_error_map({"exception": {"SomeOther": "auth"}}))
        result = handler._classify_unexpected_write_error(ProgrammingError("boom"))
        assert result.status == AckStatus.ACK_STATUS_FATAL_FAILURE
        assert (
            result.failure_category == FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT
        )

    def test_no_map_keeps_the_heuristic(self):
        handler = self._handler(None)
        result = handler._classify_unexpected_write_error(Exception("flaky"))
        assert result.status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE

    def test_heuristic_fallback_logs(self, caplog):
        import logging

        handler = self._handler(_error_map({"exception": {"SomeOther": "auth"}}))
        with caplog.at_level(logging.INFO, logger="cdk.sql.generic"):
            handler._classify_unexpected_write_error(ProgrammingError("boom"))
        assert any("class-name heuristic" in r.message for r in caplog.records)


class TestAdbcBoundary:
    def _backend(self, error_map=None) -> AdbcBackend:
        backend = AdbcBackend(SqlDialect())
        backend._error_map = error_map
        return backend

    def test_declared_fact_suppresses_the_fatal_reclassification(self):
        backend = self._backend(
            _error_map({"exception": {"ProgrammingError": "transient"}})
        )
        exc = ProgrammingError("boom")
        with pytest.raises(ProgrammingError):
            backend._reraise_driver_error(exc)

    def test_unclaimed_fatal_name_still_reclassifies(self):
        backend = self._backend(_error_map({"exception": {"SomeOther": "auth"}}))
        with pytest.raises(AdbcConfigurationError):
            backend._reraise_driver_error(ProgrammingError("boom"))

    def test_non_fatal_unclaimed_exception_reraises_raw(self):
        backend = self._backend(None)
        with pytest.raises(ValueError):
            backend._reraise_driver_error(ValueError("flaky"))


class _RecordingCursor:
    def __init__(self):
        self.executemany_calls: list[tuple[str, list]] = []

    def executemany(self, sql, rows):
        self.executemany_calls.append((sql, list(rows)))


def _plan(columns, rows_per_statement):
    address = TableAddress(table="stage")
    return StageWritePlan(
        stage=address,
        target=TableAddress(table="events"),
        scope="temp",
        transactional=True,
        create_stage_sql="",
        truncate_sql=None,
        mode_sql="",
        drop_stage_sql="",
        columns=tuple(columns),
        rows_per_statement=rows_per_statement,
    )


class TestAdbcChunking:
    def _batch(self, n):
        return pa.RecordBatch.from_pydict(
            {"id": list(range(n)), "v": [f"v{i}" for i in range(n)]}
        )

    def test_chunked_landing_never_exceeds_the_cap(self):
        backend = AdbcBackend(SqlDialect())
        cursor = _RecordingCursor()
        backend._executemany_land(cursor, _plan(["id", "v"], 4), self._batch(10))
        sizes = [len(rows) for _, rows in cursor.executemany_calls]
        assert sizes == [4, 4, 2]
        landed = [row for _, rows in cursor.executemany_calls for row in rows]
        assert landed == [(i, f"v{i}") for i in range(10)]

    def test_undeclared_cap_lands_in_one_statement(self):
        backend = AdbcBackend(SqlDialect())
        cursor = _RecordingCursor()
        backend._executemany_land(cursor, _plan(["id", "v"], None), self._batch(10))
        assert len(cursor.executemany_calls) == 1


class TestSqlAlchemyChunking:
    def _batch(self, n):
        return pa.RecordBatch.from_pydict(
            {"id": list(range(n)), "v": [f"v{i}" for i in range(n)]}
        )

    def _backend_with_recording_conn(self):
        from unittest.mock import MagicMock

        from cdk.sql.backend import SqlAlchemyBackend

        backend = SqlAlchemyBackend(SqlDialect())
        stage_table = MagicMock()
        backend._stage_table = MagicMock(return_value=stage_table)
        conn = MagicMock()
        return backend, conn

    def test_chunked_landing_never_exceeds_the_cap(self):
        backend, conn = self._backend_with_recording_conn()
        backend._executemany_land(conn, _plan(["id", "v"], 4), self._batch(10))
        sizes = [len(call.args[1]) for call in conn.execute.call_args_list]
        assert sizes == [4, 4, 2]

    def test_undeclared_cap_lands_in_one_statement(self):
        backend, conn = self._backend_with_recording_conn()
        backend._executemany_land(conn, _plan(["id", "v"], None), self._batch(10))
        assert len(conn.execute.call_args_list) == 1
        assert len(conn.execute.call_args_list[0].args[1]) == 10
