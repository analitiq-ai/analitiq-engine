"""Tests for the ADBC-only dispatch surface on `DatabaseDestinationHandler`.

These cover the small classification helpers and the parameterized
`AdbcCommitRecordError` shape — pieces that are pure logic and don't
require an open Snowflake connection. End-to-end runtime behaviour
(MERGE, TRUNCATE+ingest, batch-commit-record dispatch) is covered by
the manual Docker run noted in the PR description.
"""

from __future__ import annotations

import pytest

from src.destination.connectors.database import (
    AdbcCommitRecordError,
    AdbcConfigurationError,
    _is_fatal_adbc_error,
)


class TestIsFatalAdbcError:
    def test_programming_error_is_fatal(self):
        class ProgrammingError(Exception):
            pass

        assert _is_fatal_adbc_error(ProgrammingError("bad sql"))

    def test_integrity_error_is_fatal(self):
        class IntegrityError(Exception):
            pass

        assert _is_fatal_adbc_error(IntegrityError("PK collision"))

    def test_not_supported_error_is_fatal(self):
        class NotSupportedError(Exception):
            pass

        assert _is_fatal_adbc_error(NotSupportedError("no MERGE"))

    def test_data_error_is_fatal(self):
        class DataError(Exception):
            pass

        assert _is_fatal_adbc_error(DataError("bad value"))

    def test_operational_error_is_retryable(self):
        class OperationalError(Exception):
            pass

        # OperationalError is the canonical PEP-249 transient class —
        # keeping it retryable means a network blip doesn't shut a
        # pipeline down with a fatal classification.
        assert not _is_fatal_adbc_error(OperationalError("net glitch"))

    def test_plain_runtime_error_is_retryable(self):
        assert not _is_fatal_adbc_error(RuntimeError("anything else"))

    def test_inheritance_chain_matches(self):
        class ProgrammingError(Exception):
            pass

        class SnowflakeSpecificError(ProgrammingError):
            pass

        assert _is_fatal_adbc_error(SnowflakeSpecificError("extends Programming"))


class TestAdbcCommitRecordError:
    def test_insert_message_warns_of_duplication(self):
        err = AdbcCommitRecordError(RuntimeError("boom"), write_mode="insert")
        msg = str(err)
        assert "duplicate rows" in msg
        assert err.write_mode == "insert"
        # The inner exception is preserved as __cause__ for tracebacks.
        assert isinstance(err.__cause__, RuntimeError)

    def test_truncate_insert_message_is_idempotent(self):
        err = AdbcCommitRecordError(
            RuntimeError("boom"), write_mode="truncate_insert"
        )
        assert "idempotent" in str(err)
        assert "truncate" in str(err).lower()

    def test_upsert_message_is_idempotent(self):
        err = AdbcCommitRecordError(RuntimeError("boom"), write_mode="upsert")
        assert "idempotent" in str(err)
        assert "MERGE" in str(err) or "conflict" in str(err).lower()

    def test_unknown_mode_falls_through_with_pointer(self):
        err = AdbcCommitRecordError(
            RuntimeError("boom"), write_mode="weird_mode"
        )
        assert "weird_mode" in str(err)

    def test_default_mode_is_insert(self):
        # Insert is the only mode used by the existing Postgres fast
        # path; keep that as the default so older call sites continue
        # to surface the duplication warning.
        err = AdbcCommitRecordError(RuntimeError("boom"))
        assert "duplicate rows" in str(err)
        assert err.write_mode == "insert"


class TestAdbcConfigurationErrorContract:
    def test_is_runtime_error(self):
        # ACK_STATUS_FATAL_FAILURE handling in write_batch keys on
        # this base class — keeping it a RuntimeError lets generic
        # ``except RuntimeError`` continue to catch it.
        with pytest.raises(RuntimeError):
            raise AdbcConfigurationError("bad config")
