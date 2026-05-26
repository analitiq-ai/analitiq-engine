"""Tests for the ADBC-only dispatch surface on `DatabaseDestinationHandler`.

These cover the small classification helpers and the parameterized
`AdbcCommitRecordError` shape — pieces that are pure logic and don't
require an open Snowflake connection. End-to-end runtime behaviour
(MERGE, TRUNCATE+ingest, batch-commit-record dispatch) is covered by
the manual Docker run noted in the PR description.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.destination.connectors.database import (
    AdbcCommitRecordError,
    AdbcConfigurationError,
    _is_fatal_adbc_error,
    _reclassify_as_fatal,
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


class TestRecordBatchCommitViaAdbcIdempotency:
    """Lock in the IntegrityError-as-success contract.

    A concurrent retry races to write the same ``(run_id, stream_id,
    batch_seq)`` PK in ``_batch_commits``. The second writer must
    treat the resulting IntegrityError as success — both writers
    intended the same outcome and the row exists. Any other PEP-249
    fatal class must still propagate so an actually-broken commit-
    record path (missing table, permission denial) doesn't get
    masked.
    """

    @pytest.fixture
    def handler(self):
        from src.destination.connectors.database import (
            DatabaseDestinationHandler,
        )

        # Bypass __init__: configure_schema / connect pull a lot in
        # we don't need here. Set only the fields the SUT touches.
        h = DatabaseDestinationHandler.__new__(DatabaseDestinationHandler)
        h._adbc_conn = None  # poisoned-by-_execute_adbc_dml_sync semantics
        h._runtime = None
        # State carries schema_name for the qualified INSERT target.
        return h

    @pytest.fixture
    def state(self):
        from src.destination.connectors.database import _StreamState

        return _StreamState(schema_name="ANALYTICS")

    @pytest.mark.asyncio
    async def test_integrity_error_treated_as_success(self, handler, state):
        # Driver-level IntegrityError; the helper matches by class
        # name across the MRO so any subclass with that name works.
        class IntegrityError(Exception):
            pass

        # `_reopen_adbc_if_needed_sync` opens the cached connection.
        # Cursor.execute raises the driver's IntegrityError, which
        # `_execute_adbc_dml_sync` reclassifies via _reclassify_as_fatal
        # and `_record_batch_commit_via_adbc` must then swallow.
        cursor = MagicMock()
        cursor.execute.side_effect = IntegrityError(
            "duplicate key value violates unique constraint"
        )
        conn = MagicMock()
        conn.cursor.return_value = cursor

        with patch.object(
            handler, "_reopen_adbc_if_needed_sync", return_value=conn
        ), patch.object(handler, "_poison_adbc_connection"):
            # Must NOT raise.
            await handler._record_batch_commit_via_adbc(
                state, "run-1", "stream-1", 7, b"cursor-token", 100,
            )
        # The DML was actually attempted (proves we didn't short-
        # circuit somewhere earlier and produce a false success).
        cursor.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_programming_error_still_propagates(self, handler, state):
        class ProgrammingError(Exception):
            pass

        cursor = MagicMock()
        cursor.execute.side_effect = ProgrammingError(
            'relation "ANALYTICS._batch_commits" does not exist'
        )
        conn = MagicMock()
        conn.cursor.return_value = cursor

        with patch.object(
            handler, "_reopen_adbc_if_needed_sync", return_value=conn
        ), patch.object(handler, "_poison_adbc_connection"):
            with pytest.raises(AdbcConfigurationError) as info:
                await handler._record_batch_commit_via_adbc(
                    state, "run-1", "stream-1", 7, b"cursor-token", 100,
                )
        # The class-name prefix lives in the wrapped message — losing
        # it would silently regress operator triage.
        assert "ProgrammingError" in str(info.value)


class TestReclassifyAsFatal:
    def test_preserves_original_class_name_in_message(self):
        # The top-level engine log only renders ``str(exception)``;
        # without the class name, operators can't tell a missing
        # column (ProgrammingError) from a permission denial
        # (NotSupportedError on some drivers).
        class ProgrammingError(Exception):
            pass

        wrapped = _reclassify_as_fatal(ProgrammingError("missing column foo"))
        assert isinstance(wrapped, AdbcConfigurationError)
        assert "ProgrammingError" in str(wrapped)
        assert "missing column foo" in str(wrapped)

    def test_distinguishes_integrity_from_programming(self):
        class IntegrityError(Exception):
            pass

        class ProgrammingError(Exception):
            pass

        a = _reclassify_as_fatal(IntegrityError("dup"))
        b = _reclassify_as_fatal(ProgrammingError("dup"))
        # Same inner message but different prefixes — the difference
        # is precisely the information operators need.
        assert str(a) != str(b)
        assert "IntegrityError" in str(a)
        assert "ProgrammingError" in str(b)
