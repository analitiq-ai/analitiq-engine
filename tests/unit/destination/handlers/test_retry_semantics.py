"""Per-stream retry-safety verdicts (issue #286).

Every destination handler reports, per configured stream, whether a
same-``RUN_ID`` restart can duplicate writes. The SQL verdict is write
mode x key x transport aware; file dedups through its manifest; stdout
has nothing to dedup with. The base default is the only honest claim for
a handler that declares nothing: at-least-once.
"""

from typing import Any

import pytest

from cdk.base_handler import BaseDestinationHandler, BatchWriteResult
from cdk.sql.generic import GenericSQLConnector
from cdk.sql.generic import _StreamState as SqlStreamState
from cdk.types import Cursor, RetrySemantics, SchemaSpec
from src.destination.connectors.file import FileDestinationHandler
from src.destination.connectors.stream import StreamDestinationHandler


class _DeclareNothingHandler(BaseDestinationHandler):
    """Minimal concrete handler that inherits the base verdict."""

    async def connect(self, runtime: Any) -> None:  # pragma: no cover
        pass

    async def disconnect(self) -> None:  # pragma: no cover
        pass

    async def configure_schema(self, schema_spec: SchemaSpec) -> bool:
        return True

    async def write_batch(
        self,
        run_id: str,
        stream_id: str,
        batch_seq: int,
        record_batch: Any,
        record_ids: list[str],
        cursor: Cursor,
    ) -> BatchWriteResult:  # pragma: no cover
        raise NotImplementedError

    async def health_check(self) -> bool:  # pragma: no cover
        return True

    @property
    def connector_type(self) -> str:
        return "declare-nothing"


@pytest.mark.unit
class TestBaseDefaultVerdict:
    def test_base_default_is_at_least_once(self):
        verdict = _DeclareNothingHandler().retry_semantics("any-stream")
        assert verdict.semantics == RetrySemantics.RETRY_SEMANTICS_AT_LEAST_ONCE
        assert "declares no retry-safety" in verdict.reason


@pytest.mark.unit
class TestFileAndStdoutVerdicts:
    def test_file_reports_not_replay_safe(self):
        """The manifest dedups by batch position; a same-run restart
        re-numbers re-batched rows, so a committed position carrying
        different rows would be skipped (issue #282 row-drop class) —
        the verdict must not claim exactly-once."""
        verdict = FileDestinationHandler().retry_semantics("s1")
        assert verdict.semantics == RetrySemantics.RETRY_SEMANTICS_AT_LEAST_ONCE
        assert "batch position" in verdict.reason

    def test_stdout_reports_at_least_once(self):
        verdict = StreamDestinationHandler().retry_semantics("s1")
        assert verdict.semantics == RetrySemantics.RETRY_SEMANTICS_AT_LEAST_ONCE
        assert "prints" in verdict.reason


@pytest.mark.unit
class TestSqlVerdicts:
    """The SQL verdict must match the write path that actually runs:
    upsert dedups on its conflict keys everywhere; insert anti-joins on
    row identity only on the SQLAlchemy transport; truncate-insert
    truncates once per run (issue #307) but appends with no row-identity
    dedup, so it cannot claim replay safety."""

    def _handler(self, *, adbc_only: bool, **state_kwargs) -> GenericSQLConnector:
        handler = GenericSQLConnector()
        handler._adbc_only = adbc_only
        handler._streams["s1"] = SqlStreamState(
            table_name="t", endpoint_document={"x": 1}, **state_kwargs
        )
        return handler

    def test_upsert_exactly_once_names_conflict_keys(self):
        handler = self._handler(
            adbc_only=False, write_mode="upsert", conflict_keys=["id"]
        )
        verdict = handler.retry_semantics("s1")
        assert verdict.semantics == RetrySemantics.RETRY_SEMANTICS_EXACTLY_ONCE
        assert "id" in verdict.reason

    def test_truncate_insert_reports_not_replay_safe(self):
        """Truncate-insert truncates on the run's first batch (issue
        #307), but its append phase has no row-identity dedup, so a
        replayed already-committed later batch re-inserts its rows."""
        handler = self._handler(adbc_only=True, write_mode="truncate_insert")
        verdict = handler.retry_semantics("s1")
        assert verdict.semantics == RetrySemantics.RETRY_SEMANTICS_AT_LEAST_ONCE
        assert "first batch" in verdict.reason

    def test_keyed_insert_on_sqlalchemy_exactly_once(self):
        handler = self._handler(
            adbc_only=False, write_mode="insert", primary_keys=["id"]
        )
        verdict = handler.retry_semantics("s1")
        assert verdict.semantics == RetrySemantics.RETRY_SEMANTICS_EXACTLY_ONCE
        assert "['id']" in verdict.reason

    def test_keyless_insert_on_sqlalchemy_exactly_once_via_record_hash(self):
        handler = self._handler(adbc_only=False, write_mode="insert")
        verdict = handler.retry_semantics("s1")
        assert verdict.semantics == RetrySemantics.RETRY_SEMANTICS_EXACTLY_ONCE
        assert GenericSQLConnector.RECORD_HASH_COLUMN in verdict.reason

    def test_insert_on_adbc_at_least_once(self):
        """The ADBC path has no row-level dedup yet (issue #282 follow-up),
        keyed or keyless — the verdict must not overpromise."""
        handler = self._handler(
            adbc_only=True, write_mode="insert", primary_keys=["id"]
        )
        verdict = handler.retry_semantics("s1")
        assert verdict.semantics == RetrySemantics.RETRY_SEMANTICS_AT_LEAST_ONCE
        assert "ADBC" in verdict.reason

    def test_unconfigured_stream_falls_back_to_base_default(self):
        handler = GenericSQLConnector()
        verdict = handler.retry_semantics("ghost")
        assert verdict.semantics == RetrySemantics.RETRY_SEMANTICS_AT_LEAST_ONCE
        assert "declares no retry-safety" in verdict.reason


@pytest.mark.unit
class TestRetryVerdictValidation:
    def test_unspecified_semantics_is_rejected_at_construction(self):
        """UNSPECIFIED is the wire's absent value; a handler constructing
        it would silently degrade into the base default downstream."""
        from cdk.types import RetryVerdict

        with pytest.raises(ValueError, match="never claim UNSPECIFIED"):
            RetryVerdict(
                semantics=RetrySemantics.RETRY_SEMANTICS_UNSPECIFIED,
                reason="defective handler",
            )
