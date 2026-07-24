"""Per-stream retry-safety verdicts (issue #286).

Every destination handler reports, per configured stream, whether a
same-``RUN_ID`` restart can duplicate writes. The SQL verdict is write
mode x key aware and transport-independent (stage-then-merge, ADR
sql-write-path-v2 §9); file writes content-addressed files; stdout has
nothing to dedup with. The base default is the only honest claim for a
handler that declares nothing: at-least-once.
"""

from datetime import datetime
from typing import Any

import pytest

from cdk.base_handler import BaseDestinationHandler, BatchWriteResult
from cdk.sql.dialects import TableAddress
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
        emitted_at: datetime,
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
        """Content-addressed filenames make a true replay overwrite the
        same bytes, but a same-run restart re-reads the inclusive cursor
        boundary and writes those rows into a new file (issue #306) —
        the verdict must not claim exactly-once."""
        verdict = FileDestinationHandler().retry_semantics("s1")
        assert verdict.semantics == RetrySemantics.RETRY_SEMANTICS_AT_LEAST_ONCE
        assert "content-addressed" in verdict.reason

    def test_stdout_reports_at_least_once(self):
        verdict = StreamDestinationHandler().retry_semantics("s1")
        assert verdict.semantics == RetrySemantics.RETRY_SEMANTICS_AT_LEAST_ONCE
        assert "prints" in verdict.reason


@pytest.mark.unit
class TestSqlVerdicts:
    """The SQL verdict is transport-independent under stage-then-merge
    (ADR sql-write-path-v2 §9): upsert dedups on its conflict keys;
    insert anti-joins on row identity from the stage on both transports;
    truncate-insert truncates on the run's first batch (issue #307) but
    appends with no row-identity dedup, so it cannot claim replay
    safety. Every mode verdict is asserted for both transports."""

    def _handler(self, *, adbc_only: bool, **state_kwargs) -> GenericSQLConnector:
        handler = GenericSQLConnector()
        handler._adbc_only = adbc_only
        handler._streams["s1"] = SqlStreamState(
            address=TableAddress(table="t"),
            endpoint_document={"x": 1},
            **state_kwargs,
        )
        return handler

    @pytest.mark.parametrize("adbc_only", [False, True], ids=["sqlalchemy", "adbc"])
    def test_upsert_exactly_once_names_conflict_keys(self, adbc_only):
        handler = self._handler(
            adbc_only=adbc_only, write_mode="upsert", conflict_keys=["id"]
        )
        verdict = handler.retry_semantics("s1")
        assert verdict.semantics == RetrySemantics.RETRY_SEMANTICS_EXACTLY_ONCE
        assert "id" in verdict.reason

    @pytest.mark.parametrize("adbc_only", [False, True], ids=["sqlalchemy", "adbc"])
    def test_truncate_insert_reports_not_replay_safe(self, adbc_only):
        """Truncate-insert truncates on the run's first batch (issue
        #307), but its append phase has no row-identity dedup, so a
        replayed already-committed later batch re-inserts its rows."""
        handler = self._handler(adbc_only=adbc_only, write_mode="truncate_insert")
        verdict = handler.retry_semantics("s1")
        assert verdict.semantics == RetrySemantics.RETRY_SEMANTICS_AT_LEAST_ONCE
        assert "first batch" in verdict.reason

    @pytest.mark.parametrize("adbc_only", [False, True], ids=["sqlalchemy", "adbc"])
    def test_keyed_insert_exactly_once(self, adbc_only):
        handler = self._handler(
            adbc_only=adbc_only, write_mode="insert", primary_keys=["id"]
        )
        verdict = handler.retry_semantics("s1")
        assert verdict.semantics == RetrySemantics.RETRY_SEMANTICS_EXACTLY_ONCE
        assert "['id']" in verdict.reason

    @pytest.mark.parametrize("adbc_only", [False, True], ids=["sqlalchemy", "adbc"])
    def test_keyless_insert_exactly_once_via_record_hash(self, adbc_only):
        handler = self._handler(adbc_only=adbc_only, write_mode="insert")
        verdict = handler.retry_semantics("s1")
        assert verdict.semantics == RetrySemantics.RETRY_SEMANTICS_EXACTLY_ONCE
        assert GenericSQLConnector.RECORD_HASH_COLUMN in verdict.reason

    def test_insert_on_unenforced_pk_system_at_least_once(self):
        """The honest-verdict rule (ADR §9): the anti-join dedups every
        sequential replay, but a system that does not enforce the
        identity constraint (``pk_not_enforced`` — BigQuery) has a
        filter, not a guarantee, so its insert streams must not promise
        exactly-once."""
        from cdk.sql.dialects import SqlDialect

        class _UnenforcedPkDialect(SqlDialect):
            pk_not_enforced = True

        handler = self._handler(
            adbc_only=True, write_mode="insert", primary_keys=["id"]
        )
        handler.dialect = _UnenforcedPkDialect()
        verdict = handler.retry_semantics("s1")
        assert verdict.semantics == RetrySemantics.RETRY_SEMANTICS_AT_LEAST_ONCE
        assert "does not enforce" in verdict.reason

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
