"""Deliberate-failure test: prove the harness can go red.

Every other test asserts a happy path. If a framework bug ever swallowed a
non-zero engine exit, all of those would stay green while verifying nothing.
This test wires a pair that *must* fail inside the engine and asserts the
orchestrator surfaces it as ``PipelineRunFailed``.

The break is a source endpoint that declares a column the seeded table does
not have. The source connector builds its SELECT from the endpoint's declared
columns (``src/source/connectors/database.py::_select_columns``), so the query
references a non-existent column and the source read errors out — a real engine
failure, not a harness assertion. The destination is a normal Postgres; the run
dies before any row reaches it.
"""
from __future__ import annotations

from typing import List

import pytest

from tests.e2e_databases.databases._base import ColumnSpec
from tests.e2e_databases.databases.postgres import PostgresSpec
from tests.e2e_databases.orchestrator import E2ETestRun, PipelineRunFailed

_PHANTOM_COLUMN = "column_the_table_does_not_have"


class _PhantomColumnSource(PostgresSpec):
    """A Postgres source whose endpoint advertises a non-existent column.

    Everything else is real Postgres: same containers, same connector, same
    seeded table. Only ``columns()`` lies, appending a column the seed DDL
    never creates, so the engine's source SELECT fails.
    """

    slug = "postgres-phantom-source"

    def columns(self) -> List[ColumnSpec]:
        return super().columns() + [
            ColumnSpec(_PHANTOM_COLUMN, "INTEGER", "Int32", nullable=True)
        ]


@pytest.mark.local
def test_engine_failure_surfaces_as_pipeline_run_failed() -> None:
    source = _PhantomColumnSource()
    dest = PostgresSpec()
    for spec in (source, dest):
        reason = spec.available
        if reason is not None:
            pytest.skip(reason)

    run = E2ETestRun(source, dest, "full_refresh", write_mode="insert")
    try:
        run.setup()
        with pytest.raises(PipelineRunFailed) as exc_info:
            run.sync()
        # PipelineRunFailed fires on any non-zero engine exit, so assert it
        # failed for the intended reason: the phantom column the source SELECT
        # referenced. Otherwise an unrelated infra hiccup would keep this test
        # green while proving nothing about error propagation.
        assert _PHANTOM_COLUMN in str(exc_info.value)
    finally:
        run.teardown(keep_databases=True)
