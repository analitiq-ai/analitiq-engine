"""ADBC-only dispatch helpers on DatabaseDestinationHandler.

The big-coverage end-to-end paths require a live ADBC connection;
these tests pin the small pure helpers that gate retry classification
and SQL dispatch, where regressions silently change retry semantics.
"""

from __future__ import annotations

import pytest

from src.destination.connectors.database import (
    AdbcCommitRecordError,
    AdbcConfigurationError,
    DatabaseDestinationHandler,
    _FATAL_ADBC_ERROR_NAMES,
    _is_fatal_adbc_error,
    _reclassify_as_fatal,
)


# Use bare PEP-249 class names so the classifier (which matches on
# ``type(exc).__name__``) sees the same string driver modules raise.
class ProgrammingError(Exception):
    """Stand-in for a PEP-249 ProgrammingError raised by a driver."""


class IntegrityError(Exception):
    pass


class DataError(Exception):
    pass


class NotSupportedError(Exception):
    pass


class OperationalError(Exception):
    """Retryable category — not in _FATAL_ADBC_ERROR_NAMES."""


class TestFatalClassifier:
    def test_known_names_match(self):
        assert _is_fatal_adbc_error(ProgrammingError())
        assert _is_fatal_adbc_error(IntegrityError())
        assert _is_fatal_adbc_error(DataError())
        assert _is_fatal_adbc_error(NotSupportedError())

    def test_unknown_name_does_not_match(self):
        assert not _is_fatal_adbc_error(OperationalError())
        assert not _is_fatal_adbc_error(RuntimeError("transient"))

    def test_set_kept_in_sync(self):
        assert _FATAL_ADBC_ERROR_NAMES == frozenset({
            "ProgrammingError",
            "NotSupportedError",
            "IntegrityError",
            "DataError",
        })


class TestReclassify:
    def test_preserves_class_name_in_message(self):
        inner = ProgrammingError("missing table foo")
        wrapped = _reclassify_as_fatal(inner)
        assert isinstance(wrapped, AdbcConfigurationError)
        assert "ProgrammingError" in str(wrapped)
        assert "missing table foo" in str(wrapped)
        assert wrapped.__cause__ is inner


class TestAdbcCommitRecordError:
    def test_insert_message_warns_duplication(self):
        err = AdbcCommitRecordError(RuntimeError("commit failed"), "insert")
        assert "duplicate" in str(err)
        assert err.write_mode == "insert"

    def test_truncate_insert_message_is_idempotent(self):
        err = AdbcCommitRecordError(
            RuntimeError("commit failed"), "truncate_insert",
        )
        assert "idempotent" in str(err)
        assert err.write_mode == "truncate_insert"

    def test_upsert_message_is_idempotent_under_keys(self):
        err = AdbcCommitRecordError(RuntimeError("commit failed"), "upsert")
        assert "MERGE" in str(err) or "idempotent" in str(err)
        assert err.write_mode == "upsert"

    def test_unknown_mode_carried(self):
        err = AdbcCommitRecordError(RuntimeError("x"), "weird_mode")
        assert err.write_mode == "weird_mode"
        assert "weird_mode" in str(err)


class TestPerDriverDispatch:
    """Spot-check the per-driver fragment dispatchers don't drift."""

    def _handler_for(self, driver: str) -> DatabaseDestinationHandler:
        h = DatabaseDestinationHandler()
        h._driver = driver
        return h

    def test_native_renderer_snowflake(self):
        from src.destination.sql_types import native_to_snowflake
        assert self._handler_for("snowflake")._adbc_native_renderer() is native_to_snowflake

    def test_native_renderer_bigquery(self):
        from src.destination.sql_types import native_to_bigquery
        assert self._handler_for("bigquery")._adbc_native_renderer() is native_to_bigquery

    def test_native_renderer_postgres(self):
        from src.destination.sql_types import native_to_postgres
        assert self._handler_for("postgresql")._adbc_native_renderer() is native_to_postgres

    def test_native_renderer_unknown_raises(self):
        with pytest.raises(AdbcConfigurationError, match="no DDL renderer"):
            self._handler_for("oracle")._adbc_native_renderer()

    def test_timestamp_default_per_driver(self):
        assert self._handler_for("snowflake")._adbc_timestamp_default_type() == "TIMESTAMP_TZ"
        assert self._handler_for("bigquery")._adbc_timestamp_default_type() == "TIMESTAMP"
        assert self._handler_for("postgresql")._adbc_timestamp_default_type() == "TIMESTAMP WITH TIME ZONE"

    def test_binary_type_per_driver(self):
        assert self._handler_for("snowflake")._adbc_binary_type() == "BINARY"
        assert self._handler_for("bigquery")._adbc_binary_type() == "BYTES"
        assert self._handler_for("postgresql")._adbc_binary_type() == "BYTEA"

    def test_temp_table_syntax_per_driver(self):
        target = '"public"."orders"'
        snow = self._handler_for("snowflake")._build_adbc_temp_table_sql(
            "stage", target,
        )
        assert "OR REPLACE TEMPORARY TABLE" in snow
        assert f"LIKE {target}" in snow

        bq = self._handler_for("bigquery")._build_adbc_temp_table_sql(
            "stage", target,
        )
        assert "TEMP TABLE" in bq
        assert f"AS SELECT * FROM {target} WHERE FALSE" in bq

        pg = self._handler_for("postgresql")._build_adbc_temp_table_sql(
            "stage", target,
        )
        assert "TEMP TABLE" in pg
        assert f"LIKE {target} INCLUDING DEFAULTS" in pg


class TestSchemaIsImplicitDefault:
    def _handler_for(self, driver: str) -> DatabaseDestinationHandler:
        h = DatabaseDestinationHandler()
        h._driver = driver
        return h

    def test_snowflake_public_is_default(self):
        h = self._handler_for("snowflake")
        assert h._schema_is_implicit_default("public")
        assert h._schema_is_implicit_default("PUBLIC")
        # Anything else (e.g. an analytics schema) must be created.
        assert not h._schema_is_implicit_default("analytics")

    def test_bigquery_never_implicit(self):
        # BigQuery requires every DML to name a dataset; we never
        # auto-create one for the user — they must declare it.
        h = self._handler_for("bigquery")
        assert not h._schema_is_implicit_default("public")
        assert not h._schema_is_implicit_default("analytics")

    def test_empty_treated_as_implicit(self):
        h = self._handler_for("snowflake")
        assert h._schema_is_implicit_default("")
        assert h._schema_is_implicit_default(None)


class TestAdbcDdlBuilders:
    """Pin the shape of the auto-generated DDL.

    The DDL strings are what the destination handler executes against
    the warehouse on schema configure. Behaviour we care about:

    * synthetic ``_synced_at`` audit column appears when the contract
      doesn't declare it (and uses the right per-driver timestamp type)
    * PRIMARY KEY clause is emitted when the contract declares primary
      keys
    * ``_batch_commits`` uses the right binary type per driver
    """

    def _make(self, driver: str) -> DatabaseDestinationHandler:
        from src.destination.connectors.database import _StreamState
        h = DatabaseDestinationHandler()
        h._driver = driver
        return h

    def test_synced_at_appended_when_missing(self):
        from src.destination.connectors.database import _StreamState

        class _TypeMapperStub:
            def to_arrow_type(self, native: str) -> str:
                return {"BIGINT": "Int64", "TEXT": "Utf8"}[native]

        state = _StreamState(
            schema_name="analytics",
            table_name="orders",
            endpoint_document={
                "columns": [
                    {"name": "id", "native_type": "BIGINT", "nullable": False},
                    {"name": "status", "native_type": "TEXT", "nullable": True},
                ],
            },
            primary_keys=["id"],
        )
        h = self._make("snowflake")
        ddl = h._build_adbc_create_table_ddl(state, _TypeMapperStub())
        assert "CREATE TABLE IF NOT EXISTS" in ddl
        assert '"analytics"."orders"' in ddl
        assert '"_synced_at" TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP' in ddl
        assert "PRIMARY KEY (\"id\")" in ddl
        assert '"id" INTEGER NOT NULL' in ddl
        assert '"status" VARCHAR' in ddl

    def test_synced_at_not_double_declared(self):
        from src.destination.connectors.database import _StreamState

        class _TypeMapperStub:
            def to_arrow_type(self, native: str) -> str:
                return {"BIGINT": "Int64", "TIMESTAMP": "Timestamp(MICROSECOND)"}[native]

        state = _StreamState(
            schema_name="analytics",
            table_name="orders",
            endpoint_document={
                "columns": [
                    {"name": "id", "native_type": "BIGINT", "nullable": False},
                    {"name": "_synced_at", "native_type": "TIMESTAMP", "nullable": True},
                ],
            },
            primary_keys=["id"],
        )
        h = self._make("snowflake")
        ddl = h._build_adbc_create_table_ddl(state, _TypeMapperStub())
        # Exactly one _synced_at declaration
        assert ddl.count('"_synced_at"') == 1

    def test_batch_commits_ddl_per_driver(self):
        snow = self._make("snowflake")._build_adbc_batch_commits_ddl("analytics")
        assert '"_batch_commits"' in snow
        assert "BINARY" in snow
        assert "TIMESTAMP_NTZ" in snow

        bq = self._make("bigquery")._build_adbc_batch_commits_ddl("analytics")
        assert "BYTES" in bq
        assert "DATETIME" in bq

        pg = self._make("postgresql")._build_adbc_batch_commits_ddl("analytics")
        assert "BYTEA" in pg
        assert "TIMESTAMP" in pg
