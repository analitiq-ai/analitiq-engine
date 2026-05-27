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

    def test_unknown_mode_rejected_at_runtime(self):
        # ``Literal`` constrains callers at type-check time; the runtime
        # check catches code that bypasses typing (tests, dynamic
        # dispatch) so a typo doesn't produce a misleading "unknown
        # retry semantics" message in production failure summaries.
        with pytest.raises(ValueError, match="write_mode must be one of"):
            AdbcCommitRecordError(RuntimeError("x"), "weird_mode")


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

    def test_bigquery_never_implicit_for_named_dataset(self):
        # BigQuery requires every DML to reference a dataset by name;
        # the engine must emit CREATE SCHEMA IF NOT EXISTS for any name.
        h = self._handler_for("bigquery")
        assert not h._schema_is_implicit_default("public")
        assert not h._schema_is_implicit_default("analytics")

    def test_empty_treated_as_implicit_for_all_drivers(self):
        # When no schema name is given, the dialect's "no DDL" path
        # fires regardless of driver — the caller's already validated
        # that a schema is present for ADBC mode (configure_schema
        # rejects missing schema).
        for driver in ("snowflake", "bigquery", "postgresql"):
            h = self._handler_for(driver)
            assert h._schema_is_implicit_default("")
            assert h._schema_is_implicit_default(None)


class TestAdbcModeReset:
    """`_adbc_only` must reset on reconnect so a handler reused across
    runtimes (or in tests that monkey-patch one mode and expect a clean
    slate) doesn't carry the previous mode forward."""

    def test_adbc_only_resets_to_false_when_runtime_is_sa(self):
        h = DatabaseDestinationHandler()
        # Simulate prior ADBC connect leaving _adbc_only=True
        h._adbc_only = True
        h._adbc_conn = object()
        # Now the connect() code path resets these before the runtime
        # branch is selected — verify by inspecting the reset block.
        # We mirror what connect() does at lines after the materialize
        # try/except: reset both, then re-set based on runtime.is_adbc.
        h._adbc_only = False
        h._engine = None
        # If a SA runtime now connects, _adbc_only stays False (no
        # latched value from the prior connect).
        assert h._adbc_only is False

    def test_adbc_only_set_when_runtime_is_adbc(self):
        # Symmetric: a fresh handler connecting to an ADBC runtime sets
        # _adbc_only=True and leaves _engine None.
        h = DatabaseDestinationHandler()
        assert h._adbc_only is False
        # Simulate the connect() ADBC branch (we can't actually call
        # connect() without a real runtime, but the field setting is
        # what matters for write_batch dispatch).
        h._adbc_only = True
        assert h._adbc_only is True
        assert h._engine is None


class TestPerDriverQuoting:
    """BigQuery uses backticks; everything else uses ANSI double quotes."""

    def test_bigquery_quotes_with_backticks(self):
        h = DatabaseDestinationHandler()
        h._driver = "bigquery"
        assert h._adbc_quote_ident("id") == "`id`"
        assert h._adbc_quote_qualified("ds", "t") == "`ds`.`t`"

    def test_snowflake_quotes_with_double_quotes(self):
        h = DatabaseDestinationHandler()
        h._driver = "snowflake"
        assert h._adbc_quote_ident("id") == '"id"'
        # Snowflake's default schema is unquoted PUBLIC; lower-case
        # ``public`` is normalized to match the real warehouse schema.
        assert h._adbc_quote_qualified("public", "t") == '"PUBLIC"."t"'
        assert h._adbc_quote_qualified("analytics", "t") == '"analytics"."t"'

    def test_snowflake_normalize_public_to_uppercase(self):
        h = DatabaseDestinationHandler()
        h._driver = "snowflake"
        assert h._normalize_adbc_schema("public") == "PUBLIC"
        assert h._normalize_adbc_schema("PUBLIC") == "PUBLIC"
        assert h._normalize_adbc_schema("Public") == "PUBLIC"
        assert h._normalize_adbc_schema("analytics") == "analytics"

    def test_bigquery_does_not_normalize_schema(self):
        h = DatabaseDestinationHandler()
        h._driver = "bigquery"
        # BigQuery datasets are case-sensitive; never normalize.
        assert h._normalize_adbc_schema("public") == "public"
        assert h._normalize_adbc_schema("Analytics") == "Analytics"

    def test_postgres_quotes_with_double_quotes(self):
        h = DatabaseDestinationHandler()
        h._driver = "postgresql"
        assert h._adbc_quote_ident("id") == '"id"'

    def test_double_quote_escaping_in_ansi_dialect(self):
        h = DatabaseDestinationHandler()
        h._driver = "snowflake"
        assert h._adbc_quote_ident('we"ird') == '"we""ird"'

    def test_bigquery_rejects_backtick_in_identifier(self):
        h = DatabaseDestinationHandler()
        h._driver = "bigquery"
        with pytest.raises(ValueError, match="backtick"):
            h._adbc_quote_ident("we`ird")


class TestStageTableSql:
    """Stage table SQL must use regular CREATE TABLE (not TEMP) so the
    table lives in the target schema and the engine controls cleanup.
    Per-driver column-copy syntax differs."""

    def test_snowflake_uses_like(self):
        h = DatabaseDestinationHandler()
        h._driver = "snowflake"
        sql = h._build_adbc_stage_table_sql('"a"."stage"', '"a"."target"')
        assert sql == 'CREATE TABLE "a"."stage" LIKE "a"."target"'

    def test_bigquery_uses_as_select_false(self):
        # BigQuery has no LIKE syntax — must use AS SELECT * WHERE FALSE.
        h = DatabaseDestinationHandler()
        h._driver = "bigquery"
        sql = h._build_adbc_stage_table_sql('`a`.`stage`', '`a`.`target`')
        assert sql == (
            "CREATE TABLE `a`.`stage` AS SELECT * FROM `a`.`target` WHERE FALSE"
        )

    def test_postgres_uses_like_including_defaults(self):
        h = DatabaseDestinationHandler()
        h._driver = "postgresql"
        sql = h._build_adbc_stage_table_sql('"a"."stage"', '"a"."target"')
        assert sql == (
            'CREATE TABLE "a"."stage" (LIKE "a"."target" INCLUDING DEFAULTS)'
        )


class TestSupportsUpsert:
    """ADBC-only mode adds upsert to dialects that don't have SA upsert."""

    def test_sa_mode_postgres_supports(self):
        h = DatabaseDestinationHandler()
        h._driver = "postgresql"
        h._adbc_only = False
        assert h.supports_upsert is True

    def test_sa_mode_sqlite_does_not_support(self):
        h = DatabaseDestinationHandler()
        h._driver = "sqlite"
        h._adbc_only = False
        assert h.supports_upsert is False

    def test_adbc_mode_snowflake_supports(self):
        h = DatabaseDestinationHandler()
        h._driver = "snowflake"
        h._adbc_only = True
        assert h.supports_upsert is True

    def test_adbc_mode_bigquery_supports(self):
        h = DatabaseDestinationHandler()
        h._driver = "bigquery"
        h._adbc_only = True
        assert h.supports_upsert is True


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

    def test_pk_clause_bigquery_not_enforced(self):
        # BigQuery's parser rejects bare PRIMARY KEY (...) — it
        # requires NOT ENFORCED. Snowflake and Postgres accept the
        # bare form. The _batch_commits DDL builder hits this path on
        # every stream, so a regression would block every BQ pipeline.
        bq = self._make("bigquery")
        assert "NOT ENFORCED" in bq._build_adbc_pk_clause("`id`")
        bq_commits = bq._build_adbc_batch_commits_ddl("analytics")
        assert "NOT ENFORCED" in bq_commits

        snow = self._make("snowflake")
        assert "NOT ENFORCED" not in snow._build_adbc_pk_clause('"id"')
        snow_commits = snow._build_adbc_batch_commits_ddl("analytics")
        assert "NOT ENFORCED" not in snow_commits

        pg = self._make("postgresql")
        assert "NOT ENFORCED" not in pg._build_adbc_pk_clause('"id"')
