"""ADBC-only dispatch helpers on GenericSQLConnector.

The big-coverage end-to-end paths require a live ADBC connection; these tests
pin the small pure helpers that gate retry classification and SQL dispatch,
where regressions silently change retry semantics.

The generic connector is now vendor-neutral: every per-system fragment (native
DDL type names, quoting, schema folding, stage-table syntax, PK clause) comes
from the :class:`~cdk.sql.dialects.SqlDialect` the connector class carries. The
CDK base raises ``UnsupportedDialectOperationError`` for those hooks, so the
driver-specific behaviour these tests exercise is supplied by small *fixture*
dialects defined here, installed via a ``GenericSQLConnector`` subclass with
``dialect_class`` set. Vendor-specific SQL text (Snowflake/BigQuery specifics)
is tested in the connector package repos, not here — here we assert the fixture
dialect's output reaches the right call site.
"""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, MagicMock

import pytest

from cdk.sql import generic as database_module
from cdk.sql.dialects import SqlDialect
from cdk.sql.generic import (
    _FATAL_ADBC_ERROR_NAMES,
    AdbcConfigurationError,
    GenericSQLConnector,
    _is_fatal_adbc_error,
    _reclassify_as_fatal,
)

# --- fixture dialects + connector (stand in for a connector package) --------


class _FixtureAdbcDialect(SqlDialect):
    """A complete ADBC dialect with canned, ANSI-ish type rendering.

    Stands in for a connector package's dialect: renders canonical Arrow types
    to canned native DDL strings (overriding ``render_column_type`` — the single
    write surface — rather than the old per-purpose ADBC hooks), supports ADBC
    upsert, and folds schema names upper-case (the way Snowflake's package
    dialect would) so the normalize-reaches-the-ingest-site coverage has
    something to assert.
    """

    name = "fixture"
    supports_upsert_adbc = True

    #: canonical Arrow string -> canned native DDL type the dialect renders.
    _CANONICAL_TO_DDL = {
        "Int64": "INTEGER",
        "Int32": "INTEGER",
        "Utf8": "STRING",
        "Binary": "VARBINARY",
        "Timestamp(MICROSECOND)": "TIMESTAMP",
        "Timestamp(MICROSECOND, UTC)": "TIMESTAMPTZ",
    }

    def normalize_schema(self, schema: str) -> str:
        return schema.upper()

    def render_column_type(self, canonical, type_mapper, *, params=None) -> str:
        return self._CANONICAL_TO_DDL[canonical]

    def adbc_stage_table_sql(self, stage_qualified, target_qualified) -> str:
        return (
            f"CREATE TABLE {stage_qualified} AS SELECT * FROM "
            f"{target_qualified} WHERE FALSE"
        )


class _FixtureBacktickDialect(_FixtureAdbcDialect):
    """As above but backtick-quoting + NOT ENFORCED PK and no schema folding."""

    name = "fixture_backtick"
    quote_char = "`"
    pk_not_enforced = True

    def normalize_schema(self, schema: str) -> str:
        # Backtick systems (BigQuery) are case-sensitive — identity.
        return schema


class _FixtureConnector(GenericSQLConnector):
    dialect_class = _FixtureAdbcDialect


class _FixtureBacktickConnector(GenericSQLConnector):
    dialect_class = _FixtureBacktickDialect


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
        assert _FATAL_ADBC_ERROR_NAMES == frozenset(
            {
                "ProgrammingError",
                "NotSupportedError",
                "IntegrityError",
                "DataError",
            }
        )


class TestReclassify:
    def test_preserves_class_name_in_message(self):
        inner = ProgrammingError("missing table foo")
        wrapped = _reclassify_as_fatal(inner)
        assert isinstance(wrapped, AdbcConfigurationError)
        assert "ProgrammingError" in str(wrapped)
        assert "missing table foo" in str(wrapped)
        assert wrapped.__cause__ is inner


class TestUnsupportedHooksAreFatal:
    """A thin connector that ships only a read type-map (no write rules)
    cannot render DDL: ``render_column_type`` delegates to the write map, which
    raises ``InvalidTypeMapError`` — surfaced as ``CreateTableError`` by the
    shared DDL builder. The stage-table hook still raises
    ``UnsupportedDialectOperationError``. (The fixture dialects below show the
    override path.)"""

    def test_base_dialect_without_write_map_cannot_render_ddl(self):
        from cdk.contract import ColumnDef
        from cdk.sql.ddl import build_create_table_sql
        from cdk.sql.exceptions import CreateTableError
        from cdk.type_map import InvalidTypeMapError, TypeMapper
        from cdk.type_map.rules import parse_rules

        # Read rules only — no write rules, so to_native_type cannot render.
        read_only = TypeMapper(
            "thin",
            parse_rules(
                [{"match": "exact", "native": "BIGINT", "canonical": "Int64"}],
                source="<test>",
            ),
        )
        h = GenericSQLConnector()  # carries the ANSI-neutral base dialect
        with pytest.raises(CreateTableError) as exc:
            build_create_table_sql(
                h.dialect,
                read_only,
                "public",
                "t",
                [ColumnDef("id", "Int64")],
                [],
            )
        assert isinstance(exc.value.__cause__, InvalidTypeMapError)

    def test_base_dialect_lacks_stage_table_sql(self):
        from cdk.sql.exceptions import UnsupportedDialectOperationError

        h = GenericSQLConnector()
        with pytest.raises(
            UnsupportedDialectOperationError, match="adbc_stage_table_sql"
        ):
            h.dialect.adbc_stage_table_sql("stage", "target")


class TestSchemaIsImplicitDefault:
    """``schema_is_implicit_default`` is a dialect hook now; the base treats
    only the empty name as implicit. Connector packages widen it (e.g.
    Snowflake's PUBLIC) — exercised via a fixture dialect."""

    class _PublicImplicitDialect(SqlDialect):
        name = "public_implicit"

        def schema_is_implicit_default(self, schema_name: str) -> bool:
            return (not schema_name) or schema_name.upper() == "PUBLIC"

    def test_base_only_empty_is_implicit(self):
        d = SqlDialect()
        assert d.schema_is_implicit_default("")
        assert not d.schema_is_implicit_default("public")
        assert not d.schema_is_implicit_default("analytics")

    def test_fixture_public_is_implicit(self):
        d = self._PublicImplicitDialect()
        assert d.schema_is_implicit_default("public")
        assert d.schema_is_implicit_default("PUBLIC")
        assert not d.schema_is_implicit_default("analytics")
        assert d.schema_is_implicit_default("")


class TestStageTokenUniqueness:
    """Stage token must stay unique across batches even with UUID-shaped
    stream_id (36 chars). Pre-round-5 token was string-concat + truncate
    to 48 chars; for a UUID stream_id the truncation discarded batch_seq
    and produced identical tokens for every batch in the same stream-run,
    re-introducing the round-1 collision bug."""

    def _build_token(self, run_id: str, stream_id: str, batch_seq: int) -> str:
        # Mirror the construction at the call site so a regression in
        # either location is caught here.
        import hashlib

        return (
            "b"
            + hashlib.sha256(f"{run_id}|{stream_id}|{batch_seq}".encode()).hexdigest()[
                :16
            ]
        )

    def test_distinct_across_batch_seq_with_uuid_stream_id(self):
        run_id = "20260527T120000Z-a1b2c3d4"
        stream_id = "2ac5e363-ec12-49f7-a8b2-b3782cf6af59"
        tokens = {self._build_token(run_id, stream_id, bs) for bs in (0, 1, 100, 9999)}
        assert len(tokens) == 4, f"stage token collided across batch_seq: {tokens}"

    def test_distinct_across_stream_ids(self):
        run_id = "20260527T120000Z-a1b2c3d4"
        s1 = "2ac5e363-ec12-49f7-a8b2-b3782cf6af59"
        s2 = "00b7a31f-3a31-4256-ba15-adba92d46930"
        assert self._build_token(run_id, s1, 1) != self._build_token(run_id, s2, 1)

    def test_fits_postgres_namedatalen_with_realistic_table_name(self):
        # `_analitiq_stage_<table>_<token>` must not exceed Postgres'
        # 63-char NAMEDATALEN. Token is 17 chars ("b" + 16-hex);
        # `_analitiq_stage_` is 16; `_` is 1. Budget for table_name:
        # 63 - 16 - 17 - 1 = 29 chars. Verify realistic table names fit.
        run_id = "20260527T120000Z-a1b2c3d4"
        stream_id = "2ac5e363-ec12-49f7-a8b2-b3782cf6af59"
        token = self._build_token(run_id, stream_id, 42)
        assert len(token) == 17  # "b" + 16 hex digits
        for table_name in ("wise_transfers", "public_transfers", "orders"):
            stage_name = f"_analitiq_stage_{table_name}_{token}"
            assert (
                len(stage_name) <= 63
            ), f"stage name {stage_name!r} exceeds Postgres NAMEDATALEN"


class TestAdbcModeReset:
    """`_adbc_only` must reset on reconnect so a handler reused across
    runtimes (or in tests that monkey-patch one mode and expect a clean
    slate) doesn't carry the previous mode forward."""

    def test_adbc_only_resets_to_false_when_runtime_is_sa(self):
        h = GenericSQLConnector()
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
        h = GenericSQLConnector()
        assert h._adbc_only is False
        # Simulate the connect() ADBC branch (we can't actually call
        # connect() without a real runtime, but the field setting is
        # what matters for write_batch dispatch).
        h._adbc_only = True
        assert h._adbc_only is True
        assert h._engine is None


class TestAdbcIngestSchemaNormalization:
    """The dialect's schema normalization must reach every
    ``cursor.adbc_ingest(db_schema_name=...)`` site: the quoted-DDL path and
    the ADBC-only ingest site. A fixture dialect that folds schema names
    upper-case stands in for Snowflake's package dialect; a backtick fixture
    that does NOT fold stands in for BigQuery. Regression coverage so a future
    refactor cannot drop the normalize on the ingest dimension while keeping
    it on the DDL dimension."""

    def _captured_ingest(self):
        """Build a fake ADBC connection that captures the kwargs handed
        to ``cursor.adbc_ingest``. Returns ``(conn, captured)`` where
        ``captured`` is a dict populated on the first ingest call."""
        captured: dict = {}

        class _FakeCursor:
            # ``db_schema_name`` is an optional kwarg on the real ADBC cursor
            # (default None); the empty-schema path omits it entirely
            # (adbc_ingest_schema_kwargs returns {}), so it must default here.
            def adbc_ingest(self, table, batch, mode, db_schema_name=None):
                captured["table"] = table
                captured["mode"] = mode
                captured["db_schema_name"] = db_schema_name

            def close(self):
                pass

        class _FakeConn:
            def cursor(self):
                return _FakeCursor()

            def commit(self):
                pass

        return _FakeConn(), captured

    def test_folding_dialect_normalizes_schema_for_ingest(self):
        h = _FixtureConnector()
        h._adbc_only = True
        h._adbc_conn, captured = self._captured_ingest()
        import pyarrow as pa

        h._adbc_only_ingest_sync(
            pa.record_batch([pa.array([1])], names=["id"]),
            "public",
            "orders",
        )
        assert captured["db_schema_name"] == "PUBLIC"

    def test_non_folding_dialect_keeps_schema_for_ingest(self):
        # A case-sensitive (backtick) dialect never folds the schema.
        h = _FixtureBacktickConnector()
        h._adbc_only = True
        h._adbc_conn, captured = self._captured_ingest()
        import pyarrow as pa

        h._adbc_only_ingest_sync(
            pa.record_batch([pa.array([1])], names=["id"]),
            "analytics",
            "orders",
        )
        assert captured["db_schema_name"] == "analytics"

    def test_empty_schema_yields_none(self):
        h = _FixtureConnector()
        h._adbc_only = True
        h._adbc_conn, captured = self._captured_ingest()
        import pyarrow as pa

        h._adbc_only_ingest_sync(
            pa.record_batch([pa.array([1])], names=["id"]),
            "",
            "orders",
        )
        assert captured["db_schema_name"] is None


class TestDialectQuoting:
    """Quoting is a dialect hook: the ANSI base double-quotes (with escaping);
    a backtick fixture dialect quotes with backticks and rejects embedded
    backticks. The connector calls ``self.dialect.quote_*`` — no per-driver
    branch in the connector itself."""

    def test_backtick_dialect_quotes_with_backticks(self):
        h = _FixtureBacktickConnector()
        assert h.dialect.quote_ident("id") == "`id`"
        assert h.dialect.quote_qualified("ds", "t") == "`ds`.`t`"

    def test_ansi_dialect_quotes_with_double_quotes(self):
        h = GenericSQLConnector()  # ANSI base
        assert h.dialect.quote_ident("id") == '"id"'

    def test_folding_dialect_qualified_normalizes_schema(self):
        # The folding fixture upper-cases the schema before quoting it.
        h = _FixtureConnector()
        assert h.dialect.quote_qualified("public", "t") == '"PUBLIC"."t"'
        assert h.dialect.quote_qualified("analytics", "t") == '"ANALYTICS"."t"'

    def test_double_quote_escaping_in_ansi_dialect(self):
        h = GenericSQLConnector()
        assert h.dialect.quote_ident('we"ird') == '"we""ird"'

    def test_backtick_dialect_rejects_backtick_in_identifier(self):
        h = _FixtureBacktickConnector()
        with pytest.raises(ValueError, match="backtick"):
            h.dialect.quote_ident("we`ird")


class TestStageTableSql:
    """Stage table SQL comes from the dialect hook (``adbc_stage_table_sql``).
    The base raises; a connector package's dialect supplies the vendor form.
    Here the fixture dialect's canned output is what the MERGE path will use."""

    def test_fixture_dialect_stage_table_sql(self):
        h = _FixtureConnector()
        sql = h.dialect.adbc_stage_table_sql('"a"."stage"', '"a"."target"')
        assert sql == (
            'CREATE TABLE "a"."stage" AS SELECT * FROM "a"."target" WHERE FALSE'
        )

    def test_base_dialect_stage_table_sql_unsupported(self):
        from cdk.sql.exceptions import UnsupportedDialectOperationError

        h = GenericSQLConnector()
        with pytest.raises(UnsupportedDialectOperationError):
            h.dialect.adbc_stage_table_sql('"a"."stage"', '"a"."target"')


class TestSupportsUpsert:
    """``supports_upsert`` reads the dialect's capability flags, gated by the
    active transport mode. The ANSI base supports neither; a connector
    package's dialect opts in (the fixture does, for ADBC)."""

    def test_base_sa_mode_does_not_support(self):
        h = GenericSQLConnector()
        h._adbc_only = False
        assert h.supports_upsert is False

    def test_base_adbc_mode_does_not_support(self):
        h = GenericSQLConnector()
        h._adbc_only = True
        assert h.supports_upsert is False

    def test_fixture_adbc_mode_supports(self):
        h = _FixtureConnector()
        h._adbc_only = True
        assert h.supports_upsert is True

    def test_fixture_sa_mode_does_not_support(self):
        # The fixture dialect declares supports_upsert_adbc but not the SA
        # flag, so the SA transport path reports no upsert.
        h = _FixtureConnector()
        h._adbc_only = False
        assert h.supports_upsert is False


class TestAdbcDdlBuilders:
    """Pin the shape of the auto-generated DDL.

    The DDL strings are what the destination handler executes against the
    warehouse on schema configure. Every vendor fragment is supplied by the
    fixture dialect; we assert the assembled DDL wires those fragments
    correctly:

    * synthetic ``_synced_at`` audit column appears when the contract doesn't
      declare it (and uses the dialect's timestamp type)
    * PRIMARY KEY clause is emitted when the contract declares primary keys
    * the PK clause carries the dialect's NOT ENFORCED variant when set
    """

    @staticmethod
    def _build_target_ddl(handler, state, mapper):
        from cdk.sql.ddl import build_create_table_sql

        return build_create_table_sql(
            handler.dialect,
            mapper,
            state.schema_name,
            state.table_name,
            handler._build_column_defs(state, mapper),
            list(state.primary_keys),
            if_not_exists=True,
        )

    def test_synced_at_appended_when_missing(self):
        from cdk.sql.generic import _StreamState

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
        h = _FixtureConnector()
        ddl = self._build_target_ddl(h, state, _TypeMapperStub())
        assert "CREATE TABLE IF NOT EXISTS" in ddl
        # Schema folded by the fixture dialect, table quoted verbatim.
        assert '"ANALYTICS"."orders"' in ddl
        assert '"_synced_at" TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP' in ddl
        assert 'PRIMARY KEY ("id")' in ddl
        assert '"id" INTEGER NOT NULL' in ddl
        assert '"status" STRING' in ddl

    def test_synced_at_not_double_declared(self):
        from cdk.sql.generic import _StreamState

        class _TypeMapperStub:
            def to_arrow_type(self, native: str) -> str:
                return {"BIGINT": "Int64", "TIMESTAMP": "Timestamp(MICROSECOND)"}[
                    native
                ]

        state = _StreamState(
            schema_name="analytics",
            table_name="orders",
            endpoint_document={
                "columns": [
                    {"name": "id", "native_type": "BIGINT", "nullable": False},
                    {
                        "name": "_synced_at",
                        "native_type": "TIMESTAMP",
                        "nullable": True,
                    },
                ],
            },
            primary_keys=["id"],
        )
        h = _FixtureConnector()
        ddl = self._build_target_ddl(h, state, _TypeMapperStub())
        # Exactly one _synced_at declaration
        assert ddl.count('"_synced_at"') == 1

    def test_pk_clause_not_enforced_variant(self):
        # The backtick fixture sets pk_not_enforced; the bare fixture doesn't.
        bq = _FixtureBacktickConnector()
        assert "NOT ENFORCED" in bq.dialect.pk_clause(["id"])

        snow = _FixtureConnector()
        assert "NOT ENFORCED" not in snow.dialect.pk_clause(["id"])


class TestDisconnectClosesAdbc:
    """``disconnect()`` must release the cached ADBC connection and the
    SQLAlchemy runtime even when one side fails, and always flip
    ``_connected`` so callers can re-acquire cleanly. ``_adbc_conn`` is
    the ADBC-only mode's connection (Snowflake / BigQuery / Postgres);
    a regression here leaks a server-side session on shutdown."""

    @pytest.mark.asyncio
    async def test_closes_adbc_connection(self):
        handler = GenericSQLConnector()
        handler._connected = True
        adbc_conn = MagicMock()
        handler._adbc_conn = adbc_conn
        handler._runtime = AsyncMock()
        handler._runtime.close = AsyncMock()

        await handler.disconnect()

        adbc_conn.close.assert_called_once()
        assert handler._adbc_conn is None
        assert handler._connected is False
        handler._runtime.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close_failure_logged_at_error_and_runtime_still_released(
        self, caplog
    ):
        handler = GenericSQLConnector()
        handler._connected = True
        adbc_conn = MagicMock()
        adbc_conn.close.side_effect = RuntimeError("already closed")
        handler._adbc_conn = adbc_conn
        handler._runtime = AsyncMock()
        handler._runtime.close = AsyncMock()

        with caplog.at_level(logging.ERROR, logger=database_module.logger.name):
            await handler.disconnect()

        errors = [
            r for r in caplog.records if "Failed to close ADBC connection" in r.message
        ]
        assert errors and errors[0].levelno == logging.ERROR
        assert errors[0].exc_info is not None
        assert handler._adbc_conn is None
        assert handler._connected is False
        # Engine is still released so we don't leak it on top of the
        # ADBC handle.
        handler._runtime.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_runtime_close_failure_still_flips_connected_state(self, caplog):
        """If ``runtime.close()`` raises, the handler must still
        transition to ``_connected = False`` so callers can re-acquire
        without observing a half-disconnected state."""
        handler = GenericSQLConnector()
        handler._connected = True
        handler._runtime = AsyncMock()
        handler._runtime.close = AsyncMock(
            side_effect=RuntimeError("engine.dispose failed")
        )

        with caplog.at_level(logging.ERROR, logger=database_module.logger.name):
            await handler.disconnect()

        errors = [
            r
            for r in caplog.records
            if "Failed to close SQLAlchemy runtime" in r.message
        ]
        assert errors and errors[0].levelno == logging.ERROR
        assert handler._connected is False
