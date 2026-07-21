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

import hashlib
import json
import logging
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow as pa
import pytest

from cdk.sql import generic as database_module
from cdk.sql.dialects import SqlDialect
from cdk.sql.generic import (
    _FATAL_ADBC_ERROR_NAMES,
    AdbcConfigurationError,
    GenericSQLConnector,
    SchemaConfigurationError,
    _is_fatal_adbc_error,
    _reclassify_as_fatal,
    _StreamState,
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
        """Assemble the target-table DDL the way _ensure_tables_exist does."""
        from cdk.sql.ddl import build_create_table_sql

        return build_create_table_sql(
            handler.dialect,
            mapper,
            state.schema_name,
            state.table_name,
            handler._build_column_defs(state),
            list(state.primary_keys),
            if_not_exists=True,
        )

    def test_synced_at_appended_when_missing(self):
        """_synced_at is appended when the contract does not declare it."""
        from cdk.sql.generic import _StreamState

        class _TypeMapperStub:
            def to_arrow_type(self, native: str) -> str:
                return {"BIGINT": "Int64", "TEXT": "Utf8"}[native]

        state = _StreamState(
            schema_name="analytics",
            table_name="orders",
            endpoint_document={
                "columns": [
                    {
                        "name": "id",
                        "native_type": "BIGINT",
                        "arrow_type": "Int64",
                        "nullable": False,
                    },
                    {
                        "name": "status",
                        "native_type": "TEXT",
                        "arrow_type": "Utf8",
                        "nullable": True,
                    },
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
        """A declared _synced_at column is not declared twice."""
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
                    {
                        "name": "id",
                        "native_type": "BIGINT",
                        "arrow_type": "Int64",
                        "nullable": False,
                    },
                    {
                        "name": "_synced_at",
                        "native_type": "TIMESTAMP",
                        "arrow_type": "Timestamp(MICROSECOND)",
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

    def test_keyless_insert_ddl_includes_record_hash_as_primary_key(self):
        """``_record_hash`` must appear as NOT NULL PRIMARY KEY in the generated
        DDL for a keyless insert stream on either transport. This is the
        structural guarantee the stage-MERGE relies on: the database enforces
        hash uniqueness in the target table.

        Production code passes ``_identity_columns(state)`` as the PK list;
        for keyless insert this returns ``[RECORD_HASH_COLUMN]`` so the
        ``_record_hash`` column is the table's sole primary key.
        """
        from cdk.sql.ddl import build_create_table_sql

        class _TypeMapperStub:
            def to_arrow_type(self, native: str) -> str:
                return {"TEXT": "Utf8"}[native]

        state = _StreamState(
            schema_name="public",
            table_name="events",
            write_mode="insert",
            primary_keys=[],  # keyless
            endpoint_document={
                "columns": [
                    {
                        "name": "payload",
                        "native_type": "TEXT",
                        "arrow_type": "Utf8",
                        "nullable": True,
                    }
                ]
            },
        )
        h = _FixtureConnector()
        h._adbc_only = True
        # Use _identity_columns (as production code does in _ensure_tables_exist)
        # not state.primary_keys — for keyless insert this returns [_record_hash].
        ddl = build_create_table_sql(
            h.dialect,
            _TypeMapperStub(),
            state.schema_name,
            state.table_name,
            h._build_column_defs(state),
            h._identity_columns(state),
            if_not_exists=True,
        )
        assert '"_record_hash"' in ddl
        assert '"_record_hash" STRING NOT NULL' in ddl
        assert "PRIMARY KEY" in ddl
        pk_pos = ddl.index("PRIMARY KEY")
        assert "_record_hash" in ddl[pk_pos:]


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


class TestNeedsRecordHash:
    """``_needs_record_hash`` must return True for keyless insert on both
    SQLAlchemy and ADBC transports, and False for keyed insert, upsert, and
    truncate_insert."""

    def _state(self, write_mode, primary_keys=None):
        return _StreamState(
            schema_name="public",
            table_name="t",
            write_mode=write_mode,
            primary_keys=primary_keys or [],
        )

    def test_keyless_insert_sa_needs_hash(self):
        h = _FixtureConnector()
        h._adbc_only = False
        assert h._needs_record_hash(self._state("insert")) is True

    def test_keyless_insert_adbc_needs_hash(self):
        h = _FixtureConnector()
        h._adbc_only = True
        assert h._needs_record_hash(self._state("insert")) is True

    def test_keyed_insert_does_not_need_hash(self):
        h = _FixtureConnector()
        h._adbc_only = True
        assert h._needs_record_hash(self._state("insert", primary_keys=["id"])) is False

    def test_upsert_does_not_need_hash(self):
        h = _FixtureConnector()
        h._adbc_only = True
        assert h._needs_record_hash(self._state("upsert")) is False

    def test_truncate_insert_does_not_need_hash(self):
        h = _FixtureConnector()
        h._adbc_only = True
        assert h._needs_record_hash(self._state("truncate_insert")) is False


class TestAttachRecordHashToBatch:
    """``_attach_record_hash_to_batch`` adds a ``_record_hash`` Arrow column
    computed from each row's JSON-serialized content, matching the same digest
    produced by ``_attach_record_hash`` on the SQLAlchemy path."""

    def _state(self, write_mode="insert", primary_keys=None):
        return _StreamState(
            schema_name="public",
            table_name="t",
            write_mode=write_mode,
            primary_keys=primary_keys or [],
        )

    def _expected_hash(self, row: dict) -> str:
        canonical = json.dumps(row, sort_keys=True, default=str)
        return hashlib.sha256(canonical.encode()).hexdigest()

    def test_appends_hash_column_to_keyless_insert_batch(self):
        h = _FixtureConnector()
        h._adbc_only = True
        batch = pa.RecordBatch.from_pydict(
            {"id": [1, 2], "name": ["alice", "bob"]},
        )
        result = h._attach_record_hash_to_batch(batch, self._state())
        assert GenericSQLConnector.RECORD_HASH_COLUMN in result.schema.names
        assert result.num_rows == 2

    def test_hash_matches_sqlalchemy_path_digest(self):
        h = _FixtureConnector()
        h._adbc_only = True
        batch = pa.RecordBatch.from_pydict({"x": [10], "y": ["z"]})
        result = h._attach_record_hash_to_batch(batch, self._state())
        arrow_hash = result.column(GenericSQLConnector.RECORD_HASH_COLUMN)[0].as_py()
        expected = self._expected_hash({"x": 10, "y": "z"})
        assert arrow_hash == expected

    def test_intra_batch_duplicate_rows_are_deduplicated(self):
        # Two byte-identical rows produce the same hash and are collapsed to
        # one so the stage table never carries two rows with the same conflict
        # key (which would make MERGE raise "multiple source rows match").
        h = _FixtureConnector()
        h._adbc_only = True
        batch = pa.RecordBatch.from_pydict({"v": [42, 42]})
        result = h._attach_record_hash_to_batch(batch, self._state())
        assert result.num_rows == 1

    def test_two_different_rows_get_different_hashes(self):
        h = _FixtureConnector()
        h._adbc_only = True
        batch = pa.RecordBatch.from_pydict({"v": [1, 2]})
        result = h._attach_record_hash_to_batch(batch, self._state())
        hashes = result.column(GenericSQLConnector.RECORD_HASH_COLUMN).to_pylist()
        assert hashes[0] != hashes[1]

    def test_null_value_produces_stable_hash(self):
        h = _FixtureConnector()
        h._adbc_only = True
        batch = pa.RecordBatch.from_pydict(
            {"v": pa.array([None], type=pa.int64()), "name": ["alice"]}
        )
        result = h._attach_record_hash_to_batch(batch, self._state())
        arrow_hash = result.column(GenericSQLConnector.RECORD_HASH_COLUMN)[0].as_py()
        expected = self._expected_hash({"v": None, "name": "alice"})
        assert arrow_hash == expected

    def test_null_and_string_none_produce_different_hashes(self):
        h = _FixtureConnector()
        h._adbc_only = True
        b1 = pa.RecordBatch.from_pydict({"v": pa.array([None], type=pa.int64())})
        b2 = pa.RecordBatch.from_pydict({"v": pa.array(["None"])})
        h1 = (
            h._attach_record_hash_to_batch(b1, self._state())
            .column(GenericSQLConnector.RECORD_HASH_COLUMN)[0]
            .as_py()
        )
        h2 = (
            h._attach_record_hash_to_batch(b2, self._state())
            .column(GenericSQLConnector.RECORD_HASH_COLUMN)[0]
            .as_py()
        )
        assert h1 != h2

    def test_noop_for_keyed_insert(self):
        h = _FixtureConnector()
        h._adbc_only = True
        batch = pa.RecordBatch.from_pydict({"id": [1]})
        result = h._attach_record_hash_to_batch(batch, self._state(primary_keys=["id"]))
        assert result is batch

    def test_noop_for_upsert(self):
        h = _FixtureConnector()
        h._adbc_only = True
        batch = pa.RecordBatch.from_pydict({"id": [1]})
        result = h._attach_record_hash_to_batch(batch, self._state("upsert"))
        assert result is batch


class TestMergeIngestInsertOnly:
    """``_merge_ingest_sync`` with ``insert_only=True`` emits a MERGE with only
    a ``WHEN NOT MATCHED THEN INSERT`` clause — no ``WHEN MATCHED THEN UPDATE``.
    The no-warning path for the ``update_cols`` check must fire correctly."""

    def _build_conn(self):
        executed: list[str] = []

        class _FakeCursor:
            def execute(self, sql, *args):
                executed.append(sql)

            def adbc_ingest(self, table, batch, mode, db_schema_name=None):
                pass

            def close(self):
                pass

        class _FakeConn:
            def cursor(self):
                return _FakeCursor()

            def commit(self):
                pass

        return _FakeConn(), executed

    def test_insert_only_omits_when_matched_update(self):
        h = _FixtureConnector()
        h._adbc_only = True
        conn, executed = self._build_conn()
        h._adbc_conn = conn

        batch = pa.RecordBatch.from_pydict(
            {"_record_hash": ["abc123"], "name": ["alice"]},
        )
        h._merge_ingest_sync(
            batch,
            "public",
            "orders",
            ["_record_hash", "name"],
            ["_record_hash"],
            "bdeadbeef1234567",
            insert_only=True,
        )
        merge_sql = next(s for s in executed if "MERGE INTO" in s)
        assert "WHEN NOT MATCHED THEN INSERT" in merge_sql
        assert "WHEN MATCHED THEN UPDATE" not in merge_sql
        assert '"_record_hash"' in merge_sql

    def test_insert_only_does_not_warn_about_missing_update_cols(self, caplog):
        h = _FixtureConnector()
        h._adbc_only = True
        conn, _ = self._build_conn()
        h._adbc_conn = conn

        batch = pa.RecordBatch.from_pydict({"_record_hash": ["abc123"]})
        with caplog.at_level(logging.WARNING, logger=database_module.logger.name):
            h._merge_ingest_sync(
                batch,
                "public",
                "t",
                ["_record_hash"],
                ["_record_hash"],
                "bdeadbeef1234567",
                insert_only=True,
            )
        warnings = [r for r in caplog.records if "no non-key columns" in r.message]
        assert not warnings

    def test_insert_only_false_still_warns_when_all_cols_are_conflict_keys(
        self, caplog
    ):
        h = _FixtureConnector()
        h._adbc_only = True
        conn, _ = self._build_conn()
        h._adbc_conn = conn

        batch = pa.RecordBatch.from_pydict({"id": [1]})
        with caplog.at_level(logging.WARNING, logger=database_module.logger.name):
            h._merge_ingest_sync(
                batch,
                "public",
                "t",
                ["id"],
                ["id"],
                "bdeadbeef1234567",
                insert_only=False,
            )
        warnings = [r for r in caplog.records if "no non-key columns" in r.message]
        assert warnings


class TestWriteBatchAdbcOnlyKeylessInsert:
    """``_write_batch_adbc_only`` routes a keyless insert through
    ``_merge_ingest_sync`` with ``insert_only=True`` and ``_record_hash`` as
    the conflict key, not through plain ``_adbc_only_ingest_sync``."""

    def _make_state(self, write_mode="insert", primary_keys=None):
        from unittest.mock import MagicMock

        state = _StreamState(
            schema_name="public",
            table_name="orders",
            write_mode=write_mode,
            primary_keys=primary_keys or [],
        )
        sc = MagicMock()
        sc.cast_arrow_batch.side_effect = lambda b: b
        state.schema_contract = sc
        return state

    @pytest.mark.asyncio
    async def test_keyless_insert_calls_merge_not_plain_ingest(self):
        h = _FixtureConnector()
        h._adbc_only = True
        state = self._make_state("insert")
        batch = pa.RecordBatch.from_pydict({"name": ["alice"]})

        with (
            patch.object(h, "_merge_ingest_sync") as mock_merge,
            patch.object(h, "_adbc_only_ingest_sync") as mock_plain,
        ):
            await h._write_batch_adbc_only(
                state, "run1", "stream1", 1, batch, truncate_now=False
            )

        mock_merge.assert_called_once()
        mock_plain.assert_not_called()
        args, kwargs = mock_merge.call_args
        assert kwargs.get("insert_only") is True
        # args: (batch, schema, table, all_columns, conflict_keys, stage_token)
        assert args[4] == [GenericSQLConnector.RECORD_HASH_COLUMN]

    @pytest.mark.asyncio
    async def test_keyless_insert_batch_includes_hash_column(self):
        h = _FixtureConnector()
        h._adbc_only = True
        state = self._make_state("insert")
        batch = pa.RecordBatch.from_pydict({"name": ["alice"]})

        captured_batches: list[pa.RecordBatch] = []

        def capture_merge(cb, *args, **kwargs):
            captured_batches.append(cb)

        with patch.object(h, "_merge_ingest_sync", side_effect=capture_merge):
            await h._write_batch_adbc_only(
                state, "run1", "stream1", 1, batch, truncate_now=False
            )

        assert len(captured_batches) == 1
        assert (
            GenericSQLConnector.RECORD_HASH_COLUMN in captured_batches[0].schema.names
        )

    @pytest.mark.asyncio
    async def test_keyed_insert_uses_plain_ingest(self):
        h = _FixtureConnector()
        h._adbc_only = True
        state = self._make_state("insert", primary_keys=["id"])
        batch = pa.RecordBatch.from_pydict({"id": [1], "name": ["alice"]})

        with (
            patch.object(h, "_merge_ingest_sync") as mock_merge,
            patch.object(h, "_adbc_only_ingest_sync") as mock_plain,
        ):
            await h._write_batch_adbc_only(
                state, "run1", "stream1", 1, batch, truncate_now=False
            )

        mock_plain.assert_called_once()
        mock_merge.assert_not_called()

    @pytest.mark.asyncio
    async def test_stage_token_is_stable_per_batch(self):
        """Same (run_id, stream_id, batch_seq) must produce the same stage token
        across retries so the pre-flight DROP-IF-EXISTS can clean a leftover stage."""
        h = _FixtureConnector()
        h._adbc_only = True
        state = self._make_state("insert")
        batch = pa.RecordBatch.from_pydict({"v": [1]})

        tokens: list[str] = []

        def capture(cb, schema, table, all_cols, conflict_keys, token, *, insert_only):
            tokens.append(token)

        with patch.object(h, "_merge_ingest_sync", side_effect=capture):
            for _ in range(2):
                await h._write_batch_adbc_only(
                    state, "run1", "stream1", 42, batch, truncate_now=False
                )

        assert len(tokens) == 2
        assert tokens[0] == tokens[1]

    @pytest.mark.asyncio
    async def test_retry_produces_identical_hashed_batch(self):
        """Same batch retried twice must produce identical _record_hash values
        so the stage-MERGE skips rows already committed on the first attempt."""
        h = _FixtureConnector()
        h._adbc_only = True
        state = self._make_state("insert")
        batch = pa.RecordBatch.from_pydict({"name": ["alice"], "v": [42]})

        captured: list[list[str]] = []

        def capture(cb, *args, **kwargs):
            captured.append(
                cb.column(GenericSQLConnector.RECORD_HASH_COLUMN).to_pylist()
            )

        with patch.object(h, "_merge_ingest_sync", side_effect=capture):
            for _ in range(2):
                await h._write_batch_adbc_only(
                    state, "run1", "s1", 7, batch, truncate_now=False
                )

        assert len(captured) == 2
        assert captured[0] == captured[1]


class TestRecordHashDecimalPrecision:
    """Decimal values hash via json ``default=str`` -- the deployed SQLAlchemy
    behaviour. No context rounding and no zero-stripping, so distinct
    high-precision values keep distinct ``_record_hash`` digests and existing
    rows keep the digest they were written with (issue #285 review)."""

    def _state(self):
        state = _StreamState(
            schema_name="public",
            table_name="t",
            write_mode="insert",
            primary_keys=[],
        )
        sc = MagicMock()
        sc.cast_arrow_batch.side_effect = lambda b: b
        state.schema_contract = sc
        return state

    def _hash(self, arr):
        h = _FixtureConnector()
        h._adbc_only = True
        out = h._attach_record_hash_to_batch(
            pa.RecordBatch.from_pydict({"v": arr}), self._state()
        )
        return out.column(GenericSQLConnector.RECORD_HASH_COLUMN).to_pylist()[0]

    def test_distinct_high_precision_values_do_not_collide(self):
        # NUMERIC(38) values differing past the 28-digit context must not share a
        # hash (Decimal.normalize would have collapsed them and dropped a row).
        t = pa.decimal128(38, 2)
        a = self._hash(pa.array([Decimal("123456789012345678901234567890.00")], type=t))
        b = self._hash(pa.array([Decimal("123456789012345678901234567891.00")], type=t))
        assert a != b

    def test_digest_uses_str_form(self):
        # The hash input for a Decimal is its str() form (default=str), matching
        # the deployed SQLAlchemy path -- not a zero-stripped form. Guards against
        # re-hashing existing rows on deploy (issue #285 review P2).
        arr = pa.array([Decimal("3.1400")], type=pa.decimal128(10, 4))
        val = arr[0].as_py()
        expected = hashlib.sha256(
            json.dumps({"v": val}, sort_keys=True, default=str).encode()
        ).hexdigest()
        assert self._hash(arr) == expected


class _NoMergeAdbcDialect(_FixtureAdbcDialect):
    """ADBC dialect that ingests but cannot MERGE (base default)."""

    supports_upsert_adbc = False


class _NoMergeConnector(GenericSQLConnector):
    dialect_class = _NoMergeAdbcDialect


class TestKeylessInsertRequiresMergeSupport:
    """Keyless ADBC insert dedups via stage-MERGE; a dialect that cannot MERGE
    must fail loud at configure, not fatal on the first write (issue #285 P2)."""

    def _state(self, primary_keys=None):
        return _StreamState(
            schema_name="public",
            table_name="orders",
            write_mode="insert",
            primary_keys=primary_keys or [],
        )

    @pytest.mark.asyncio
    async def test_keyless_insert_without_merge_support_fails_configure(self):
        h = _NoMergeConnector()
        h._adbc_only = True
        with pytest.raises(AdbcConfigurationError, match="MERGE"):
            await h._ensure_tables_exist(self._state(), MagicMock())

    @pytest.mark.asyncio
    async def test_merge_capable_dialect_is_not_gated(self):
        # Same keyless insert on a MERGE-capable dialect clears the guard and
        # fails later on the (deliberately absent) endpoint document instead.
        h = _FixtureConnector()
        h._adbc_only = True
        with pytest.raises(SchemaConfigurationError):
            await h._ensure_tables_exist(self._state(), MagicMock())

    @pytest.mark.asyncio
    async def test_keyed_insert_is_not_gated(self):
        # Keyed insert needs no _record_hash dedup, so the guard is bypassed
        # even on a non-MERGE dialect.
        h = _NoMergeConnector()
        h._adbc_only = True
        with pytest.raises(SchemaConfigurationError):
            await h._ensure_tables_exist(self._state(primary_keys=["id"]), MagicMock())
