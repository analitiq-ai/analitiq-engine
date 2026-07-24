"""ADBC-only dispatch on GenericSQLConnector under stage-then-merge (#389).

The facade is transport-uniform: an ADBC handler prepares the batch in
Arrow space, builds the same ``StageWritePlan`` the SQLAlchemy path
builds, and hands it to its transport backend. These tests pin the
facade-side pieces the ADBC transport exercises — the fatal-error
classification, the record-hash identity semantics, the uniform
capability advertisement, the DDL builders, and the plan a keyless
insert produces. The backend's own mechanics (cycle shapes, session
guard, poisoning) live in ``tests/unit/cdk_tests/sql/test_adbc_backend``.

The generic connector is vendor-neutral: every per-system fragment
(quoting, schema folding, stage-table syntax, PK clause) comes from the
:class:`~cdk.sql.dialects.SqlDialect` the connector class carries. The
CDK base raises ``UnsupportedDialectOperationError`` for those hooks, so
the driver-specific behaviour these tests exercise is supplied by small
*fixture* dialects defined here, installed via a ``GenericSQLConnector``
subclass with ``dialect_class`` set. Vendor-specific SQL text
(Snowflake/BigQuery specifics) is tested in the connector package repos.
"""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pyarrow as pa
import pytest

from cdk.sql._adbc_utils import (
    _FATAL_ADBC_ERROR_NAMES,
    _is_fatal_adbc_error,
    _reclassify_as_fatal,
)
from cdk.sql.capabilities import SqlCapabilities
from cdk.sql.dialects import SqlDialect, TableAddress
from cdk.sql.generic import AdbcConfigurationError, GenericSQLConnector, _StreamState


def _caps(**overrides) -> SqlCapabilities:
    """A declared sql_capabilities object, defaults merge-capable/per-statement.

    Stands in for the block a connector.json declares; tests assign it to
    ``handler._capabilities`` the way ``_bind_capabilities`` would at
    connect() time, overriding exactly the fact under test.
    """
    block = {
        "catalog": "none",
        "session_targeting": "per_statement",
        "merge_form": "merge",
        "bulk_load": {"adbc": "adbc_ingest"},
        "stage": {"scope": "real", "schema": "target", "transactional_ddl": False},
    }
    block.update(overrides)
    return SqlCapabilities.from_declaration(block)


# --- fixture dialects + connector (stand in for a connector package) --------


class _FixtureAdbcDialect(SqlDialect):
    """A complete ADBC dialect with canned, ANSI-ish type rendering.

    Stands in for a connector package's dialect: renders canonical Arrow types
    to canned native DDL strings (overriding ``render_column_type`` — the single
    write surface), implements the stage-then-merge rendering hooks, and folds
    identifiers upper-case (the way a case-folding system's package dialect
    would) so the normalize-reaches-every-sink coverage has something to
    assert. Whether the system can MERGE is declared data
    (``sql_capabilities.merge_form``), not a dialect fact — tests set it on
    the handler via ``_caps``.
    """

    name = "fixture"

    #: canonical Arrow string -> canned native DDL type the dialect renders.
    _CANONICAL_TO_DDL = {
        "Int64": "INTEGER",
        "Int32": "INTEGER",
        "Utf8": "STRING",
        "Binary": "VARBINARY",
        "Timestamp(MICROSECOND)": "TIMESTAMP",
        "Timestamp(MICROSECOND, UTC)": "TIMESTAMPTZ",
    }

    def normalize_ident(self, name: str) -> str:
        return name.upper()

    def render_column_type(self, canonical, type_mapper, *, params=None) -> str:
        return self._CANONICAL_TO_DDL[canonical]

    def stage_table_sql(self, stage, target, *, temp) -> str:
        keyword = "CREATE TEMPORARY TABLE" if temp else "CREATE TABLE"
        return (
            f"{keyword} {self.quote_table(stage)} AS SELECT * FROM "
            f"{self.quote_table(target)} WHERE FALSE"
        )

    def merge_statement_sql(self, stage, target, conflict_keys, columns) -> str:
        on = " AND ".join(
            f"t.{self.quote_ident(k)} = s.{self.quote_ident(k)}" for k in conflict_keys
        )
        return (
            f"MERGE INTO {self.quote_table(target)} t "
            f"USING {self.quote_table(stage)} s ON {on}"
        )


class _FixtureBacktickDialect(_FixtureAdbcDialect):
    """As above but backtick-quoting + NOT ENFORCED PK and no schema folding."""

    name = "fixture_backtick"
    quote_char = "`"
    pk_not_enforced = True

    def normalize_ident(self, name: str) -> str:
        # Backtick systems (BigQuery) are case-sensitive — identity.
        return name


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
    ``UnsupportedDialectOperationError``. (The fixture dialects above show the
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
                h.dialect.table_address("t", schema="public"),
                [ColumnDef("id", "Int64")],
                [],
            )
        assert isinstance(exc.value.__cause__, InvalidTypeMapError)

    def test_base_dialect_lacks_stage_table_sql(self):
        from cdk.sql.exceptions import UnsupportedDialectOperationError

        h = GenericSQLConnector()
        with pytest.raises(UnsupportedDialectOperationError, match="stage_table_sql"):
            h.dialect.stage_table_sql(
                TableAddress(table="stage"),
                TableAddress(table="target"),
                temp=False,
            )


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


class TestDialectQuoting:
    """Quoting is a dialect hook: the ANSI base double-quotes (with escaping);
    a backtick fixture dialect quotes with backticks and rejects embedded
    backticks. The connector calls ``self.dialect.quote_*`` — no per-driver
    branch in the connector itself."""

    def test_backtick_dialect_quotes_with_backticks(self):
        h = _FixtureBacktickConnector()
        assert h.dialect.quote_ident("id") == "`id`"
        address = h.dialect.table_address("t", schema="ds")
        assert h.dialect.quote_table(address) == "`ds`.`t`"

    def test_ansi_dialect_quotes_with_double_quotes(self):
        h = GenericSQLConnector()  # ANSI base
        assert h.dialect.quote_ident("id") == '"id"'

    def test_folding_dialect_qualified_normalizes_all_components(self):
        # The folding fixture upper-cases every component before quoting —
        # schema AND table fold through the same normalize_ident rule.
        h = _FixtureConnector()
        assert (
            h.dialect.quote_table(h.dialect.table_address("t", schema="public"))
            == '"PUBLIC"."T"'
        )
        assert (
            h.dialect.quote_table(h.dialect.table_address("t", schema="analytics"))
            == '"ANALYTICS"."T"'
        )

    def test_double_quote_escaping_in_ansi_dialect(self):
        h = GenericSQLConnector()
        assert h.dialect.quote_ident('we"ird') == '"we""ird"'

    def test_backtick_dialect_rejects_backtick_in_identifier(self):
        h = _FixtureBacktickConnector()
        with pytest.raises(ValueError, match="backtick"):
            h.dialect.quote_ident("we`ird")


class TestSupportsUpsert:
    """``supports_upsert`` advertises upsert only when it is declared
    (``sql_capabilities.merge_form``) AND the dialect renders the stage
    and merge statements — the same gates on both transports, so
    ``GetCapabilities`` never advertises a mode ``configure_schema``
    would refuse for every stream."""

    def test_undeclared_does_not_support(self):
        h = GenericSQLConnector()
        assert h.supports_upsert is False

    @pytest.mark.parametrize("adbc_only", [False, True], ids=["sqlalchemy", "adbc"])
    def test_declared_merge_form_with_renderers_supports(self, adbc_only):
        h = _FixtureConnector()
        h._adbc_only = adbc_only
        h._capabilities = _caps(merge_form="merge")
        assert h.supports_upsert is True

    @pytest.mark.parametrize("adbc_only", [False, True], ids=["sqlalchemy", "adbc"])
    def test_declared_none_does_not_support(self, adbc_only):
        h = _FixtureConnector()
        h._adbc_only = adbc_only
        h._capabilities = _caps(merge_form="none")
        assert h.supports_upsert is False

    @pytest.mark.parametrize("adbc_only", [False, True], ids=["sqlalchemy", "adbc"])
    def test_declaration_without_renderers_does_not_advertise(self, adbc_only):
        # The base dialect renders neither the stage DDL nor a merge
        # statement; a declaring connector without the overrides cannot
        # run the stage cycle on either transport.
        h = GenericSQLConnector()
        h._adbc_only = adbc_only
        h._capabilities = _caps(merge_form="merge")
        assert h.supports_upsert is False

    def test_any_declared_merge_form_advertises_on_adbc(self):
        # The interim MERGE-only ADBC machinery shape is gone (#389): a
        # declared ON CONFLICT form with a rendering dialect advertises.
        h = _FixtureConnector()
        h._adbc_only = True
        h._capabilities = _caps(merge_form="insert_on_conflict")
        assert h.supports_upsert is True


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
            state.address,
            handler._build_column_defs(state),
            list(state.primary_keys),
            if_not_exists=True,
        )

    def test_synced_at_appended_when_missing(self):
        """_synced_at is appended when the contract does not declare it."""

        class _TypeMapperStub:
            def to_arrow_type(self, native: str) -> str:
                return {"BIGINT": "Int64", "TEXT": "Utf8"}[native]

        h = _FixtureConnector()
        state = _StreamState(
            address=h.dialect.table_address("orders", schema="analytics"),
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
        ddl = self._build_target_ddl(h, state, _TypeMapperStub())
        assert "CREATE TABLE IF NOT EXISTS" in ddl
        # Every address component folds through normalize_ident — schema
        # and table alike.
        assert '"ANALYTICS"."ORDERS"' in ddl
        assert '"_synced_at" TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP' in ddl
        assert 'PRIMARY KEY ("id")' in ddl
        assert '"id" INTEGER NOT NULL' in ddl
        assert '"status" STRING' in ddl

    def test_synced_at_not_double_declared(self):
        """A declared _synced_at column is not declared twice."""

        class _TypeMapperStub:
            def to_arrow_type(self, native: str) -> str:
                return {"BIGINT": "Int64", "TIMESTAMP": "Timestamp(MICROSECOND)"}[
                    native
                ]

        h = _FixtureConnector()
        state = _StreamState(
            address=h.dialect.table_address("orders", schema="analytics"),
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
        structural guarantee the insert anti-join relies on: the database
        enforces hash uniqueness in the target table.

        Production code passes ``_identity_columns(state)`` as the PK list;
        for keyless insert this returns ``[RECORD_HASH_COLUMN]`` so the
        ``_record_hash`` column is the table's sole primary key.
        """
        from cdk.sql.ddl import build_create_table_sql

        class _TypeMapperStub:
            def to_arrow_type(self, native: str) -> str:
                return {"TEXT": "Utf8"}[native]

        h = _FixtureConnector()
        h._adbc_only = True
        state = _StreamState(
            address=h.dialect.table_address("events", schema="public"),
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
        # Use _identity_columns (as production code does in _ensure_tables_exist)
        # not state.primary_keys — for keyless insert this returns [_record_hash].
        ddl = build_create_table_sql(
            h.dialect,
            _TypeMapperStub(),
            state.address,
            h._build_column_defs(state),
            h._identity_columns(state),
            if_not_exists=True,
        )
        assert '"_record_hash"' in ddl
        assert '"_record_hash" STRING NOT NULL' in ddl
        assert "PRIMARY KEY" in ddl
        pk_pos = ddl.index("PRIMARY KEY")
        assert "_record_hash" in ddl[pk_pos:]


class TestDisconnectClosesBackend:
    """``disconnect()`` must release the transport backend (which closes a
    cached ADBC connection) and the runtime even when one side fails, and
    always flip ``_connected`` so callers can re-acquire cleanly. A
    regression here leaks a server-side session on shutdown."""

    @pytest.mark.asyncio
    async def test_closes_adbc_connection_through_the_backend(self):
        from cdk.sql.adbc_backend import AdbcBackend

        handler = GenericSQLConnector()
        handler._connected = True
        backend = AdbcBackend(handler.dialect)
        adbc_conn = MagicMock()
        backend._conn = adbc_conn
        handler._backend = backend
        handler._runtime = AsyncMock()
        handler._runtime.close = AsyncMock()

        await handler.disconnect()

        adbc_conn.close.assert_called_once()
        assert backend._conn is None
        assert handler._backend is None
        assert handler._connected is False
        handler._runtime.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close_failure_logged_at_error_and_runtime_still_released(
        self, caplog
    ):
        import logging

        from cdk.sql import generic as database_module
        from cdk.sql.adbc_backend import AdbcBackend

        handler = GenericSQLConnector()
        handler._connected = True
        backend = AdbcBackend(handler.dialect)
        adbc_conn = MagicMock()
        adbc_conn.close.side_effect = RuntimeError("already closed")
        backend._conn = adbc_conn
        handler._backend = backend
        handler._runtime = AsyncMock()
        handler._runtime.close = AsyncMock()

        with caplog.at_level(logging.ERROR, logger=database_module.logger.name):
            await handler.disconnect()

        errors = [
            r
            for r in caplog.records
            if "Failed to close transport backend" in r.message
        ]
        assert errors and errors[0].levelno == logging.ERROR
        assert errors[0].exc_info is not None
        assert handler._connected is False
        # Runtime is still released so we don't leak it on top of the
        # ADBC handle.
        handler._runtime.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_runtime_close_failure_still_flips_connected_state(self, caplog):
        """If ``runtime.close()`` raises, the handler must still
        transition to ``_connected = False`` so callers can re-acquire
        without observing a half-disconnected state."""
        import logging

        from cdk.sql import generic as database_module

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
            address=TableAddress(table="t", schema="public"),
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
    computed from each row's JSON-serialized content — one digest formula on
    both transports, so a row keeps its identity across transports and
    deploys."""

    def _state(self, write_mode="insert", primary_keys=None):
        return _StreamState(
            address=TableAddress(table="t", schema="public"),
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

    def test_hash_matches_content_digest(self):
        h = _FixtureConnector()
        h._adbc_only = True
        batch = pa.RecordBatch.from_pydict({"x": [10], "y": ["z"]})
        result = h._attach_record_hash_to_batch(batch, self._state())
        arrow_hash = result.column(GenericSQLConnector.RECORD_HASH_COLUMN)[0].as_py()
        expected = self._expected_hash({"x": 10, "y": "z"})
        assert arrow_hash == expected

    def test_intra_batch_duplicate_rows_are_deduplicated(self):
        # Two byte-identical rows produce the same hash and are collapsed to
        # one so the stage never carries two rows with the same identity
        # (both copies would pass the set-based anti-join otherwise).
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


class TestAdbcWriteBatchPlansTheSharedPrimitive:
    """``write_batch`` on an ADBC-only handler builds the same
    ``StageWritePlan`` the SQLAlchemy path builds and hands the prepared
    Arrow batch to the backend — no ADBC-private routing left."""

    def _handler(self, write_mode="insert", primary_keys=None, conflict_keys=None):
        from datetime import datetime, timezone

        h = _FixtureConnector()
        h._connected = True
        h._adbc_only = True
        h.dialect.capabilities = _caps()
        h._capabilities = h.dialect.capabilities
        contract = MagicMock()
        contract.cast_arrow_batch.side_effect = lambda b: b
        h._streams["s1"] = _StreamState(
            address=h.dialect.table_address("orders", schema="public"),
            write_mode=write_mode,
            primary_keys=primary_keys or [],
            conflict_keys=conflict_keys or [],
            schema_contract=contract,
        )
        backend = MagicMock()
        backend.execute_write = AsyncMock()
        backend.run_ddl = AsyncMock()
        h._backend = backend
        h._emitted_at = datetime(2026, 7, 21, 9, 0, 0, tzinfo=timezone.utc)
        return h

    async def _write(self, h, *, seq=1, rows=None, run_id="run1"):
        from cdk.types import Cursor

        return await h.write_batch(
            run_id=run_id,
            stream_id="s1",
            batch_seq=seq,
            record_batch=pa.RecordBatch.from_pylist(
                rows if rows is not None else [{"name": "alice"}]
            ),
            record_ids=["r1"],
            cursor=Cursor(token=b"t"),
            emitted_at=h._emitted_at,
        )

    @staticmethod
    def _sent(h):
        (call,) = h._backend.execute_write.call_args_list
        return call.args[0], call.args[1]

    @pytest.mark.asyncio
    async def test_keyless_insert_plans_anti_join_on_record_hash(self):
        h = self._handler("insert")
        result = await self._write(h)
        assert result.success
        plan, batch = self._sent(h)
        assert "WHERE NOT EXISTS" in plan.mode_sql
        assert '"_record_hash"' in plan.mode_sql
        # The prepared batch carries the content-derived identity column.
        assert GenericSQLConnector.RECORD_HASH_COLUMN in batch.schema.names

    @pytest.mark.asyncio
    async def test_keyed_insert_plans_anti_join_on_the_primary_key(self):
        h = self._handler("insert", primary_keys=["name"])
        await self._write(h)
        plan, batch = self._sent(h)
        assert "WHERE NOT EXISTS" in plan.mode_sql
        # Column identifiers pass through quote_ident verbatim — they are
        # the prepared batch's schema names, not address components.
        assert 't."name" = s."name"' in plan.mode_sql
        assert GenericSQLConnector.RECORD_HASH_COLUMN not in batch.schema.names

    @pytest.mark.asyncio
    async def test_upsert_plans_the_dialect_merge_statement(self):
        h = self._handler("upsert", conflict_keys=["name"])
        await self._write(h)
        plan, _ = self._sent(h)
        assert plan.mode_sql.startswith("MERGE INTO")

    @pytest.mark.asyncio
    async def test_stage_name_is_stable_per_batch(self):
        """Same (run_id, stream_id, batch_seq) must produce the same stage
        name across retries so the pre-flight drop can clean a leftover."""
        h = self._handler("insert")
        await self._write(h, seq=42)
        await self._write(h, seq=42)
        plans = [c.args[0] for c in h._backend.execute_write.call_args_list]
        assert plans[0].stage == plans[1].stage
        assert plans[0].stage.table.startswith("_analitiq_stage_b")

    @pytest.mark.asyncio
    async def test_retry_produces_identical_hashed_batch(self):
        """Same batch retried twice must produce identical _record_hash
        values so the anti-join skips rows already committed on the first
        attempt."""
        h = self._handler("insert")
        rows = [{"name": "alice"}]
        await self._write(h, seq=7, rows=rows)
        await self._write(h, seq=7, rows=rows)
        hashes = [
            c.args[1].column(GenericSQLConnector.RECORD_HASH_COLUMN).to_pylist()
            for c in h._backend.execute_write.call_args_list
        ]
        assert hashes[0] == hashes[1]


class TestKeylessInsertNeedsNoMergeForm:
    """Keyless insert dedups via the ANSI anti-join from the stage (#389)
    — it no longer needs a declared merge form. A merge-less system keeps
    exactly-once keyless inserts as long as it can stage."""

    @pytest.mark.asyncio
    async def test_keyless_insert_plans_anti_join_with_merge_form_none(self):
        from datetime import datetime, timezone

        from cdk.types import Cursor

        h = _FixtureConnector()
        h._connected = True
        h._adbc_only = True
        h.dialect.capabilities = _caps(merge_form="none")
        h._capabilities = h.dialect.capabilities
        contract = MagicMock()
        contract.cast_arrow_batch.side_effect = lambda b: b
        h._streams["s1"] = _StreamState(
            address=h.dialect.table_address("orders", schema="public"),
            write_mode="insert",
            primary_keys=[],
            schema_contract=contract,
        )
        backend = MagicMock()
        backend.execute_write = AsyncMock()
        h._backend = backend

        result = await h.write_batch(
            run_id="run1",
            stream_id="s1",
            batch_seq=1,
            record_batch=pa.RecordBatch.from_pylist([{"name": "alice"}]),
            record_ids=["r1"],
            cursor=Cursor(token=b"t"),
            emitted_at=datetime(2026, 7, 21, 9, 0, 0, tzinfo=timezone.utc),
        )
        assert result.success
        plan = backend.execute_write.call_args.args[0]
        assert "WHERE NOT EXISTS" in plan.mode_sql


class TestRecordHashDecimalPrecision:
    """Decimal values hash via json ``default=str`` -- the deployed
    behaviour. No context rounding and no zero-stripping, so distinct
    high-precision values keep distinct ``_record_hash`` digests and existing
    rows keep the digest they were written with (issue #285 review)."""

    def _state(self):
        state = _StreamState(
            address=TableAddress(table="t", schema="public"),
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
        # the deployed path -- not a zero-stripped form. Guards against
        # re-hashing existing rows on deploy (issue #285 review P2).
        arr = pa.array([Decimal("3.1400")], type=pa.decimal128(10, 4))
        val = arr[0].as_py()
        expected = hashlib.sha256(
            json.dumps({"v": val}, sort_keys=True, default=str).encode()
        ).hexdigest()
        assert self._hash(arr) == expected
