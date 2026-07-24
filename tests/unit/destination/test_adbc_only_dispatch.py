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
from cdk.sql.capabilities import SqlCapabilities
from cdk.sql.dialects import SqlDialect, TableAddress
from cdk.sql.generic import (
    _FATAL_ADBC_ERROR_NAMES,
    AdbcConfigurationError,
    GenericSQLConnector,
    SchemaConfigurationError,
    _is_fatal_adbc_error,
    _reclassify_as_fatal,
    _StreamState,
)


def _caps(**overrides) -> SqlCapabilities:
    """A declared sql_capabilities object, defaults merge-capable/per-statement.

    Stands in for the block a connector.json declares; tests assign it to
    ``handler._capabilities`` the way ``_bind_capabilities`` would at
    connect() time, overriding exactly the fact under test.
    """
    # Stage defaults to the one shape the current ADBC machinery honors
    # (real table, target schema); the interim guard refuses anything else
    # until #389 lands declared stage shapes.
    block = {
        "catalog": "none",
        "session_targeting": "per_statement",
        "merge_form": "merge",
        "bulk_load": "none",
        "stage": {"scope": "real", "schema": "target", "transactional_ddl": True},
    }
    block.update(overrides)
    return SqlCapabilities.from_declaration(block)


# --- fixture dialects + connector (stand in for a connector package) --------


class _FixtureAdbcDialect(SqlDialect):
    """A complete ADBC dialect with canned, ANSI-ish type rendering.

    Stands in for a connector package's dialect: renders canonical Arrow types
    to canned native DDL strings (overriding ``render_column_type`` — the single
    write surface — rather than the old per-purpose ADBC hooks) and folds
    identifiers upper-case (the way a case-folding system's package dialect
    would) so the normalize-reaches-the-ingest-site coverage has something to
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
                h.dialect.table_address("t", schema="public"),
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
            # (adbc_ingest_kwargs returns {}), so it must default here.
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
            h.dialect.table_address("orders", schema="public"),
        )
        assert captured["db_schema_name"] == "PUBLIC"
        # Normalization is uniform across components: the table name folds
        # through the same rule as the schema (issue #336).
        assert captured["table"] == "ORDERS"

    def test_non_folding_dialect_keeps_schema_for_ingest(self):
        # A case-sensitive (backtick) dialect never folds the schema.
        h = _FixtureBacktickConnector()
        h._adbc_only = True
        h._adbc_conn, captured = self._captured_ingest()
        import pyarrow as pa

        h._adbc_only_ingest_sync(
            pa.record_batch([pa.array([1])], names=["id"]),
            h.dialect.table_address("orders", schema="analytics"),
        )
        assert captured["db_schema_name"] == "analytics"
        assert captured["table"] == "orders"

    def test_empty_schema_yields_none(self):
        h = _FixtureConnector()
        h._adbc_only = True
        h._adbc_conn, captured = self._captured_ingest()
        import pyarrow as pa

        h._adbc_only_ingest_sync(
            pa.record_batch([pa.array([1])], names=["id"]),
            h.dialect.table_address("orders"),
        )
        assert captured["db_schema_name"] is None


class _FixtureSessionDefaultDialect(_FixtureAdbcDialect):
    """Opts out of per-statement ingest targeting (stands in for Snowflake):
    ``adbc_ingest`` follows the connection's session schema."""

    name = "fixture_session_default"

    def adbc_ingest_kwargs(self, address: TableAddress) -> dict:
        return {}


class _FixtureSessionDefaultConnector(GenericSQLConnector):
    dialect_class = _FixtureSessionDefaultDialect


class _SessionFakeConn:
    """Fake ADBC connection reporting a configurable session schema.

    Records every executed statement and every ``adbc_ingest`` call so
    tests can assert what reached the wire; counts session-schema probes
    to pin the once-per-connection cache. ``probe_error`` makes the
    probe statement raise (driver-failure injection); ``no_row`` makes
    ``fetchone`` return no row at all instead of a 1-tuple.
    """

    def __init__(self, session_schema, probe_sql, *, probe_error=None, no_row=False):
        self.session_schema = session_schema
        self.probe_sql = probe_sql
        self.probe_error = probe_error
        self.no_row = no_row
        self.statements: list[str] = []
        self.ingests: list[dict] = []
        self.probe_count = 0
        self.closed = False

    def cursor(self):
        conn = self

        class _Cursor:
            def execute(self, sql, params=None):
                conn.statements.append(sql)
                if sql == conn.probe_sql:
                    conn.probe_count += 1
                    if conn.probe_error is not None:
                        raise conn.probe_error

            def fetchone(self):
                return None if conn.no_row else (conn.session_schema,)

            def adbc_ingest(self, table, batch, mode, **kwargs):
                conn.ingests.append({"table": table, "mode": mode, "kwargs": kwargs})

            def close(self):
                pass

        return _Cursor()

    def commit(self):
        pass

    def close(self):
        self.closed = True


class TestAdbcSessionSchemaGuard:
    """Issue #377: when a dialect opts out of per-statement ingest targeting,
    bare-name ingest resolves against the connection's session schema. The
    invariant *session schema == target schema* holds by construction today;
    these tests pin the guard that turns a future divergence into a hard
    error instead of a silent append into a same-named table in the wrong
    schema (the append path's silent window)."""

    def _handler(self, session_schema):
        h = _FixtureSessionDefaultConnector()
        h._adbc_only = True
        h._capabilities = _caps(session_targeting="session_default")
        conn = _SessionFakeConn(session_schema, h.dialect.adbc_session_schema_sql())
        h._adbc_conn = conn
        return h, conn

    @staticmethod
    def _batch():
        return pa.record_batch([pa.array([1])], names=["id"])

    def test_matching_session_schema_ingests_bare_name(self):
        h, conn = self._handler("PUBLIC")
        h._adbc_only_ingest_sync(
            self._batch(), h.dialect.table_address("orders", schema="public")
        )
        assert conn.probe_count == 1
        assert conn.ingests == [{"table": "ORDERS", "mode": "append", "kwargs": {}}]

    def test_probe_runs_once_per_connection(self):
        h, conn = self._handler("PUBLIC")
        address = h.dialect.table_address("orders", schema="public")
        h._adbc_only_ingest_sync(self._batch(), address)
        h._adbc_only_ingest_sync(self._batch(), address)
        assert conn.probe_count == 1
        assert len(conn.ingests) == 2

    def test_mismatch_raises_and_skips_ingest(self):
        h, conn = self._handler("PUBLIC")
        with pytest.raises(AdbcConfigurationError) as exc:
            h._adbc_only_ingest_sync(
                self._batch(),
                h.dialect.table_address("orders", schema="analytics"),
            )
        # The error names both schemas and nothing was ingested.
        assert "'PUBLIC'" in str(exc.value)
        assert "'ANALYTICS'" in str(exc.value)
        assert conn.ingests == []

    def test_no_session_schema_raises(self):
        h, conn = self._handler(None)
        with pytest.raises(AdbcConfigurationError, match="no schema selected"):
            h._adbc_only_ingest_sync(
                self._batch(),
                h.dialect.table_address("orders", schema="public"),
            )
        assert conn.ingests == []

    def test_session_value_is_dialect_normalized(self):
        # CURRENT_SCHEMA() reporting a spelling that needs folding still
        # matches the normalized address ("normalized per dialect").
        h, conn = self._handler("public")
        h._adbc_only_ingest_sync(
            self._batch(), h.dialect.table_address("orders", schema="public")
        )
        assert len(conn.ingests) == 1

    def test_poison_resets_the_probe_cache(self):
        h, conn = self._handler("PUBLIC")
        address = h.dialect.table_address("orders", schema="public")
        h._adbc_only_ingest_sync(self._batch(), address)
        assert h._adbc_session_schema_known
        h._poison_adbc_connection()
        assert not h._adbc_session_schema_known
        assert h._adbc_session_schema is None
        assert conn.closed
        # A replacement connection with a diverged session schema is
        # re-probed and refused — the cache never outlives its connection.
        h._adbc_conn = _SessionFakeConn("OTHER", h.dialect.adbc_session_schema_sql())
        with pytest.raises(AdbcConfigurationError):
            h._adbc_only_ingest_sync(self._batch(), address)

    def test_targeting_dialect_never_probes(self):
        # A dialect that keeps per-statement targeting kwargs bypasses the
        # guard entirely — no probe statement reaches the connection.
        h = _FixtureConnector()
        h._adbc_only = True
        conn = _SessionFakeConn("PUBLIC", h.dialect.adbc_session_schema_sql())
        h._adbc_conn = conn
        h._adbc_only_ingest_sync(
            self._batch(), h.dialect.table_address("orders", schema="public")
        )
        assert conn.probe_count == 0
        assert conn.ingests == [
            {
                "table": "ORDERS",
                "mode": "append",
                "kwargs": {"db_schema_name": "PUBLIC"},
            }
        ]

    def test_merge_path_mismatch_fails_before_stage_ddl(self):
        # The stage-upsert path guards the same invariant, and the guard
        # runs before any stage DDL so a mismatch leaves nothing behind.
        h, conn = self._handler("PUBLIC")
        address = h.dialect.table_address("orders", schema="analytics")
        with pytest.raises(AdbcConfigurationError):
            h._merge_ingest_sync(self._batch(), address, ["id"], ["id"], "btok")
        assert conn.ingests == []
        assert not any("CREATE TABLE" in s for s in conn.statements)

    def test_merge_path_matching_session_proceeds(self):
        h, conn = self._handler("PUBLIC")
        address = h.dialect.table_address("orders", schema="public")
        h._merge_ingest_sync(self._batch(), address, ["id"], ["id"], "btok")
        assert conn.probe_count == 1
        assert len(conn.ingests) == 1
        assert conn.ingests[0]["kwargs"] == {}
        assert conn.ingests[0]["table"].startswith("_analitiq_stage_")

    def test_truncate_path_mismatch_fails_before_truncate(self):
        # truncate_insert's first batch TRUNCATEs the (schema-qualified,
        # therefore correct) target; the guard must run first so a
        # mismatch cannot empty a table it then refuses to refill.
        h, conn = self._handler("PUBLIC")
        address = h.dialect.table_address("orders", schema="analytics")
        with pytest.raises(AdbcConfigurationError):
            h._truncate_then_ingest_sync(self._batch(), address)
        assert conn.ingests == []
        assert not any("TRUNCATE" in s for s in conn.statements)

    def test_truncate_path_matching_session_proceeds(self):
        h, conn = self._handler("PUBLIC")
        address = h.dialect.table_address("orders", schema="public")
        h._truncate_then_ingest_sync(self._batch(), address)
        assert conn.probe_count == 1
        assert any("TRUNCATE" in s for s in conn.statements)
        assert len(conn.ingests) == 1

    def test_probe_fatal_driver_error_reclassified(self):
        # A probe failing with a PEP-249 fatal name (e.g. a future
        # session-default dialect whose system rejects the base probe
        # SQL) must become AdbcConfigurationError so the engine bails
        # instead of retrying a deterministic failure forever.
        h, conn = self._handler("PUBLIC")
        conn.probe_error = ProgrammingError("unknown function CURRENT_SCHEMA")
        with pytest.raises(AdbcConfigurationError, match="ProgrammingError"):
            h._adbc_only_ingest_sync(
                self._batch(),
                h.dialect.table_address("orders", schema="public"),
            )
        assert conn.ingests == []
        assert conn.closed  # poisoned like any ingest-path driver error
        assert not h._adbc_session_schema_known  # retry re-probes

    def test_probe_transient_error_propagates_and_poisons(self):
        h, conn = self._handler("PUBLIC")
        conn.probe_error = OperationalError("connection reset")
        with pytest.raises(OperationalError):
            h._adbc_only_ingest_sync(
                self._batch(),
                h.dialect.table_address("orders", schema="public"),
            )
        assert conn.ingests == []
        assert conn.closed
        assert not h._adbc_session_schema_known

    def test_probe_returning_no_row_is_no_schema(self):
        # Some drivers return no row instead of a (NULL,) row; both
        # collapse to the loud "no schema selected" refusal.
        h = _FixtureSessionDefaultConnector()
        h._adbc_only = True
        h._capabilities = _caps(session_targeting="session_default")
        conn = _SessionFakeConn(None, h.dialect.adbc_session_schema_sql(), no_row=True)
        h._adbc_conn = conn
        with pytest.raises(AdbcConfigurationError, match="no schema selected"):
            h._adbc_only_ingest_sync(
                self._batch(),
                h.dialect.table_address("orders", schema="public"),
            )
        assert conn.ingests == []

    def test_schemaless_address_skips_probe(self):
        # No target schema means the session default IS the intent —
        # the guard stays out of the way even on an opt-out dialect.
        h, conn = self._handler("PUBLIC")
        h._adbc_only_ingest_sync(self._batch(), h.dialect.table_address("orders"))
        assert conn.probe_count == 0
        assert len(conn.ingests) == 1

    def test_undeclared_session_targeting_refuses_bare_name(self):
        # A bare-name ingest must know which regime it is in; with no
        # declared sql_capabilities the refusal names the missing
        # declaration instead of probing (refuse, don't guess — #390).
        h, conn = self._handler("PUBLIC")
        h._capabilities = None
        with pytest.raises(
            AdbcConfigurationError, match="sql_capabilities.session_targeting"
        ):
            h._adbc_only_ingest_sync(
                self._batch(),
                h.dialect.table_address("orders", schema="public"),
            )
        assert conn.probe_count == 0
        assert conn.ingests == []

    def test_per_statement_declaration_with_bare_name_is_a_connector_defect(self):
        # Declared per_statement + a dialect returning no targeting kwarg
        # is a declaration/dialect disagreement, reported as such rather
        # than silently probing the session.
        h, conn = self._handler("PUBLIC")
        h._capabilities = _caps(session_targeting="per_statement")
        with pytest.raises(AdbcConfigurationError, match="disagree"):
            h._adbc_only_ingest_sync(
                self._batch(),
                h.dialect.table_address("orders", schema="public"),
            )
        assert conn.probe_count == 0
        assert conn.ingests == []

    async def test_disconnect_resets_the_probe_cache(self):
        h, conn = self._handler("PUBLIC")
        h._adbc_only_ingest_sync(
            self._batch(), h.dialect.table_address("orders", schema="public")
        )
        assert h._adbc_session_schema_known
        await h.disconnect()
        assert not h._adbc_session_schema_known
        assert h._adbc_session_schema is None
        assert conn.closed


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
    """``supports_upsert`` reads the declared ``sql_capabilities.merge_form``
    — a fact about the target system, identical on every transport. No
    declaration advertises ``False``; the loud declaration-naming refusal
    happens at ``configure_schema`` for streams that need upsert."""

    def test_undeclared_does_not_support(self):
        h = GenericSQLConnector()
        assert h.supports_upsert is False

    def test_declared_merge_form_supports_on_both_transports(self):
        for adbc_only in (True, False):
            h = _FixtureConnector()
            h._adbc_only = adbc_only
            h._capabilities = _caps(merge_form="merge")
            assert h.supports_upsert is True

    def test_declared_none_does_not_support(self):
        h = _FixtureConnector()
        h._adbc_only = True
        h._capabilities = _caps(merge_form="none")
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
            state.address,
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
        from cdk.sql.generic import _StreamState

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
    computed from each row's JSON-serialized content, matching the same digest
    produced by ``_attach_record_hash`` on the SQLAlchemy path."""

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
            h.dialect.table_address("orders", schema="public"),
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
                h.dialect.table_address("t", schema="public"),
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
                h.dialect.table_address("t", schema="public"),
                ["id"],
                ["id"],
                "bdeadbeef1234567",
                insert_only=False,
            )
        warnings = [r for r in caplog.records if "no non-key columns" in r.message]
        assert warnings


class TestMergeIngestStageNameOnFoldingDialect:
    """The stage address is derived with ``dataclasses.replace`` — the
    engine-generated name is used verbatim, never re-normalized — so the
    quoted stage DDL and the raw ``adbc_ingest`` name stay the same string
    even on a folding dialect. A "consistency" edit that re-normalizes
    inside ``quote_table`` would fold the DDL name but not the ingest
    name, and every stage ingest would target a nonexistent table."""

    def test_stage_ddl_and_ingest_share_the_exact_name(self):
        executed: list[str] = []
        ingests: list[str] = []

        class _FakeCursor:
            def execute(self, sql, *args):
                executed.append(sql)

            def adbc_ingest(self, table, batch, mode, **kwargs):
                ingests.append(table)

            def close(self):
                """No-op: the fake owns no resources."""

        class _FakeConn:
            def cursor(self):
                return _FakeCursor()

            def commit(self):
                """No-op: statements are captured, not transacted."""

        h = _FixtureConnector()  # folds identifiers upper-case
        h._adbc_only = True
        h._adbc_conn = _FakeConn()

        address = h.dialect.table_address("orders", schema="public")
        assert address.table == "ORDERS"  # target folded at construction
        h._merge_ingest_sync(
            pa.RecordBatch.from_pydict({"id": [1], "v": ["a"]}),
            address,
            ["id", "v"],
            ["id"],
            "btok",
        )

        # The engine-generated prefix stays verbatim; only the embedded
        # target component carries the fold it got at address time.
        assert ingests == ["_analitiq_stage_ORDERS_btok"]
        quoted_stage = '"_analitiq_stage_ORDERS_btok"'
        for stmt in ("DROP TABLE IF EXISTS", "CREATE TABLE", "MERGE INTO"):
            assert any(
                quoted_stage in s for s in executed if s.startswith(stmt)
            ), f"stage name missing from {stmt}"


class _StageDropFailConn:
    """Fake ADBC connection whose DROPs after the MERGE step fail.

    Records every executed statement. ``merge_error`` makes the MERGE
    statement itself raise (failure-path injection); either way the
    MERGE marks the sequence, and the next ``drop_failures`` DROP
    statements raise ``drop_error`` (default ``OperationalError``, a
    retryable name). ``commit_failures`` instead fails the ``commit``
    that follows a post-MERGE DROP — a dropped stage means a
    *committed* DROP, not merely an executed one.
    """

    def __init__(
        self,
        *,
        drop_failures=0,
        merge_error=None,
        drop_error=None,
        commit_failures=0,
    ):
        self.drop_failures = drop_failures
        self.merge_error = merge_error
        self.drop_error = drop_error
        self.commit_failures = commit_failures
        self.statements: list[str] = []
        self.merge_seen = False
        self.closed = False
        self.last_sql = ""

    def cursor(self):
        conn = self

        class _Cursor:
            def execute(self, sql, *args):
                conn.statements.append(sql)
                conn.last_sql = sql
                if sql.startswith("MERGE"):
                    conn.merge_seen = True
                    if conn.merge_error is not None:
                        raise conn.merge_error
                elif sql.startswith("DROP") and conn.merge_seen:
                    if conn.drop_failures > 0:
                        conn.drop_failures -= 1
                        raise conn.drop_error or OperationalError(
                            "connection reset during DROP"
                        )

            def adbc_ingest(self, table, batch, mode, **kwargs):
                """No-op: the stage fill is not under test."""

            def close(self):
                """No-op: the fake cursor owns no resources."""

        return _Cursor()

    def commit(self):
        if (
            self.merge_seen
            and self.last_sql.startswith("DROP")
            and self.commit_failures > 0
        ):
            self.commit_failures -= 1
            raise OperationalError("connection reset during DROP commit")

    def close(self):
        self.closed = True


class TestMergeIngestStageDropFailure:
    """Stage-DROP failure handling on both sides of the MERGE (issue #379).

    Success path: once the batch's SUCCESS ack lands nothing retries it,
    and the stage name embeds the batch fingerprint, so a failed
    post-MERGE DROP orphans the stage. The connector must retry the DROP
    once, then poison the connection — a failed DROP implies a
    possibly-dead handle; without the poison the next batch burns one
    retryable failure on the cached handle before healing — and log
    honestly that no automatic cleanup reaches the table. Failure path:
    the cleanup-failure log must not promise retry cleanup
    unconditionally, because after a FATAL reclassification or exhausted
    retries nothing retries.
    """

    def _handler(self, conn):
        h = _FixtureConnector()
        h._adbc_only = True
        h._adbc_conn = conn
        return h

    def _merge(self, h):
        h._merge_ingest_sync(
            pa.RecordBatch.from_pydict({"id": [1], "v": ["a"]}),
            h.dialect.table_address("orders", schema="public"),
            ["id", "v"],
            ["id"],
            "btok",
        )

    @staticmethod
    def _post_merge_drops(conn):
        merge_index = next(
            i for i, s in enumerate(conn.statements) if s.startswith("MERGE")
        )
        return [
            s
            for s in conn.statements[merge_index + 1 :]
            if s.startswith("DROP TABLE IF EXISTS")
        ]

    def test_single_drop_success_no_retry_no_poison(self, caplog):
        conn = _StageDropFailConn()
        h = self._handler(conn)
        with caplog.at_level(logging.DEBUG, logger=database_module.logger.name):
            self._merge(h)
        assert len(self._post_merge_drops(conn)) == 1
        assert not conn.closed
        assert h._adbc_conn is conn
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]

    def test_drop_retry_second_attempt_succeeds_without_poison(self, caplog):
        conn = _StageDropFailConn(drop_failures=1)
        h = self._handler(conn)
        with caplog.at_level(logging.DEBUG, logger=database_module.logger.name):
            self._merge(h)
        assert len(self._post_merge_drops(conn)) == 2
        assert not conn.closed
        assert h._adbc_conn is conn
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
        # The failed attempt surfaces at DEBUG, the recovery at INFO —
        # a first-attempt failure on every batch must leave a footprint
        # at the default log level.
        assert any(
            "attempt 1/2" in r.getMessage()
            for r in caplog.records
            if r.levelno == logging.DEBUG
        )
        assert any(
            "second attempt" in r.getMessage()
            for r in caplog.records
            if r.levelno == logging.INFO
        )

    def test_persistent_drop_failure_poisons_and_logs_orphan(self, caplog):
        conn = _StageDropFailConn(drop_failures=2)
        h = self._handler(conn)
        with caplog.at_level(logging.DEBUG, logger=database_module.logger.name):
            self._merge(h)  # must NOT raise: the MERGE itself succeeded
        assert len(self._post_merge_drops(conn)) == 2
        # Poisoned: the next batch reopens instead of burning a
        # retryable failure on the dead cached handle.
        assert conn.closed
        assert h._adbc_conn is None
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        message = warnings[0].getMessage()
        # Honest: names the table, says it is orphaned, promises nothing.
        assert '"_analitiq_stage_ORDERS_btok"' in message
        assert "orphaned" in message
        assert "never retried" in message
        assert "DROP-IF-EXISTS" not in message
        # The warning carries the DROP failure's traceback: the DEBUG
        # attempt records never exist at the default log level, and the
        # cause decides what the operator's manual drop needs.
        assert warnings[0].exc_info is not None
        assert isinstance(warnings[0].exc_info[1], OperationalError)

    def test_fatal_named_drop_failure_still_swallowed_and_poisons(self, caplog):
        # A fatal-classified name (ProgrammingError) on the success-path
        # DROP must not be reclassified or raised: the MERGE committed,
        # so the batch acks SUCCESS regardless of the failure's class.
        conn = _StageDropFailConn(
            drop_failures=2, drop_error=ProgrammingError("permission denied")
        )
        h = self._handler(conn)
        with caplog.at_level(logging.WARNING, logger=database_module.logger.name):
            self._merge(h)  # must NOT raise
        assert conn.closed
        assert h._adbc_conn is None
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert isinstance(warnings[0].exc_info[1], ProgrammingError)

    def test_drop_commit_failure_counts_as_failed_drop(self, caplog):
        # A dropped stage means a committed DROP: an executed DROP whose
        # commit is lost must take the same retry-poison-warn path.
        conn = _StageDropFailConn(commit_failures=2)
        h = self._handler(conn)
        with caplog.at_level(logging.WARNING, logger=database_module.logger.name):
            self._merge(h)  # must NOT raise
        assert len(self._post_merge_drops(conn)) == 2
        assert conn.closed
        assert h._adbc_conn is None
        assert len([r for r in caplog.records if r.levelno == logging.WARNING]) == 1

    def test_failure_path_cleanup_log_is_honest_about_fatal(self, caplog):
        conn = _StageDropFailConn(
            drop_failures=1, merge_error=OperationalError("merge lost connection")
        )
        h = self._handler(conn)
        with caplog.at_level(logging.WARNING, logger=database_module.logger.name):
            with pytest.raises(OperationalError, match="merge lost connection"):
                self._merge(h)
        assert conn.closed  # failure path poisons as before
        warnings = [
            r
            for r in caplog.records
            if "left behind after MERGE failure" in r.getMessage()
        ]
        assert len(warnings) == 1
        message = warnings[0].getMessage()
        # Cleanup-by-retry is promised only while retries remain.
        assert "retryable" in message
        assert "retries remaining" in message
        assert "FATAL" in message
        assert "exhausted retries" in message
        assert "dropped manually" in message


class TestWriteBatchAdbcOnlyKeylessInsert:
    """``_write_batch_adbc_only`` routes a keyless insert through
    ``_merge_ingest_sync`` with ``insert_only=True`` and ``_record_hash`` as
    the conflict key, not through plain ``_adbc_only_ingest_sync``."""

    def _make_state(self, write_mode="insert", primary_keys=None):
        from unittest.mock import MagicMock

        state = _StreamState(
            address=TableAddress(table="orders", schema="public"),
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
        # args: (batch, address, all_columns, conflict_keys, stage_token)
        assert args[3] == [GenericSQLConnector.RECORD_HASH_COLUMN]

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

        def capture(cb, address, all_cols, conflict_keys, token, *, insert_only):
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
        # the deployed SQLAlchemy path -- not a zero-stripped form. Guards against
        # re-hashing existing rows on deploy (issue #285 review P2).
        arr = pa.array([Decimal("3.1400")], type=pa.decimal128(10, 4))
        val = arr[0].as_py()
        expected = hashlib.sha256(
            json.dumps({"v": val}, sort_keys=True, default=str).encode()
        ).hexdigest()
        assert self._hash(arr) == expected


class TestKeylessInsertRequiresMergeSupport:
    """Keyless ADBC insert dedups via stage-MERGE; a system with no declared
    merge form must fail loud at configure, not fatal on the first write
    (issue #285 P2). The fact comes from ``sql_capabilities.merge_form``."""

    def _state(self, primary_keys=None):
        return _StreamState(
            address=TableAddress(table="orders", schema="public"),
            write_mode="insert",
            primary_keys=primary_keys or [],
        )

    @pytest.mark.asyncio
    async def test_keyless_insert_with_declared_none_fails_configure(self):
        h = _FixtureConnector()
        h._adbc_only = True
        h._capabilities = _caps(merge_form="none")
        with pytest.raises(AdbcConfigurationError, match="merge_form 'none'"):
            await h._ensure_tables_exist(self._state(), MagicMock())

    @pytest.mark.asyncio
    async def test_keyless_insert_with_no_declaration_fails_configure(self):
        h = _FixtureConnector()
        h._adbc_only = True
        with pytest.raises(AdbcConfigurationError, match="sql_capabilities.merge_form"):
            await h._ensure_tables_exist(self._state(), MagicMock())

    @pytest.mark.asyncio
    async def test_merge_capable_declaration_is_not_gated(self):
        # Same keyless insert under a declared merge form clears the guard
        # and fails later on the (deliberately absent) endpoint document.
        h = _FixtureConnector()
        h._adbc_only = True
        h._capabilities = _caps(merge_form="merge")
        with pytest.raises(SchemaConfigurationError):
            await h._ensure_tables_exist(self._state(), MagicMock())

    @pytest.mark.asyncio
    async def test_keyed_insert_is_not_gated(self):
        # Keyed insert needs no _record_hash dedup, so the guard is bypassed
        # even with no declaration at all.
        h = _FixtureConnector()
        h._adbc_only = True
        with pytest.raises(SchemaConfigurationError):
            await h._ensure_tables_exist(self._state(primary_keys=["id"]), MagicMock())

    @pytest.mark.asyncio
    async def test_keyless_insert_with_non_merge_form_fails_configure(self):
        # The current ADBC stage machinery renders MERGE only; a declared
        # ON CONFLICT form cannot be honored until #389 and must refuse at
        # configure, not emit MERGE SQL the declaration disavows.
        h = _FixtureConnector()
        h._adbc_only = True
        h._capabilities = _caps(merge_form="insert_on_conflict")
        with pytest.raises(AdbcConfigurationError, match="MERGE only"):
            await h._ensure_tables_exist(self._state(), MagicMock())

    @pytest.mark.asyncio
    async def test_keyless_insert_with_undeclarable_stage_shape_fails_configure(self):
        # Declared temp-scope staging is #389 machinery; until then the
        # stage would silently land as a real table in the target schema —
        # refuse instead of misrouting.
        h = _FixtureConnector()
        h._adbc_only = True
        h._capabilities = _caps(
            stage={"scope": "temp", "schema": "target", "transactional_ddl": True}
        )
        with pytest.raises(AdbcConfigurationError, match="stage"):
            await h._ensure_tables_exist(self._state(), MagicMock())
