"""Declared SQL dialect capabilities (issue #390).

Three surfaces:

* the parse: a declared ``sql_capabilities`` block validates against the
  published vocabulary fail-loud at the process boundary — partial or
  off-vocabulary declarations never reach a consumer site;
* the refusal shape: every needed-but-undeclared fact refuses through
  ``undeclared_capability_error``, naming the missing declaration;
* the payload channel: the block rides ``resolve_spec()`` into the worker
  bootstrap and rebuilds identically via ``from_resolved_payload`` — the
  worker consumes the same declaration the engine validated, never a
  guessed default because ``connector.json`` was out of reach.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from cdk.connection_runtime import ConnectionRuntime
from cdk.sql.capabilities import (
    SqlCapabilities,
    SqlCapabilitiesError,
    parse_declared_capabilities,
    undeclared_capability_error,
)
from cdk.sql.dialects import SqlDialect
from cdk.sql.exceptions import SchemaConfigurationError
from cdk.sql.generic import GenericSQLConnector
from cdk.types import SchemaSpec, WriteMode

from .conftest import caps_block


class TestParse:
    def test_full_block_parses(self):
        caps = SqlCapabilities.from_declaration(
            caps_block(
                catalog="full",
                session_targeting="session_default",
                merge_form="merge",
                bulk_load="copy_from",
                stage_scope="real",
                stage_schema="dedicated",
                dedicated_schema="_analitiq",
                transactional_ddl=False,
            )
        )
        assert caps.catalog == "full"
        assert caps.session_targeting == "session_default"
        assert caps.merge_form == "merge"
        assert caps.bulk_load == "copy_from"
        assert caps.stage.scope == "real"
        assert caps.stage.schema == "dedicated"
        assert caps.stage.dedicated_schema == "_analitiq"
        assert caps.stage.transactional_ddl is False

    def test_none_is_undeclared(self):
        assert parse_declared_capabilities(None) is None

    def test_declared_block_parses_through_the_single_entry_point(self):
        caps = parse_declared_capabilities(caps_block())
        assert isinstance(caps, SqlCapabilities)

    @pytest.mark.parametrize(
        "field,value",
        [
            ("catalog", "sometimes"),
            ("session_targeting", "both"),
            ("merge_form", "upsert"),
            ("bulk_load", "fast"),
        ],
    )
    def test_off_vocabulary_value_fails_naming_the_field(self, field, value):
        block = caps_block()
        block[field] = value
        with pytest.raises(SqlCapabilitiesError, match=f"sql_capabilities.{field}"):
            SqlCapabilities.from_declaration(block)

    def test_missing_fact_fails(self):
        # All five facts are required inside a declared block: a partial
        # declaration is a configuration error, not implicit defaults.
        block = caps_block()
        del block["merge_form"]
        with pytest.raises(SqlCapabilitiesError, match="merge_form"):
            SqlCapabilities.from_declaration(block)

    def test_unknown_field_fails(self):
        block = caps_block()
        block["fast_mode"] = True
        with pytest.raises(SqlCapabilitiesError, match="fast_mode"):
            SqlCapabilities.from_declaration(block)

    def test_missing_stage_fails(self):
        block = caps_block()
        del block["stage"]
        with pytest.raises(SqlCapabilitiesError, match="stage"):
            SqlCapabilities.from_declaration(block)

    @pytest.mark.parametrize(
        "field,value,path",
        [
            ("scope", "global", "sql_capabilities.stage.scope"),
            ("schema", "session", "sql_capabilities.stage.schema"),
        ],
    )
    def test_off_vocabulary_stage_value_names_the_full_path(self, field, value, path):
        block = caps_block()
        block["stage"][field] = value
        with pytest.raises(SqlCapabilitiesError, match=path):
            SqlCapabilities.from_declaration(block)

    def test_unknown_stage_field_fails(self):
        block = caps_block()
        block["stage"]["auto_expire"] = True
        with pytest.raises(SqlCapabilitiesError, match="auto_expire"):
            SqlCapabilities.from_declaration(block)

    def test_empty_dedicated_schema_fails(self):
        with pytest.raises(SqlCapabilitiesError, match="dedicated_schema"):
            SqlCapabilities.from_declaration(
                caps_block(stage_schema="dedicated", dedicated_schema="")
            )

    def test_dedicated_schema_required_iff_dedicated(self):
        with pytest.raises(SqlCapabilitiesError, match="dedicated_schema"):
            SqlCapabilities.from_declaration(caps_block(stage_schema="dedicated"))
        with pytest.raises(SqlCapabilitiesError, match="dedicated_schema"):
            SqlCapabilities.from_declaration(
                caps_block(stage_schema="target", dedicated_schema="_analitiq")
            )

    def test_non_bool_transactional_ddl_fails(self):
        block = caps_block()
        block["stage"]["transactional_ddl"] = "yes"
        with pytest.raises(SqlCapabilitiesError, match="transactional_ddl"):
            SqlCapabilities.from_declaration(block)

    def test_non_mapping_block_fails(self):
        with pytest.raises(SqlCapabilitiesError, match="must be an object"):
            parse_declared_capabilities("full")

    def test_supports_upsert_derives_from_merge_form(self):
        assert SqlCapabilities.from_declaration(
            caps_block(merge_form="insert_on_conflict")
        ).supports_upsert
        assert not SqlCapabilities.from_declaration(
            caps_block(merge_form="none")
        ).supports_upsert


class TestRefusalShape:
    def test_names_the_declaration_and_the_need(self):
        err = undeclared_capability_error("catalog", need="this read targets 'x'")
        message = str(err)
        assert "sql_capabilities.catalog" in message
        assert "this read targets 'x'" in message
        assert "never guesses" in message


class _RenderingDialect(SqlDialect):
    """Dialect implementing the stage-then-merge rendering hooks, as a
    declaring connector's package dialect must (issue #388)."""

    name = "rendering"

    def stage_table_sql(self, stage, target, *, temp):
        return f"CREATE TABLE {self.quote_table(stage)} LIKE {self.quote_table(target)}"

    def merge_statement_sql(self, stage, target, conflict_keys, columns):
        return "INSERT ... ON CONFLICT DO UPDATE"


class _RenderingConnector(GenericSQLConnector):
    dialect_class = _RenderingDialect


def _upsert_handler(cls=GenericSQLConnector):
    handler = cls()
    handler._connected = True
    handler._engine = MagicMock()
    handler._runtime = MagicMock()
    handler._runtime.type_mapper_for = MagicMock(return_value=MagicMock())
    handler._endpoint_refs = {
        "s1": {"scope": "connector", "connection_id": "c", "endpoint_id": "e"}
    }
    handler._stream_endpoints = {
        "s1": {
            "database_object": {"name": "orders", "schema": "public"},
            "columns": [
                {
                    "name": "id",
                    "native_type": "BIGINT",
                    "arrow_type": "Int64",
                    "nullable": False,
                }
            ],
            "primary_keys": ["id"],
        }
    }
    handler.set_stream_conflict_keys({"s1": ["id"]})
    handler._ensure_tables_exist = AsyncMock()
    return handler


def _upsert_spec() -> SchemaSpec:
    return SchemaSpec(
        stream_id="s1",
        version=1,
        write_mode=WriteMode.WRITE_MODE_UPSERT,
        ack_timeout_seconds=30,
    )


class TestConfigureSchemaUpsertGate:
    """An upsert stream refuses at handshake time — before any DDL — when
    the merge form is undeclared or declared 'none' (the issue #390
    acceptance shape: a customer-safe config error naming the declaration)."""

    @pytest.mark.asyncio
    async def test_undeclared_merge_form_refuses_naming_the_declaration(self):
        handler = _upsert_handler()
        with pytest.raises(
            SchemaConfigurationError, match="sql_capabilities.merge_form"
        ):
            await handler.configure_schema(_upsert_spec())
        handler._ensure_tables_exist.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_declared_none_refuses(self):
        handler = _upsert_handler()
        handler._capabilities = SqlCapabilities.from_declaration(
            caps_block(merge_form="none")
        )
        with pytest.raises(SchemaConfigurationError, match="merge_form 'none'"):
            await handler.configure_schema(_upsert_spec())
        handler._ensure_tables_exist.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_declared_merge_form_with_renderer_configures(self):
        handler = _upsert_handler(cls=_RenderingConnector)
        handler._capabilities = SqlCapabilities.from_declaration(
            caps_block(merge_form="insert_on_conflict")
        )
        assert await handler.configure_schema(_upsert_spec()) is True

    @pytest.mark.asyncio
    async def test_declared_merge_form_without_sa_renderer_refuses(self):
        # Declaration/dialect disagreement caught at handshake: the
        # upsert's stage-to-target statement renders through the dialect's
        # merge_statement_sql; a declaring connector whose dialect lacks
        # it must fail before DDL, not on the first batch.
        handler = _upsert_handler()
        handler._capabilities = SqlCapabilities.from_declaration(
            caps_block(merge_form="insert_on_conflict")
        )
        with pytest.raises(SchemaConfigurationError, match="merge_statement_sql"):
            await handler.configure_schema(_upsert_spec())

    @pytest.mark.asyncio
    async def test_declared_stage_without_stage_renderer_refuses(self):
        # The stage gate covers every SQLAlchemy-path write mode: a
        # declared stage shape whose dialect cannot render the stage DDL
        # is a disagreement, refused at handshake.
        handler = _upsert_handler()
        handler._capabilities = SqlCapabilities.from_declaration(caps_block())
        handler._streams.clear()
        spec = SchemaSpec(
            stream_id="s1",
            version=1,
            write_mode=WriteMode.WRITE_MODE_INSERT,
            ack_timeout_seconds=30,
        )
        with pytest.raises(SchemaConfigurationError, match="stage_table_sql"):
            await handler.configure_schema(spec)

    @pytest.mark.asyncio
    async def test_undeclared_block_refuses_any_sqlalchemy_write(self):
        # Not just upsert: every SQLAlchemy-path write lands in a stage,
        # so an undeclared block refuses insert streams too — at
        # handshake, naming the missing declaration.
        handler = _upsert_handler()
        spec = SchemaSpec(
            stream_id="s1",
            version=1,
            write_mode=WriteMode.WRITE_MODE_INSERT,
            ack_timeout_seconds=30,
        )
        with pytest.raises(SchemaConfigurationError, match="sql_capabilities.stage"):
            await handler.configure_schema(spec)
        handler._ensure_tables_exist.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_adbc_upsert_gate_is_transport_uniform(self):
        # The gate is the same on the ADBC transport (issue #389): any
        # declared merge form with a rendering dialect configures — the
        # interim MERGE-only / real-stage-only machinery shape is gone.
        handler = _upsert_handler(cls=_RenderingConnector)
        handler._engine = None
        handler._adbc_only = True
        handler._capabilities = SqlCapabilities.from_declaration(
            caps_block(merge_form="insert_on_conflict", stage_scope="temp")
        )
        assert await handler.configure_schema(_upsert_spec()) is True

    @pytest.mark.asyncio
    async def test_adbc_upsert_without_merge_renderer_refuses(self):
        # Declaration/dialect disagreement on the ADBC transport: the
        # upsert statement renders through the same merge_statement_sql
        # hook as the SQLAlchemy path; a declaring connector without the
        # override fails at handshake, not on the first batch.
        handler = _upsert_handler()
        handler._engine = None
        handler._adbc_only = True
        handler._capabilities = SqlCapabilities.from_declaration(
            caps_block(merge_form="merge", stage_scope="real")
        )
        with pytest.raises(SchemaConfigurationError, match="merge_statement_sql"):
            await handler.configure_schema(_upsert_spec())

    @pytest.mark.asyncio
    async def test_adbc_stage_gate_applies_to_every_write_mode(self):
        # The stage gate covers the ADBC transport identically: a
        # declared stage shape whose dialect cannot render the stage DDL
        # refuses an insert stream at handshake.
        handler = _upsert_handler()
        handler._engine = None
        handler._adbc_only = True
        handler._capabilities = SqlCapabilities.from_declaration(caps_block())
        spec = SchemaSpec(
            stream_id="s1",
            version=1,
            write_mode=WriteMode.WRITE_MODE_INSERT,
            ack_timeout_seconds=30,
        )
        with pytest.raises(SchemaConfigurationError, match="stage_table_sql"):
            await handler.configure_schema(spec)


class TestConnectBinding:
    @pytest.mark.asyncio
    async def test_malformed_declaration_fails_at_connect_and_releases_runtime(
        self,
    ):
        # The trusted side already parses at config load; connect()
        # re-validates at the process boundary once the transport is live.
        # materialize() acquired the runtime and the caller never
        # disconnects a handler whose connect() raised, so the failed bind
        # must release the ref itself.
        handler = GenericSQLConnector()
        runtime = MagicMock()
        runtime.connector_id = "demo"
        runtime.declared_sql_capabilities = caps_block(catalog="everything")
        runtime.declared_error_map = None
        runtime.close = AsyncMock()
        from unittest.mock import patch

        with (
            patch("cdk.sql.generic.materialize_runtime", new=AsyncMock()),
            pytest.raises(SqlCapabilitiesError, match="sql_capabilities.catalog"),
        ):
            await handler.connect(runtime)
        runtime.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_connect_binds_facade_and_dialect(self):
        handler = GenericSQLConnector()
        runtime = MagicMock()
        runtime.connector_id = "demo"
        runtime.declared_sql_capabilities = caps_block(catalog="full")
        runtime.declared_error_map = None
        runtime.is_adbc = False
        runtime.is_sync_sqlalchemy = False
        runtime.driver = "postgresql"
        runtime.engine = MagicMock()
        from unittest.mock import patch

        with patch("cdk.sql.generic.materialize_runtime", new=AsyncMock()):
            await handler.connect(runtime)
        assert handler._capabilities is not None
        assert handler._capabilities.catalog == "full"
        assert handler.dialect.capabilities is handler._capabilities

    @staticmethod
    def _adbc_runtime(**overrides):
        runtime = MagicMock()
        runtime.connector_id = "demo"
        runtime.declared_sql_capabilities = caps_block(bulk_load="adbc_ingest")
        runtime.declared_error_map = None
        runtime.is_adbc = True
        runtime.driver = "snowflake"
        runtime.close = AsyncMock()
        for name, value in overrides.items():
            setattr(runtime, name, value)
        return runtime

    @pytest.mark.asyncio
    async def test_adbc_connect_selects_the_adbc_backend_and_opens_eagerly(self):
        from unittest.mock import patch

        from cdk.sql.adbc_backend import AdbcBackend

        handler = GenericSQLConnector()
        adbc_conn = MagicMock()
        runtime = self._adbc_runtime(
            open_adbc_connection=MagicMock(return_value=adbc_conn)
        )
        with patch("cdk.sql.generic.materialize_runtime", new=AsyncMock()):
            await handler.connect(runtime)
        assert handler._adbc_only is True
        assert isinstance(handler._backend, AdbcBackend)
        # Eager open: a bad credential fails at connect(), not first batch.
        runtime.open_adbc_connection.assert_called_once()
        assert handler._backend._conn is adbc_conn
        # The backend read the declared bulk mechanism off the binding.
        assert handler._backend._bulk_load == "adbc_ingest"

    @pytest.mark.asyncio
    async def test_adbc_eager_open_failure_releases_runtime_and_wraps(self):
        from unittest.mock import patch

        handler = GenericSQLConnector()
        runtime = self._adbc_runtime(
            open_adbc_connection=MagicMock(side_effect=RuntimeError("bad key"))
        )
        with (
            patch("cdk.sql.generic.materialize_runtime", new=AsyncMock()),
            pytest.raises(ConnectionError, match="ADBC connection failed"),
        ):
            await handler.connect(runtime)
        # materialize() acquired the runtime; the failed connect releases
        # the ref itself (the caller never disconnects a handler whose
        # connect() raised).
        runtime.close.assert_awaited_once()
        assert handler._backend is None
        assert handler._connected is False

    @pytest.mark.asyncio
    async def test_runtime_release_failure_never_masks_the_connect_error(self):
        from unittest.mock import patch

        handler = GenericSQLConnector()
        runtime = self._adbc_runtime(
            open_adbc_connection=MagicMock(side_effect=RuntimeError("bad key")),
            close=AsyncMock(side_effect=RuntimeError("dispose failed")),
        )
        with (
            patch("cdk.sql.generic.materialize_runtime", new=AsyncMock()),
            pytest.raises(ConnectionError, match="bad key"),
        ):
            await handler.connect(runtime)


class TestPayloadChannel:
    """The declared block crosses the trusted/worker boundary verbatim."""

    def _trusted_runtime(self, definition):
        return ConnectionRuntime(
            raw_config={"path": "/tmp/out"},
            connection_id="db-1",
            connector_id="demo",
            connector_type="database",
            resolver=AsyncMock(resolve=AsyncMock(return_value={})),
            connector_definition=definition,
        )

    @pytest.mark.asyncio
    async def test_declared_block_rides_resolve_spec_and_rebuilds(self):
        declared = caps_block(catalog="read", merge_form="insert_on_conflict")
        runtime = self._trusted_runtime(
            {"connector_id": "demo", "sql_capabilities": declared}
        )
        assert runtime.declared_sql_capabilities == declared

        payload = runtime_payload = await runtime.resolve_spec()
        assert runtime_payload["sql_capabilities"] == declared

        worker_runtime = ConnectionRuntime.from_resolved_payload(payload)
        assert worker_runtime.declared_sql_capabilities == declared
        # Parsed worker-side exactly as the trusted side would parse it.
        caps = parse_declared_capabilities(worker_runtime.declared_sql_capabilities)
        assert caps is not None and caps.catalog == "read"

    @pytest.mark.asyncio
    async def test_undeclared_stays_undeclared_across_the_boundary(self):
        runtime = self._trusted_runtime({"connector_id": "demo"})
        assert runtime.declared_sql_capabilities is None
        payload = await runtime.resolve_spec()
        assert payload["sql_capabilities"] is None
        worker_runtime = ConnectionRuntime.from_resolved_payload(payload)
        assert worker_runtime.declared_sql_capabilities is None
