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
from cdk.sql.generic import AdbcConfigurationError, GenericSQLConnector
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
    """Dialect implementing the SA upsert statement hook, as a declaring
    connector's package dialect must until #388 replaces the machinery."""

    name = "rendering"

    def build_sqlalchemy_upsert(self, table, records, conflict_keys):
        return MagicMock()


class _RenderingConnector(GenericSQLConnector):
    dialect_class = _RenderingDialect


class _StageRenderingDialect(SqlDialect):
    """Dialect implementing the ADBC stage DDL hook, as a declaring ADBC
    connector's package dialect must until #389 replaces the machinery."""

    name = "stage_rendering"

    def adbc_stage_table_sql(self, stage_qualified, target_qualified):
        return (
            f"CREATE TABLE {stage_qualified} AS SELECT * FROM "
            f"{target_qualified} WHERE FALSE"
        )


class _StageRenderingConnector(GenericSQLConnector):
    dialect_class = _StageRenderingDialect


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
        # Declaration/dialect disagreement caught at handshake: the SA
        # upsert machinery renders through build_sqlalchemy_upsert until
        # #388; a declaring connector whose dialect lacks it must fail
        # before DDL, not on the first batch.
        handler = _upsert_handler()
        handler._capabilities = SqlCapabilities.from_declaration(
            caps_block(merge_form="insert_on_conflict")
        )
        with pytest.raises(SchemaConfigurationError, match="build_sqlalchemy_upsert"):
            await handler.configure_schema(_upsert_spec())

    @pytest.mark.asyncio
    async def test_adbc_upsert_with_non_merge_form_refuses(self):
        # The current ADBC stage machinery renders MERGE only (#389 lands
        # declared forms); an ON CONFLICT declaration refuses at handshake.
        handler = _upsert_handler()
        handler._engine = None
        handler._adbc_only = True
        handler._capabilities = SqlCapabilities.from_declaration(
            caps_block(merge_form="insert_on_conflict", stage_scope="real")
        )
        with pytest.raises(AdbcConfigurationError, match="MERGE only"):
            await handler.configure_schema(_upsert_spec())

    @pytest.mark.asyncio
    async def test_adbc_upsert_with_undeclarable_stage_shape_refuses(self):
        # Temp-scope staging is #389 machinery; refusing beats silently
        # landing a real stage table in the target schema.
        handler = _upsert_handler()
        handler._engine = None
        handler._adbc_only = True
        handler._capabilities = SqlCapabilities.from_declaration(
            caps_block(merge_form="merge", stage_scope="temp")
        )
        with pytest.raises(AdbcConfigurationError, match="stage"):
            await handler.configure_schema(_upsert_spec())

    @pytest.mark.asyncio
    async def test_adbc_upsert_without_stage_renderer_refuses(self):
        # Declaration/dialect disagreement, ADBC flavor: the stage-MERGE
        # path renders its stage through adbc_stage_table_sql; a declaring
        # connector without the override fails at handshake, not on the
        # first batch.
        handler = _upsert_handler()
        handler._engine = None
        handler._adbc_only = True
        handler._capabilities = SqlCapabilities.from_declaration(
            caps_block(merge_form="merge", stage_scope="real")
        )
        with pytest.raises(AdbcConfigurationError, match="adbc_stage_table_sql"):
            await handler.configure_schema(_upsert_spec())

    @pytest.mark.asyncio
    async def test_adbc_upsert_with_current_machinery_shape_configures(self):
        handler = _upsert_handler(cls=_StageRenderingConnector)
        handler._engine = None
        handler._adbc_only = True
        handler._capabilities = SqlCapabilities.from_declaration(
            caps_block(merge_form="merge", stage_scope="real")
        )
        assert await handler.configure_schema(_upsert_spec()) is True


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
        runtime.close = AsyncMock()
        from unittest.mock import patch

        with patch("cdk.sql.generic.materialize_runtime", new=AsyncMock()):
            with pytest.raises(SqlCapabilitiesError, match="sql_capabilities.catalog"):
                await handler.connect(runtime)
        runtime.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_connect_binds_facade_and_dialect(self):
        handler = GenericSQLConnector()
        runtime = MagicMock()
        runtime.connector_id = "demo"
        runtime.declared_sql_capabilities = caps_block(catalog="full")
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
