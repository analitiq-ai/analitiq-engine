"""StageWritePlan building: naming, placement, and the ANSI renderers.

The plan is the whole interface between the facade's semantics and a
transport backend's mechanics (ADR sql-write-path-v2 §3), so its rendering
rules are pinned here as pure-function tests: the deterministic stage name
whose uniqueness token no identifier budget can cut off, the declared
scope/schema placement, and the two statements the CDK renders itself.
"""

from __future__ import annotations

import logging

import pytest

from cdk.sql.capabilities import SqlCapabilities
from cdk.sql.dialects import SqlDialect, TableAddress
from cdk.sql.exceptions import SchemaConfigurationError
from cdk.sql.write_plan import (
    build_stage_write_plan,
    render_anti_join_insert_sql,
    render_append_sql,
    stage_table_name,
)

from .conftest import caps_block


class _StagingDialect(SqlDialect):
    name = "staging"

    def stage_table_sql(self, stage, target, *, temp):
        keyword = "CREATE TEMPORARY TABLE" if temp else "CREATE TABLE"
        return f"{keyword} {self.quote_table(stage)} LIKE {self.quote_table(target)}"

    def merge_statement_sql(self, stage, target, conflict_keys, columns):
        return f"MERGE INTO {self.quote_table(target)} USING {self.quote_table(stage)}"


def _caps(**kwargs) -> SqlCapabilities:
    return SqlCapabilities.from_declaration(caps_block(**kwargs))


def _plan(**overrides):
    args = dict(
        target=TableAddress(table="events", schema="public"),
        columns=("id", "v"),
        write_mode="insert",
        conflict_keys=[],
        identity=["id"],
        truncate_now=False,
        run_id="r1",
        stream_id="s1",
        batch_seq=1,
    )
    caps = overrides.pop("caps", _caps())
    args.update(overrides)
    return build_stage_write_plan(_StagingDialect(), caps, **args)


class TestStageName:
    def test_deterministic_per_batch_identity(self):
        a = stage_table_name(
            "t", run_id="r", stream_id="s", batch_seq=1, max_identifier_length=63
        )
        b = stage_table_name(
            "t", run_id="r", stream_id="s", batch_seq=1, max_identifier_length=63
        )
        c = stage_table_name(
            "t", run_id="r", stream_id="s", batch_seq=2, max_identifier_length=63
        )
        assert a == b
        assert a != c

    def test_token_precedes_the_tail(self):
        # Hash-first grammar: the fixed prefix and the uniqueness token
        # come before the readability tail, so truncation can only ever
        # cut the tail — two long-named targets can never collapse into
        # one stage name.
        name = stage_table_name(
            "orders", run_id="r", stream_id="s", batch_seq=1, max_identifier_length=63
        )
        assert name.startswith("_analitiq_stage_b")
        assert name.endswith("_orders")

    def test_tail_truncates_to_the_identifier_budget(self):
        long_target = "a" * 100
        name = stage_table_name(
            long_target,
            run_id="r",
            stream_id="s",
            batch_seq=1,
            max_identifier_length=63,
        )
        assert len(name) == 63
        short = stage_table_name(
            long_target,
            run_id="r",
            stream_id="s",
            batch_seq=1,
            max_identifier_length=34,
        )
        # No room for any tail: the token alone is the name.
        assert short == stage_table_name(
            "", run_id="r", stream_id="s", batch_seq=1, max_identifier_length=34
        )
        assert len(short) <= 34

    def test_different_streams_never_collide(self):
        a = stage_table_name(
            "t", run_id="r", stream_id="s1", batch_seq=1, max_identifier_length=63
        )
        b = stage_table_name(
            "t", run_id="r", stream_id="s2", batch_seq=1, max_identifier_length=63
        )
        assert a != b


class TestStagePlacement:
    def test_temp_scope_stage_has_no_schema(self):
        plan = _plan(caps=_caps(stage_scope="temp"))
        assert plan.scope == "temp"
        assert plan.stage.schema == ""
        assert plan.stage.catalog == ""
        assert "TEMPORARY" in plan.create_stage_sql

    def test_real_scope_target_placement_shares_the_target_schema(self):
        plan = _plan(caps=_caps(stage_scope="real"))
        assert plan.scope == "real"
        assert plan.stage.schema == "public"
        assert "TEMPORARY" not in plan.create_stage_sql

    def test_real_scope_dedicated_placement_uses_the_declared_schema(self):
        plan = _plan(
            caps=_caps(
                stage_scope="real",
                stage_schema="dedicated",
                dedicated_schema="_analitiq",
            )
        )
        assert plan.stage.schema == "_analitiq"

    def test_transaction_shape_comes_from_the_declaration(self):
        assert _plan(caps=_caps(transactional_ddl=True)).transactional is True
        assert _plan(caps=_caps(transactional_ddl=False)).transactional is False


class TestModeStatements:
    def test_insert_renders_one_set_based_anti_join(self):
        plan = _plan()
        assert plan.mode_sql == render_anti_join_insert_sql(
            _StagingDialect(), plan.stage, plan.target, ("id", "v"), ["id"]
        )
        assert "WHERE NOT EXISTS" in plan.mode_sql
        assert 't."id" = s."id"' in plan.mode_sql

    def test_insert_without_identity_refuses(self):
        with pytest.raises(SchemaConfigurationError, match="identity"):
            _plan(identity=[])

    def test_upsert_renders_through_the_dialect_merge_hook(self):
        plan = _plan(write_mode="upsert", conflict_keys=["id"])
        assert plan.mode_sql.startswith("MERGE INTO")

    def test_upsert_with_only_key_columns_warns(self, caplog):
        with caplog.at_level(logging.WARNING, logger="cdk.sql.write_plan"):
            _plan(write_mode="upsert", conflict_keys=["id", "v"])
        assert any("no non-key columns" in r.getMessage() for r in caplog.records)

    def test_truncate_insert_renders_a_plain_append(self):
        plan = _plan(write_mode="truncate_insert", truncate_now=True)
        assert plan.mode_sql == render_append_sql(
            _StagingDialect(), plan.stage, plan.target, ("id", "v")
        )
        assert "NOT EXISTS" not in plan.mode_sql
        # The emptying statement is the dialect's DELETE render, present
        # only on the read's first batch.
        assert plan.truncate_sql == 'DELETE FROM "public"."events"'
        assert _plan(write_mode="truncate_insert").truncate_sql is None

    def test_drop_stage_is_idempotent(self):
        plan = _plan()
        assert plan.drop_stage_sql.startswith("DROP TABLE IF EXISTS")
