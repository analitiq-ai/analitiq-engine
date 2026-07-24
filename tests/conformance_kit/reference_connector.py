"""The reference connector the kit's acceptance runs against.

A minimal but real PostgreSQL connector on the sanctioned v2 surface:
the connector class carries ``dialect_class`` and nothing else, and the
dialect implements exactly the hooks its declaration
(``reference/definition/connector.json``) routes calls to. It lives in
the engine repo's tests — the CDK itself ships no per-system dialect —
and doubles as the worked example of the post-ADR override surface.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import sqlalchemy

from cdk.sql.dialects import SqlDialect, TableAddress
from cdk.sql.generic import GenericSQLConnector


class ReferencePostgresDialect(SqlDialect):
    """PostgreSQL renderings for the stage-then-merge write path."""

    name = "conformance_reference"

    system_schemas = ("information_schema", "pg_catalog", "pg_toast")

    def stage_table_sql(
        self, stage: TableAddress, target: TableAddress, *, temp: bool
    ) -> str:
        """``CREATE [TEMPORARY] TABLE`` shaped like the target."""
        keyword = "CREATE TEMPORARY TABLE" if temp else "CREATE TABLE"
        return (
            f"{keyword} {self.quote_table(stage)} "
            f"(LIKE {self.quote_table(target)} INCLUDING DEFAULTS)"
        )

    def merge_statement_sql(
        self,
        stage: TableAddress,
        target: TableAddress,
        conflict_keys: Sequence[str],
        columns: Sequence[str],
    ) -> str:
        """PostgreSQL's declared merge form: ``INSERT ... ON CONFLICT``."""
        column_list = ", ".join(self.quote_ident(c) for c in columns)
        key_list = ", ".join(self.quote_ident(c) for c in conflict_keys)
        update_columns = [c for c in columns if c not in set(conflict_keys)]
        # Identifiers are dialect-quoted; values never enter this text.
        statement = (
            f"INSERT INTO {self.quote_table(target)} ({column_list}) "  # nosec B608
            f"SELECT {column_list} FROM {self.quote_table(stage)} "
            f"ON CONFLICT ({key_list}) "
        )
        if not update_columns:
            return statement + "DO NOTHING"
        assignments = ", ".join(
            f"{self.quote_ident(c)} = EXCLUDED.{self.quote_ident(c)}"
            for c in update_columns
        )
        return statement + f"DO UPDATE SET {assignments}"

    def bulk_land(
        self,
        conn: Any,
        stage: TableAddress,
        batch: Any,
        *,
        runtime: Any,
    ) -> bool:
        """Land the batch through the declared bulk mechanism.

        The fixture's mechanism is a single multi-row text INSERT on the
        transport connection — the COPY wire protocol itself would add
        driver plumbing without adding anything the kit certifies (the
        contract is that the mechanism's landed contents equal the
        executemany fallback's, which tier 2 compares).
        """
        rows = batch.to_pylist()
        if not rows:
            return False
        columns = list(batch.schema.names)
        column_list = ", ".join(self.quote_ident(c) for c in columns)
        placeholders = ", ".join(f":{c}" for c in columns)
        # Identifiers are dialect-quoted; values bind as parameters.
        statement = sqlalchemy.text(
            f"INSERT INTO {self.quote_table(stage)} "  # nosec B608
            f"({column_list}) VALUES ({placeholders})"
        )
        conn.execute(statement, rows)
        return True

    def schema_is_implicit_default(self, schema_name: str) -> bool:
        """``public`` exists in every database; never CREATE it."""
        return not schema_name or schema_name.lower() == "public"

    def sqlalchemy_pre_ddl(self, schema_name: str) -> list[str]:
        """Create a non-default schema before the table DDL."""
        if self.schema_is_implicit_default(schema_name):
            return []
        return [f"CREATE SCHEMA IF NOT EXISTS {self.quote_ident(schema_name)}"]


class ReferenceConnector(GenericSQLConnector):
    """The reference connector: ``dialect_class`` and nothing else."""

    dialect_class = ReferencePostgresDialect
