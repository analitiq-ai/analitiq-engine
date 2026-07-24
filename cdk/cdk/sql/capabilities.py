"""Declared SQL dialect capabilities (issue #390; ADR sql-write-path-v2 §5).

SQL-shape capabilities are facts about the target system — catalog
addressability, session-targeting regime, merge form, bulk-load mechanism,
stage shape. They are not derivable from protocol conformance, so they are
declared as data in the connector definition's ``sql_capabilities`` block
and validated by the published contract engine-side. The dialect class keeps
only *rendering*; whether the system has a shape comes from this block.

This module is the CDK's typed view of that block. The engine folds the
declared block into the resolved worker payload (the same channel that
delivers transport specs), and both sides parse it here — fail-loud, at the
process boundary, so a malformed or partially-declared block never reaches a
consumer site. ``None`` (no block declared) is legal at parse time; every
consumer treats a needed-but-undeclared shape fact as a loud configuration
error via :func:`undeclared_capability_error` — no base-class default ever
fills in a guess. The one exception is the ``limits`` member (issue #401),
whose absence is additive: an undeclared cap means "no declared cap" and
current behavior applies.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

CATALOG_VALUES = ("none", "read", "full")
SESSION_TARGETING_VALUES = ("per_statement", "session_default")
MERGE_FORM_VALUES = ("merge", "insert_on_conflict", "insert_on_duplicate_key", "none")
BULK_LOAD_VALUES = (
    "none",
    "copy_from",
    "load_data_local_infile",
    "adbc_ingest",
    "load_job",
)
STAGE_SCOPE_VALUES = ("temp", "real")
STAGE_SCHEMA_VALUES = ("target", "dedicated")


class SqlCapabilitiesError(ValueError):
    """The ``sql_capabilities`` declaration is malformed or missing a needed fact.

    A configuration defect: the connector definition (or the resolved payload
    built from it) either carries a block that does not match the published
    vocabulary, or omits a fact a consumer site needs. Deterministic —
    retrying cannot succeed; the fix is authoring-side in the connector's
    ``connector.json``.
    """


def undeclared_capability_error(fact: str, *, need: str) -> SqlCapabilitiesError:
    """Build the one refusal for a needed-but-undeclared capability.

    Every consumer site raises through here so the error always names the
    missing declaration (`sql_capabilities.<fact>`) and what needed it.
    """
    return SqlCapabilitiesError(
        f"connector declares no sql_capabilities.{fact}, but {need}. "
        f"Declare sql_capabilities in the connector definition "
        f"(connector.json); the engine never guesses an undeclared "
        f"capability."
    )


def _require_enum(
    value: Any, path: str, allowed: tuple[str, ...], *, source: str
) -> str:
    """Validate one declared fact; *path* is the full field path the error names."""
    if value not in allowed:
        raise SqlCapabilitiesError(
            f"sql_capabilities.{path} in {source} is {value!r}; expected "
            f"one of {list(allowed)}"
        )
    return str(value)


@dataclass(frozen=True)
class StageCapabilities:
    """Declared stage-table shape (``sql_capabilities.stage``)."""

    scope: str
    schema: str
    dedicated_schema: str | None
    transactional_ddl: bool


@dataclass(frozen=True)
class SqlLimits:
    """Declared system limits (``sql_capabilities.limits``, issue #401).

    Additive semantics, unlike the shape facts: an undeclared limit is
    ``None`` — no declared cap, current behavior applies. It never blocks a
    write; a runtime failure caused by an undeclared cap is a connector
    defect, fixed by declaring the cap.

    - ``max_bind_params``: the statement's bind-parameter ceiling (MSSQL
      2100, SQLite 999/32766). The executemany stage landing chunks rows so
      no statement exceeds it.
    - ``max_identifier_len``: the identifier byte ceiling. Stage-name
      rendering and DDL validate against it instead of assuming the
      dialect default.
    """

    max_bind_params: int | None
    max_identifier_len: int | None

    @classmethod
    def undeclared(cls) -> SqlLimits:
        return cls(max_bind_params=None, max_identifier_len=None)


@dataclass(frozen=True)
class SqlCapabilities:
    """Typed view of a connector's declared ``sql_capabilities`` block."""

    catalog: str
    session_targeting: str
    merge_form: str
    bulk_load: str
    stage: StageCapabilities
    limits: SqlLimits = field(default_factory=SqlLimits.undeclared)

    @property
    def supports_upsert(self) -> bool:
        """Whether the declared merge form gives the system an upsert path."""
        return self.merge_form != "none"

    @classmethod
    def from_declaration(
        cls, block: Mapping[str, Any], *, source: str = "<connector definition>"
    ) -> SqlCapabilities:
        """Parse a declared block, failing loud on any vocabulary mismatch.

        The published contract validates the same shape engine-side; this
        parse re-validates because the block crosses the process boundary in
        the resolved worker payload. All five shape facts are required inside
        a declared block — a partial declaration is a configuration error,
        not a set of implicit defaults. ``limits`` (issue #401) is the one
        additive member: caps are optional facts whose absence means "no
        declared cap", never a refusal.
        """
        if not isinstance(block, Mapping):
            raise SqlCapabilitiesError(
                f"sql_capabilities in {source} must be an object, "
                f"got {type(block).__name__}"
            )
        required = {"catalog", "session_targeting", "merge_form", "bulk_load", "stage"}
        unknown = set(block) - required - {"limits"}
        if unknown:
            raise SqlCapabilitiesError(
                f"sql_capabilities in {source} carries unknown fields "
                f"{sorted(unknown)}; expected {sorted(required)} plus "
                f"optional 'limits'"
            )
        stage_block = block.get("stage")
        if not isinstance(stage_block, Mapping):
            raise SqlCapabilitiesError(
                f"sql_capabilities.stage in {source} must be an object; "
                f"it declares the stage-table shape (scope, schema placement, "
                f"transactional_ddl)"
            )
        stage = cls._parse_stage(stage_block, source=source)
        return cls(
            catalog=_require_enum(
                block.get("catalog"), "catalog", CATALOG_VALUES, source=source
            ),
            session_targeting=_require_enum(
                block.get("session_targeting"),
                "session_targeting",
                SESSION_TARGETING_VALUES,
                source=source,
            ),
            merge_form=_require_enum(
                block.get("merge_form"), "merge_form", MERGE_FORM_VALUES, source=source
            ),
            bulk_load=_require_enum(
                block.get("bulk_load"), "bulk_load", BULK_LOAD_VALUES, source=source
            ),
            stage=stage,
            limits=cls._parse_limits(block.get("limits"), source=source),
        )

    @staticmethod
    def _parse_limits(block: Any, *, source: str) -> SqlLimits:
        if block is None:
            return SqlLimits.undeclared()
        if not isinstance(block, Mapping):
            raise SqlCapabilitiesError(
                f"sql_capabilities.limits in {source} must be an object, "
                f"got {type(block).__name__}"
            )
        known = {"max_bind_params", "max_identifier_len"}
        unknown = set(block) - known
        if unknown:
            raise SqlCapabilitiesError(
                f"sql_capabilities.limits in {source} carries unknown fields "
                f"{sorted(unknown)}; expected a subset of {sorted(known)}"
            )
        values: dict[str, int | None] = {}
        for name in known:
            value = block.get(name)
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value < 1
            ):
                raise SqlCapabilitiesError(
                    f"sql_capabilities.limits.{name} in {source} is "
                    f"{value!r}; expected a positive integer"
                )
            values[name] = value
        return SqlLimits(
            max_bind_params=values["max_bind_params"],
            max_identifier_len=values["max_identifier_len"],
        )

    @staticmethod
    def _parse_stage(block: Mapping[str, Any], *, source: str) -> StageCapabilities:
        known = {"scope", "schema", "dedicated_schema", "transactional_ddl"}
        unknown = set(block) - known
        if unknown:
            raise SqlCapabilitiesError(
                f"sql_capabilities.stage in {source} carries unknown fields "
                f"{sorted(unknown)}; expected a subset of {sorted(known)}"
            )
        scope = _require_enum(
            block.get("scope"), "stage.scope", STAGE_SCOPE_VALUES, source=source
        )
        schema = _require_enum(
            block.get("schema"), "stage.schema", STAGE_SCHEMA_VALUES, source=source
        )
        dedicated = block.get("dedicated_schema")
        if schema == "dedicated":
            if not isinstance(dedicated, str) or not dedicated:
                raise SqlCapabilitiesError(
                    f"sql_capabilities.stage in {source} declares schema "
                    f"'dedicated' but no dedicated_schema name"
                )
        elif dedicated is not None:
            raise SqlCapabilitiesError(
                f"sql_capabilities.stage in {source} declares "
                f"dedicated_schema {dedicated!r} but schema placement "
                f"{schema!r}; dedicated_schema is only meaningful with "
                f"schema 'dedicated'"
            )
        transactional = block.get("transactional_ddl")
        if not isinstance(transactional, bool):
            raise SqlCapabilitiesError(
                f"sql_capabilities.stage.transactional_ddl in {source} is "
                f"{transactional!r}; expected true or false"
            )
        return StageCapabilities(
            scope=scope,
            schema=schema,
            dedicated_schema=dedicated if schema == "dedicated" else None,
            transactional_ddl=transactional,
        )


def parse_declared_capabilities(
    block: Any, *, source: str = "<connector definition>"
) -> SqlCapabilities | None:
    """Parse an optional declaration: ``None`` stays ``None`` (undeclared).

    The single entry point both sides use — the trusted engine reading the
    connector definition and the worker reading its resolved payload — so
    "undeclared" means the same thing everywhere.
    """
    if block is None:
        return None
    return SqlCapabilities.from_declaration(block, source=source)


def bind_dialect_capabilities(dialect: Any, runtime: Any) -> SqlCapabilities | None:
    """Parse the runtime's declared block and attach it to *dialect*.

    The one binding rule for every entry point that receives a runtime —
    the ``GenericSQLConnector`` facade and the standalone control-plane
    helpers (``create_table``, ``list_schemas`` / ``list_tables`` /
    ``list_columns``) — so a declaring connector's catalog gate behaves
    identically however the CDK is driven. Reads the runtime's
    ``declared_sql_capabilities`` strictly: a runtime object without the
    attribute is a wiring defect, not an undeclared connector.
    """
    caps = parse_declared_capabilities(
        runtime.declared_sql_capabilities,
        source=f"connector {runtime.connector_id!r}",
    )
    dialect.capabilities = caps
    return caps
