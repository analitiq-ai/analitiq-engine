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
consumer treats a needed-but-undeclared fact as a loud configuration error
via :func:`undeclared_capability_error` — no base-class default ever fills
in a guess.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

CATALOG_VALUES = ("none", "read", "full")
SESSION_TARGETING_VALUES = ("per_statement", "session_default")
MERGE_FORM_VALUES = ("merge", "insert_on_conflict", "insert_on_duplicate_key", "none")
STAGE_SCOPE_VALUES = ("temp", "real")
STAGE_SCHEMA_VALUES = ("target", "dedicated")

#: SQL transport families a bulk mechanism can be declared for. A bulk
#: mechanism is a fact about a transport, not about the connector as a
#: whole — ``copy_from`` needs the driver's wire connection,
#: ``adbc_ingest`` needs an ADBC cursor — so ``bulk_load`` maps each
#: family to its mechanism instead of declaring one connector-wide value
#: that only one family could run.
SQL_TRANSPORT_TYPES = ("sqlalchemy", "adbc")

#: Mechanisms implemented by the connector's dialect (its ``bulk_land``
#: hook). ``adbc_ingest`` is not among them: it is the ADBC backend's own
#: native landing and involves no dialect code.
DIALECT_IMPLEMENTED_BULK_MECHANISMS = frozenset(
    {"copy_from", "load_data_local_infile", "load_job"}
)

#: The mechanisms each transport family can actually run. Declaring a
#: mechanism for a family that cannot run it is unrepresentable — the
#: parse refuses — instead of a state downstream checks must catch
#: (``adbc_ingest`` on the SQLAlchemy family would silently fall back to
#: executemany on every batch).
BULK_MECHANISMS_BY_TRANSPORT: dict[str, frozenset[str]] = {
    "sqlalchemy": DIALECT_IMPLEMENTED_BULK_MECHANISMS,
    "adbc": DIALECT_IMPLEMENTED_BULK_MECHANISMS | {"adbc_ingest"},
}


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
class SqlCapabilities:
    """Typed view of a connector's declared ``sql_capabilities`` block.

    ``bulk_load`` maps a SQL transport family (``sqlalchemy`` / ``adbc``)
    to the bulk mechanism its connections land with; an absent family
    lands via executemany (the default that needs no declaration). An
    empty mapping declares no bulk mechanism anywhere.
    """

    catalog: str
    session_targeting: str
    merge_form: str
    bulk_load: Mapping[str, str]
    stage: StageCapabilities

    @property
    def supports_upsert(self) -> bool:
        """Whether the declared merge form gives the system an upsert path."""
        return self.merge_form != "none"

    def bulk_mechanism(self, transport_type: str) -> str | None:
        """Return the declared mechanism for *transport_type*, if any.

        ``None`` means the family lands via executemany — the default,
        not a refusal: bulk is a declared speed slot, and absence of a
        declaration is the one capability fact whose meaning is defined
        (ADR sql-write-path-v2 section 2) rather than guessed.
        """
        return self.bulk_load.get(transport_type)

    @classmethod
    def from_declaration(
        cls, block: Mapping[str, Any], *, source: str = "<connector definition>"
    ) -> SqlCapabilities:
        """Parse a declared block, failing loud on any vocabulary mismatch.

        The published contract validates the same shape engine-side; this
        parse re-validates because the block crosses the process boundary in
        the resolved worker payload. All five facts are required inside a
        declared block — a partial declaration is a configuration error, not
        a set of implicit defaults.
        """
        if not isinstance(block, Mapping):
            raise SqlCapabilitiesError(
                f"sql_capabilities in {source} must be an object, "
                f"got {type(block).__name__}"
            )
        known = {"catalog", "session_targeting", "merge_form", "bulk_load", "stage"}
        unknown = set(block) - known
        if unknown:
            raise SqlCapabilitiesError(
                f"sql_capabilities in {source} carries unknown fields "
                f"{sorted(unknown)}; expected exactly {sorted(known)}"
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
            bulk_load=cls._parse_bulk_load(block.get("bulk_load"), source=source),
            stage=stage,
        )

    @staticmethod
    def _parse_bulk_load(block: Any, *, source: str) -> dict[str, str]:
        """Parse the per-transport bulk mapping, refusing unrunnable pairs."""
        if not isinstance(block, Mapping):
            raise SqlCapabilitiesError(
                f"sql_capabilities.bulk_load in {source} must be an object "
                f"mapping a SQL transport type to its bulk mechanism (an "
                f"empty object declares none), got {block!r}"
            )
        mechanisms: dict[str, str] = {}
        for transport_type, mechanism in block.items():
            if transport_type not in SQL_TRANSPORT_TYPES:
                raise SqlCapabilitiesError(
                    f"sql_capabilities.bulk_load in {source} names transport "
                    f"type {transport_type!r}; expected one of "
                    f"{list(SQL_TRANSPORT_TYPES)}"
                )
            allowed = BULK_MECHANISMS_BY_TRANSPORT[transport_type]
            if not isinstance(mechanism, str) or mechanism not in allowed:
                detail = ""
                if mechanism == "adbc_ingest":
                    detail = (
                        " (adbc_ingest is the ADBC backend's own landing "
                        "and cannot run on the sqlalchemy transport)"
                    )
                raise SqlCapabilitiesError(
                    f"sql_capabilities.bulk_load.{transport_type} in {source} "
                    f"is {mechanism!r}; expected one of "
                    f"{sorted(allowed)}{detail}"
                )
            mechanisms[transport_type] = str(mechanism)
        return mechanisms

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
