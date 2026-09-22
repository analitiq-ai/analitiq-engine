"""Declared SQL dialect capabilities (issue #390; spec sql-write-path §5).

SQL-shape capabilities are facts about the target system — catalog
addressability, session-targeting regime, merge form, bulk-load mechanism,
stage shape. They are not derivable from protocol conformance, so they are
declared as data in the connector definition's ``sql_capabilities`` block
and validated by the published contract before anything here reads it. The
dialect class keeps only *rendering*; whether the system has a shape comes
from this block.

This module is the CDK's typed view of that block. The engine folds the
declared block into the resolved worker payload (the same channel that
delivers transport specs), and every side — engine, worker, conformance
kit — reads it here: the engine and the kit off the validated model, the
worker off the block the engine folded in, which came from the same
model. ``None`` (no block declared) is legal; every consumer treats a
needed-but-undeclared shape fact as a loud configuration error via
:func:`undeclared_capability_error` — no base-class default ever fills in
a guess. The one exception is the ``limits`` member (issue #401), whose
absence is additive: an undeclared cap means "no declared cap" and
current behavior applies.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, get_args

from analitiq.contracts.connector import SqlCapabilities as ContractSqlCapabilities
from analitiq.contracts.connector import SqlStageCapabilities
from pydantic import BaseModel

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


#: The values the consumer sites branch on, per shape fact. Hand-kept because
#: each records what the branches were written to handle; deriving them from
#: the contract would make a value no branch handles look handled. Checked
#: against the contract's ``Literal`` below, so a contract release that adds
#: a value fails at import instead of falling into an else-branch mid-run.
_HANDLED_VALUES: Mapping[tuple[type[BaseModel], str], frozenset[str]] = {
    (ContractSqlCapabilities, "catalog"): frozenset({"none", "read", "full"}),
    (ContractSqlCapabilities, "session_targeting"): frozenset(
        {"per_statement", "session_default"}
    ),
    (ContractSqlCapabilities, "merge_form"): frozenset(
        {"merge", "insert_on_conflict", "insert_on_duplicate_key", "none"}
    ),
    (SqlStageCapabilities, "scope"): frozenset({"temp", "real"}),
    (SqlStageCapabilities, "schema_"): frozenset({"target", "dedicated"}),
}

for (_model, _fact), _handled in _HANDLED_VALUES.items():
    _declared = frozenset(get_args(_model.model_fields[_fact].annotation))
    if _handled != _declared:
        raise TypeError(
            f"{_model.__name__}.{_fact}: the contract declares "
            f"{sorted(_declared)} but the engine handles {sorted(_handled)}"
        )


class SqlCapabilitiesError(ValueError):
    """A consumer site needs a shape fact the connector does not declare.

    The one thing this can mean: the contract owns the block's shape and
    vocabulary, so a declaration that reaches here is well formed — what it
    can still be is silent about a fact some site needs. Deterministic —
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
    limits: SqlLimits = field(default_factory=SqlLimits.undeclared)

    @property
    def supports_upsert(self) -> bool:
        """Whether the declared merge form gives the system an upsert path."""
        return self.merge_form != "none"

    def bulk_mechanism(self, transport_type: str) -> str | None:
        """Return the declared mechanism for *transport_type*, if any.

        ``None`` means the family lands via executemany — the default,
        not a refusal: bulk is a declared speed slot, and absence of a
        declaration is the one capability fact whose meaning is defined
        (spec sql-write-path section 2) rather than guessed.
        """
        return self.bulk_load.get(transport_type)

    @classmethod
    def from_declaration(cls, block: Mapping[str, Any]) -> SqlCapabilities:
        """Read a declared block the published contract has already validated.

        The contract requires all five shape facts inside a declared block
        and closes their vocabularies, so they are read as given. ``limits``
        (issue #401) is the one additive member: caps are optional facts
        whose absence means "no declared cap", never a refusal.
        """
        stage = block["stage"]
        limits = block.get("limits") or {}
        return cls(
            catalog=block["catalog"],
            session_targeting=block["session_targeting"],
            merge_form=block["merge_form"],
            bulk_load=dict(block["bulk_load"]),
            stage=StageCapabilities(
                scope=stage["scope"],
                schema=stage["schema"],
                dedicated_schema=stage.get("dedicated_schema"),
                transactional_ddl=stage["transactional_ddl"],
            ),
            limits=SqlLimits(
                max_bind_params=limits.get("max_bind_params"),
                max_identifier_len=limits.get("max_identifier_len"),
            ),
        )


def parse_declared_capabilities(block: Any) -> SqlCapabilities | None:
    """Read an optional declaration: ``None`` stays ``None`` (undeclared).

    The single entry point every side uses — the engine reading the
    connector definition, the worker reading its resolved payload, the
    conformance kit reading the definition under test — so "undeclared"
    means the same thing everywhere.
    """
    if block is None:
        return None
    return SqlCapabilities.from_declaration(block)
