"""Connector-level declared facts (issue #401): error taxonomy + concurrency.

Two families of per-system knowledge move from engine guessing (text
heuristics, class-name matching) and per-connector code into declared,
testable facts in the connector definition:

- ``error_map`` — how the system's driver identifies failures. Connectors
  declare facts about their driver's taxonomy (SQLSTATE classes, exception
  class names, vendor codes, HTTP statuses); the engine alone derives the
  verdicts (``AckStatus``, ``FailureCategory``, ``ErrorCode``, backoff).
  Connectors never self-declare verdicts.
- ``concurrency`` — the system's connection ceiling (``max_connections``),
  consumed by the engine's stream fan-out pacing and transport pool sizing.
  Connector-level (not a SQL fact): API systems have connection ceilings too.

Absence is additive, unlike the shape capabilities in
:mod:`cdk.sql.capabilities`: a missing ``merge_form`` blocks an upsert, but a
missing limit or error mapping cannot block anything — absence means "no
declared cap / no declared mapping" and current behavior applies. A runtime
failure caused by an undeclared cap or mapping is a connector defect, fixed
by declaring it — never worked around in the engine. Declared content is
still validated fail-loud: an off-vocabulary category, a malformed
identifier, or an unknown field is a configuration error.

Both blocks reach the worker via the resolved payload channel
(``ConnectionRuntime.resolve_spec`` / ``from_resolved_payload``), the same
channel that delivers the ``sql_capabilities`` block.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .types import AckStatus, FailureCategory

# The engine-owned category vocabulary, aligned with the existing
# FailureCategory and ErrorCode surfaces. The derivation from a category to
# a verdict is engine-owned too — the per-context tables below are defined
# once here so every consumer site (write ack ladder, read classification)
# derives identically. Never in connector.json: connectors declare facts,
# the engine decides verdicts.
ERROR_CATEGORY_VALUES = (
    "transient",
    "config",
    "auth",
    "unreachable",
    "rate_limited",
    "write_rejected",
)

# Write context: declared category -> (ack status, failure category).
# Retryable categories carry WRITE_REJECTED so an exhausted retry classifies
# as a destination write failure, exactly like the undeclared retryable
# branch of the ack ladder.
DECLARED_WRITE_VERDICTS: dict[str, tuple[AckStatus, FailureCategory]] = {
    "transient": (
        AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
        FailureCategory.FAILURE_CATEGORY_WRITE_REJECTED,
    ),
    "unreachable": (
        AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
        FailureCategory.FAILURE_CATEGORY_WRITE_REJECTED,
    ),
    "rate_limited": (
        AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
        FailureCategory.FAILURE_CATEGORY_WRITE_REJECTED,
    ),
    "auth": (
        AckStatus.ACK_STATUS_FATAL_FAILURE,
        FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT,
    ),
    "config": (
        AckStatus.ACK_STATUS_FATAL_FAILURE,
        FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT,
    ),
    "write_rejected": (
        AckStatus.ACK_STATUS_FATAL_FAILURE,
        FailureCategory.FAILURE_CATEGORY_WRITE_REJECTED,
    ),
}

# Read context: declared category -> deterministic (retrying cannot heal it;
# the engine shell fails the stream fatally instead of retrying).
# ``write_rejected`` reads as deterministic: the system refused the
# operation, and an identical request cannot fare better.
DECLARED_READ_DETERMINISTIC: dict[str, bool] = {
    "transient": False,
    "unreachable": False,
    "rate_limited": False,
    "auth": True,
    "config": True,
    "write_rejected": True,
}

# Totality, enforced at import: a future vocabulary member without a verdict
# in every context must fail at startup, not fall through inside a failure
# path.
for _table_name, _table in (
    ("DECLARED_WRITE_VERDICTS", DECLARED_WRITE_VERDICTS),
    ("DECLARED_READ_DETERMINISTIC", DECLARED_READ_DETERMINISTIC),
):
    _unmapped = set(ERROR_CATEGORY_VALUES) - set(_table)
    if _unmapped:
        raise RuntimeError(
            f"{_table_name} must map every declared error category; "
            f"missing: {sorted(_unmapped)}"
        )

# Identifier grammar per family. Keys are driver-taxonomy facts:
# - sqlstate: a 2-char SQLSTATE class ("08") or a full 5-char state ("28000")
# - exception: a Python exception class name, matched anywhere in the MRO
# - vendor_code: the driver's numeric vendor code, as a string ("1045")
# - http: an HTTP status code ("429")
_SQLSTATE_KEY = re.compile(r"^[0-9A-Z]{2}([0-9A-Z]{3})?$")
_EXCEPTION_KEY = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_VENDOR_CODE_KEY = re.compile(r"^-?[0-9]+$")
_HTTP_KEY = re.compile(r"^[1-5][0-9]{2}$")

# Cap the chain walk so a self-referential cause chain can never spin.
_MAX_CHAIN_DEPTH = 50


class ConnectorDeclarationError(ValueError):
    """A connector-level declared block (``error_map`` / ``concurrency``) is malformed.

    A configuration defect: the connector definition (or the resolved payload
    built from it) carries a block that does not match the published
    vocabulary or identifier grammar. Deterministic — retrying cannot
    succeed; the fix is authoring-side in the connector's ``connector.json``.
    """


@dataclass(frozen=True)
class DeclaredMatch:
    """One declared classification: which fact matched and what it declares.

    ``family``/``identifier`` name the connector's declared fact (a class
    name, an SQLSTATE, a code) — developer-chosen identifiers, safe for
    logs and failure summaries. ``category`` is the engine-vocabulary value
    the consumer derives its verdict from.
    """

    family: str
    identifier: str
    category: str


def _require_category(value: Any, path: str, *, source: str) -> str:
    if value not in ERROR_CATEGORY_VALUES:
        raise ConnectorDeclarationError(
            f"error_map.{path} in {source} is {value!r}; expected one of "
            f"{list(ERROR_CATEGORY_VALUES)}"
        )
    return str(value)


def _parse_family(
    block: Mapping[str, Any],
    family: str,
    key_grammar: re.Pattern[str],
    *,
    source: str,
) -> dict[str, str]:
    raw = block.get(family)
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        raise ConnectorDeclarationError(
            f"error_map.{family} in {source} must be an object mapping "
            f"identifiers to categories, got {type(raw).__name__}"
        )
    parsed: dict[str, str] = {}
    for key, value in raw.items():
        if not isinstance(key, str) or not key_grammar.match(key):
            raise ConnectorDeclarationError(
                f"error_map.{family} in {source} declares malformed "
                f"identifier {key!r}; expected the {family} key grammar "
                f"({key_grammar.pattern})"
            )
        parsed[key] = _require_category(value, f"{family}.{key}", source=source)
    return parsed


@dataclass(frozen=True)
class ErrorMap:
    """Typed view of a connector's declared ``error_map`` block.

    Lookup precedence within one exception is most-specific-first: full
    SQLSTATE, then SQLSTATE class, then vendor code, then exception class
    name (MRO order, subclass before base). Across a chained exception the
    outermost member that yields any match wins. HTTP statuses are not
    read off exceptions — HTTP call sites pass the status explicitly to
    :meth:`match_http`.
    """

    sqlstate: Mapping[str, str]
    exception: Mapping[str, str]
    vendor_code: Mapping[str, str]
    http: Mapping[int, str]

    @classmethod
    def from_declaration(
        cls, block: Mapping[str, Any], *, source: str = "<connector definition>"
    ) -> ErrorMap:
        """Parse a declared block, failing loud on any grammar mismatch.

        Every family is optional (absence declares nothing); a declared
        family's identifiers and categories are validated strictly, and an
        unknown family fails.
        """
        if not isinstance(block, Mapping):
            raise ConnectorDeclarationError(
                f"error_map in {source} must be an object, "
                f"got {type(block).__name__}"
            )
        known = {"sqlstate", "exception", "vendor_code", "http"}
        unknown = set(block) - known
        if unknown:
            raise ConnectorDeclarationError(
                f"error_map in {source} carries unknown families "
                f"{sorted(unknown)}; expected a subset of {sorted(known)}"
            )
        http_raw = _parse_family(block, "http", _HTTP_KEY, source=source)
        return cls(
            sqlstate=_parse_family(block, "sqlstate", _SQLSTATE_KEY, source=source),
            exception=_parse_family(block, "exception", _EXCEPTION_KEY, source=source),
            vendor_code=_parse_family(
                block, "vendor_code", _VENDOR_CODE_KEY, source=source
            ),
            http={int(key): category for key, category in http_raw.items()},
        )

    # ------------------------------------------------------------------
    # Lookup
    # ------------------------------------------------------------------

    def match_http(self, status: int) -> DeclaredMatch | None:
        """Return the declared classification for an HTTP status, if any."""
        category = self.http.get(status)
        if category is None:
            return None
        return DeclaredMatch(family="http", identifier=str(status), category=category)

    def match_names(self, names: Any) -> DeclaredMatch | None:
        """Match declared exception class names against a name collection.

        For sites where only class names survive (the engine side of a
        process boundary, where the live type is a wrapper and the original
        class name was promoted from the worker's ``error_type:`` prefix).
        Iteration order of *names* is the caller's specificity order; the
        first declared name wins.
        """
        for name in names:
            category = self.exception.get(name)
            if category is not None:
                return DeclaredMatch(
                    family="exception", identifier=name, category=category
                )
        return None

    def match_exception(self, exc: BaseException) -> DeclaredMatch | None:
        """Return the declared classification for a live exception, if any.

        Walks the exception chain outermost-first; per member the
        most-specific declared fact wins (full SQLSTATE, SQLSTATE class,
        vendor code, then exception class name in MRO order).
        """
        for member in _walk_chain(exc):
            match = self._match_member(member)
            if match is not None:
                return match
        return None

    def _match_member(self, member: BaseException) -> DeclaredMatch | None:
        sqlstate = _read_sqlstate(member)
        if sqlstate is not None:
            for candidate in (sqlstate, sqlstate[:2]):
                category = self.sqlstate.get(candidate)
                if category is not None:
                    return DeclaredMatch(
                        family="sqlstate", identifier=candidate, category=category
                    )
        vendor = _read_vendor_code(member)
        if vendor is not None:
            category = self.vendor_code.get(vendor)
            if category is not None:
                return DeclaredMatch(
                    family="vendor_code", identifier=vendor, category=category
                )
        return self.match_names(cls.__name__ for cls in type(member).__mro__)


def _read_sqlstate(member: BaseException) -> str | None:
    """Read a driver's SQLSTATE off the common attribute spellings.

    ``sqlstate`` is the DBAPI/ADBC spelling; ``pgcode`` is psycopg's. Only a
    well-formed 5-char state is used — a driver that stuffs something else
    into the attribute declares nothing.
    """
    for attr in ("sqlstate", "pgcode"):
        value = getattr(member, attr, None)
        if isinstance(value, str):
            candidate = value.upper()
            if len(candidate) == 5 and _SQLSTATE_KEY.match(candidate):
                return candidate
    return None


def _read_vendor_code(member: BaseException) -> str | None:
    """Read a driver's numeric vendor code off the common attribute spellings.

    ``errno`` is the MySQL-family spelling — but on an ``OSError`` the same
    attribute is the operating-system errno (ECONNREFUSED is 111, not a
    driver fact), so OSError subclasses never contribute a vendor code.
    """
    value = getattr(member, "vendor_code", None)
    if value is None and not isinstance(member, OSError):
        value = getattr(member, "errno", None)
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str) and _VENDOR_CODE_KEY.match(value):
        return value
    return None


def _walk_chain(exc: BaseException) -> list[BaseException]:
    """Flatten an exception, its group members, and its cause/context chain.

    Outermost-first, depth-capped, identity-deduplicated — the same walk
    shape the engine's classifier uses, kept local because the CDK cannot
    import engine modules.
    """
    seen: set[int] = set()
    out: list[BaseException] = []
    stack: list[tuple[BaseException, int]] = [(exc, 0)]
    while stack:
        current, depth = stack.pop()
        if current is None or id(current) in seen or depth > _MAX_CHAIN_DEPTH:
            continue
        seen.add(id(current))
        out.append(current)
        nested: list[BaseException] = []
        if isinstance(current, BaseExceptionGroup):
            nested.extend(current.exceptions)
        for attr in ("__cause__", "__context__"):
            linked = getattr(current, attr, None)
            if isinstance(linked, BaseException):
                nested.append(linked)
        for child in nested:
            stack.append((child, depth + 1))
    return out


def parse_declared_error_map(
    block: Any, *, source: str = "<connector definition>"
) -> ErrorMap | None:
    """Parse an optional ``error_map`` declaration: ``None`` stays ``None``.

    The single entry point both sides use — the trusted engine reading the
    connector definition and the worker reading its resolved payload — so
    "undeclared" means the same thing everywhere.
    """
    if block is None:
        return None
    return ErrorMap.from_declaration(block, source=source)


def parse_declared_concurrency(
    block: Any, *, source: str = "<connector definition>"
) -> int | None:
    """Parse an optional ``concurrency`` declaration to its connection ceiling.

    Returns the declared ``max_connections`` (a positive int), or ``None``
    when the block is absent — no declared ceiling, current behavior applies.
    """
    if block is None:
        return None
    if not isinstance(block, Mapping):
        raise ConnectorDeclarationError(
            f"concurrency in {source} must be an object, got {type(block).__name__}"
        )
    known = {"max_connections"}
    unknown = set(block) - known
    if unknown:
        raise ConnectorDeclarationError(
            f"concurrency in {source} carries unknown fields {sorted(unknown)}; "
            f"expected a subset of {sorted(known)}"
        )
    value = block.get("max_connections")
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ConnectorDeclarationError(
            f"concurrency.max_connections in {source} is {value!r}; "
            f"expected a positive integer"
        )
    return value
