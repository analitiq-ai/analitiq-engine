"""Connector-level declared facts (issue #401, generalized by #513): error taxonomy
+ concurrency.

Two families of per-system knowledge move from engine guessing (text
heuristics, class-name matching) and per-connector code into declared,
testable facts in the connector definition:

- ``error_map`` — how the system's driver identifies failures. A connector
  declares which of its own exception's attributes carries its native error
  signal (``key_attrs``, e.g. ``"sqlstate"``, ``"vendor_code"``, or the
  reserved ``"__exception_class__"`` sentinel for matching the exception's
  class name), and a flat map from that native code to one of the engine's
  six categories (``codes``). A provider whose failures ride an HTTP status
  declares that separately (``http``), since a status is read from the
  response, never the exception. The engine reads the declared attribute
  generically — it no longer hardcodes which attribute a family reads or in
  what order (issue #513 retired the four fixed, closed families —
  ``sqlstate``/``exception``/``vendor_code``/``http`` as parallel
  engine-typed blocks — because a closed family list cannot express every
  source/destination's native error shape). A connector whose signal needs
  more than one flat attribute read (nested body inspection, a computed
  match) overrides :meth:`~cdk.base_handler.BaseDestinationHandler.classify_error`
  instead — the code escape hatch, consulted only when the declarative map
  finds nothing, mirroring :class:`~cdk.sql.dialects.SqlDialect`'s
  thin-declarative-default / thick-code-override pattern. The engine alone
  derives the verdicts (``AckStatus``, ``FailureCategory``, ``ErrorCode`` —
  and with them whether the engine's bounded retry applies) from the
  category either path returns. Connectors never self-declare verdicts.
- ``concurrency`` — the system's connection ceiling (``max_connections``),
  consumed by the engine's stream fan-out pacing. Connector-level (not a
  SQL fact): API systems have connection ceilings too.

Absence is additive, unlike the shape capabilities in
:mod:`cdk.sql.capabilities`: a missing ``merge_form`` blocks an upsert, but a
missing limit or error mapping cannot block anything — absence means "no
declared cap / no declared mapping" and current behavior applies. A runtime
failure caused by an undeclared cap or mapping is a connector defect, fixed
by declaring it (or implementing ``classify_error``) — never worked around
in the engine. Declared content is still validated fail-loud: an
off-vocabulary category or a malformed block is a configuration error. A
connector definition still carrying the retired fixed-family shape (a
top-level ``sqlstate``/``exception``/``vendor_code`` key) fails the same
way: unknown fields, not a silently reinterpreted block.

Both blocks reach the worker via the resolved payload channel
(``ConnectionRuntime.resolve_spec`` / ``from_resolved_payload``), the same
channel that delivers the ``sql_capabilities`` block.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from .types import AckStatus, FailureCategory

logger = logging.getLogger(__name__)

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
# branch of the ack ladder. MappingProxyType: consumers read these tables at
# verdict time; handing out the live dict would let any importer rewrite
# engine verdicts.
DECLARED_WRITE_VERDICTS: Mapping[str, tuple[AckStatus, FailureCategory]] = {
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
DECLARED_READ_DETERMINISTIC: Mapping[str, bool] = {
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

DECLARED_WRITE_VERDICTS = MappingProxyType(dict(DECLARED_WRITE_VERDICTS))
DECLARED_READ_DETERMINISTIC = MappingProxyType(dict(DECLARED_READ_DETERMINISTIC))

# Reserved ``key_attrs`` entry: match the exception's class name (walked up
# the MRO, subclass before base) instead of reading a plain attribute. The
# one piece of matching logic that isn't a bare ``getattr`` — every driver
# exception has a class name, so this needs no connector code, unlike a
# computed or nested signal (which is what ``classify_error`` is for).
CLASS_NAME_SIGNAL = "__exception_class__"

# A ``key_attrs`` entry is either the sentinel above or a plain attribute
# name (whatever the connector's own driver exposes -- "sqlstate",
# "pgcode", "vendor_code", "errno", anything). No engine-enforced grammar
# beyond "a non-empty identifier-shaped string": the attribute's existence
# and meaning are the connector's own driver's business, not the engine's.
_KEY_ATTR = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# HTTP status keys are a genuinely universal, fixed-width scalar (unlike a
# driver attribute name), so this one grammar stays enforced.
_HTTP_KEY = re.compile(r"^[1-5][0-9]{2}$")


class ConnectorDeclarationError(ValueError):
    """A connector-level declared block (``error_map`` / ``concurrency``) is malformed.

    A configuration defect: the connector definition (or the resolved payload
    built from it) carries a block that does not match the published
    vocabulary or identifier grammar. Deterministic — retrying cannot
    succeed; the fix is authoring-side in the connector's ``connector.json``.
    """


@dataclass(frozen=True)
class DeclaredMatch:
    """One declared classification: which signal matched and what it declares.

    ``signal``/``value`` name the connector's declared fact (a ``key_attrs``
    entry and the native code read off it, or ``"http"`` and a status) —
    developer-chosen identifiers, safe for logs and failure summaries.
    ``category`` is the engine-vocabulary value the consumer derives its
    verdict from; membership is re-checked here so a match constructed
    outside :class:`ErrorMap` can never smuggle an off-vocabulary category
    into a verdict-table lookup.
    """

    signal: str
    value: str
    category: str

    def __post_init__(self) -> None:
        if self.category not in ERROR_CATEGORY_VALUES:
            raise ConnectorDeclarationError(
                f"DeclaredMatch category {self.category!r} is not in the "
                f"engine vocabulary {list(ERROR_CATEGORY_VALUES)}"
            )


def require_declared_category(category: str, *, source: str) -> str:
    """Validate a category returned by a connector-authored code hook.

    Used at the two code-hook return paths —
    :meth:`~cdk.base_handler.BaseDestinationHandler.classify_error` and a
    dialect's ``classify()`` — so an off-vocabulary string from either
    fails loud at the classification site instead of a ``KeyError`` inside
    :data:`DECLARED_WRITE_VERDICTS` or :data:`DECLARED_READ_DETERMINISTIC`.
    The declarative ``error_map`` lookup is validated separately, at parse
    time (:func:`_require_category`) and again on
    :class:`DeclaredMatch` construction — a hook's return is checked here
    instead because it is computed at classification time, not parse time.
    """
    if category not in ERROR_CATEGORY_VALUES:
        raise ConnectorDeclarationError(
            f"{source} classified an error as {category!r}, which is not "
            f"in the engine vocabulary {list(ERROR_CATEGORY_VALUES)}"
        )
    return category


def call_declared_hook(
    hook: Callable[..., str | None], *args: Any, source: str
) -> str | None:
    """Call a connector-authored classification hook, never letting it raise.

    Covers both shapes this mechanism has: ``classify_error(exc)`` and a
    dialect's ``classify(status, body)``. Both are untrusted,
    potentially-AI-authored connector code, called from inside the
    ``except`` block that is in the middle of reporting the *original*
    failure — a crash here must not displace it, mirroring the same
    guarantee :meth:`ErrorMap.match_exception` already makes for the
    declarative read (see its docstring). An off-vocabulary *return* is a
    different failure mode — the hook did run and answered, just wrongly —
    and still raises loud via :func:`require_declared_category`, since that
    is the hook's own declared output, not an implementation crash.
    """
    try:
        category = hook(*args)
    except Exception:
        # *args may carry an untrusted response body -- never interpolated
        # here, only the source label (a class/method name this process
        # chose, not connector-controlled content).
        logger.warning(
            "%s raised; treating this classification attempt as unclaimed",
            source,
            exc_info=True,
        )
        return None
    if category is None:
        return None
    return require_declared_category(category, source=source)


def _require_category(value: Any, path: str, *, source: str) -> str:
    if value not in ERROR_CATEGORY_VALUES:
        raise ConnectorDeclarationError(
            f"error_map.{path} in {source} is {value!r}; expected one of "
            f"{list(ERROR_CATEGORY_VALUES)}"
        )
    return str(value)


def _parse_key_attrs(block: Mapping[str, Any], *, source: str) -> tuple[str, ...]:
    raw = block.get("key_attrs")
    if raw is None:
        return ()
    if not isinstance(raw, list) or not raw:
        raise ConnectorDeclarationError(
            f"error_map.key_attrs in {source} must be a non-empty list of "
            f"attribute names, got {type(raw).__name__}"
        )
    parsed: list[str] = []
    for entry in raw:
        if not isinstance(entry, str) or not (
            entry == CLASS_NAME_SIGNAL or _KEY_ATTR.match(entry)
        ):
            raise ConnectorDeclarationError(
                f"error_map.key_attrs in {source} declares malformed entry "
                f"{entry!r}; expected an identifier-shaped attribute name "
                f"or {CLASS_NAME_SIGNAL!r}"
            )
        parsed.append(entry)
    return tuple(parsed)


def _parse_codes(block: Mapping[str, Any], *, source: str) -> dict[str, str]:
    raw = block.get("codes")
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        raise ConnectorDeclarationError(
            f"error_map.codes in {source} must be an object mapping native "
            f"codes to categories, got {type(raw).__name__}"
        )
    parsed: dict[str, str] = {}
    for key, value in raw.items():
        if not isinstance(key, str) or not key:
            raise ConnectorDeclarationError(
                f"error_map.codes in {source} declares a malformed key "
                f"{key!r}; expected a non-empty string -- the connector's "
                f"own native code, in whatever shape its driver uses"
            )
        parsed[key] = _require_category(value, f"codes.{key}", source=source)
    return parsed


def _parse_http(block: Mapping[str, Any], *, source: str) -> dict[int, str]:
    raw = block.get("http")
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        raise ConnectorDeclarationError(
            f"error_map.http in {source} must be an object mapping status "
            f"codes to categories, got {type(raw).__name__}"
        )
    parsed: dict[int, str] = {}
    for key, value in raw.items():
        if not isinstance(key, str) or not _HTTP_KEY.match(key):
            raise ConnectorDeclarationError(
                f"error_map.http in {source} declares malformed status "
                f"{key!r}; expected the http key grammar ({_HTTP_KEY.pattern})"
            )
        parsed[int(key)] = _require_category(value, f"http.{key}", source=source)
    return parsed


@dataclass(frozen=True)
class ErrorMap:
    """Typed view of a connector's declared ``error_map`` block.

    ``key_attrs`` names, in the connector's own declared precedence order,
    which attributes of its exception carry a native error signal (or the
    reserved :data:`CLASS_NAME_SIGNAL` to match the exception's class name).
    ``codes`` maps whatever native value each attribute reads to an
    engine-vocabulary category, with no engine-enforced shape on the native
    code itself -- SQLSTATEs, vendor codes, anything a driver emits are all
    just strings to this lookup. ``http`` is unrelated to the exception
    attributes: a status is read at the HTTP call site, never off a raised
    exception.

    Lookup precedence is the connector's own ``key_attrs`` order times the
    birth-site pair (the caught exception and its single explicit driver
    link) -- both members are consulted for the first ``key_attrs`` entry
    before either is consulted for the second, so a connector that lists
    its most specific signal first gets that precedence honored exactly.
    """

    key_attrs: tuple[str, ...]
    codes: Mapping[str, str]
    http: Mapping[int, str]

    def __post_init__(self) -> None:
        # Make the frozenness real: these maps live on long-lived handlers
        # and decide ack verdicts, so the field values must not be
        # rewritable through the plain dicts from_declaration builds.
        object.__setattr__(self, "codes", MappingProxyType(dict(self.codes)))
        object.__setattr__(self, "http", MappingProxyType(dict(self.http)))

    @classmethod
    def from_declaration(
        cls, block: Mapping[str, Any], *, source: str = "<connector definition>"
    ) -> ErrorMap:
        """Parse a declared block, failing loud on any grammar mismatch.

        Every field is optional (absence declares nothing); a declared
        field's identifiers and categories are validated strictly, and an
        unknown top-level field fails -- including the retired fixed-family
        keys (``sqlstate``/``exception``/``vendor_code``), which is what
        makes a leftover pre-#513 declaration fail loud here rather than
        being silently reinterpreted.
        """
        if not isinstance(block, Mapping):
            raise ConnectorDeclarationError(
                f"error_map in {source} must be an object, "
                f"got {type(block).__name__}"
            )
        known = {"key_attrs", "codes", "http"}
        unknown = set(block) - known
        if unknown:
            raise ConnectorDeclarationError(
                f"error_map in {source} carries unknown fields "
                f"{sorted(unknown)}; expected a subset of {sorted(known)} "
                f"(the fixed sqlstate/exception/vendor_code families are "
                f"retired -- issue #513 -- declare key_attrs + codes instead)"
            )
        key_attrs = _parse_key_attrs(block, source=source)
        codes = _parse_codes(block, source=source)
        if bool(key_attrs) != bool(codes):
            raise ConnectorDeclarationError(
                f"error_map in {source} declares key_attrs without codes "
                f"(or codes without key_attrs); both are required together "
                f"or neither"
            )
        return cls(
            key_attrs=key_attrs,
            codes=codes,
            http=_parse_http(block, source=source),
        )

    # ------------------------------------------------------------------
    # Lookup
    # ------------------------------------------------------------------

    def match_http(self, status: int) -> DeclaredMatch | None:
        """Return the declared classification for an HTTP status, if any."""
        category = self.http.get(status)
        if category is None:
            return None
        return DeclaredMatch(signal="http", value=str(status), category=category)

    def match_exception(self, exc: BaseException) -> DeclaredMatch | None:
        """Return the declared classification for a live exception, if any.

        Birth-site matching, deliberately narrow: the caller is the
        boundary that just caught the driver's failure, so the input is
        *exc* itself plus at most its single explicit driver link —
        SQLAlchemy's ``orig`` (the raw DBAPI exception its wrapper carries)
        or ``raise ... from`` (``__cause__``), one hop, no recursion. There
        is no chain walking: classification happens where the error is
        born and crosses process boundaries as structured verdicts, never
        by re-deriving from whatever exotic chain a wrapper accumulated.

        For each ``key_attrs`` entry in the connector's own declared order,
        both members are checked before moving to the next entry — the
        connector's declared order IS the specificity order, unlike the
        old fixed-family precedence the engine used to impose. Never
        raises: the members are untrusted connector/driver objects whose
        attributes may be misbehaving properties, and a classifier crash
        here would displace the original failure at the exact moment it is
        being reported — a member whose attribute read raises anything
        other than ``AttributeError`` logs a WARNING and matches nothing
        for that entry. A ``key_attrs`` name backed by a property whose own
        body raises ``AttributeError`` is indistinguishable from a plain
        absent attribute (``getattr``'s three-argument form, and Python's
        attribute protocol generally, make no distinction here) and matches
        nothing silently, with no WARNING — an inherent limit of attribute
        access, not something this method can detect.
        """
        if not self.key_attrs:
            return None
        members = _birth_site_members(exc)
        for key_attr in self.key_attrs:
            for member in members:
                try:
                    match = self._match_signal(member, key_attr)
                except Exception:
                    logger.warning(
                        "declared error_map lookup failed reading %s off "
                        "%s; treating the member as unmatched for this "
                        "signal",
                        key_attr,
                        type(member).__name__,
                        exc_info=True,
                    )
                    continue
                if match is not None:
                    return match
        return None

    def _match_signal(
        self, member: BaseException, key_attr: str
    ) -> DeclaredMatch | None:
        """Match one signal (a class name walk or a plain attribute) on *member*."""
        if key_attr == CLASS_NAME_SIGNAL:
            for cls in type(member).__mro__:
                category = self.codes.get(cls.__name__)
                if category is not None:
                    return DeclaredMatch(
                        signal=key_attr, value=cls.__name__, category=category
                    )
            return None
        value = getattr(member, key_attr, None)
        if value is None or isinstance(value, bool):
            return None
        native = str(value)
        category = self.codes.get(native)
        if category is None:
            return None
        return DeclaredMatch(signal=key_attr, value=native, category=category)


def _birth_site_members(exc: BaseException) -> list[BaseException]:
    """Collect the exception and its single explicit driver link, one hop only.

    ``orig`` is SQLAlchemy's raw-DBAPI-exception slot (the member carrying
    driver-native facts); ``__cause__`` covers ``raise ... from`` at a
    driver boundary. No recursion and no ``__context__``: the caller is
    the birth site of the failure, so anything deeper is not the failure
    being classified. The reads are guarded — the members are untrusted
    objects, and a misbehaving link property must not crash the caller in
    the middle of reporting the original failure. Generic, not
    family-specific: every driver's wrapping convention is the same two
    attribute names, so this stays engine-owned even though the family
    regexes it used to feed (issue #401) are gone (issue #513).
    """
    members = [exc]
    for attr in ("orig", "__cause__"):
        try:
            linked = getattr(exc, attr, None)
        except Exception:
            linked = None
        if isinstance(linked, BaseException) and not any(
            linked is member for member in members
        ):
            members.append(linked)
    return members


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


def error_map_for(runtime: Any) -> ErrorMap | None:
    """Parse a runtime's declared ``error_map`` with the canonical source label.

    The one call shape every consumer site uses — the SQL facade, the ADBC
    backend, the API connectors, the source worker — so a new consumer
    cannot forget the parse or label the error source inconsistently. Reads
    ``runtime.declared_error_map`` strictly: a runtime object without the
    attribute is a wiring defect, not an undeclared connector.
    """
    return parse_declared_error_map(
        runtime.declared_error_map,
        source=f"connector {runtime.connector_id!r}",
    )


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
    return int(value)
