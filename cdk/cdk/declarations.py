"""Connector-level declared facts: error taxonomy + concurrency (#401, #513).

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
in the engine. The published contract validates both blocks at config
load; this module only reads them.

Both blocks reach the worker via the resolved payload channel
(``ConnectionRuntime.resolve_spec`` / ``from_resolved_payload``), the same
channel that delivers the ``sql_capabilities`` block.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
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


class ConnectorDeclarationError(ValueError):
    """A category outside the engine vocabulary reached a verdict lookup.

    Raised only by :func:`require_declared_category`, where it means the
    engine itself built a :class:`DeclaredMatch` wrong.
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
        # Every construction site today reads its category from a block the
        # contract validated, so this cannot fire; it stops a future site
        # that builds a match from anything else.
        require_declared_category(self.category, source="DeclaredMatch")


def require_declared_category(category: str, *, source: str) -> str:
    """Validate a category against the engine vocabulary. Raises loud.

    Reserved for contexts where an off-vocabulary value can only mean an
    *engine* bug, never a connector one -- :class:`DeclaredMatch`
    construction is the one caller, and every real construction site reads
    a category the contract already validated, so a failure here means
    declarations.py itself built a ``DeclaredMatch`` wrong, which should
    stop the process rather than be routed around.

    A category discovered at *runtime* from connector-authored code (a
    ``classify_error``/``classify()`` hook's return, a birth-site
    ``declared_category`` stamped on a typed error) is never validated
    with this function -- raising there would displace the original
    failure being reported. Those call sites map an off-vocabulary value
    to ``"config"`` instead; see :func:`classify_via_hook`.
    """
    if category not in ERROR_CATEGORY_VALUES:
        raise ConnectorDeclarationError(
            f"{source} classified an error as {category!r}, which is not "
            f"in the engine vocabulary {list(ERROR_CATEGORY_VALUES)}"
        )
    return category


def classify_via_hook(owner: Any, attr: str, *args: Any, source: str) -> str | None:
    """Resolve ``owner.attr`` and call it as a classification hook. Never raises.

    The single entry point for both connector-authored hook shapes this
    mechanism has: ``owner.classify_error(exc)`` and a dialect's
    ``owner.classify(status, body)``. There is deliberately no way to
    resolve the attribute and call it as two separate steps -- an earlier
    version of this PR had ``resolve_declared_hook`` (guard the read) and
    ``call_declared_hook`` (guard the call) as two functions a caller had
    to remember to compose, and every call site that instead passed a
    bare, already-resolved attribute expression (``self.classify_error``,
    ``dialect.classify``) skipped the read-side guard -- four separate
    review rounds each found one more such site. Collapsing both steps
    into one function makes that mistake unrepresentable: there is no
    shorter path that bypasses the guard.

    Both the read and the call are untrusted, potentially-AI-authored
    connector code, invoked from inside the ``except`` block that is in
    the middle of reporting the *original* failure -- nothing from here
    may displace it, mirroring the guarantee :meth:`ErrorMap.match_exception`
    already makes for the declarative read (see its docstring). Three
    failure modes collapse to the same answer:

    - the attribute is genuinely absent (no override) -- neutral, returns
      ``None``, the same answer the engine's own thin default gives;
    - resolving it raises anything else (a descriptor, a custom
      ``__getattr__``), or calling it raises -- a broken classification
      *mechanism*;
    - it returns something other than a genuine vocabulary string -- the
      mechanism ran and answered, just wrongly.

    The last two are the same fact stated two ways: the engine does not
    guess its way around a broken mechanism (that would mean trusting an
    exception's Python *type* to stand in for a taxonomy the connector
    itself failed to supply) -- both map straight to ``"config"``, the
    engine's own category for "this needs a fix in the connector," fatal
    and non-retryable in both the write and read verdict tables.
    """
    try:
        hook = getattr(owner, attr)
    except AttributeError:
        return None
    except Exception:
        logger.warning(
            "resolving %s raised; treating the connector's classification "
            "as broken (config)",
            source,
            exc_info=True,
        )
        return "config"
    try:
        category = hook(*args)
    except Exception:
        # *args may carry an untrusted response body -- never interpolated
        # here, only the source label (a class/method name this process
        # chose, not connector-controlled content).
        logger.warning(
            "%s raised; treating the connector's classification as broken " "(config)",
            source,
            exc_info=True,
        )
        return "config"
    if category is None:
        return None
    if not isinstance(category, str) or category not in ERROR_CATEGORY_VALUES:
        logger.warning(
            "%s classified an error as %r, which is not in the engine "
            "vocabulary %s; treating the connector's classification as "
            "broken (config)",
            source,
            category,
            list(ERROR_CATEGORY_VALUES),
        )
        return "config"
    return category


def birth_site_category(exc: BaseException) -> str | None:
    """Read a typed error's birth-site ``declared_category``. Never raises.

    ``ReadError``/``TransientReadError``/``ApiResponseError`` all accept
    ``declared_category`` as a public constructor kwarg with no
    construction-time check, and untrusted connector code can raise any
    of them directly -- so the value here is exactly as untrusted as a
    ``classify_error`` return, and gets the same treatment as
    :func:`classify_via_hook`: reading the attribute is guarded (a
    property that raises is not different from a hook that raises), and
    anything other than a genuine vocabulary member -- wrong type, wrong
    string, absent entirely being the one exception -- maps to
    ``"config"`` rather than being passed through to a verdict-table
    lookup or silently ignored in favor of guessing from a further
    fallback source.
    """
    try:
        declared = getattr(exc, "declared_category", None)
    except Exception:
        logger.warning(
            "reading %s.declared_category raised; treating the connector's "
            "classification as broken (config)",
            type(exc).__name__,
            exc_info=True,
        )
        return "config"
    if declared is None:
        return None
    if not isinstance(declared, str) or declared not in ERROR_CATEGORY_VALUES:
        logger.warning(
            "%s.declared_category %r is not a valid category in %s; "
            "treating the connector's classification as broken (config)",
            type(exc).__name__,
            declared,
            list(ERROR_CATEGORY_VALUES),
        )
        return "config"
    return declared


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
    def from_declaration(cls, block: Mapping[str, Any]) -> ErrorMap:
        """Read a declared block the published contract has already validated.

        Every field is optional; absence declares nothing.
        """
        return cls(
            key_attrs=tuple(block.get("key_attrs") or ()),
            codes=dict(block.get("codes") or {}),
            http={
                int(status): category
                for status, category in (block.get("http") or {}).items()
            },
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


def parse_declared_error_map(block: Any) -> ErrorMap | None:
    """Read an optional ``error_map`` declaration: ``None`` stays ``None``."""
    if block is None:
        return None
    return ErrorMap.from_declaration(block)


def error_map_for(runtime: Any) -> ErrorMap | None:
    """Read a runtime's declared ``error_map``.

    The one call shape every consumer site uses — the SQL facade, the ADBC
    backend, the API connectors, the source worker. Reads
    ``runtime.declared_error_map`` strictly: a runtime object without the
    attribute is a wiring defect, not an undeclared connector.
    """
    return parse_declared_error_map(runtime.declared_error_map)


def parse_declared_concurrency(block: Any) -> int | None:
    """Read an optional ``concurrency`` declaration's connection ceiling.

    Returns the declared ``max_connections`` (a positive int, as the
    published contract requires), or ``None`` when the block or the cap is
    absent — no declared ceiling, current behavior applies.
    """
    if block is None:
        return None
    max_connections: int | None = block.get("max_connections")
    return max_connections
