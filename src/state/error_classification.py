"""Customer-safe classification of a terminating pipeline exception.

At run end the runner emits a pipeline-level metrics record to stdout
(``ANALITIQ_METRICS::{...}``). On failure the only reason historically carried
was ``str(exception)`` -- unstructured and unsafe to show external customers,
because it can embed connector/driver internals, query fragments, or
post-expansion connection values (passwords in a DSN).

This module turns the terminating exception into two safe-to-expose values plus
one internal-only value:

- ``error_code`` -- a small, stable enum (:class:`ErrorCode`). This is the
  published contract the control plane forwards to external, API-key customers.
- ``customer_message(code)`` -- a short, fixed, per-code message. It contains no
  exception text at all, so it cannot leak secrets or driver internals.
- ``error_detail`` -- a structured, allowlisted summary built from the failure's
  stage tags and exception *class names* only (never message text). This is the
  metrics record's internal-only ``error_detail`` field.

How classification works (issue #429): the boundary that knows the cause
says so, and nothing downstream guesses.

The engine knows the failure's stage and side at the raise site -- the extract /
transform / load stage boundaries, the destination handshake, the config phase.
Each of those sites stamps the exception with a :class:`FailureTag` (a definite
``(error_code, stage)``) via :func:`tag_failure`, unconditionally. There is no
text matching anywhere in this module: no phrase table, no exception-class-name
table. A failure that reaches :func:`classify_exception` with no tag is a
*missing boundary* -- a findable engine defect -- and classifies as
:attr:`ErrorCode.INTERNAL` rather than as a string somebody guessed at.

Two things supply a code more precise than the stage's own default, and both
are structured:

1. A declared category. The source worker declares an error category against
   the connector's ``error_map`` at the failure's birth site
   (:func:`source_code_for_declared_category`); a destination handler declares
   a :class:`~cdk.types.FailureCategory` on the batch ack
   (:func:`code_for_declared_category`). A declaration is signal, not trust:
   an off-vocabulary value is logged and dropped, never promoted to a code.
2. The raising stage itself, via :func:`default_code_for_stage`, when nothing
   was declared.

The two vocabularies are deliberately not merged. A declared category is what
an untrusted peer says about one attempt; :class:`ErrorCode` is what the
customer is told about the run. Merging them would let a connector name a
customer-facing outcome (see ``docs/adr/0001-two-failure-vocabularies.md``).

There is no data-vs-schema mismatch code: the engine performs no schema
validation (the destination only configures its own table via DDL), so type-map
misses, mapping/transform errors, and destination schema-configuration failures
are all configuration defects (``CONFIG_INVALID``).

``error_detail`` carries only allowlisted-safe fields -- stage labels, error
codes, and exception class names -- so there is nothing to scrub: secrets live
in driver *message* text, which never enters the detail. The full message stays
engine-side in the logs (``logger.exception``), not in the metrics record.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum

from cdk.declarations import ERROR_CATEGORY_VALUES
from cdk.types import FailureCategory

logger = logging.getLogger(__name__)


class ErrorCode(str, Enum):
    """Stable, machine-readable, customer-safe pipeline failure category.

    PUBLISHED CONTRACT. Values are forwarded to external customers via the
    control plane's public run-status endpoint and coordinated with the control
    plane's error-code catalog. Treat the string values as stable: add new
    members as new failure semantics appear, but do not rename or repurpose
    existing ones.
    """

    SOURCE_AUTH_FAILED = "SOURCE_AUTH_FAILED"
    SOURCE_UNREACHABLE = "SOURCE_UNREACHABLE"
    DESTINATION_WRITE_FAILED = "DESTINATION_WRITE_FAILED"
    RATE_LIMITED = "RATE_LIMITED"
    CONFIG_INVALID = "CONFIG_INVALID"
    INTERNAL = "INTERNAL"


class FailureStage(str, Enum):
    """The pipeline stage a failure was raised in.

    Stamped on the exception at the raise site (where the engine knows it) so
    classification is deterministic instead of reverse-engineered from text.
    """

    CONFIG = "config"
    SOURCE_EXTRACT = "source_extract"
    TRANSFORM = "transform"
    DESTINATION_LOAD = "destination_load"


@dataclass(frozen=True)
class FailureTag:
    """A definite ``(error_code, stage)`` the engine attached at the raise site.

    Carries no message text -- only the safe, structured signal. Read back by
    :func:`classify_exception` (for ``error_code``) and :func:`build_error_detail`
    (for the stage label).
    """

    code: ErrorCode
    stage: FailureStage


# The code a stage names when nothing more precise was declared (issue #429).
# Each stage knows which side of the pipeline broke, so it can always answer
# -- that is what lets every boundary tag unconditionally and lets the phrase
# tables go.
#
# SOURCE_EXTRACT is INTERNAL, not SOURCE_UNREACHABLE, and the asymmetry with
# DESTINATION_LOAD is deliberate. "The write failed" is the whole of what a
# destination-load failure claims, and the stage establishes it. Its source
# counterparts each claim a *mechanism* -- the host did not answer, the
# credentials were refused, the quota ran out -- and the stage establishes
# none of them. A connector that declares its error_map supplies the
# mechanism; one that does not leaves a gap that INTERNAL reports honestly
# and a guess would paper over. The side is never lost: it rides the tag's
# stage into error_detail as "source_extract/INTERNAL:ReadError".
_STAGE_DEFAULT_CODE: dict[FailureStage, ErrorCode] = {
    FailureStage.CONFIG: ErrorCode.CONFIG_INVALID,
    FailureStage.SOURCE_EXTRACT: ErrorCode.INTERNAL,
    FailureStage.TRANSFORM: ErrorCode.CONFIG_INVALID,
    FailureStage.DESTINATION_LOAD: ErrorCode.DESTINATION_WRITE_FAILED,
}

# Totality, enforced at import (the same instinct as _CODE_PRIORITY below):
# every boundary tags through this table, so a new FailureStage with no
# default would raise inside the failure-reporting path.
_stageless = set(FailureStage) - set(_STAGE_DEFAULT_CODE)
if _stageless:
    raise RuntimeError(
        f"_STAGE_DEFAULT_CODE must give every FailureStage a default; "
        f"missing: {sorted(s.value for s in _stageless)}"
    )


def default_code_for_stage(stage: FailureStage) -> ErrorCode:
    """Return the code ``stage`` names when nothing was declared."""
    return _STAGE_DEFAULT_CODE[stage]


# Short, fixed, customer-facing message per code. These carry no exception text,
# so they cannot leak secrets, driver internals, or stack traces.
_CUSTOMER_MESSAGES: dict[ErrorCode, str] = {
    ErrorCode.SOURCE_AUTH_FAILED: (
        "Authentication to the source system failed. Check the connection credentials."
    ),
    ErrorCode.SOURCE_UNREACHABLE: (
        "The source system could not be reached. It may be offline or unreachable "
        "from the engine."
    ),
    ErrorCode.DESTINATION_WRITE_FAILED: ("Writing to the destination system failed."),
    ErrorCode.RATE_LIMITED: (
        "The source system rate-limited the request. Try again later."
    ),
    ErrorCode.CONFIG_INVALID: ("The pipeline configuration is invalid or incomplete."),
    ErrorCode.INTERNAL: ("The pipeline failed due to an internal error."),
}


def customer_message(code: ErrorCode) -> str:
    """Return the short, fixed, customer-safe message for an error code."""
    return _CUSTOMER_MESSAGES.get(code, _CUSTOMER_MESSAGES[ErrorCode.INTERNAL])


def classify_for_metrics(exc: BaseException) -> tuple[ErrorCode, str, str | None]:
    """Turn a terminating exception into the three metrics-record error values.

    Returns ``(error_code, error_message, error_detail)`` where ``error_code``
    and ``error_message`` are customer-safe and ``error_detail`` is the
    structured, allowlisted-safe summary for the internal-only field. Both the
    runner (pipeline-level) and the engine (stream-level) call this so the values
    are produced identically wherever a metrics record is built.
    """
    code = classify_exception(exc)
    return code, customer_message(code), build_error_detail(exc)


# --------------------------------------------------------------------------- #
# Structured failure tags
# --------------------------------------------------------------------------- #

# Attribute name under which a FailureTag rides an exception instance. A single
# underscore-prefixed attr keeps it out of the way of any connector/driver state.
_TAG_ATTR = "_analitiq_failure_tag"


def tag_failure(
    exc: BaseException, *, code: ErrorCode, stage: FailureStage
) -> BaseException:
    """Stamp ``exc`` with a :class:`FailureTag` and return it (for ``raise``).

    No-overwrite by construction: if any exception already in ``exc``'s chain
    carries a tag, this is a no-op, so a precise inner signal (e.g. a worker's
    deterministic-config tag) is never clobbered by a coarser outer-stage
    default. The "innermost / most-specific tag wins" rule is enforced here
    once -- an outer stage boundary can call ``tag_failure`` unconditionally
    instead of each re-implementing the same ``read_failure_tag(e) is None``
    guard (and risking a silent clobber if one is forgotten).
    """
    if read_failure_tag(exc) is None:
        setattr(exc, _TAG_ATTR, FailureTag(code=code, stage=stage))
    return exc


# Priority when several leaves of an ExceptionGroup carry different tags: the
# most specific / most actionable cause wins, mirroring the fallback rule order.
_CODE_PRIORITY: tuple[ErrorCode, ...] = (
    ErrorCode.CONFIG_INVALID,
    ErrorCode.DESTINATION_WRITE_FAILED,
    ErrorCode.SOURCE_AUTH_FAILED,
    ErrorCode.RATE_LIMITED,
    ErrorCode.SOURCE_UNREACHABLE,
    ErrorCode.INTERNAL,
)

# Totality, enforced at import: read_failure_tag ranks tags via
# _CODE_PRIORITY.index(code), which raises ValueError for an unranked code --
# inside the failure-reporting path, the worst place for a new exception. Fail
# loud at startup instead if a future ErrorCode member is added without a rank.
_unranked = set(ErrorCode) - set(_CODE_PRIORITY)
if _unranked:
    raise RuntimeError(
        f"_CODE_PRIORITY must rank every ErrorCode; "
        f"missing: {sorted(c.value for c in _unranked)}"
    )


def _iter_tags(exc: BaseException) -> list[FailureTag]:
    """Every :class:`FailureTag` stamped anywhere in the exception chain."""
    tags: list[FailureTag] = []
    for member in _walk_chain(exc):
        tag = getattr(member, _TAG_ATTR, None)
        if isinstance(tag, FailureTag):
            tags.append(tag)
    return tags


def read_failure_tag(exc: BaseException) -> FailureTag | None:
    """Return the dominant tag across the chain, or None when nothing is tagged.

    Dominant = the highest-priority code present (so an aggregated group resolves
    to its most actionable leaf). Used both to classify and to gate outer-stage
    tagging against an existing inner tag.
    """
    tags = _iter_tags(exc)
    if not tags:
        return None
    return min(tags, key=lambda t: _CODE_PRIORITY.index(t.code))


# --------------------------------------------------------------------------- #
# Exception-chain inspection
# --------------------------------------------------------------------------- #

# Cap the walk so a self-referential or deeply nested cause chain can never spin.
_MAX_CHAIN_DEPTH = 50


def _walk_chain(exc: BaseException) -> list[BaseException]:
    """Flatten an exception and everything it carries into a list.

    Follows ``__cause__`` / ``__context__``, the members of an
    ``ExceptionGroup``, and the engine's own ``original_error`` attribute
    (``StreamProcessingError`` wraps the underlying driver error there). A
    seen-set on object identity plus a depth cap guard against cycles.
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
        for attr in ("original_error", "__cause__", "__context__"):
            linked = getattr(current, attr, None)
            if isinstance(linked, BaseException):
                nested.append(linked)
        for child in nested:
            stack.append((child, depth + 1))
    return out


# Extract context (issue #401): declared error category -> the concrete
# source ErrorCode. ``transient`` and ``write_rejected`` claim no source
# code — they speak to retryability, not to which published code names the
# terminal cause — so they take the extract stage's own default.
_DECLARED_SOURCE_CODES: dict[str, ErrorCode | None] = {
    "auth": ErrorCode.SOURCE_AUTH_FAILED,
    "unreachable": ErrorCode.SOURCE_UNREACHABLE,
    "rate_limited": ErrorCode.RATE_LIMITED,
    "config": ErrorCode.CONFIG_INVALID,
    "transient": None,
    "write_rejected": None,
}

# Totality, enforced at import (same instinct as _CODE_PRIORITY): a future
# vocabulary member must take an explicit position here, never silently
# reach the stage default as though nothing was declared.
_undeclared_source = set(ERROR_CATEGORY_VALUES) - set(_DECLARED_SOURCE_CODES)
if _undeclared_source:
    raise RuntimeError(
        f"_DECLARED_SOURCE_CODES must position every declared error "
        f"category; missing: {sorted(_undeclared_source)}"
    )


def source_code_for_declared_category(category: str) -> ErrorCode | None:
    """Return the source :class:`ErrorCode` a declared category names, if any.

    Consumes the ``declared_category`` the source worker carried across the
    wire (issue #401): the worker classified the failure at its birth site
    against the connector's declarations, and the engine derives the
    published code here — connectors declare facts, the engine owns codes.
    ``None`` for ``transient``/``write_rejected`` (they claim no source
    code) and for an off-vocabulary value (wire skew is logged, never
    trusted into a code).
    """
    if category not in _DECLARED_SOURCE_CODES:
        logger.warning(
            "declared_category %r off the wire is not in the engine "
            "vocabulary; ignoring it",
            category,
        )
        return None
    return _DECLARED_SOURCE_CODES[category]


class SchemaHandshakeOutcome(str, Enum):
    """How a destination schema handshake ended.

    The gRPC client sees the difference directly -- it either got a
    ``SchemaAck``, or the stream died, or the ack never came -- so it
    reports the outcome as a fact instead of the engine re-deriving one
    from the rejection wording (issue #429). Each outcome names exactly
    one thing that happened; :func:`classify_handshake_failure` owns the
    mapping to a customer-facing code.
    """

    ACCEPTED = "accepted"
    # The destination answered with accepted=False: it was reachable and
    # refused the stream.
    REJECTED = "rejected"
    # The stream died, or no ack arrived within the budget: the engine
    # never learned what the destination thought.
    TRANSPORT_FAILURE = "transport_failure"
    # Something other than a SchemaAck came back first. Neither peer is at
    # fault in a way the customer can act on.
    PROTOCOL_VIOLATION = "protocol_violation"


# What each handshake outcome means in customer-facing terms. configure_schema
# only prepares the destination's own table via DDL -- it never validates data
# -- so a destination that answers "no" is naming a configuration defect, while
# one that never answers is a write-path failure.
_HANDSHAKE_OUTCOME_CODES: dict[SchemaHandshakeOutcome, ErrorCode] = {
    SchemaHandshakeOutcome.REJECTED: ErrorCode.CONFIG_INVALID,
    SchemaHandshakeOutcome.TRANSPORT_FAILURE: ErrorCode.DESTINATION_WRITE_FAILED,
    SchemaHandshakeOutcome.PROTOCOL_VIOLATION: ErrorCode.INTERNAL,
}

# Totality, enforced at import: every non-accepted outcome must name a code,
# so a new outcome cannot reach the raise site with nothing to say.
_uncoded_outcomes = (
    set(SchemaHandshakeOutcome)
    - {SchemaHandshakeOutcome.ACCEPTED}
    - set(_HANDSHAKE_OUTCOME_CODES)
)
if _uncoded_outcomes:
    raise RuntimeError(
        f"_HANDSHAKE_OUTCOME_CODES must map every failing "
        f"SchemaHandshakeOutcome; missing: "
        f"{sorted(o.value for o in _uncoded_outcomes)}"
    )


def classify_handshake_failure(
    outcome: SchemaHandshakeOutcome, *, declared: FailureCategory
) -> ErrorCode:
    """Return the code a failed destination handshake reports.

    Two structured inputs, in the same order as everywhere else in this
    module: what the destination declared on the rejected ack, then -- when
    it declared nothing -- what the client observed. Neither is the
    rejection text, because the same wording can come from a destination
    that refused the stream and from one that died mid-handshake.

    The declaration outranks the outcome because it is more specific: a
    destination shell that could not reach its connector worker sends a
    perfectly ordinary rejection, and only its NOT_READY declaration says
    the refusal was not the customer's configuration.
    """
    if outcome is SchemaHandshakeOutcome.ACCEPTED:
        raise ValueError("classify_handshake_failure needs a failed handshake")
    return code_for_declared_category(declared) or _HANDSHAKE_OUTCOME_CODES[outcome]


# The engine-side meaning of each declared destination failure category
# (issue #351). CONFIG_DEFECT is a deterministic, user-fixable configuration
# defect; WRITE_REJECTED is a write the destination attempted and failed.
# NOT_READY means nothing was attempted at all -- the handler was never
# connected or its schema never configured when the batch arrived. That is an
# orchestration fault, not a destination rejection and not a user-fixable
# config defect, so it maps to INTERNAL -- the interim choice from issue #351
# that avoids adding a member to the published ErrorCode contract (a dedicated
# DESTINATION_NOT_READY code remains open, coordinated with the control
# plane's error-code catalog). INTERNAL says the engine or the connector has
# a bug (issue #429); it is the one category whose name and code already agree.
_CATEGORY_TO_CODE: dict[FailureCategory, ErrorCode] = {
    FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT: ErrorCode.CONFIG_INVALID,
    FailureCategory.FAILURE_CATEGORY_WRITE_REJECTED: ErrorCode.DESTINATION_WRITE_FAILED,
    FailureCategory.FAILURE_CATEGORY_NOT_READY: ErrorCode.INTERNAL,
    FailureCategory.FAILURE_CATEGORY_INTERNAL: ErrorCode.INTERNAL,
}

# Totality, enforced at import (same instinct as _CODE_PRIORITY above): every
# declarable category must map to a code, so a future FailureCategory member
# cannot silently reach the stage default as though nothing was declared.
_unmapped = (
    set(FailureCategory)
    - {FailureCategory.FAILURE_CATEGORY_UNSPECIFIED}
    - set(_CATEGORY_TO_CODE)
)
if _unmapped:
    raise RuntimeError(
        f"_CATEGORY_TO_CODE must map every declarable FailureCategory; "
        f"missing: {sorted(c.name for c in _unmapped)}"
    )


def code_for_declared_category(category: FailureCategory) -> ErrorCode | None:
    """Return the engine-side :class:`ErrorCode` for a declared category.

    None for UNSPECIFIED -- nothing was declared, so the raising stage names
    the code instead. The category -> code mapping read by
    :func:`classify_destination_failure`, public so the strategy-parity tests
    can pin it directly.
    """
    return _CATEGORY_TO_CODE.get(category)


def dominant_error_code(codes: Iterable[ErrorCode]) -> ErrorCode | None:
    """Return the highest-priority code among ``codes``, or None when empty.

    The same dominance rule :func:`read_failure_tag` applies across an
    ``ExceptionGroup``'s leaves, for callers aggregating per-batch codes
    instead of exceptions (the dlq/skip partial-run path).
    """
    ranked = list(codes)
    if not ranked:
        return None
    return min(ranked, key=_CODE_PRIORITY.index)


def _read_failure_category(exc: BaseException) -> FailureCategory:
    """Return the dominant declared failure category in the exception chain.

    The engine stamps the batch ack's category onto the exception it raises
    (``StreamProcessingError.failure_category``); this walks the chain so a
    wrapped or aggregated failure still surfaces it. Dominant = the category
    whose mapped code ranks highest in :data:`_CODE_PRIORITY` -- the same
    rule :func:`read_failure_tag` applies to tags -- so an aggregated group
    resolves to its most actionable leaf instead of ``_walk_chain``'s
    traversal order. Matched duck-typed (an attribute holding a
    :class:`FailureCategory`) so this module does not import engine
    exception types. UNSPECIFIED means nothing was declared.
    """
    declared = [
        category
        for member in _walk_chain(exc)
        if isinstance(
            category := getattr(member, "failure_category", None), FailureCategory
        )
        and category != FailureCategory.FAILURE_CATEGORY_UNSPECIFIED
    ]
    if not declared:
        return FailureCategory.FAILURE_CATEGORY_UNSPECIFIED
    return min(declared, key=lambda c: _CODE_PRIORITY.index(_CATEGORY_TO_CODE[c]))


def classify_destination_failure(exc: BaseException) -> ErrorCode:
    """Classify a destination-load failure as config-defect vs write-failure.

    The destination declares the failure category on the batch ack at the
    site that caught the exception (``BatchWriteResult.failure_category``,
    issue #351), and the engine stamps it onto the exception it raises. A
    declared category is used verbatim; an undeclared (UNSPECIFIED) one --
    a thick connector's own ack, or a failure with no ack at all --
    resolves from the raising stage, which is DESTINATION_LOAD by
    construction here (issue #429). Only the load boundary calls this, so
    a source-side cause can never reach it.

    The stage default is what makes an undeclared failure honest rather
    than guessed: the load stage establishes that the write did not
    happen, and that is exactly what DESTINATION_WRITE_FAILED claims.
    """
    declared = code_for_declared_category(_read_failure_category(exc))
    if declared is not None:
        return declared
    logger.info(
        "no declared failure category on the destination failure (%s); "
        "the load stage names the code",
        type(exc).__name__,
    )
    return default_code_for_stage(FailureStage.DESTINATION_LOAD)


def classify_exception(exc: BaseException) -> ErrorCode:
    """Classify a terminating pipeline exception into a customer-safe code.

    If any stage stamped a :class:`FailureTag`, the dominant tag's code is
    returned verbatim. An exception that reaches here with no tag at all
    got past every boundary untagged, which is an engine defect rather
    than a failure with an unrecognised shape -- so it is INTERNAL, and
    the miss is logged loudly enough to find (issue #429). Nothing here
    inspects the exception's type or message.
    """
    tag = read_failure_tag(exc)
    if tag is not None:
        return tag.code

    logger.error(
        "no stage tag on the terminating exception (%s): a raise site is "
        "missing its boundary tag; reporting INTERNAL",
        type(exc).__name__,
    )
    return ErrorCode.INTERNAL


# --------------------------------------------------------------------------- #
# Structured error detail (internal-only field)
# --------------------------------------------------------------------------- #

_MAX_DETAIL_LEN = 2000


def build_error_detail(exc: BaseException) -> str | None:
    """Build the internal ``error_detail`` from allowlisted-safe fields only.

    Walks the whole chain (so an ``ExceptionGroup`` keeps every per-stream leaf,
    not just ``All streams failed (N sub-exceptions)``) and emits, per member, a
    ``stage/CODE:ExceptionType`` token where a stage tagged it and the exception
    class name otherwise.

    Every token is a stage label, an error code, or a live exception class name
    (``type(member).__name__``) -- developer-chosen identifiers read off the
    object, never message text. Message text is deliberately excluded: a
    connector is untrusted AI-authored code, so its message could begin with a
    secret, and the worker ``error_type:`` prefix is matched only for
    classification (against a fixed allowlist) and never emitted here. With no
    free text there is nothing to scrub; the full message lives in the engine
    logs, not in this record.
    """
    tokens: list[str] = []
    seen: set[str] = set()

    def add(token: str) -> None:
        if token and token not in seen:
            seen.add(token)
            tokens.append(token)

    for member in _walk_chain(exc):
        cls = type(member).__name__
        tag = getattr(member, _TAG_ATTR, None)
        if isinstance(tag, FailureTag):
            add(f"{tag.stage.value}/{tag.code.value}:{cls}")
        else:
            add(cls)

    if not tokens:
        return None
    detail = " | ".join(tokens)
    if len(detail) > _MAX_DETAIL_LEN:
        detail = detail[:_MAX_DETAIL_LEN] + "...[truncated]"
    return detail


def detail_for_code(code: ErrorCode, *, stage: FailureStage, reason: str) -> str:
    """Build a structured ``error_detail`` for a non-exception failure.

    Used where the run failed without a terminating exception to classify -- a
    partial run whose batches were dead-lettered. ``reason`` must be a fixed,
    developer-authored phrase (never connector/driver text), keeping the same
    allowlisted-safe guarantee as :func:`build_error_detail`.
    """
    return f"{stage.value}/{code.value}: {reason}"
