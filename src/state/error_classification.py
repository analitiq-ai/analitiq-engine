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

How classification works (structured-first):

The engine knows the failure's stage and side at the raise site -- the extract /
transform / load stage boundaries, the destination handshake, the config phase.
Each of those sites stamps the exception with a :class:`FailureTag` (a definite
``(error_code, stage)``) via :func:`tag_failure`. :func:`classify_exception`
reads those tags first and uses them verbatim: deterministic, no text matching,
no cross-stage confusion (a destination HTTP error can never read as source
auth, because the destination-load boundary tags it ``DESTINATION_WRITE_FAILED``
outright). For an aggregated ``ExceptionGroup`` the highest-priority tag across
the leaves wins.

The name/phrase heuristics below remain only as:

1. The within-source-extract fine split. The source side genuinely cannot know
   auth-vs-unreachable-vs-rate for an opaque driver error without inspecting it;
   :func:`classify_source_extract` does that one narrow, *source-only* split when
   the extract boundary tags a stream. There is no cross-stage ambiguity left,
   because only the source boundary ever runs it.
2. The destination-load fallback for an undeclared failure
   (:func:`classify_destination_failure` when the batch ack carries no
   ``FailureCategory`` -- a thick connector's own ack, or a failure with no
   ack at all).
3. A defensive fallback (:func:`classify_exception`) for any exception that
   reaches the runner with no tag at all. It mirrors the existing name-based
   pattern in ``cdk.sql._adbc_utils._is_fatal_adbc_error``.

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
import re
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum

from cdk.declarations import ERROR_CATEGORY_VALUES, ErrorMap
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


# The gRPC source worker re-raises across the boundary as
# ``ReadError``/``RuntimeError`` with the original class name preserved as an
# ``error_type:`` prefix (readable.py: f"{err.error_type}: {err.message} ...").
# Promote that leading identifier into the name set so the name-based rules
# (e.g. SecretNotFoundError -> CONFIG_INVALID) fire even though the live type is
# only the wrapper. The captured token is a class name -- safe for error_detail.
_ERROR_TYPE_PREFIX = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*:")


def _signature(exc: BaseException) -> tuple[set[str], str]:
    """Collect the class names and message text across the whole chain.

    Returns a set of every class name in every chained exception's MRO (plus any
    worker ``error_type:`` prefix promoted from the message) and the lowercased
    concatenation of their messages. Both feed the heuristic rules: types are
    matched precisely by name; driver text and the worker prefix's tail are
    matched in the message.
    """
    names: set[str] = set()
    messages: list[str] = []
    for member in _walk_chain(exc):
        for klass in type(member).__mro__:
            names.add(klass.__name__)
        message = str(member)
        prefix = _ERROR_TYPE_PREFIX.match(message)
        if prefix:
            names.add(prefix.group(1))
        messages.append(message)
    return names, " \n ".join(messages).lower()


def is_local_io_error(exc: BaseException) -> bool:
    """Return True if the chain carries a builtin local-filesystem error.

    Lets the runner keep such a failure as the engine/infra fault it is
    (``INTERNAL``) instead of the config-phase ``CONFIG_INVALID`` default -- e.g.
    an unreadable manifest/connection JSON raises ``PermissionError`` during
    config load, or a full/read-only volume raises a bare ``OSError``; both are
    volume/permissions problems, not a bad config.
    """
    names, _ = _signature(exc)
    return _is_local_filesystem_error(names)


# --------------------------------------------------------------------------- #
# Heuristic rules (source-extract fine split + untagged fallback)
# --------------------------------------------------------------------------- #
#
# Each rule names the exception classes (matched anywhere in the chain's MRO)
# and the message phrases that imply its code. These drive two things now:
# ``classify_source_extract`` (the source-only auth/rate/unreachable split a tag
# cannot make for an opaque driver error) and ``classify_exception``'s fallback
# for an untagged exception. Order matters: config (setup defect, the root cause
# when present) -> destination handshake (config vs transport) -> destination
# write -> source auth -> rate -> source unreachable -> internal.

# Config / contract / connector-definition / type-map / mapping defects. There
# is no schema validation in the engine (the destination only configures its own
# table via DDL), so type-map misses, mapping/transform errors, and destination
# schema-configuration failures are all configuration defects, not a data-vs-
# schema mismatch -- they live here. ``TypeMapError`` covers UnmappedTypeError /
# InvalidTypeMapError / TypeMapNotFoundError.
_CONFIG_NAMES = frozenset(
    {
        "ConfigError",
        "ConfigNotFoundError",
        "ConfigValidationError",
        "ConnectorNotFoundError",
        "EndpointNotFoundError",
        "ConnectionConfigError",
        "ContractValidationError",
        "ConfigurationError",
        "StreamConfigurationError",
        "PipelineValidationError",
        "StageConfigurationError",
        "TransportSpecError",
        "TypeMapError",
        "InvalidTypeMapError",
        "TypeMapNotFoundError",
        "UnmappedTypeError",
        "UnsupportedDialectOperationError",
        "AdbcConfigurationError",
        "CatalogAddressingError",
        "SqlCapabilitiesError",
        "ConnectorDeclarationError",
        "ConnectorNotRegisteredError",
        "UnresolvedValueError",
        "SchemaError",
        "SchemaConfigurationError",
        "TransformationError",
        # Secret resolution (missing/denied/malformed credentials, placeholder
        # expansion) is config/setup, not source-system auth -- match the base so
        # every subclass routes to CONFIG_INVALID.
        "SecretResolutionError",
        "SecretNotFoundError",
        "SecretAccessDeniedError",
        "PlaceholderExpansionError",
    }
)

# Config-exception class names that survive only as text across a process
# boundary (forwarded as "{TypeName}: ..." in a worker prefix, a destination
# fatal-ack summary, or a handshake reason), where the live type is just the
# wrapper and the name is mid-message. Matching them keeps a config defect
# classified as CONFIG_INVALID wherever it surfaces. These are our own
# controlled class names, not arbitrary driver text.
_CONFIG_PHRASES = (
    "secretresolutionerror",
    "secretnotfounderror",
    "secretaccessdeniederror",
    "placeholderexpansionerror",
    "unmappedtypeerror",
    "invalidtypemaperror",
    "typemapnotfounderror",
    "transportspecerror",
    "adbcconfigurationerror",
    "catalogaddressingerror",
    "sqlcapabilitieserror",
    "connectordeclarationerror",
    "unsupporteddialectoperationerror",
    "schemaconfigurationerror",
    "transformationerror",
    "configvalidationerror",
    "connectornotregisterederror",
)

# Source-system auth failures surface only as driver/HTTP text (this engine has
# no typed source-auth exception; secret-store errors are config, see above).
_AUTH_NAMES: frozenset[str] = frozenset()
_AUTH_PHRASES = (
    "authentication failed",
    "password authentication failed",
    "permission denied",
    "access denied",
    "access is denied",
    "invalid credentials",
    "login failed",
    "unauthorized",
    "not authorized",
    "forbidden",
)

_RATE_PHRASES = (
    "rate limit",
    "rate-limit",
    "ratelimit",
    "too many requests",
    "throttl",
)

# HTTP status codes: the digits must not be adjacent to a word char or "/", so a
# code in a URL path ("/users/403"), an ID ("object 4035"), or a count ("1429
# records") does not match -- only a standalone code ("status 401", "got 429")
# does. The common textual companions (unauthorized / forbidden / too many
# requests) are already covered by the phrase lists above.
_HTTP_AUTH_STATUS = re.compile(r"(?<![\w/])40[13](?![\w/])")
_HTTP_RATE_STATUS = re.compile(r"(?<![\w/])429(?![\w/])")

# Matched against every class name in the exception chain's MRO.
# ``ConnectorConnectionError`` is the source connectors' own class (it does not
# derive from the builtin ``ConnectionError``), so it has to be named here in
# its own right for an API connect failure to classify as unreachable.
_UNREACHABLE_NAMES = frozenset(
    {
        "ConnectionError",
        "ConnectorConnectionError",
        "ConnectionRefusedError",
        "ConnectionResetError",
        "ConnectionAbortedError",
        "TimeoutError",
        "gaierror",
    }
)
_UNREACHABLE_PHRASES = (
    "connection refused",
    "could not connect",
    "failed to connect",
    "could not translate host name",
    "name or service not known",
    "temporary failure in name resolution",
    "getaddrinfo",
    "no route to host",
    "network is unreachable",
    "connection reset",
    "connection timed out",
    "timed out",
    "unreachable",
    "host is down",
)

# The destination "schema" handshake (configure_schema) only prepares the
# destination's own table via DDL -- it never validates data against a schema --
# so a failed handshake is either a destination configuration defect or a
# transport failure, not a schema mismatch. The engine raises one wording,
# "Destination did not accept the stream ...", and forwards the concrete reason
# (including the inner reason on the worker-proxy path). These transport reasons
# (engine/proxy-generated, a controlled set) mean the destination was
# unreachable mid-handshake -> DESTINATION_WRITE_FAILED; any other reason is a
# configuration defect -> CONFIG_INVALID.
_HANDSHAKE_MARKER = "did not accept the stream"
_HANDSHAKE_TRANSPORT_PHRASES = (
    "before schema ack",
    "before sending schema ack",
    "closed stream",
    "did not acknowledge the schema",
    "channel did not connect",
    "did not connect",
)

# PEP-249 driver names (ProgrammingError / IntegrityError / NotSupportedError)
# are deliberately NOT listed: they are transport-ambiguous. A destination write
# failure never reaches the engine as a bare driver instance -- it crosses the
# gRPC boundary as a failure_summary string wrapped by the StreamProcessor load
# path (matched by the phrases below) or is recorded as the DLQ dominant cause.
# A live driver
# exception in the chain therefore comes from the source worker, so routing those
# names to the destination would mislabel source read failures.
_DESTINATION_NAMES = frozenset(
    {
        "WriteError",
        "CreateTableError",
    }
)
_DESTINATION_PHRASES = (
    "failed to connect to grpc destination",
    "write to destination",
    "destination write",
    "load stage",
    # Wrappers StreamProcessor raises around a destination ack failure
    # (_handle_send_outcome / _handle_exhausted_batch / the synthetic
    # truncate); the wrapper text itself is the destination signal.
    "fatal failure",
    "retries:",
    "unknown ack status",
)

# Builtin filesystem errors are a local engine/infra fault, never a source or
# destination failure. They are matched by type so a local "[Errno 13]
# Permission denied" from creating the state/deadletter directories is not read
# as source auth by the "permission denied" phrase below.
_LOCAL_IO_NAMES = frozenset(
    {
        "PermissionError",
        "FileExistsError",
        "IsADirectoryError",
        "NotADirectoryError",
        "InterruptedError",
        "BlockingIOError",
    }
)


def _is_local_filesystem_error(names: set[str]) -> bool:
    """Return True for a builtin local-filesystem OSError, excluding network ones.

    The named subclasses in :data:`_LOCAL_IO_NAMES` cover the common cases, but a
    disk-full / read-only-volume failure (``ENOSPC`` / ``EROFS``) raises a bare
    ``OSError`` with no dedicated subclass. Match that too -- but never a network
    OSError, whose connection subclasses live in :data:`_UNREACHABLE_NAMES` and
    mean a *remote* fault (source unreachable, or a destination write failure),
    not a local-disk problem. Defined once so every classifier's local-I/O guard
    treats engine/infra filesystem faults identically.
    """
    if names & _LOCAL_IO_NAMES:
        return True
    return "OSError" in names and not (names & _UNREACHABLE_NAMES)


def _matches(
    names: set[str], text: str, name_set: frozenset, phrases: tuple[str, ...]
) -> bool:
    if names & name_set:
        return True
    return any(phrase in text for phrase in phrases)


# Extract context (issue #401): declared error category -> the concrete
# source ErrorCode. ``transient`` and ``write_rejected`` claim no source
# code — they speak to retryability, not to which published code names the
# terminal cause — so they fall through to the text heuristics below.
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
# fall through to text matching.
_undeclared_source = set(ERROR_CATEGORY_VALUES) - set(_DECLARED_SOURCE_CODES)
if _undeclared_source:
    raise RuntimeError(
        f"_DECLARED_SOURCE_CODES must position every declared error "
        f"category; missing: {sorted(_undeclared_source)}"
    )


def classify_source_extract(
    exc: BaseException, *, error_map: ErrorMap | None = None
) -> ErrorCode:
    """Classify a source-extract failure into its concrete source code.

    Declared map before the text split (issue #401; the local-filesystem
    guard still runs first — an engine-infra fault is never the source's to
    classify): the source connector's ``error_map`` claims the failure
    structurally — off the live exception chain (SQLSTATE, vendor code,
    class MRO) or, across the worker boundary where only class names
    survive, off the collapsed name set. Only an unclaimed failure falls to
    the text split, and that fallback is logged.

    The extract stage knows the failure is source-side, but auth-vs-unreachable-
    vs-rate for an opaque driver/HTTP error is only legible from the error
    itself. This is the one narrow, source-scoped text split; because only the
    source boundary calls it, a destination port (``host:401``) or path can never
    reach it, so the HTTP-code ambiguity that plagued whole-chain classification
    is gone. A deterministic source-config error (a bad endpoint document, a
    type-map miss) still resolves to CONFIG_INVALID; anything unrecognised stays
    INTERNAL. A builtin local-filesystem error (checkpoint/cursor storage, worker
    bootstrap) is an engine/infra fault and stays INTERNAL, never source auth.
    """
    names, text = _signature(exc)
    if _is_local_filesystem_error(names):
        # Guard before the "permission denied" auth phrase, exactly as
        # classify_exception does: a local "[Errno 13] Permission denied" (or a
        # bare OSError from a full/read-only volume) from the engine's own
        # checkpoint/state I/O is infra, not source auth.
        return ErrorCode.INTERNAL
    match = None
    if error_map is not None:
        # sorted(): across a process boundary the chain collapses to a name
        # set with no MRO order left; a map declaring several matching names
        # should agree with itself, and the lexicographic pick keeps a
        # disagreeing one deterministic.
        match = error_map.match_exception(exc) or error_map.match_names(sorted(names))
        if match is not None:
            declared_code = _DECLARED_SOURCE_CODES[match.category]
            if declared_code is not None:
                logger.info(
                    "declared error_map classified the source failure: " "%s %s -> %s",
                    match.family,
                    match.identifier,
                    match.category,
                )
                return declared_code
    code = _classify_source_extract_by_text(names, text)
    if match is not None:
        logger.info(
            "declared error_map matched %s %s -> %s, which claims no source "
            "code; text heuristic classified it %s",
            match.family,
            match.identifier,
            match.category,
            code.value,
        )
    else:
        logger.info(
            "no declaration claimed the source failure; text heuristic "
            "classified it %s",
            code.value,
        )
    return code


def _classify_source_extract_by_text(names: set[str], text: str) -> ErrorCode:
    """Run the demoted last-resort text split (issue #401 resolution order)."""
    if _matches(names, text, _CONFIG_NAMES, _CONFIG_PHRASES):
        return ErrorCode.CONFIG_INVALID
    if _matches(names, text, _AUTH_NAMES, _AUTH_PHRASES) or _HTTP_AUTH_STATUS.search(
        text
    ):
        return ErrorCode.SOURCE_AUTH_FAILED
    if _matches(names, text, frozenset(), _RATE_PHRASES) or _HTTP_RATE_STATUS.search(
        text
    ):
        return ErrorCode.RATE_LIMITED
    if _matches(names, text, _UNREACHABLE_NAMES, _UNREACHABLE_PHRASES):
        return ErrorCode.SOURCE_UNREACHABLE
    return ErrorCode.INTERNAL


def classify_handshake_failure(reason: str | None) -> ErrorCode:
    """Classify a destination-handshake failure as transport vs config.

    The destination handshake (configure_schema) only prepares the destination's
    own table via DDL -- it never validates data -- so a failure is either a
    transport problem (the destination was unreachable mid-handshake ->
    DESTINATION_WRITE_FAILED) or a destination-config defect (-> CONFIG_INVALID).
    ``reason`` is the engine/proxy-generated handshake reason (a controlled
    string, including the inner reason forwarded across the worker proxy), not
    arbitrary driver text. The engine calls this at the raise site so the tag is
    definite; the same transport-phrase set backs the untagged fallback in
    :func:`classify_exception`.
    """
    lowered = (reason or "").lower()
    if any(phrase in lowered for phrase in _HANDSHAKE_TRANSPORT_PHRASES):
        return ErrorCode.DESTINATION_WRITE_FAILED
    return ErrorCode.CONFIG_INVALID


# The engine-side meaning of each declared destination failure category
# (issue #351). CONFIG_DEFECT is a deterministic, user-fixable configuration
# defect; WRITE_REJECTED is a write the destination attempted and failed.
# NOT_READY means nothing was attempted at all -- the handler was never
# connected or its schema never configured when the batch arrived. That is an
# orchestration fault, not a destination rejection and not a user-fixable
# config defect, so it maps to INTERNAL -- the interim choice from issue #351
# that avoids adding a member to the published ErrorCode contract (a dedicated
# DESTINATION_NOT_READY code remains open, coordinated with the control
# plane's error-code catalog).
_CATEGORY_TO_CODE: dict[FailureCategory, ErrorCode] = {
    FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT: ErrorCode.CONFIG_INVALID,
    FailureCategory.FAILURE_CATEGORY_WRITE_REJECTED: ErrorCode.DESTINATION_WRITE_FAILED,
    FailureCategory.FAILURE_CATEGORY_NOT_READY: ErrorCode.INTERNAL,
}

# Totality, enforced at import (same instinct as _CODE_PRIORITY above): every
# declarable category must map to a code, so a future FailureCategory member
# cannot silently fall through to text matching.
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

    None for UNSPECIFIED -- nothing was declared, so classification falls back
    to the name/phrase rules. The category -> code mapping read by
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

    Structured-first, mirroring :func:`classify_exception`: the destination
    declares the failure category on the batch ack at the site that caught the
    exception (``BatchWriteResult.failure_category``, issue #351), and the
    engine stamps it onto the exception it raises -- so when a category is
    declared it is used verbatim, no text matching. Only an undeclared
    (UNSPECIFIED) failure -- a thick connector's own ack, or a failure with no
    ack at all -- falls back to the name/phrase rules, defaulting to
    DESTINATION_WRITE_FAILED so a genuine write failure (constraint,
    permission, transport) still routes there. Only the load boundary calls
    it, so a source-side cause can never reach it.
    """
    declared = code_for_declared_category(_read_failure_category(exc))
    if declared is not None:
        return declared
    names, text = _signature(exc)
    if _is_local_filesystem_error(names):
        # The load stage's try also runs engine-owned local I/O (checkpoint save,
        # DLQ write, batch-commit journal, metrics emission); a builtin
        # filesystem error there is an engine/infra fault, not a destination
        # write rejection. This is the only classifier that defaults to a
        # non-INTERNAL code, so the bare-OSError case (a full/read-only volume)
        # matters most here -- _is_local_filesystem_error catches it while still
        # letting a network OSError fall through to DESTINATION_WRITE_FAILED.
        return ErrorCode.INTERNAL
    code = (
        ErrorCode.CONFIG_INVALID
        if _matches(names, text, _CONFIG_NAMES, _CONFIG_PHRASES)
        else ErrorCode.DESTINATION_WRITE_FAILED
    )
    logger.info(
        "no declared failure category on the destination failure; text "
        "heuristic classified it %s",
        code.value,
    )
    return code


def classify_exception(exc: BaseException) -> ErrorCode:
    """Classify a terminating pipeline exception into a customer-safe code.

    Structured-first: if any stage stamped a :class:`FailureTag`, the dominant
    tag's code is returned verbatim (deterministic, no text matching). Only an
    exception that reaches here with no tag at all falls back to the name/phrase
    heuristics, in priority order, defaulting to :attr:`ErrorCode.INTERNAL`.
    """
    tag = read_failure_tag(exc)
    if tag is not None:
        return tag.code

    # Untagged: the name/phrase heuristics are the last resort (issue #401
    # resolution order — declared map, then stage tags, then text), and
    # their use is logged so a silent heuristic verdict never masquerades
    # as a structured one.
    logger.info(
        "no stage tag on the terminating exception (%s); falling back to "
        "the name/phrase heuristics",
        type(exc).__name__,
    )
    names, text = _signature(exc)

    if _is_local_filesystem_error(names):
        return ErrorCode.INTERNAL
    if _matches(names, text, _CONFIG_NAMES, _CONFIG_PHRASES):
        return ErrorCode.CONFIG_INVALID
    if _HANDSHAKE_MARKER in text:
        # Destination handshake failure: a transport reason means the
        # destination was unreachable; anything else is a config defect.
        if any(phrase in text for phrase in _HANDSHAKE_TRANSPORT_PHRASES):
            return ErrorCode.DESTINATION_WRITE_FAILED
        return ErrorCode.CONFIG_INVALID
    if _matches(names, text, _DESTINATION_NAMES, _DESTINATION_PHRASES):
        return ErrorCode.DESTINATION_WRITE_FAILED
    if _matches(names, text, _AUTH_NAMES, _AUTH_PHRASES) or _HTTP_AUTH_STATUS.search(
        text
    ):
        return ErrorCode.SOURCE_AUTH_FAILED
    if _matches(names, text, frozenset(), _RATE_PHRASES) or _HTTP_RATE_STATUS.search(
        text
    ):
        return ErrorCode.RATE_LIMITED
    if _matches(names, text, _UNREACHABLE_NAMES, _UNREACHABLE_PHRASES):
        return ErrorCode.SOURCE_UNREACHABLE
    if "ReadError" in names and "deterministic" in text:
        # A source-worker ReadError marked deterministic is a contract/config
        # defect (the worker reserves deterministic for non-transient
        # contract/config failures, e.g. an invalid pagination contract). If it
        # was not auth/rate/unreachable above, it is a configuration problem.
        return ErrorCode.CONFIG_INVALID
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
