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
2. A defensive fallback for any exception that reaches the runner with no tag at
   all (e.g. a failure outside the tagged stages). It mirrors the existing
   name-based pattern in ``cdk.sql.generic._is_fatal_adbc_error``.

``error_detail`` carries only allowlisted-safe fields -- stage labels, error
codes, and exception class names -- so there is nothing to scrub: secrets live
in driver *message* text, which never enters the detail. The full message stays
engine-side in the logs (``logger.exception``), not in the metrics record.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Set, Tuple


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
    SCHEMA_MISMATCH = "SCHEMA_MISMATCH"
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
    ErrorCode.DESTINATION_WRITE_FAILED: (
        "Writing to the destination system failed."
    ),
    ErrorCode.SCHEMA_MISMATCH: (
        "The data did not match the expected schema or types."
    ),
    ErrorCode.RATE_LIMITED: (
        "The source system rate-limited the request. Try again later."
    ),
    ErrorCode.CONFIG_INVALID: (
        "The pipeline configuration is invalid or incomplete."
    ),
    ErrorCode.INTERNAL: (
        "The pipeline failed due to an internal error."
    ),
}


def customer_message(code: ErrorCode) -> str:
    """Return the short, fixed, customer-safe message for an error code."""
    return _CUSTOMER_MESSAGES.get(code, _CUSTOMER_MESSAGES[ErrorCode.INTERNAL])


def classify_for_metrics(exc: BaseException) -> Tuple[ErrorCode, str, Optional[str]]:
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


def tag_failure(exc: BaseException, *, code: ErrorCode, stage: FailureStage) -> BaseException:
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
_CODE_PRIORITY: Tuple[ErrorCode, ...] = (
    ErrorCode.CONFIG_INVALID,
    ErrorCode.SCHEMA_MISMATCH,
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
        f"_CODE_PRIORITY must rank every ErrorCode; missing: {sorted(c.value for c in _unranked)}"
    )


def _iter_tags(exc: BaseException) -> List[FailureTag]:
    """Every :class:`FailureTag` stamped anywhere in the exception chain."""
    tags: List[FailureTag] = []
    for member in _walk_chain(exc):
        tag = getattr(member, _TAG_ATTR, None)
        if isinstance(tag, FailureTag):
            tags.append(tag)
    return tags


def read_failure_tag(exc: BaseException) -> Optional[FailureTag]:
    """The dominant tag across the chain, or None when nothing is tagged.

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


def _walk_chain(exc: BaseException) -> List[BaseException]:
    """Flatten an exception and everything it carries into a list.

    Follows ``__cause__`` / ``__context__``, the members of an
    ``ExceptionGroup``, and the engine's own ``original_error`` attribute
    (``StreamProcessingError`` wraps the underlying driver error there). A
    seen-set on object identity plus a depth cap guard against cycles.
    """
    seen: Set[int] = set()
    out: List[BaseException] = []
    stack: List[Tuple[BaseException, int]] = [(exc, 0)]
    while stack:
        current, depth = stack.pop()
        if current is None or id(current) in seen or depth > _MAX_CHAIN_DEPTH:
            continue
        seen.add(id(current))
        out.append(current)
        nested: List[BaseException] = []
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


def _signature(exc: BaseException) -> Tuple[Set[str], str]:
    """Collect the class names and message text across the whole chain.

    Returns a set of every class name in every chained exception's MRO (plus any
    worker ``error_type:`` prefix promoted from the message) and the lowercased
    concatenation of their messages. Both feed the heuristic rules: types are
    matched precisely by name; driver text and the worker prefix's tail are
    matched in the message.
    """
    names: Set[str] = set()
    messages: List[str] = []
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
    """True if the chain carries a builtin local-filesystem error.

    Lets the runner keep such a failure as the engine/infra fault it is
    (``INTERNAL``) instead of the config-phase ``CONFIG_INVALID`` default -- e.g.
    an unreadable manifest/connection JSON raises ``PermissionError`` during
    config load, which is a volume/permissions problem, not a bad config.
    """
    names, _ = _signature(exc)
    return bool(names & _LOCAL_IO_NAMES)


# --------------------------------------------------------------------------- #
# Heuristic rules (source-extract fine split + untagged fallback)
# --------------------------------------------------------------------------- #
#
# Each rule names the exception classes (matched anywhere in the chain's MRO)
# and the message phrases that imply its code. These drive two things now:
# ``classify_source_extract`` (the source-only auth/rate/unreachable split a tag
# cannot make for an opaque driver error) and ``classify_exception``'s fallback
# for an untagged exception. Order matters: config (setup defect, the root cause
# when present) -> schema -> destination -> source auth -> rate -> source
# unreachable -> internal.

# Config / contract / connector-definition defects. A bad setup is the root
# cause whenever it appears, so this is checked first.
# Note: the type-map base ``TypeMapError`` is deliberately NOT listed. A broken
# or missing type-map file (InvalidTypeMapError / TypeMapNotFoundError) is a
# config defect, but UnmappedTypeError -- a value with no rule -- subclasses the
# same base and must reach SCHEMA_MISMATCH, so we match only the concrete
# config subclasses, not the shared base.
_CONFIG_NAMES = frozenset({
    "ConfigError", "ConfigNotFoundError", "ConfigValidationError",
    "ConnectorNotFoundError", "EndpointNotFoundError", "ConnectionConfigError",
    "ContractValidationError", "ConfigurationError", "StreamConfigurationError",
    "PipelineValidationError", "StageConfigurationError",
    "TransportSpecError", "InvalidTypeMapError",
    "TypeMapNotFoundError", "UnsupportedDialectOperationError",
    "AdbcConfigurationError", "ConnectorNotRegisteredError",
    "UnresolvedValueError", "SecretNotFoundError", "PlaceholderExpansionError",
})

_AUTH_NAMES = frozenset({"SecretAccessDeniedError"})
_AUTH_PHRASES = (
    "authentication failed", "password authentication failed",
    "permission denied", "access denied", "access is denied",
    "invalid credentials", "login failed", "unauthorized", "not authorized",
    "forbidden",
)

_RATE_PHRASES = (
    "rate limit", "rate-limit", "ratelimit", "too many requests", "throttl",
)

# HTTP status codes: the digits must not be adjacent to a word char or "/", so a
# code in a URL path ("/users/403"), an ID ("object 4035"), or a count ("1429
# records") does not match -- only a standalone code ("status 401", "got 429")
# does. The common textual companions (unauthorized / forbidden / too many
# requests) are already covered by the phrase lists above.
_HTTP_AUTH_STATUS = re.compile(r"(?<![\w/])40[13](?![\w/])")
_HTTP_RATE_STATUS = re.compile(r"(?<![\w/])429(?![\w/])")

_UNREACHABLE_NAMES = frozenset({
    "ConnectionError", "ConnectionRefusedError", "ConnectionResetError",
    "ConnectionAbortedError", "TimeoutError", "CircuitBreakerOpenError",
    "gaierror",
})
_UNREACHABLE_PHRASES = (
    "connection refused", "could not connect", "failed to connect",
    "could not translate host name", "name or service not known",
    "temporary failure in name resolution", "getaddrinfo", "no route to host",
    "network is unreachable", "connection reset", "connection timed out",
    "timed out", "unreachable", "host is down",
)

_SCHEMA_NAMES = frozenset({
    "SchemaError", "UnmappedTypeError", "TransformationError",
    "SchemaConfigurationError", "DataError",
})
_SCHEMA_PHRASES = (
    "schema mismatch", "destination rejected schema", "type mismatch",
    "incompatible type", "cannot convert", "could not convert",
    "type-map rule", "schema validation",
)

# PEP-249 driver names (ProgrammingError / IntegrityError / NotSupportedError)
# are deliberately NOT listed: they are transport-ambiguous. A destination write
# failure never reaches the engine as a bare driver instance -- it crosses the
# gRPC boundary as a failure_summary string wrapped by _load_stage (matched by
# the phrases below) or is recorded as the DLQ dominant cause. A live driver
# exception in the chain therefore comes from the source worker, so routing those
# names to the destination would mislabel source read failures.
_DESTINATION_NAMES = frozenset({
    "WriteError", "CreateTableError", "AdbcCommitRecordError",
})
_DESTINATION_PHRASES = (
    "failed to connect to grpc destination", "destination rejected",
    "write to destination", "destination write", "load stage",
    "destination handshake failed",
    # Load-stage wrappers from the engine (_load_stage) around a destination
    # ack failure; the wrapper text itself is the destination signal.
    "fatal failure", "retries:", "unknown ack status",
)

# Builtin filesystem errors are a local engine/infra fault, never a source or
# destination failure. They are matched by type so a local "[Errno 13]
# Permission denied" from creating the state/deadletter directories is not read
# as source auth by the "permission denied" phrase below.
_LOCAL_IO_NAMES = frozenset({
    "PermissionError", "FileExistsError", "IsADirectoryError",
    "NotADirectoryError", "InterruptedError", "BlockingIOError",
})


def _matches(names: Set[str], text: str, name_set: frozenset, phrases: Tuple[str, ...]) -> bool:
    if names & name_set:
        return True
    return any(phrase in text for phrase in phrases)


def classify_source_extract(exc: BaseException) -> ErrorCode:
    """Classify a source-extract failure into its concrete source code.

    The extract stage knows the failure is source-side, but auth-vs-unreachable-
    vs-rate for an opaque driver/HTTP error is only legible from the error
    itself. This is the one narrow, source-scoped text split; because only the
    source boundary calls it, a destination port (``host:401``) or path can never
    reach it, so the HTTP-code ambiguity that plagued whole-chain classification
    is gone. A deterministic source-config error (e.g. a bad endpoint document)
    still resolves to CONFIG_INVALID, and a source-side type-map miss to
    SCHEMA_MISMATCH; anything unrecognised stays INTERNAL.
    """
    names, text = _signature(exc)
    if _matches(names, text, _CONFIG_NAMES, ()):
        return ErrorCode.CONFIG_INVALID
    if _matches(names, text, _SCHEMA_NAMES, _SCHEMA_PHRASES):
        return ErrorCode.SCHEMA_MISMATCH
    if _matches(names, text, _AUTH_NAMES, _AUTH_PHRASES) or _HTTP_AUTH_STATUS.search(text):
        return ErrorCode.SOURCE_AUTH_FAILED
    if _matches(names, text, frozenset(), _RATE_PHRASES) or _HTTP_RATE_STATUS.search(text):
        return ErrorCode.RATE_LIMITED
    if _matches(names, text, _UNREACHABLE_NAMES, _UNREACHABLE_PHRASES):
        return ErrorCode.SOURCE_UNREACHABLE
    return ErrorCode.INTERNAL


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

    names, text = _signature(exc)
    if names & _LOCAL_IO_NAMES:
        return ErrorCode.INTERNAL
    if _matches(names, text, _CONFIG_NAMES, ()):
        return ErrorCode.CONFIG_INVALID
    if _matches(names, text, _SCHEMA_NAMES, _SCHEMA_PHRASES):
        return ErrorCode.SCHEMA_MISMATCH
    if _matches(names, text, _DESTINATION_NAMES, _DESTINATION_PHRASES):
        return ErrorCode.DESTINATION_WRITE_FAILED
    if _matches(names, text, _AUTH_NAMES, _AUTH_PHRASES) or _HTTP_AUTH_STATUS.search(text):
        return ErrorCode.SOURCE_AUTH_FAILED
    if _matches(names, text, frozenset(), _RATE_PHRASES) or _HTTP_RATE_STATUS.search(text):
        return ErrorCode.RATE_LIMITED
    if _matches(names, text, _UNREACHABLE_NAMES, _UNREACHABLE_PHRASES):
        return ErrorCode.SOURCE_UNREACHABLE
    return ErrorCode.INTERNAL


# --------------------------------------------------------------------------- #
# Structured error detail (internal-only field)
# --------------------------------------------------------------------------- #

_MAX_DETAIL_LEN = 2000


def build_error_detail(exc: BaseException) -> Optional[str]:
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
    tokens: List[str] = []
    seen: Set[str] = set()

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
    """A structured ``error_detail`` for a non-exception failure.

    Used where the run failed without a terminating exception to classify -- a
    partial run whose batches were dead-lettered. ``reason`` must be a fixed,
    developer-authored phrase (never connector/driver text), keeping the same
    allowlisted-safe guarantee as :func:`build_error_detail`.
    """
    return f"{stage.value}/{code.value}: {reason}"
