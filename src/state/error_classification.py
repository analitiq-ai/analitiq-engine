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
- ``sanitize_detail(text)`` -- the raw exception text with credential-bearing
  substrings redacted. This is for the metrics record's internal-only
  ``error_detail`` field; the control plane must never forward it externally.

The engine is the right layer to classify: it is the only place that sees the
exception type and the source/destination context at the failure site. The
control plane only ever sees free text in logs and cannot classify it after the
fact safely.

Classification is intentionally import-light. It matches on exception class
*names* gathered from the whole exception chain (cause, context, group members,
and the engine's ``original_error`` attribute) plus the message text, rather
than importing every exception class. This mirrors the existing name-based
pattern in ``cdk.sql.generic._is_fatal_adbc_error`` and handles the gRPC worker
boundary, where the original exception type is collapsed to ``ReadError`` /
``RuntimeError`` but its class name survives as an ``error_type:`` prefix in the
message.

Known limitations (best-effort heuristics over free text):

- ``error_code`` is the stable, audited contract; the textual classification of
  un-typed driver/HTTP errors is best-effort. An ambiguous message can fall to a
  neighbouring code or ``INTERNAL`` -- it is never a secret leak, only a coarser
  label. Known soft spots: a bare HTTP status with no companion word, a
  deterministic source-config error that crosses the worker boundary as plain
  text, and a config sub-cause embedded inside a real schema rejection.
- ``sanitize_detail`` redacts the common credential shapes (URL userinfo,
  ``key=value`` / quoted / hyphenated secret keys, Authorization headers) but
  cannot guarantee completeness over arbitrary free text -- e.g. a multi-line PEM
  block or a password-only URL form may slip through. ``error_detail`` is
  internal-only for exactly this reason; the control plane must never forward it.
  The durable fix for both tails is structured, engine-side error reporting
  (tagging failures with stage/side/category at the raise site and passing only
  allowlisted fields), tracked separately.
"""

from __future__ import annotations

import re
from enum import Enum
from typing import List, Set, Tuple


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


def classify_for_metrics(exc: BaseException) -> Tuple[ErrorCode, str, str]:
    """Turn a terminating exception into the three metrics-record error values.

    Returns ``(error_code, error_message, error_detail)`` where ``error_code``
    and ``error_message`` are customer-safe and ``error_detail`` is the
    credential-scrubbed raw text for the internal-only field. Both the runner
    (pipeline-level) and the engine (stream-level) call this so the values are
    produced identically wherever a metrics record is built.
    """
    code = classify_exception(exc)
    return code, customer_message(code), sanitize_detail(_detail_text(exc))


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
# only the wrapper.
_ERROR_TYPE_PREFIX = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*:")


def _signature(exc: BaseException) -> Tuple[Set[str], str]:
    """Collect the class names and message text across the whole chain.

    Returns a set of every class name in every chained exception's MRO (plus any
    worker ``error_type:`` prefix promoted from the message) and the lowercased
    concatenation of their messages. Both feed the classification rules: types
    are matched precisely by name; driver text and the worker prefix's tail are
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


def _detail_text(exc: BaseException) -> str:
    """Build the internal ``error_detail`` text from the whole chain.

    ``str(exc)`` alone loses information for the most important case: an
    ``ExceptionGroup`` renders only ``All streams failed (N sub-exceptions)``,
    dropping every per-stream cause. Join the de-duplicated messages of every
    chained member (leaves included) so the always-emitted pipeline detail keeps
    the actual causes. Scrubbing and truncation happen in ``sanitize_detail``.
    """
    parts: List[str] = []
    for member in _walk_chain(exc):
        message = str(member).strip()
        if message and message not in parts:
            parts.append(message)
    return " | ".join(parts)


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
# Classification rules
# --------------------------------------------------------------------------- #
#
# Each rule names the exception classes (matched anywhere in the chain's MRO)
# and the message phrases that imply its code. Rules are evaluated in priority
# order by ``classify_exception``: the first rule that matches wins, so a more
# specific / more actionable cause takes precedence over a vaguer one. For an
# aggregated ``ExceptionGroup`` (every stream failed) this yields the dominant
# cause -- the highest-priority signal present across all leaves.
#
# Order: config (setup defect, the root cause when present) -> schema (specific,
# actionable) -> destination (so a destination connect/permission failure is not
# mislabelled SOURCE_*, since the enum's auth/unreachable/rate codes are
# source-side) -> source auth -> rate limit -> source unreachable -> internal.

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


def classify_exception(exc: BaseException) -> ErrorCode:
    """Classify a terminating pipeline exception into a customer-safe code.

    Inspects the whole exception chain (cause, context, ``ExceptionGroup``
    members, and the engine's ``original_error``) by class name and message
    text, then applies the rules in priority order. Returns
    :attr:`ErrorCode.INTERNAL` when nothing more specific matches.
    """
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
# Detail sanitization (internal-only field)
# --------------------------------------------------------------------------- #

# Redact credentials embedded in a connection URL: scheme://user:pass@host -> ***
# The scheme run is length-bounded so a long alphanumeric run that never reaches
# "://" cannot be re-scanned from every start position (keeps this linear even
# when run on untruncated text).
_URL_CREDENTIALS = re.compile(r"(?P<scheme>[a-zA-Z][a-zA-Z0-9+.\-]{0,40}://)[^/\s:@]+:[^/\s@]+@")

# Redact the whole value of an Authorization header. The value is redacted in
# full (not just the first token) so multi-token schemes -- e.g. SigV4
# "AWS4-HMAC-SHA256 Credential=..., Signature=..." -- cannot leak their
# credential/signature parameters. A quoted value (JSON / dict text) is redacted
# up to its closing quote so surrounding fields survive; an unquoted header value
# is redacted to end of line.
_AUTH_HEADER = re.compile(
    r"(?i)(?P<head>\bauthorization[\"']?\s*[:=]\s*)"
    r"(?P<val>\"[^\"\r\n]*\"|'[^'\r\n]*'|[^\r\n]+)"
)

# Redact key=value / key: value pairs whose key names a secret. Each pattern
# treats "-" and "_" interchangeably, so header-style ("x-api-key", "X-Auth-
# Token") and snake_case ("api_key") spellings are both covered. The separator
# allows an optional closing quote before it so quoted keys are covered too --
# JSON and Python-dict/repr forms (e.g. {"password": "x"}, 'api-key': 'x') are
# the dominant shape in driver / pydantic error text.
_SECRET_KEY_PATTERNS = (
    r"x[-_]?api[-_]?key", r"api[-_]?key", r"apikey",
    r"x[-_]?auth[-_]?token", r"auth[-_]?token",
    r"access[-_]?token", r"refresh[-_]?token", r"id[-_]?token",
    r"client[-_]?secret", r"secret[-_]?key", r"private[-_]?key",
    r"access[-_]?key", r"secret[-_]?access[-_]?key",
    r"password", r"passwd", r"pwd", r"secret", r"token",
)
_SECRET_KV = re.compile(
    r"(?i)\b(?P<key>" + "|".join(_SECRET_KEY_PATTERNS) + r")"
    r"(?P<sep>[\"']?\s*[=:]\s*)"
    r"(?P<val>\"[^\"]*\"|'[^']*'|[^\s,;&)]+)"
)

_REDACTED = "***"
_MAX_DETAIL_LEN = 2000


def sanitize_detail(text: str) -> str:
    """Redact credential-bearing substrings from raw exception text.

    Defense in depth for the metrics record's internal-only ``error_detail``
    field: even though the control plane never forwards it externally, scrubbing
    keeps post-expansion connection values (DSN passwords, tokens, API keys) from
    sprawling into the metrics store. ``str(exception)`` carries no traceback, so
    nothing here strips one; the length cap guards against an unusually long
    message. Customer-facing text uses :func:`customer_message` instead and never
    passes through here.
    """
    if not text:
        return text
    # Scrub before truncating: a credential whose value runs past the cap would
    # otherwise lose the trailing "@host" the URL rule needs, leaving the start
    # of the secret behind. The rules are all linear (the URL scheme run is
    # length-bounded; the others only advance from an "authorization"/key word),
    # so running them on untruncated text stays cheap.
    redacted = _URL_CREDENTIALS.sub(lambda m: f"{m.group('scheme')}{_REDACTED}:{_REDACTED}@", text)
    redacted = _AUTH_HEADER.sub(lambda m: f"{m.group('head')}{_REDACTED}", redacted)
    redacted = _SECRET_KV.sub(lambda m: f"{m.group('key')}{m.group('sep')}{_REDACTED}", redacted)
    if len(redacted) > _MAX_DETAIL_LEN:
        redacted = redacted[:_MAX_DETAIL_LEN] + "...[truncated]"
    return redacted
