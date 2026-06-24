"""Unit tests for customer-safe pipeline error classification (issue #258).

Locks three things the public run-status endpoint depends on:

- ``classify_exception`` maps the real engine/CDK exception hierarchy onto the
  stable :class:`ErrorCode` enum, including the gRPC worker boundary (where the
  original type is collapsed but its name survives in the message) and
  aggregated ``ExceptionGroup``s.
- ``customer_message`` / the customer-facing ``error_message`` carry no
  exception text, secrets, or stack traces.
- ``sanitize_detail`` redacts credential-bearing substrings from the
  internal-only ``error_detail`` field.
"""

from __future__ import annotations

import json

import pytest

from src.state.error_classification import (
    ErrorCode,
    classify_exception,
    classify_for_metrics,
    customer_message,
    is_local_io_error,
    sanitize_detail,
)

pytestmark = pytest.mark.unit


def _make(name: str, base: type = Exception, message: str = "") -> BaseException:
    """Build an instance of a dynamically named exception class.

    Driver errors (PEP-249 ``IntegrityError`` etc.) and the worker-boundary
    re-raises are matched by class name, so a synthetic class with the right
    name exercises the same path as the real one without importing every driver.
    """
    return type(name, (base,), {})(message)


# --------------------------------------------------------------------------- #
# CONFIG_INVALID -- real exception classes, to lock the name contract
# --------------------------------------------------------------------------- #

def test_config_real_exceptions_classify_config_invalid():
    from src.config.exceptions import (
        ConfigValidationError,
        ConfigNotFoundError,
        ConnectorNotFoundError,
    )
    from src.engine.exceptions import (
        ConfigurationError,
        StreamConfigurationError,
        PipelineValidationError,
    )
    from cdk.exceptions import TransportSpecError
    from cdk.secrets.exceptions import SecretNotFoundError, PlaceholderExpansionError
    from cdk.type_map import InvalidTypeMapError

    exceptions = [
        ConfigValidationError("invalid", field="streams"),
        ConfigNotFoundError("/x/pipeline.json"),
        ConnectorNotFoundError("postgres"),
        ConfigurationError("bad config"),
        StreamConfigurationError("missing source", stream_id="s1"),
        PipelineValidationError("validation failed"),
        TransportSpecError("malformed transport spec"),
        SecretNotFoundError("conn-1"),
        PlaceholderExpansionError("password", "conn-1"),
        InvalidTypeMapError("type map is broken"),
    ]
    for exc in exceptions:
        assert classify_exception(exc) is ErrorCode.CONFIG_INVALID, exc


# --------------------------------------------------------------------------- #
# Each category
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("message", [
    "password authentication failed for user 'analitiq'",
    "401 Unauthorized",
    "permission denied",
])
def test_source_auth_failed_by_message(message):
    assert classify_exception(_make("RuntimeError", message=message)) is ErrorCode.SOURCE_AUTH_FAILED


def test_secret_resolution_failures_are_config_invalid():
    # Secret-store resolution failures (missing / access-denied / malformed
    # credentials, placeholder expansion) are config/setup -- not source-system
    # auth, which the engine only ever sees as driver/HTTP text.
    from cdk.secrets.exceptions import (
        SecretResolutionError,
        SecretNotFoundError,
        SecretAccessDeniedError,
        PlaceholderExpansionError,
    )

    for exc in [
        SecretResolutionError("malformed credentials.json"),
        SecretNotFoundError("conn-1"),
        SecretAccessDeniedError("conn-1"),
        PlaceholderExpansionError("password", "conn-1"),
    ]:
        assert classify_exception(exc) is ErrorCode.CONFIG_INVALID, exc
    # Across the worker boundary it survives only as an "error_type:" prefix.
    boundary = _make("RuntimeError", message="SecretResolutionError: bad file (worker src-worker:pg:s1)")
    assert classify_exception(boundary) is ErrorCode.CONFIG_INVALID


@pytest.mark.parametrize("message", [
    "TransientReadError: 429 Too Many Requests (worker src)",
    "upstream returned rate limit; retry later",
    "throttled by the API",
])
def test_rate_limited(message):
    assert classify_exception(_make("RuntimeError", message=message)) is ErrorCode.RATE_LIMITED


@pytest.mark.parametrize("message", [
    "could not connect to server: Connection refused",
    "could not translate host name 'db' to address",
    "connection timed out",
])
def test_source_unreachable_by_message(message):
    assert classify_exception(_make("RuntimeError", message=message)) is ErrorCode.SOURCE_UNREACHABLE


def test_source_unreachable_by_builtin_type():
    assert classify_exception(ConnectionRefusedError("refused")) is ErrorCode.SOURCE_UNREACHABLE


# There is no schema validation in the engine, so type-map / mapping / schema-
# configuration failures are configuration defects, not a data-vs-schema
# mismatch -- they classify as CONFIG_INVALID.
@pytest.mark.parametrize("name,message", [
    ("UnmappedTypeError", "No forward type-map rule for 'geography' in connector 'postgres'"),
    ("TransformationError", "cannot convert value to Int64"),
    ("SchemaError", "source schema could not be read"),
    ("SchemaConfigurationError", "unsupported proto write mode"),
])
def test_type_and_mapping_defects_are_config_invalid(name, message):
    assert classify_exception(_make(name, message=message)) is ErrorCode.CONFIG_INVALID


@pytest.mark.parametrize("name,message", [
    ("StreamProcessingError", "Failed to connect to gRPC destination for stream orders"),
    ("WriteError", "destination write failed"),
    # Load-stage wrappers from the engine around a destination ack failure: the
    # destination summary alone (e.g. "permission denied") would look like auth,
    # so the wrapper text must route it to the destination bucket.
    ("StreamProcessingError", "Batch 3 fatal failure: permission denied for table orders"),
    ("StreamProcessingError", "Batch 3 failed after 3 retries: connection reset by peer"),
    ("StreamProcessingError", "Batch 3 fatal failure: duplicate key value violates unique constraint"),
    ("StreamProcessingError", "Batch 3 unknown ACK status: 7"),
])
def test_destination_write_failed(name, message):
    assert classify_exception(_make(name, message=message)) is ErrorCode.DESTINATION_WRITE_FAILED


@pytest.mark.parametrize("summary,expected", [
    # A destination batch write that fails with a CONFIG-typed cause (forwarded
    # in the fatal-ack summary) is a config defect, not a write failure.
    ("UnmappedTypeError: no rule for 'geography'", ErrorCode.CONFIG_INVALID),
    ("SchemaConfigurationError: unsupported write mode", ErrorCode.CONFIG_INVALID),
    ("type-map: no reverse rule", ErrorCode.CONFIG_INVALID),
    # A genuine write failure (constraint / permission on the destination) stays
    # a write failure.
    ("duplicate key value violates unique constraint", ErrorCode.DESTINATION_WRITE_FAILED),
    ("permission denied for table orders", ErrorCode.DESTINATION_WRITE_FAILED),
])
def test_destination_write_config_cause_is_config(summary, expected):
    msg = f"Batch 3 fatal failure: {summary}"
    assert classify_exception(_make("StreamProcessingError", message=msg)) is expected


@pytest.mark.parametrize("message,expected", [
    # PEP-249 driver names are transport-ambiguous; a live driver exception in
    # the chain comes from the SOURCE worker (the "src-worker" label), so it must
    # not be routed to DESTINATION_WRITE_FAILED. It classifies by its message.
    ("ProgrammingError: permission denied for relation users (worker src-worker:pg:s1)",
     ErrorCode.SOURCE_AUTH_FAILED),
    ("IntegrityError: null value in column (worker src-worker:pg:s1)",
     ErrorCode.INTERNAL),
])
def test_source_driver_error_not_destination(message, expected):
    assert classify_exception(_make("RuntimeError", message=message)) is expected


def test_real_typemap_mro_routing():
    # A type-map miss and a broken/missing type-map file are both configuration
    # defects (no schema validation exists in the engine).
    from cdk.type_map import UnmappedTypeError, InvalidTypeMapError

    unmapped = UnmappedTypeError("postgres", "forward", "geography")
    assert classify_exception(unmapped) is ErrorCode.CONFIG_INVALID
    assert classify_exception(InvalidTypeMapError("broken file")) is ErrorCode.CONFIG_INVALID


@pytest.mark.parametrize("error_type,expected", [
    ("SecretNotFoundError", ErrorCode.CONFIG_INVALID),
    ("ConnectorNotRegisteredError", ErrorCode.CONFIG_INVALID),
    ("SecretAccessDeniedError", ErrorCode.CONFIG_INVALID),
])
def test_worker_error_type_prefix_promoted_to_name(error_type, expected):
    # The gRPC worker collapses the type to ReadError/RuntimeError but preserves
    # the original class name as an "error_type:" prefix; the name-based rules
    # must still fire off that prefix even with no matching message phrase.
    msg = f"{error_type}: opaque detail (worker src, deterministic)"
    assert classify_exception(_make("ReadError", base=RuntimeError, message=msg)) is expected


def test_unknown_falls_back_to_internal():
    assert classify_exception(_make("RuntimeError", message="something weird")) is ErrorCode.INTERNAL
    assert classify_exception(ValueError("nope")) is ErrorCode.INTERNAL


def test_local_filesystem_error_is_internal_not_source_auth():
    # A builtin PermissionError from creating local state/deadletter dirs is an
    # engine/infra fault; its "[Errno 13] Permission denied" text must not be
    # read as source auth.
    assert classify_exception(
        PermissionError("[Errno 13] Permission denied: '/app/state'")
    ) is ErrorCode.INTERNAL
    # A real source DB permission failure (not a builtin OSError) still routes to auth.
    assert classify_exception(
        _make("OperationalError", message="FATAL: permission denied for database")
    ) is ErrorCode.SOURCE_AUTH_FAILED


@pytest.mark.parametrize("reason,expected", [
    # transport reasons -> the destination was unreachable mid-handshake. These
    # cover the direct path and the worker-proxy path (the proxy forwards the
    # inner reason text), so no proto change is needed to tell them apart.
    ("destination did not acknowledge the schema within 30s", ErrorCode.DESTINATION_WRITE_FAILED),
    ("stream reader/writer exited before schema ACK: boom", ErrorCode.DESTINATION_WRITE_FAILED),
    ("destination closed stream before sending schema ACK", ErrorCode.DESTINATION_WRITE_FAILED),
    ("destination worker channel did not connect", ErrorCode.DESTINATION_WRITE_FAILED),
    # configuration reasons -> a destination config defect (configure_schema only
    # builds the destination's own table; nothing is validated against a schema).
    ("SchemaConfigurationError: unsupported write mode", ErrorCode.CONFIG_INVALID),
    ("PlaceholderExpansionError: failed to expand 'pw'", ErrorCode.CONFIG_INVALID),
    ("Schema configuration failed", ErrorCode.CONFIG_INVALID),
])
def test_destination_handshake_failure_routing(reason, expected):
    msg = f"Destination did not accept the stream for orders: {reason}"
    assert classify_exception(_make("StreamProcessingError", message=msg)) is expected


@pytest.mark.parametrize("message,expected", [
    # HTTP status codes are matched with word boundaries: standalone codes
    # classify, but digits inside counts / IDs must not collide.
    ("request returned 401", ErrorCode.SOURCE_AUTH_FAILED),
    ("got HTTP 403 from source", ErrorCode.SOURCE_AUTH_FAILED),
    ("received 429 from api", ErrorCode.RATE_LIMITED),
    ("failed after 1429 records", ErrorCode.INTERNAL),
    ("object 4035 not found", ErrorCode.INTERNAL),
    ("bad id 24015 in row", ErrorCode.INTERNAL),
    # A status code embedded in a URL path is not the response status.
    ("GET https://api.example.com/users/403 -> status 404", ErrorCode.INTERNAL),
    ("path /429/items failed with status 500", ErrorCode.INTERNAL),
])
def test_http_status_codes_use_word_boundaries(message, expected):
    assert classify_exception(_make("RuntimeError", message=message)) is expected


# --------------------------------------------------------------------------- #
# Chain handling: worker boundary, ExceptionGroup, cause/original_error, cycles
# --------------------------------------------------------------------------- #

def test_worker_boundary_prefix_drives_subcategory():
    # Worker collapses the type to ReadError but keeps the original class name as
    # an "error_type:" prefix; classification must read it from the message.
    exc = _make(
        "ReadError",
        base=RuntimeError,
        message="ConnectionError: connection refused (worker src, deterministic)",
    )
    assert classify_exception(exc) is ErrorCode.SOURCE_UNREACHABLE


def test_exception_group_picks_dominant_cause():
    group = ExceptionGroup("All streams failed", [
        _make("RuntimeError", message="some internal thing"),
        _make("ConnectionError", message="connection refused"),
    ])
    # source-unreachable outranks the unclassifiable internal leaf
    assert classify_exception(group) is ErrorCode.SOURCE_UNREACHABLE


def test_exception_group_detail_includes_leaf_messages():
    # str(ExceptionGroup) is only the summary; error_detail must carry the
    # per-stream causes so the all-streams-failed case is actionable.
    group = ExceptionGroup("All streams failed", [
        _make("StreamProcessingError", message="Stream s1: connection refused at upstream"),
        _make("StreamProcessingError", message="Stream s2: Batch 4 fatal failure: duplicate key"),
    ])
    _, _, detail = classify_for_metrics(group)
    assert "connection refused at upstream" in detail
    assert "duplicate key" in detail


@pytest.mark.parametrize("exc,expected", [
    (PermissionError("[Errno 13] Permission denied: '/app/connections/c.json'"), True),
    (FileExistsError("exists"), True),
    (RuntimeError("Could not find pipelines/manifest.json"), False),
    (ValueError("manifest.json missing required key"), False),
])
def test_is_local_io_error(exc, expected):
    assert is_local_io_error(exc) is expected


def test_original_error_attribute_is_followed():
    from src.engine.exceptions import StreamProcessingError

    wrapped = StreamProcessingError(
        "Stream processing failed",
        stream_id="s1",
        original_error=_make("RuntimeError", message="password authentication failed"),
    )
    assert classify_exception(wrapped) is ErrorCode.SOURCE_AUTH_FAILED


def test_cause_chain_is_followed():
    inner = _make("ConnectionError", message="could not connect: connection refused")
    outer = RuntimeError("stream failed")
    outer.__cause__ = inner
    assert classify_exception(outer) is ErrorCode.SOURCE_UNREACHABLE


def test_self_referential_chain_does_not_hang():
    exc = RuntimeError("loop")
    exc.__cause__ = exc  # pathological; must not spin
    assert classify_exception(exc) is ErrorCode.INTERNAL


# --------------------------------------------------------------------------- #
# Customer message: safe, fixed, non-empty
# --------------------------------------------------------------------------- #

def test_customer_message_is_safe_for_every_code():
    for code in tuple(ErrorCode):
        msg = customer_message(code)
        assert msg and isinstance(msg, str)
        lowered = msg.lower()
        for leak in ("password", "secret", "token", "traceback", "://"):
            assert leak not in lowered


# --------------------------------------------------------------------------- #
# sanitize_detail
# --------------------------------------------------------------------------- #

def test_sanitize_detail_redacts_url_credentials():
    out = sanitize_detail("could not connect: postgresql://app:s3cr3t@db.host:5432/prod")
    assert "s3cr3t" not in out
    assert "app" not in out
    assert "db.host:5432/prod" in out  # non-secret topology kept


# The "secret" values below are deliberately fake, low-entropy placeholders so a
# secret scanner does not flag them; the scrubber keys off the field name, not
# the value, so realism is irrelevant.
@pytest.mark.parametrize("raw,secret", [
    ("auth failed token=fake-token-aa boom", "fake-token-aa"),
    ("password = 'fake-pw-bb' rejected", "fake-pw-bb"),
    ('api_key="fake-key-cc" denied', "fake-key-cc"),
    ("Authorization: Bearer fake-bearer-dd", "fake-bearer-dd"),
])
def test_sanitize_detail_redacts_secret_key_values(raw, secret):
    out = sanitize_detail(raw)
    assert secret not in out
    assert "***" in out


@pytest.mark.parametrize("raw,secret", [
    ('{"password": "fake-pw-ee"}', "fake-pw-ee"),       # JSON body
    ("{'password': 'fake-pw-ff'}", "fake-pw-ff"),       # Python dict repr
    ("{'api_key': 'fake-key-gg', 'x': 1}", "fake-key-gg"),  # dict among other keys
    ('"client_secret" : "fake-cs-hh"', "fake-cs-hh"),   # spaced quoted key
])
def test_sanitize_detail_redacts_quoted_secret_keys(raw, secret):
    out = sanitize_detail(raw)
    assert secret not in out
    assert "***" in out


@pytest.mark.parametrize("raw,secret", [
    ("X-API-Key: fake-key-ii", "fake-key-ii"),          # header style, hyphenated
    ('{"api-key": "fake-key-jj"}', "fake-key-jj"),       # hyphenated JSON key
    ("x-auth-token=fake-token-kk", "fake-token-kk"),     # hyphenated header token
    ("secret_access_key: fake/value-ll", "value-ll"),
])
def test_sanitize_detail_redacts_hyphenated_secret_keys(raw, secret):
    out = sanitize_detail(raw)
    assert secret not in out
    assert "***" in out


def test_sanitize_detail_redacts_multitoken_authorization():
    # SigV4-style headers carry the credential across several space/comma-
    # separated parameters; the whole value must be redacted, not just the
    # first token.
    out = sanitize_detail(
        "Authorization: AWS4-HMAC-SHA256 Credential=fake-id/x, Signature=fake-sig-mm"
    )
    assert "fake-sig-mm" not in out
    assert "fake-id" not in out
    assert out == "Authorization: ***"


def test_sanitize_detail_redacts_quoted_authorization_keeps_siblings():
    out = sanitize_detail('{"Authorization": "Bearer fake-bearer-nn", "user": "keepme"}')
    assert "fake-bearer-nn" not in out
    assert "keepme" in out  # sibling field preserved (quoted value stops at quote)


def test_sanitize_detail_passes_through_plain_text():
    assert sanitize_detail("table orders not found") == "table orders not found"


def test_sanitize_detail_truncates_long_text():
    out = sanitize_detail("x" * 5000)
    assert len(out) <= 2100
    assert out.endswith("...[truncated]")


def test_sanitize_detail_handles_empty():
    assert sanitize_detail("") == ""


def test_sanitize_detail_bounds_long_unbroken_token():
    # A long unbroken alphanumeric run must be truncated before the regexes run,
    # so scrubbing stays bounded (no quadratic scan looking for "://").
    out = sanitize_detail("Authorization: " + "a" * 200000)
    assert len(out) <= 2100
    assert "aaaaaaaaaa" not in out  # the token did not survive


# --------------------------------------------------------------------------- #
# classify_for_metrics: the (code, customer_message, detail) triple
# --------------------------------------------------------------------------- #

def test_classify_for_metrics_returns_safe_triple():
    exc = _make("RuntimeError", message="password authentication failed; dsn=postgres://u:p@h/db")
    code, message, detail = classify_for_metrics(exc)
    assert code is ErrorCode.SOURCE_AUTH_FAILED
    assert message == customer_message(ErrorCode.SOURCE_AUTH_FAILED)
    assert "p@h" not in detail and ":p@" not in detail  # raw creds scrubbed in detail


# --------------------------------------------------------------------------- #
# Metrics record wiring + JSON shape (the published contract surface)
# --------------------------------------------------------------------------- #

def test_metrics_record_failure_carries_safe_fields():
    from datetime import datetime, timezone
    from src.state.metrics_storage import create_metrics_record

    code, message, detail = classify_for_metrics(
        _make("ConfigValidationError", message="bad pipeline.json")
    )
    rec = create_metrics_record(
        run_id="r1",
        pipeline_id="p1",
        start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end_time=datetime(2026, 1, 1, 0, 0, 5, tzinfo=timezone.utc),
        status="failed",
        error_code=code,
        error_message=message,
        error_detail=detail,
    )
    assert rec.error_code is ErrorCode.CONFIG_INVALID
    assert rec.error_message == customer_message(ErrorCode.CONFIG_INVALID)
    assert rec.error_detail == "bad pipeline.json"

    # The error_code must serialize as the plain enum string in the emitted line,
    # exactly as emit_metrics_log -> json.dumps(..., default=str) would write it.
    emitted = json.dumps({"type": "pipeline", **rec.model_dump()}, default=str)
    assert '"error_code": "CONFIG_INVALID"' in emitted
    assert "ErrorCode." not in emitted


def test_metrics_record_success_leaves_error_fields_none():
    from datetime import datetime, timezone
    from src.state.metrics_storage import create_metrics_record

    rec = create_metrics_record(
        run_id="r1",
        pipeline_id="p1",
        start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end_time=datetime(2026, 1, 1, 0, 0, 5, tzinfo=timezone.utc),
        records_processed=10,
        status="success",
    )
    assert rec.status == "success"
    assert rec.error_code is None
    assert rec.error_message is None
    assert rec.error_detail is None
    assert rec.records_total == 10
