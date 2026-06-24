"""Unit tests for customer-safe pipeline error classification (issues #258, #264).

Locks the surface the public run-status endpoint depends on:

- Structured-first: a ``FailureTag`` stamped at the raise site drives
  ``classify_exception`` deterministically and outranks any conflicting text.
- ``classify_source_extract`` makes the one residual source-only split
  (auth/unreachable/rate), and ``classify_handshake_failure`` the destination
  transport-vs-config split, for opaque text the engine controls.
- The name/phrase heuristics still classify an *untagged* exception (the
  defensive fallback) across the real engine/CDK hierarchy and the gRPC worker
  boundary.
- ``customer_message`` / ``error_message`` carry no exception text.
- ``build_error_detail`` emits only allowlisted-safe tokens (stage labels, error
  codes, exception class names) -- never message text, so nothing to scrub.
"""

from __future__ import annotations

import json

import pytest

from src.state.error_classification import (
    ErrorCode,
    FailureStage,
    FailureTag,
    build_error_detail,
    classify_exception,
    classify_for_metrics,
    classify_handshake_failure,
    classify_source_extract,
    customer_message,
    detail_for_code,
    is_local_io_error,
    read_failure_tag,
    tag_failure,
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
# Structured tags: the primary, deterministic path (issue #264)
# --------------------------------------------------------------------------- #

def test_tag_outranks_conflicting_text():
    # The message would classify SOURCE_UNREACHABLE by phrase, but a stage tag is
    # authoritative: the engine knew it was a destination-load failure.
    exc = _make("RuntimeError", message="connection refused")
    tag_failure(exc, code=ErrorCode.DESTINATION_WRITE_FAILED, stage=FailureStage.DESTINATION_LOAD)
    assert classify_exception(exc) is ErrorCode.DESTINATION_WRITE_FAILED


def test_tag_survives_wrapping_via_original_error():
    from src.engine.exceptions import StreamProcessingError

    inner = tag_failure(
        RuntimeError("opaque driver text"),
        code=ErrorCode.CONFIG_INVALID,
        stage=FailureStage.TRANSFORM,
    )
    wrapped = StreamProcessingError("Stream processing failed", stream_id="s1", original_error=inner)
    assert classify_exception(wrapped) is ErrorCode.CONFIG_INVALID


def test_tag_failure_does_not_overwrite_an_existing_chain_tag():
    # The no-overwrite invariant is enforced in tag_failure itself, so an outer
    # stage boundary calling it cannot clobber a precise inner tag.
    inner = tag_failure(RuntimeError("worker config error"),
                        code=ErrorCode.CONFIG_INVALID, stage=FailureStage.SOURCE_EXTRACT)
    tag_failure(inner, code=ErrorCode.DESTINATION_WRITE_FAILED, stage=FailureStage.DESTINATION_LOAD)
    assert read_failure_tag(inner) == FailureTag(
        code=ErrorCode.CONFIG_INVALID, stage=FailureStage.SOURCE_EXTRACT
    )
    # A deeper tag on the cause chain also blocks an outer tag on the wrapper.
    outer = RuntimeError("outer")
    outer.__cause__ = tag_failure(RuntimeError("deep"),
                                  code=ErrorCode.SOURCE_UNREACHABLE, stage=FailureStage.SOURCE_EXTRACT)
    tag_failure(outer, code=ErrorCode.INTERNAL, stage=FailureStage.DESTINATION_LOAD)
    assert read_failure_tag(outer).code is ErrorCode.SOURCE_UNREACHABLE


def test_read_failure_tag_picks_dominant_across_group():
    g = ExceptionGroup("All streams failed", [
        tag_failure(RuntimeError("a"), code=ErrorCode.SOURCE_UNREACHABLE, stage=FailureStage.SOURCE_EXTRACT),
        tag_failure(RuntimeError("b"), code=ErrorCode.CONFIG_INVALID, stage=FailureStage.CONFIG),
    ])
    assert read_failure_tag(g) == FailureTag(code=ErrorCode.CONFIG_INVALID, stage=FailureStage.CONFIG)
    assert classify_exception(g) is ErrorCode.CONFIG_INVALID


def test_untagged_exception_has_no_tag():
    assert read_failure_tag(RuntimeError("nothing here")) is None


def test_code_priority_ranks_every_error_code():
    from src.state.error_classification import _CODE_PRIORITY

    assert set(_CODE_PRIORITY) == set(ErrorCode)


# --------------------------------------------------------------------------- #
# classify_source_extract: the source-only fine split (issue #264)
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("message,expected", [
    ("password authentication failed for user 'analitiq'", ErrorCode.SOURCE_AUTH_FAILED),
    ("request returned 401", ErrorCode.SOURCE_AUTH_FAILED),
    ("upstream rate limit; 429 too many requests", ErrorCode.RATE_LIMITED),
    ("could not connect to server: Connection refused", ErrorCode.SOURCE_UNREACHABLE),
    ("some opaque driver failure", ErrorCode.INTERNAL),
])
def test_classify_source_extract_splits(message, expected):
    assert classify_source_extract(_make("RuntimeError", message=message)) is expected


def test_classify_source_extract_routes_config():
    # A deterministic source-config error / type-map miss is a config defect (the
    # engine validates no data schema).
    assert classify_source_extract(
        _make("SecretNotFoundError", message="missing ${password}")
    ) is ErrorCode.CONFIG_INVALID
    assert classify_source_extract(
        _make("UnmappedTypeError", message="no forward rule for geography")
    ) is ErrorCode.CONFIG_INVALID


def test_destination_http_code_never_read_as_source_auth():
    # The cross-stage tail: a destination-load failure whose cause text carries a
    # "401" must classify DESTINATION_WRITE_FAILED, because the stage is tagged at
    # the raise site -- the source split never runs on it.
    exc = _make("RuntimeError", message="POST https://api.dest/v1 -> 401 from destination")
    tag_failure(exc, code=ErrorCode.DESTINATION_WRITE_FAILED, stage=FailureStage.DESTINATION_LOAD)
    assert classify_exception(exc) is ErrorCode.DESTINATION_WRITE_FAILED


@pytest.mark.parametrize("reason,expected", [
    ("destination did not acknowledge the schema within 30s", ErrorCode.DESTINATION_WRITE_FAILED),
    ("destination closed stream before sending schema ACK", ErrorCode.DESTINATION_WRITE_FAILED),
    ("destination worker channel did not connect", ErrorCode.DESTINATION_WRITE_FAILED),
    ("SchemaConfigurationError: unsupported write mode", ErrorCode.CONFIG_INVALID),
    ("Schema configuration failed", ErrorCode.CONFIG_INVALID),
    (None, ErrorCode.CONFIG_INVALID),
])
def test_classify_handshake_failure_splits_transport_vs_config(reason, expected):
    # The engine calls this at the handshake raise site so the tag is definite;
    # the same transport-phrase set backs the untagged fallback.
    assert classify_handshake_failure(reason) is expected


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
    # in the fatal-ack summary) is a config defect, not a write failure. These
    # cover write_batch's controlled prefixes (cdk/sql/generic.py).
    ("UnmappedTypeError: no rule for 'geography'", ErrorCode.CONFIG_INVALID),
    ("SchemaConfigurationError: unsupported write mode", ErrorCode.CONFIG_INVALID),
    ("type-map: no reverse rule", ErrorCode.CONFIG_INVALID),
    ("dialect: upsert not supported", ErrorCode.CONFIG_INVALID),
    ("write-config: invalid conflict key", ErrorCode.CONFIG_INVALID),
    ("adbc: missing driver package", ErrorCode.CONFIG_INVALID),
    # A genuine write failure (constraint / permission on the destination) stays
    # a write failure.
    ("duplicate key value violates unique constraint", ErrorCode.DESTINATION_WRITE_FAILED),
    ("permission denied for table orders", ErrorCode.DESTINATION_WRITE_FAILED),
])
def test_destination_write_config_cause_is_config(summary, expected):
    msg = f"Batch 3 fatal failure: {summary}"
    assert classify_exception(_make("StreamProcessingError", message=msg)) is expected


@pytest.mark.parametrize("message,expected", [
    # A source-worker ReadError marked deterministic is a contract/config defect.
    ("ReadError: offset pagination requires offset.param (worker src-worker:api:s1, deterministic)",
     ErrorCode.CONFIG_INVALID),
    # ...unless it carries an auth/rate signal, which wins first.
    ("ReadError: 403 Forbidden (worker src-worker:api:s1, deterministic)",
     ErrorCode.SOURCE_AUTH_FAILED),
    # A non-deterministic (retryable) worker error stays INTERNAL, not config.
    ("RuntimeError: transient blip (worker src-worker:api:s1)", ErrorCode.INTERNAL),
])
def test_deterministic_source_read_error_is_config(message, expected):
    assert classify_exception(_make("ReadError", base=RuntimeError, message=message)) is expected


def test_partial_stream_group_classifies_dominant_cause():
    # A partial run keeps ALL failed-stream exceptions; classification picks the
    # dominant across them (config/auth outrank an unclassifiable internal leaf).
    group = ExceptionGroup("Partial stream failures", [
        _make("StreamProcessingError", message="Stream processing failed: weird internal thing"),
        _make("StreamProcessingError", message="Stream processing failed: password authentication failed"),
    ])
    assert classify_exception(group) is ErrorCode.SOURCE_AUTH_FAILED


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


def test_exception_group_detail_enumerates_leaves_by_class_name():
    # str(ExceptionGroup) is only the summary; build_error_detail must carry every
    # per-stream leaf so the all-streams-failed case is actionable -- as safe
    # class-name tokens, never the raw messages.
    group = ExceptionGroup("All streams failed", [
        _make("ReadError", message="Stream s1: connection refused at upstream"),
        _make("WriteError", message="Stream s2: Batch 4 fatal failure: duplicate key"),
    ])
    _, _, detail = classify_for_metrics(group)
    assert "ReadError" in detail
    assert "WriteError" in detail
    # The raw messages never enter the detail.
    assert "connection refused at upstream" not in detail
    assert "duplicate key" not in detail


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
# build_error_detail: structured, allowlisted-safe, no message text (issue #264)
# --------------------------------------------------------------------------- #

def test_error_detail_carries_no_message_text():
    # The raw text holds a DSN with a password; the detail must contain only the
    # class name -- nothing from the message, so nothing to scrub.
    exc = _make("RuntimeError", message="postgresql://app:s3cr3t@db.host:5432/prod")
    detail = build_error_detail(exc)
    assert detail == "RuntimeError"
    assert "s3cr3t" not in detail
    assert "postgresql" not in detail  # scheme prefix is never promoted into detail


def test_error_detail_excludes_message_text_even_on_the_tagged_token_path():
    # The tagged token is built by interpolation (stage/CODE:ClassName). A secret
    # in the message must never reach it -- the higher-risk path, since the token
    # format touches exception fields.
    exc = tag_failure(
        RuntimeError("Authorization: Bearer leaked-token-xyz dsn=postgres://u:p@h/db"),
        code=ErrorCode.DESTINATION_WRITE_FAILED,
        stage=FailureStage.DESTINATION_LOAD,
    )
    detail = build_error_detail(exc)
    assert detail == "destination_load/DESTINATION_WRITE_FAILED:RuntimeError"
    assert "leaked-token-xyz" not in detail
    assert "p@h" not in detail


def test_error_detail_includes_stage_and_code_for_tagged():
    exc = tag_failure(
        _make("StreamProcessingError", message="opaque"),
        code=ErrorCode.DESTINATION_WRITE_FAILED,
        stage=FailureStage.DESTINATION_LOAD,
    )
    assert build_error_detail(exc) == "destination_load/DESTINATION_WRITE_FAILED:StreamProcessingError"


def test_error_detail_enumerates_mixed_tagged_and_untagged_group():
    group = ExceptionGroup("All streams failed", [
        tag_failure(_make("ReadError"), code=ErrorCode.SOURCE_UNREACHABLE, stage=FailureStage.SOURCE_EXTRACT),
        _make("ValueError", message="opaque untagged failure"),
    ])
    assert classify_exception(group) is ErrorCode.SOURCE_UNREACHABLE
    detail = build_error_detail(group)
    assert "source_extract/SOURCE_UNREACHABLE:ReadError" in detail
    assert "ValueError" in detail


def test_error_detail_bounds_length():
    detail = build_error_detail(RuntimeError("a" * 5000))
    assert len(detail) <= 2100


def test_detail_for_code_is_structured_and_safe():
    out = detail_for_code(
        ErrorCode.DESTINATION_WRITE_FAILED,
        stage=FailureStage.DESTINATION_LOAD,
        reason="records dead-lettered after retries",
    )
    assert out == "destination_load/DESTINATION_WRITE_FAILED: records dead-lettered after retries"


# --------------------------------------------------------------------------- #
# classify_for_metrics: the (code, customer_message, detail) triple
# --------------------------------------------------------------------------- #

def test_classify_for_metrics_returns_safe_triple():
    exc = _make("RuntimeError", message="password authentication failed; dsn=postgres://u:p@h/db")
    code, message, detail = classify_for_metrics(exc)
    assert code is ErrorCode.SOURCE_AUTH_FAILED
    assert message == customer_message(ErrorCode.SOURCE_AUTH_FAILED)
    # detail is the class name only -- no message text, so no credentials.
    assert detail == "RuntimeError"
    assert "p@h" not in detail and ":p@" not in detail


def test_classify_for_metrics_prefers_tag():
    exc = tag_failure(
        _make("RuntimeError", message="connection refused"),
        code=ErrorCode.CONFIG_INVALID,
        stage=FailureStage.CONFIG,
    )
    code, message, detail = classify_for_metrics(exc)
    assert code is ErrorCode.CONFIG_INVALID
    assert detail == "config/CONFIG_INVALID:RuntimeError"


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
    # Structured, safe: the class name, not the raw "bad pipeline.json" message.
    assert rec.error_detail == "ConfigValidationError"

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
