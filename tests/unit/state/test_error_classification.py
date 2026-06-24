"""Unit tests for customer-safe pipeline error classification (issues #258, #264).

Locks the surface the public run-status endpoint depends on:

- Structured-first: a ``FailureTag`` stamped at the raise site drives
  ``classify_exception`` deterministically and outranks any conflicting text.
- ``classify_source_extract`` makes the one residual source-only split
  (auth/unreachable/rate) for an opaque driver error.
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
    # The engine wraps each per-stream failure in StreamProcessingError(
    # original_error=...); the tag on the inner error must still be read.
    from src.engine.exceptions import StreamProcessingError

    inner = tag_failure(
        RuntimeError("opaque driver text"),
        code=ErrorCode.SCHEMA_MISMATCH,
        stage=FailureStage.TRANSFORM,
    )
    wrapped = StreamProcessingError("Stream processing failed", stream_id="s1", original_error=inner)
    assert classify_exception(wrapped) is ErrorCode.SCHEMA_MISMATCH


def test_read_failure_tag_picks_dominant_across_group():
    # A group with several tagged leaves resolves to the highest-priority code.
    g = ExceptionGroup("All streams failed", [
        tag_failure(RuntimeError("a"), code=ErrorCode.SOURCE_UNREACHABLE, stage=FailureStage.SOURCE_EXTRACT),
        tag_failure(RuntimeError("b"), code=ErrorCode.CONFIG_INVALID, stage=FailureStage.CONFIG),
    ])
    assert read_failure_tag(g) == FailureTag(code=ErrorCode.CONFIG_INVALID, stage=FailureStage.CONFIG)
    assert classify_exception(g) is ErrorCode.CONFIG_INVALID


def test_untagged_exception_has_no_tag():
    assert read_failure_tag(RuntimeError("nothing here")) is None


def test_tag_failure_does_not_overwrite_an_existing_chain_tag():
    # The no-overwrite invariant is enforced in tag_failure itself, so an outer
    # stage boundary calling it cannot clobber a precise inner tag -- "innermost
    # wins" holds without a guard at every call site.
    inner = tag_failure(RuntimeError("worker config error"),
                        code=ErrorCode.CONFIG_INVALID, stage=FailureStage.SOURCE_EXTRACT)
    # A coarser outer stage tries to tag the same exception: must be a no-op.
    tag_failure(inner, code=ErrorCode.DESTINATION_WRITE_FAILED, stage=FailureStage.DESTINATION_LOAD)
    assert read_failure_tag(inner) == FailureTag(
        code=ErrorCode.CONFIG_INVALID, stage=FailureStage.SOURCE_EXTRACT
    )
    # A deeper tag on the cause chain also blocks an outer tag on the wrapper.
    outer = RuntimeError("outer")
    outer.__cause__ = tag_failure(RuntimeError("deep"),
                                  code=ErrorCode.SCHEMA_MISMATCH, stage=FailureStage.TRANSFORM)
    tag_failure(outer, code=ErrorCode.INTERNAL, stage=FailureStage.DESTINATION_LOAD)
    assert read_failure_tag(outer).code is ErrorCode.SCHEMA_MISMATCH


def test_code_priority_ranks_every_error_code():
    # read_failure_tag ranks via _CODE_PRIORITY.index(code); a missing member
    # would raise mid-classification. The import-time guard keeps it total.
    from src.state.error_classification import _CODE_PRIORITY

    assert set(_CODE_PRIORITY) == set(ErrorCode)


# --------------------------------------------------------------------------- #
# classify_source_extract: the one source-only fine split (issue #264)
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("message,expected", [
    ("password authentication failed for user 'analitiq'", ErrorCode.SOURCE_AUTH_FAILED),
    ("request returned 401", ErrorCode.SOURCE_AUTH_FAILED),
    ("upstream rate limit; 429 too many requests", ErrorCode.RATE_LIMITED),
    ("could not connect to server: Connection refused", ErrorCode.SOURCE_UNREACHABLE),
    ("connection timed out", ErrorCode.SOURCE_UNREACHABLE),
    ("some opaque driver failure", ErrorCode.INTERNAL),
])
def test_classify_source_extract_splits(message, expected):
    assert classify_source_extract(_make("RuntimeError", message=message)) is expected


def test_classify_source_extract_routes_config_and_schema():
    # A deterministic source-config error stays CONFIG_INVALID; a source-side
    # type-map miss stays SCHEMA_MISMATCH (must not be upgraded to config).
    assert classify_source_extract(
        _make("SecretNotFoundError", message="missing ${password}")
    ) is ErrorCode.CONFIG_INVALID
    assert classify_source_extract(
        _make("UnmappedTypeError", message="no forward rule for geography")
    ) is ErrorCode.SCHEMA_MISMATCH


def test_destination_http_code_never_read_as_source_auth():
    # The cross-stage tail from #261: a destination-load failure whose cause text
    # carries a "401" must classify DESTINATION_WRITE_FAILED, because the stage is
    # tagged at the raise site -- the source split never runs on it.
    exc = _make("RuntimeError", message="POST https://api.dest/v1 -> 401 from destination")
    tag_failure(exc, code=ErrorCode.DESTINATION_WRITE_FAILED, stage=FailureStage.DESTINATION_LOAD)
    assert classify_exception(exc) is ErrorCode.DESTINATION_WRITE_FAILED


# --------------------------------------------------------------------------- #
# Fallback heuristics: untagged exceptions (defensive path)
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


@pytest.mark.parametrize("message", [
    "password authentication failed for user 'analitiq'",
    "401 Unauthorized",
    "permission denied",
])
def test_source_auth_failed_by_message(message):
    assert classify_exception(_make("RuntimeError", message=message)) is ErrorCode.SOURCE_AUTH_FAILED


def test_source_auth_failed_by_type():
    from cdk.secrets.exceptions import SecretAccessDeniedError

    assert classify_exception(SecretAccessDeniedError("conn-1")) is ErrorCode.SOURCE_AUTH_FAILED


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


@pytest.mark.parametrize("name,message", [
    ("UnmappedTypeError", "No forward type-map rule for 'geography' in connector 'postgres'"),
    ("TransformationError", "cannot convert value to Int64"),
    ("SchemaError", "schema validation failed"),
    ("StreamProcessingError", "Destination rejected schema for stream orders"),
])
def test_schema_mismatch(name, message):
    assert classify_exception(_make(name, message=message)) is ErrorCode.SCHEMA_MISMATCH


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
    # UnmappedTypeError subclasses TypeMapError but must reach SCHEMA_MISMATCH,
    # while a broken/missing type-map file stays CONFIG_INVALID -- the classifier
    # must not match the shared base.
    from cdk.type_map import UnmappedTypeError, InvalidTypeMapError

    unmapped = UnmappedTypeError("postgres", "forward", "geography")
    assert classify_exception(unmapped) is ErrorCode.SCHEMA_MISMATCH
    assert classify_exception(InvalidTypeMapError("broken file")) is ErrorCode.CONFIG_INVALID


@pytest.mark.parametrize("error_type,expected", [
    ("SecretNotFoundError", ErrorCode.CONFIG_INVALID),
    ("ConnectorNotRegisteredError", ErrorCode.CONFIG_INVALID),
    ("SecretAccessDeniedError", ErrorCode.SOURCE_AUTH_FAILED),
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


def test_handshake_transport_failure_is_destination_not_schema():
    # start_stream() returning False for a transport reason (timeout / peer
    # close) must be a destination write failure, not a schema mismatch.
    assert classify_exception(_make(
        "StreamProcessingError",
        message="Destination handshake failed for stream orders: "
                "destination did not acknowledge the schema within 30s",
    )) is ErrorCode.DESTINATION_WRITE_FAILED
    # An explicit SchemaAck NACK stays a schema mismatch.
    assert classify_exception(_make(
        "StreamProcessingError",
        message="Destination rejected schema for stream orders: unsupported write mode",
    )) is ErrorCode.SCHEMA_MISMATCH


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


def test_original_error_attribute_is_followed():
    from src.engine.exceptions import StreamProcessingError
    from cdk.secrets.exceptions import SecretAccessDeniedError

    wrapped = StreamProcessingError(
        "Stream processing failed",
        stream_id="s1",
        original_error=SecretAccessDeniedError("conn-1"),
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


def test_error_detail_includes_stage_and_code_for_tagged():
    exc = tag_failure(
        _make("StreamProcessingError", message="opaque"),
        code=ErrorCode.DESTINATION_WRITE_FAILED,
        stage=FailureStage.DESTINATION_LOAD,
    )
    assert build_error_detail(exc) == "destination_load/DESTINATION_WRITE_FAILED:StreamProcessingError"


def test_error_detail_excludes_message_text_even_on_the_tagged_token_path():
    # The tagged token is built by interpolation (stage/CODE:ClassName). A secret
    # in the message must never reach it -- this is the higher-risk path than the
    # bare-class-name case, since the token format touches exception fields.
    exc = tag_failure(
        RuntimeError("Authorization: Bearer leaked-token-xyz dsn=postgres://u:p@h/db"),
        code=ErrorCode.DESTINATION_WRITE_FAILED,
        stage=FailureStage.DESTINATION_LOAD,
    )
    detail = build_error_detail(exc)
    assert detail == "destination_load/DESTINATION_WRITE_FAILED:RuntimeError"
    assert "leaked-token-xyz" not in detail
    assert "p@h" not in detail


def test_error_detail_enumerates_mixed_tagged_and_untagged_group():
    # The realistic all-streams-failed group is mixed: one stream tagged, one
    # raised an untagged opaque error. classify picks the dominant tag; detail
    # keeps the untagged leaf's class name without crashing on its None tag.
    group = ExceptionGroup("All streams failed", [
        tag_failure(_make("ReadError"), code=ErrorCode.SOURCE_UNREACHABLE, stage=FailureStage.SOURCE_EXTRACT),
        _make("ValueError", message="opaque untagged failure"),
    ])
    assert classify_exception(group) is ErrorCode.SOURCE_UNREACHABLE
    detail = build_error_detail(group)
    assert "source_extract/SOURCE_UNREACHABLE:ReadError" in detail
    assert "ValueError" in detail


def test_error_detail_enumerates_group_leaves_by_tag():
    # A group with distinctly tagged leaves keeps both, not just the summary.
    group = ExceptionGroup("All streams failed", [
        tag_failure(_make("ReadError"), code=ErrorCode.SOURCE_UNREACHABLE, stage=FailureStage.SOURCE_EXTRACT),
        tag_failure(_make("WriteError"), code=ErrorCode.DESTINATION_WRITE_FAILED, stage=FailureStage.DESTINATION_LOAD),
    ])
    detail = build_error_detail(group)
    assert "source_extract/SOURCE_UNREACHABLE:ReadError" in detail
    assert "destination_load/DESTINATION_WRITE_FAILED:WriteError" in detail


def test_error_detail_handles_empty_and_bounds_length():
    assert build_error_detail(BaseExceptionGroup("empty", [ValueError("x")])) is not None
    # Length is bounded even for a deep chain.
    exc = RuntimeError("a" * 5000)
    detail = build_error_detail(exc)
    assert len(detail) <= 2100


def test_detail_for_code_is_structured_and_safe():
    out = detail_for_code(
        ErrorCode.DESTINATION_WRITE_FAILED,
        stage=FailureStage.DESTINATION_LOAD,
        reason="records dead-lettered after retries",
    )
    assert out == "destination_load/DESTINATION_WRITE_FAILED: records dead-lettered after retries"


# --------------------------------------------------------------------------- #
# is_local_io_error
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("exc,expected", [
    (PermissionError("[Errno 13] Permission denied: '/app/connections/c.json'"), True),
    (FileExistsError("exists"), True),
    (RuntimeError("Could not find pipelines/manifest.json"), False),
    (ValueError("manifest.json missing required key"), False),
])
def test_is_local_io_error(exc, expected):
    assert is_local_io_error(exc) is expected


# --------------------------------------------------------------------------- #
# classify_for_metrics: the (code, customer_message, detail) triple
# --------------------------------------------------------------------------- #

def test_classify_for_metrics_returns_safe_triple():
    exc = _make("RuntimeError", message="password authentication failed; dsn=postgres://u:p@h/db")
    code, message, detail = classify_for_metrics(exc)
    assert code is ErrorCode.SOURCE_AUTH_FAILED
    assert message == customer_message(ErrorCode.SOURCE_AUTH_FAILED)
    # detail is the class name only -- no message text, so no credentials. Assert
    # both the exact shape and, explicitly, that the DSN password never leaks
    # (the negative guard survives a future token-format change).
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
    assert message == customer_message(ErrorCode.CONFIG_INVALID)
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
