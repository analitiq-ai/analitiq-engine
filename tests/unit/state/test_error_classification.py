"""Unit tests for customer-safe pipeline error classification.

Issues #258, #264, #351, #401, #429.

Locks the surface the public run-status endpoint depends on:

- The boundary that knows the cause says so. A ``FailureTag`` stamped at the
  raise site drives ``classify_exception`` verbatim, and an exception that
  arrives untagged is an engine defect reported as INTERNAL -- never a code
  reverse-engineered from the exception's type or message.
- A declared category outranks the stage default, and is range-checked rather
  than trusted.
- ``FailureCategory`` and ``ErrorCode`` stay separate vocabularies, so a
  connector can never name a customer-facing outcome.
- ``customer_message`` / ``error_message`` carry no exception text.
- ``build_error_detail`` emits only allowlisted-safe tokens (stage labels, error
  codes, exception class names) -- never message text, so nothing to scrub.
"""

from __future__ import annotations

import ast
import json
import logging
from pathlib import Path

import pytest

from cdk.types import FailureCategory
from src.state.error_classification import (
    ErrorCode,
    FailureStage,
    FailureTag,
    SchemaHandshakeOutcome,
    build_error_detail,
    classify_destination_failure,
    classify_exception,
    classify_for_metrics,
    classify_handshake_failure,
    code_for_declared_category,
    customer_message,
    default_code_for_stage,
    detail_for_code,
    dominant_error_code,
    read_failure_tag,
    tag_failure,
)

pytestmark = pytest.mark.unit

_MODULE_PATH = Path("src/state/error_classification.py")


def _make(name: str, base: type = Exception, message: str = "") -> BaseException:
    """Build an instance of a dynamically named exception class.

    Nothing classifies by class name any more; the synthetic classes exist so a
    test can assert that a *suggestive* name or message changes nothing.
    """
    return type(name, (base,), {})(message)


# --------------------------------------------------------------------------- #
# Every boundary tags, and an untagged failure is INTERNAL
# --------------------------------------------------------------------------- #


def test_untagged_exception_is_internal_whatever_it_says():
    # The heart of it. Each message below used to classify as a different
    # code by phrase; with the tables gone they are all one thing: a failure
    # that got past every boundary without being tagged.
    for message in (
        "password authentication failed for user 'analitiq'",
        "could not connect to server: Connection refused",
        "429 too many requests",
        "duplicate key value violates unique constraint",
        "request returned 401",
    ):
        assert classify_exception(RuntimeError(message)) is ErrorCode.INTERNAL
    assert classify_exception(ConnectionRefusedError("refused")) is ErrorCode.INTERNAL
    assert classify_exception(_make("UnmappedTypeError")) is ErrorCode.INTERNAL


def test_untagged_exception_is_logged_as_a_missing_boundary(caplog):
    # An untagged failure is a defect to fix, not a routine fallback, so it is
    # logged at ERROR rather than swallowed into a plausible code.
    with caplog.at_level(logging.ERROR, logger="src.state.error_classification"):
        classify_exception(RuntimeError("gremlins"))
    assert any("missing its boundary tag" in r.message for r in caplog.records)


def test_every_stage_names_a_code():
    assert default_code_for_stage(FailureStage.CONFIG) is ErrorCode.CONFIG_INVALID
    assert default_code_for_stage(FailureStage.SOURCE_EXTRACT) is ErrorCode.INTERNAL
    assert default_code_for_stage(FailureStage.TRANSFORM) is ErrorCode.CONFIG_INVALID
    assert (
        default_code_for_stage(FailureStage.DESTINATION_LOAD)
        is ErrorCode.DESTINATION_WRITE_FAILED
    )
    # Totality: the boundaries all tag through this table, so a stage with no
    # default would raise inside the failure-reporting path.
    for stage in tuple(FailureStage):
        assert isinstance(default_code_for_stage(stage), ErrorCode)


def test_tag_is_used_verbatim_whatever_the_text_says():
    exc = _make("RuntimeError", message="connection refused")
    tag_failure(
        exc,
        code=ErrorCode.DESTINATION_WRITE_FAILED,
        stage=FailureStage.DESTINATION_LOAD,
    )
    assert classify_exception(exc) is ErrorCode.DESTINATION_WRITE_FAILED


def test_tag_survives_wrapping_via_original_error():
    from src.engine.exceptions import StreamProcessingError

    inner = tag_failure(
        RuntimeError("opaque driver text"),
        code=ErrorCode.CONFIG_INVALID,
        stage=FailureStage.TRANSFORM,
    )
    wrapped = StreamProcessingError(
        "Stream processing failed", stream_id="s1", original_error=inner
    )
    assert classify_exception(wrapped) is ErrorCode.CONFIG_INVALID


def test_tag_failure_does_not_overwrite_an_existing_chain_tag():
    # The no-overwrite invariant is enforced in tag_failure itself, so an outer
    # stage boundary calling it unconditionally cannot clobber a precise
    # inner tag -- which is what makes unconditional tagging safe.
    inner = tag_failure(
        RuntimeError("worker config error"),
        code=ErrorCode.CONFIG_INVALID,
        stage=FailureStage.SOURCE_EXTRACT,
    )
    tag_failure(
        inner,
        code=ErrorCode.DESTINATION_WRITE_FAILED,
        stage=FailureStage.DESTINATION_LOAD,
    )
    assert read_failure_tag(inner) == FailureTag(
        code=ErrorCode.CONFIG_INVALID, stage=FailureStage.SOURCE_EXTRACT
    )
    # A deeper tag on the cause chain also blocks an outer tag on the wrapper.
    outer = RuntimeError("outer")
    outer.__cause__ = tag_failure(
        RuntimeError("deep"),
        code=ErrorCode.SOURCE_UNREACHABLE,
        stage=FailureStage.SOURCE_EXTRACT,
    )
    tag_failure(outer, code=ErrorCode.INTERNAL, stage=FailureStage.DESTINATION_LOAD)
    assert read_failure_tag(outer).code is ErrorCode.SOURCE_UNREACHABLE


def test_read_failure_tag_picks_dominant_across_group():
    g = ExceptionGroup(
        "All streams failed",
        [
            tag_failure(
                RuntimeError("a"),
                code=ErrorCode.SOURCE_UNREACHABLE,
                stage=FailureStage.SOURCE_EXTRACT,
            ),
            tag_failure(
                RuntimeError("b"),
                code=ErrorCode.CONFIG_INVALID,
                stage=FailureStage.CONFIG,
            ),
        ],
    )
    assert read_failure_tag(g) == FailureTag(
        code=ErrorCode.CONFIG_INVALID, stage=FailureStage.CONFIG
    )
    assert classify_exception(g) is ErrorCode.CONFIG_INVALID


def test_untagged_exception_has_no_tag():
    assert read_failure_tag(RuntimeError("nothing here")) is None


def test_code_priority_ranks_every_error_code():
    from src.state.error_classification import _CODE_PRIORITY

    assert set(_CODE_PRIORITY) == set(ErrorCode)


def test_self_referential_chain_does_not_hang():
    exc = RuntimeError("loop")
    exc.__cause__ = exc  # pathological; must not spin
    assert classify_exception(exc) is ErrorCode.INTERNAL


# --------------------------------------------------------------------------- #
# Structural: no tables survive, and every boundary tags
# --------------------------------------------------------------------------- #
#
# Justified by history: six commits authored the phrase table, and one of them
# was titled "replace heuristic classification" while 104 entries survived it.
# A behavioural test cannot catch a table that is reintroduced beside the
# structured path, because both would agree on the cases anyone thought to
# write down.


def _module_ast() -> ast.Module:
    return ast.parse(_MODULE_PATH.read_text())


def test_no_phrase_or_class_name_table_exists():
    tree = _module_ast()
    offenders = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        value = node.value
        # A "table" is any literal collection of three or more strings: the
        # shape both _CONFIG_PHRASES (a tuple) and _CONFIG_NAMES (a
        # frozenset(...) call over a set literal) had.
        literals = []
        if isinstance(value, (ast.Tuple, ast.Set, ast.List)):
            literals = value.elts
        elif (
            isinstance(value, ast.Call)
            and isinstance(value.func, ast.Name)
            and value.func.id == "frozenset"
        ):
            for arg in value.args:
                if isinstance(arg, (ast.Set, ast.Tuple, ast.List)):
                    literals = arg.elts
        strings = [
            e
            for e in literals
            if isinstance(e, ast.Constant) and isinstance(e.value, str)
        ]
        if len(strings) >= 3:
            offenders.append(ast.unparse(node.targets[0]))
    assert not offenders, f"string tables are back in {_MODULE_PATH}: {offenders}"


def test_module_matches_no_text():
    tree = _module_ast()
    imported = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    }
    assert "re" not in imported, "regex matching is back in the classifier"
    lowered = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "lower"
    ]
    assert not lowered, "case-folding a message means text matching is back"


def test_every_failure_stage_has_a_tagging_boundary():
    # All four boundaries must tag. A stage nothing tags is a hole
    # through which failures reach the runner untagged and read as INTERNAL.
    sources = "\n".join(
        path.read_text() for path in Path("src").rglob("*.py") if path.is_file()
    )
    for stage in tuple(FailureStage):
        assert (
            f"stage=FailureStage.{stage.name}" in sources
        ), f"no raise site tags {stage.name}"


# --------------------------------------------------------------------------- #
# FailureCategory and ErrorCode stay separate vocabularies
# --------------------------------------------------------------------------- #


def test_the_two_vocabularies_are_not_merged():
    # A connector declares a FailureCategory; the customer is told an
    # ErrorCode. Half of ErrorCode is source-side and a destination handler
    # must never be able to declare any of it, so the sets must not converge.
    category_names = {c.name for c in FailureCategory}
    code_names = {c.name for c in ErrorCode}
    assert not (category_names & code_names)
    # The mapping is many-to-one and engine-owned: NOT_READY and INTERNAL both
    # land on INTERNAL, and no declarable category reaches a source code.
    declared_codes = {
        code_for_declared_category(c)
        for c in FailureCategory
        if c is not FailureCategory.FAILURE_CATEGORY_UNSPECIFIED
    }
    assert not (
        declared_codes
        & {
            ErrorCode.SOURCE_AUTH_FAILED,
            ErrorCode.SOURCE_UNREACHABLE,
            ErrorCode.RATE_LIMITED,
        }
    )


# --------------------------------------------------------------------------- #
# A declaration is signal, not trust
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "category,expected",
    [
        (FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT, ErrorCode.CONFIG_INVALID),
        (
            FailureCategory.FAILURE_CATEGORY_WRITE_REJECTED,
            ErrorCode.DESTINATION_WRITE_FAILED,
        ),
        (FailureCategory.FAILURE_CATEGORY_NOT_READY, ErrorCode.INTERNAL),
        (FailureCategory.FAILURE_CATEGORY_INTERNAL, ErrorCode.INTERNAL),
    ],
)
def test_classify_destination_failure_reads_declared_category(category, expected):
    from src.engine.exceptions import StreamProcessingError

    exc = StreamProcessingError(
        "Batch 3 failed after 4 attempts: opaque driver text",
        failure_category=category,
    )
    assert classify_destination_failure(exc) is expected


def test_undeclared_destination_failure_resolves_from_the_stage():
    # The other half: UNSPECIFIED resolves from the raising stage, never
    # from the summary. Each summary below used to steer the verdict by
    # phrase; now only the stage speaks.
    for summary in (
        "Batch 3 fatal failure: SchemaConfigurationError: unsupported write mode",
        "Batch 3 fatal failure: duplicate key value violates unique constraint",
        "Batch 3 failed after 4 attempts: connection reset by peer",
        "Batch 3 fatal failure: type-map: no reverse rule",
    ):
        assert (
            classify_destination_failure(
                _make("StreamProcessingError", message=summary)
            )
            is ErrorCode.DESTINATION_WRITE_FAILED
        )


def test_an_unreadable_declaration_degrades_to_unspecified(caplog):
    # proto3 enums are open, so an untrusted connector can declare a value
    # this build has no member for. It degrades instead of raising, and the
    # stage default answers -- the declaration never becomes a code.
    from src.grpc.client import _known_failure_category

    with caplog.at_level(logging.WARNING, logger="src.grpc.client"):
        degraded = _known_failure_category(99, "batch 7")
    assert degraded is FailureCategory.FAILURE_CATEGORY_UNSPECIFIED
    assert any("degrading to UNSPECIFIED" in r.message for r in caplog.records)
    assert code_for_declared_category(degraded) is None


def test_code_for_declared_category_mirrors_classification():
    # The non-raising dlq/skip partial-run path maps a declared category via
    # this helper; it must agree with what classify_destination_failure would
    # return for the same declaration, or the reported code would depend on
    # the error strategy (#351).
    from src.engine.exceptions import StreamProcessingError

    for category in tuple(FailureCategory):
        code = code_for_declared_category(category)
        if category is FailureCategory.FAILURE_CATEGORY_UNSPECIFIED:
            assert code is None
            continue
        exc = StreamProcessingError("opaque", failure_category=category)
        assert code is classify_destination_failure(exc)


def test_declared_category_outranks_conflicting_text():
    # The near-miss from issue #351: a readiness-guard summary phrased as
    # "Handler could not connect" would once phrase-match source vocabulary.
    from src.engine.exceptions import StreamProcessingError

    exc = StreamProcessingError(
        "Batch 3 failed after 4 attempts: Handler could not connect",
        failure_category=FailureCategory.FAILURE_CATEGORY_NOT_READY,
    )
    assert classify_destination_failure(exc) is ErrorCode.INTERNAL


def test_declared_category_survives_wrapping():
    from src.engine.exceptions import StreamProcessingError

    inner = StreamProcessingError(
        "Batch 1 fatal failure: type-map: no reverse rule",
        failure_category=FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT,
    )
    wrapped = ExceptionGroup("All streams failed", [inner])
    assert classify_destination_failure(wrapped) is ErrorCode.CONFIG_INVALID


def test_aggregated_categories_resolve_to_dominant_not_traversal_order():
    from src.engine.exceptions import StreamProcessingError

    not_ready = StreamProcessingError(
        "Batch 2 failed after 4 attempts: Handler not connected",
        failure_category=FailureCategory.FAILURE_CATEGORY_NOT_READY,
    )
    config_defect = StreamProcessingError(
        "Batch 5 fatal failure: type-map: no reverse rule",
        failure_category=FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT,
    )
    for leaves in ([not_ready, config_defect], [config_defect, not_ready]):
        group = ExceptionGroup("All streams failed", leaves)
        assert classify_destination_failure(group) is ErrorCode.CONFIG_INVALID


def test_dominant_error_code_prefers_actionable_cause():
    assert dominant_error_code([]) is None
    assert (
        dominant_error_code([ErrorCode.INTERNAL, ErrorCode.DESTINATION_WRITE_FAILED])
        is ErrorCode.DESTINATION_WRITE_FAILED
    )
    assert (
        dominant_error_code(
            [ErrorCode.DESTINATION_WRITE_FAILED, ErrorCode.CONFIG_INVALID]
        )
        is ErrorCode.CONFIG_INVALID
    )


def test_destination_http_code_never_read_as_source_auth():
    # The cross-stage tail: a destination-load failure whose cause text carries
    # a "401" classifies from its stage tag, so the source codes are
    # unreachable from it by construction.
    exc = _make(
        "RuntimeError", message="POST https://api.dest/v1 -> 401 from destination"
    )
    tag_failure(
        exc,
        code=ErrorCode.DESTINATION_WRITE_FAILED,
        stage=FailureStage.DESTINATION_LOAD,
    )
    assert classify_exception(exc) is ErrorCode.DESTINATION_WRITE_FAILED


# --------------------------------------------------------------------------- #
# The CDK vocabulary mirrors the wire vocabulary
# --------------------------------------------------------------------------- #


def test_the_cdk_enum_mirrors_the_wire_enum():
    # cdk.types.FailureCategory is hand-written to mirror stream.proto's enum
    # so values pass straight into a protobuf ack without a lookup table.
    # Proto3 enums are open, so a drifted member would be accepted silently
    # at runtime -- this is the only cross-check between the two vocabularies.
    from src.grpc.generated.analitiq.v1 import stream_pb2

    for member in FailureCategory:
        assert stream_pb2.FailureCategory.Value(member.name) == member.value
    assert len(FailureCategory) == len(stream_pb2.FailureCategory.keys())


# --------------------------------------------------------------------------- #
# The destination handshake classifies from its outcome, not its wording
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "outcome,expected",
    [
        (SchemaHandshakeOutcome.REJECTED, ErrorCode.CONFIG_INVALID),
        (SchemaHandshakeOutcome.TRANSPORT_FAILURE, ErrorCode.DESTINATION_WRITE_FAILED),
        (SchemaHandshakeOutcome.PROTOCOL_VIOLATION, ErrorCode.INTERNAL),
    ],
)
def test_handshake_outcome_names_the_code(outcome, expected):
    assert (
        classify_handshake_failure(
            outcome, declared=FailureCategory.FAILURE_CATEGORY_UNSPECIFIED
        )
        is expected
    )


def test_handshake_declaration_outranks_the_outcome():
    # A destination shell that could not reach its connector worker sends an
    # ordinary rejection; only its NOT_READY declaration says the refusal was
    # not the customer's configuration. This is the case the deleted
    # transport-phrase list used to catch by matching forwarded reason text.
    assert (
        classify_handshake_failure(
            SchemaHandshakeOutcome.REJECTED,
            declared=FailureCategory.FAILURE_CATEGORY_NOT_READY,
        )
        is ErrorCode.INTERNAL
    )


def test_handshake_classification_refuses_an_accepted_handshake():
    with pytest.raises(ValueError):
        classify_handshake_failure(
            SchemaHandshakeOutcome.ACCEPTED,
            declared=FailureCategory.FAILURE_CATEGORY_UNSPECIFIED,
        )


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
    exc = _make("RuntimeError", message="postgresql://app:s3cr3t@db.host:5432/prod")
    detail = build_error_detail(exc)
    assert detail == "RuntimeError"
    assert "s3cr3t" not in detail
    assert "postgresql" not in detail


def test_error_detail_excludes_message_text_even_on_the_tagged_token_path():
    exc = tag_failure(
        RuntimeError("Authorization: Bearer leaked-token-xyz dsn=postgres://u:p@h/db"),
        code=ErrorCode.DESTINATION_WRITE_FAILED,
        stage=FailureStage.DESTINATION_LOAD,
    )
    detail = build_error_detail(exc)
    assert detail == "destination_load/DESTINATION_WRITE_FAILED:RuntimeError"
    assert "leaked-token-xyz" not in detail
    assert "p@h" not in detail


def test_error_detail_keeps_the_side_an_internal_code_does_not_name():
    # The cost of the INTERNAL default is that the code stops naming the
    # side. The stage label carries it instead, so an undeclared source
    # failure is still triageable.
    exc = tag_failure(
        _make("ReadError", base=RuntimeError, message="opaque"),
        code=default_code_for_stage(FailureStage.SOURCE_EXTRACT),
        stage=FailureStage.SOURCE_EXTRACT,
    )
    assert build_error_detail(exc) == "source_extract/INTERNAL:ReadError"


def test_error_detail_enumerates_mixed_tagged_and_untagged_group():
    group = ExceptionGroup(
        "All streams failed",
        [
            tag_failure(
                _make("ReadError"),
                code=ErrorCode.SOURCE_UNREACHABLE,
                stage=FailureStage.SOURCE_EXTRACT,
            ),
            _make("ValueError", message="opaque untagged failure"),
        ],
    )
    assert classify_exception(group) is ErrorCode.SOURCE_UNREACHABLE
    detail = build_error_detail(group)
    assert "source_extract/SOURCE_UNREACHABLE:ReadError" in detail
    assert "ValueError" in detail


def test_exception_group_detail_enumerates_leaves_by_class_name():
    # str(ExceptionGroup) is only the summary; build_error_detail must carry
    # every per-stream leaf -- as safe class-name tokens, never raw messages.
    group = ExceptionGroup(
        "All streams failed",
        [
            _make("ReadError", message="Stream s1: connection refused at upstream"),
            _make(
                "StreamProcessingError",
                message="Stream s2: Batch 4 fatal failure: duplicate key",
            ),
        ],
    )
    _, _, detail = classify_for_metrics(group)
    assert "ReadError" in detail
    assert "StreamProcessingError" in detail
    assert "connection refused at upstream" not in detail
    assert "duplicate key" not in detail


def test_error_detail_bounds_length():
    detail = build_error_detail(RuntimeError("a" * 5000))
    assert len(detail) <= 2100


def test_detail_for_code_is_structured_and_safe():
    out = detail_for_code(
        ErrorCode.DESTINATION_WRITE_FAILED,
        stage=FailureStage.DESTINATION_LOAD,
        reason="records dead-lettered after retries",
    )
    assert out == (
        "destination_load/DESTINATION_WRITE_FAILED: "
        "records dead-lettered after retries"
    )


# --------------------------------------------------------------------------- #
# Chain handling: the tag rides wrappers, groups and cause chains
# --------------------------------------------------------------------------- #


def test_original_error_attribute_is_followed():
    from src.engine.exceptions import StreamProcessingError

    wrapped = StreamProcessingError(
        "Stream processing failed",
        stream_id="s1",
        original_error=tag_failure(
            _make("RuntimeError", message="opaque"),
            code=ErrorCode.SOURCE_AUTH_FAILED,
            stage=FailureStage.SOURCE_EXTRACT,
        ),
    )
    assert classify_exception(wrapped) is ErrorCode.SOURCE_AUTH_FAILED


def test_cause_chain_is_followed():
    inner = tag_failure(
        _make("ConnectionError", message="opaque"),
        code=ErrorCode.SOURCE_UNREACHABLE,
        stage=FailureStage.SOURCE_EXTRACT,
    )
    outer = RuntimeError("stream failed")
    outer.__cause__ = inner
    assert classify_exception(outer) is ErrorCode.SOURCE_UNREACHABLE


def test_partial_stream_group_classifies_dominant_cause():
    # A partial run keeps ALL failed-stream exceptions; classification picks
    # the dominant tag across them.
    group = ExceptionGroup(
        "Partial stream failures",
        [
            _make("StreamProcessingError", message="weird internal thing"),
            tag_failure(
                _make("StreamProcessingError", message="opaque"),
                code=ErrorCode.SOURCE_AUTH_FAILED,
                stage=FailureStage.SOURCE_EXTRACT,
            ),
        ],
    )
    assert classify_exception(group) is ErrorCode.SOURCE_AUTH_FAILED


# --------------------------------------------------------------------------- #
# classify_for_metrics: the (code, customer_message, detail) triple
# --------------------------------------------------------------------------- #


def test_classify_for_metrics_returns_safe_triple():
    exc = tag_failure(
        _make(
            "RuntimeError",
            message="password authentication failed; dsn=postgres://u:p@h/db",
        ),
        code=ErrorCode.SOURCE_AUTH_FAILED,
        stage=FailureStage.SOURCE_EXTRACT,
    )
    code, message, detail = classify_for_metrics(exc)
    assert code is ErrorCode.SOURCE_AUTH_FAILED
    assert message == customer_message(ErrorCode.SOURCE_AUTH_FAILED)
    assert detail == "source_extract/SOURCE_AUTH_FAILED:RuntimeError"
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
        tag_failure(
            _make("ConfigValidationError", message="bad pipeline.json"),
            code=ErrorCode.CONFIG_INVALID,
            stage=FailureStage.CONFIG,
        )
    )
    rec = create_metrics_record(
        run_id="r1",
        pipeline_id="p1",
        start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end_time=datetime(2026, 1, 1, 0, 0, 5, tzinfo=timezone.utc),
        records_skipped=0,
        status="failed",
        error_code=code,
        error_message=message,
        error_detail=detail,
    )
    assert rec.error_code is ErrorCode.CONFIG_INVALID
    assert rec.error_message == customer_message(ErrorCode.CONFIG_INVALID)
    # Structured, safe: stage, code and class name -- not "bad pipeline.json".
    assert rec.error_detail == "config/CONFIG_INVALID:ConfigValidationError"

    # The error_code must serialize as the plain enum string in the emitted
    # line, exactly as emit_metrics_log -> json.dumps(..., default=str) would.
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
        records_skipped=0,
        status="success",
    )
    assert rec.status == "success"
    assert rec.error_code is None
    assert rec.error_message is None
    assert rec.error_detail is None
    assert rec.records_total == 10
