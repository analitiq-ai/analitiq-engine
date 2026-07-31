"""No module outside the policy decides a failing batch's fate (issue #428).

This one is structural on purpose, and the justification is history: three
commits over seven weeks authored three sites that each mapped the same
transport failure to their own answer -- retryable at the client's timeout,
fatal at its peer-close, retryable again at the proxy's remap -- and a
fourth added a second call path that reached them differently. Every one of
those was a correct-looking local change. Only the shape of the tree shows
the duplication, so the tree is what this asserts.
"""

from __future__ import annotations

import ast
from dataclasses import fields
from pathlib import Path

import pytest

from src.grpc.client import BatchResult

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
POLICY = REPO_ROOT / "src" / "engine" / "batch_policy.py"
CLIENT = REPO_ROOT / "src" / "grpc" / "client.py"

#: The verdict vocabulary. Constructing one of these IS deciding a batch's
#: fate, so the constructors may appear in exactly one module.
DISPOSITION_VARIANTS = frozenset(
    {"Committed", "AlreadyCommitted", "DeadLetter", "Skipped", "Failed"}
)


def _engine_sources() -> list[Path]:
    """Every hand-written module the engine and the CDK ship."""
    return [
        path
        for root in (REPO_ROOT / "src", REPO_ROOT / "cdk" / "cdk")
        for path in root.rglob("*.py")
        if "generated" not in path.parts
    ]


def _called_names(tree: ast.AST) -> set[str]:
    return {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }


def test_only_the_policy_constructs_a_disposition():
    """A second module producing verdicts is a second opinion, which is how
    the hops came to disagree in the first place."""
    offenders = {
        path.relative_to(REPO_ROOT).as_posix()
        for path in _engine_sources()
        if path != POLICY
        and _called_names(ast.parse(path.read_text())) & DISPOSITION_VARIANTS
    }
    assert offenders == set(), (
        "a batch's disposition is decided in src/engine/batch_policy.py alone; "
        f"these modules construct one too: {sorted(offenders)}"
    )


def test_the_client_builds_a_verdict_in_exactly_two_places():
    """One reader for acks that arrived, one builder for sends that reached
    none. A third site is the shape the divergence took last time."""
    tree = ast.parse(CLIENT.read_text())
    builders = {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and "BatchResult" in _called_names(node)
    }
    assert builders == {"_transport_failure", "_process_ack"}


def test_no_flag_survives_for_another_hop_to_re_read():
    """``transport_failure`` was the channel the proxy used to overrule the
    client's status. With the answer decided at detection there is nothing
    left to re-derive, and no field inviting it."""
    assert "transport_failure" not in {field.name for field in fields(BatchResult)}


@pytest.mark.parametrize(
    "module",
    ["src/engine/stream_processor.py", "src/worker/proxy.py"],
)
def test_the_stream_path_and_the_forwarding_hop_name_no_ack_status(module):
    """The processor reads dispositions and the proxy translates; neither
    interprets a status, so neither can decide what one costs the stream."""
    source = (REPO_ROOT / module).read_text()
    assert "ACK_STATUS_" not in source


def test_the_declared_retry_semantics_stays_a_log_line():
    """Decision 1.1b, structurally: the per-stream replay-safety verdict is
    reported to the operator and never consulted by the retry. A policy that
    could see it would be one condition away from failing a batch on an
    at-least-once sink to avoid a duplicate."""
    assert "RetrySemantics" not in POLICY.read_text()
    readers = {
        path.relative_to(REPO_ROOT).as_posix()
        for path in (REPO_ROOT / "src" / "engine").rglob("*.py")
        if "stream_retry_semantics" in path.read_text()
    }
    assert readers == {"src/engine/stream_processor.py"}
