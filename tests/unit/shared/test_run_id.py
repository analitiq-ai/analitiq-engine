"""Unit tests for run_id resolution and the RUN_ID env contract."""

import re

from src.shared.run_id import RUN_ID_VAR, get_or_generate_run_id

# Shape produced by _generate_run_id: "YYYYMMDDTHHMMSSZ-<8 hex>".
GENERATED_RE = re.compile(r"^\d{8}T\d{6}Z-[0-9a-f]{8}$")


def test_existing_run_id_wins(monkeypatch):
    """A non-empty RUN_ID is returned verbatim."""
    monkeypatch.setenv(RUN_ID_VAR, "explicit-run-id")
    assert get_or_generate_run_id() == "explicit-run-id"


def test_blank_run_id_falls_back_to_generated(monkeypatch):
    """A blank/whitespace RUN_ID is treated as unset and a fresh id is generated."""
    monkeypatch.setenv(RUN_ID_VAR, "   ")
    monkeypatch.delenv("AWS_BATCH_JOB_ID", raising=False)
    assert GENERATED_RE.match(get_or_generate_run_id())


def test_aws_batch_job_id_is_ignored(monkeypatch):
    """AWS_BATCH_JOB_ID is no longer honored (cloud reference removed): with
    RUN_ID unset the function generates an id rather than returning the batch id."""
    monkeypatch.delenv(RUN_ID_VAR, raising=False)
    monkeypatch.setenv("AWS_BATCH_JOB_ID", "batch-123")
    result = get_or_generate_run_id()
    assert result != "batch-123"
    assert GENERATED_RE.match(result)


def test_generates_id_when_nothing_set(monkeypatch):
    """With no RUN_ID and no AWS var, a generated id is returned."""
    monkeypatch.delenv(RUN_ID_VAR, raising=False)
    monkeypatch.delenv("AWS_BATCH_JOB_ID", raising=False)
    assert GENERATED_RE.match(get_or_generate_run_id())
