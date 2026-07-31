"""A bad volume is not a bad configuration.

``load_json_file`` is the config layer's only file read, which makes it the
one place that can tell the two apart: failing to *read* a file is an
engine/infra fault, failing to *parse* what was read is a genuine config
defect. The runner's config boundary tags everything CONFIG_INVALID
unconditionally, so without the split here a full disk or an unreadable
mount reaches the customer as "your pipeline configuration is invalid" --
something they cannot act on, while the real outage stays hidden.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.config.utils import load_json_file
from src.state.error_classification import (
    ErrorCode,
    FailureStage,
    classify_exception,
    read_failure_tag,
    tag_failure,
)

pytestmark = pytest.mark.unit


def test_a_readable_document_parses(tmp_path: Path) -> None:
    path = tmp_path / "pipeline.json"
    path.write_text(json.dumps({"pipeline_id": "p1"}))
    assert load_json_file(path) == {"pipeline_id": "p1"}


def test_an_unreadable_file_is_infra_not_bad_config(tmp_path: Path) -> None:
    path = tmp_path / "locked.json"
    path.write_text("{}")
    path.chmod(0o000)
    try:
        with pytest.raises(OSError) as caught:
            load_json_file(path)
    finally:
        path.chmod(0o600)
    assert classify_exception(caught.value) is ErrorCode.INTERNAL
    tag = read_failure_tag(caught.value)
    assert tag is not None and tag.stage is FailureStage.CONFIG


def test_a_missing_file_is_infra_not_bad_config(tmp_path: Path) -> None:
    # Every caller guards with is_file() first, so a FileNotFoundError
    # escaping this read is a vanished mount or a race, not a bad reference.
    with pytest.raises(FileNotFoundError) as caught:
        load_json_file(tmp_path / "gone.json")
    assert classify_exception(caught.value) is ErrorCode.INTERNAL


def test_malformed_json_stays_a_config_defect(tmp_path: Path) -> None:
    path = tmp_path / "broken.json"
    path.write_text("{not json")
    with pytest.raises(ValueError) as caught:
        load_json_file(path)
    # Untagged by the read, so the runner's config boundary names it.
    assert read_failure_tag(caught.value) is None
    tag_failure(caught.value, code=ErrorCode.CONFIG_INVALID, stage=FailureStage.CONFIG)
    assert classify_exception(caught.value) is ErrorCode.CONFIG_INVALID


def test_the_read_verdict_survives_the_runners_coarser_tag(tmp_path: Path) -> None:
    # The mechanism the split depends on: the runner tags the config phase
    # CONFIG_INVALID unconditionally, and tag_failure is no-overwrite, so
    # the inner INTERNAL verdict is what the customer is told.
    with pytest.raises(FileNotFoundError) as caught:
        load_json_file(tmp_path / "gone.json")
    tag_failure(caught.value, code=ErrorCode.CONFIG_INVALID, stage=FailureStage.CONFIG)
    assert classify_exception(caught.value) is ErrorCode.INTERNAL
