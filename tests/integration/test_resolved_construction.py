"""Construction smoke test for the resolved-runtime factories.

Walks every active pipeline declared in `pipelines/manifest.json`, feeds
each one through `load_resolved_pipeline`, and asserts it constructs
without raising `ResolveError`. A missing or mistyped field surfaces as
`file:$.path: message`.

This replaces the dropped Layer-A "fixture conformance" test from the
engine-schema-alignment plan §3.1.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.engine.resolved import (
    ResolveError,
    ResolvedPipeline,
    discover_pipeline_ids,
    load_resolved_pipeline,
)

REPO_ROOT = Path(__file__).resolve().parents[2]


def _has_active_pipelines() -> bool:
    manifest = REPO_ROOT / "pipelines" / "manifest.json"
    return manifest.is_file()


pytestmark = pytest.mark.skipif(
    not _has_active_pipelines(),
    reason="No pipelines/manifest.json on disk; smoke test only runs in a populated workspace.",
)


@pytest.fixture(scope="module")
def pipeline_ids() -> list[str]:
    return discover_pipeline_ids(REPO_ROOT)


def test_at_least_one_pipeline_in_manifest(pipeline_ids: list[str]) -> None:
    assert pipeline_ids, "expected at least one active pipeline in manifest.json"


@pytest.mark.parametrize(
    "pipeline_id",
    discover_pipeline_ids(REPO_ROOT) if _has_active_pipelines() else [],
)
def test_pipeline_constructs(pipeline_id: str) -> None:
    """Construction must succeed; ResolveError points at file:$.field."""
    pipeline = load_resolved_pipeline(REPO_ROOT, pipeline_id)
    assert isinstance(pipeline, ResolvedPipeline)
    assert pipeline.pipeline_id == pipeline_id
    assert pipeline.streams, "resolved pipeline must have at least one stream"
    for stream in pipeline.streams:
        assert stream.source.connection.connection_id, "source connection must resolve"
        assert stream.destinations, "stream must have at least one destination"
        for dest in stream.destinations:
            assert dest.connection.connection_id
            assert dest.endpoint.endpoint_id
