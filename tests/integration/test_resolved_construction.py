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


@pytest.mark.parametrize(
    "pipeline_id",
    discover_pipeline_ids(REPO_ROOT) if _has_active_pipelines() else [],
)
def test_pipeline_engine_glue(pipeline_id: str, monkeypatch, tmp_path) -> None:
    """End-to-end engine-glue check on every on-disk pipeline.

    Runs each pipeline through :class:`PipelineConfigPrep`, instantiates a
    :class:`StreamingEngine` from the resolved spec, and builds the
    per-stream typed context that the stages consume. No network/DB IO —
    the gRPC client and source connector are unused at this level. Catches
    type-map / transport / runtime drift that the Layer-B factories alone
    would not see.
    """
    from src.engine.engine import StreamingEngine
    from src.engine.pipeline_config_prep import PipelineConfigPrep

    monkeypatch.setenv("PIPELINE_ID", pipeline_id)
    monkeypatch.chdir(REPO_ROOT)
    resolved, runtimes, raw_endpoints = PipelineConfigPrep().create_config()

    engine = StreamingEngine.from_resolved(resolved)
    assert engine.pipeline_id == pipeline_id

    for stream in resolved.streams:
        ctx = engine._build_context(stream, resolved, runtimes, raw_endpoints)
        assert ctx.source is stream.source
        assert ctx.destination is stream.destinations[0]
        assert ctx.src_runtime is runtimes[stream.source.connection.connection_id]
        assert ctx.dest_runtime is runtimes[stream.destinations[0].connection.connection_id]
        assert ctx.src_endpoint_raw, "source endpoint JSON must be present"
        assert ctx.dest_endpoint_raw, "destination endpoint JSON must be present"
