"""Unit tests for pipeline translation helpers in src.engine.pipeline."""

from __future__ import annotations

from typing import Any, Dict
from unittest.mock import MagicMock

import pytest

from src.engine.pipeline import _translate_source_config
from src.models.resolved import ResolvedSource
from src.models.stream import EndpointRef


def _make_runtime(kind: str) -> MagicMock:
    runtime = MagicMock()
    runtime.connector_type = kind
    return runtime


def _make_source(stream_source: Dict[str, Any] | None = None) -> ResolvedSource:
    return ResolvedSource(
        endpoint_ref=EndpointRef(scope="connector", connection_id="conn-1", endpoint_id="ep-1"),
        connection_ref="conn-1",
        runtime=MagicMock(),
        endpoint_document={"endpoint_id": "ep-1"},
        stream_source=stream_source or {"replication": {"strategy": "full"}},
    )


def _make_endpoint() -> Dict[str, Any]:
    return {
        "$schema": "https://schemas.analitiq.ai/database-endpoint/latest.json",
        "endpoint_id": "ep-1",
        "database_object": {"schema": "public", "name": "orders"},
    }


class TestTranslateSourceConfig:
    def test_api_kind_includes_stream_filters(self) -> None:
        filters = [{"field": "status", "op": "eq", "value": "active"}]
        source = _make_source(stream_source={"filters": filters})
        endpoint = {"$schema": "https://schemas.analitiq.ai/api-endpoint/latest.json"}
        result = _translate_source_config(
            stream=MagicMock(),
            source=source,
            endpoint=endpoint,
            runtime=_make_runtime("api"),
        )
        assert result["connector_type"] == "api"
        assert result["endpoint_document"] == endpoint
        assert result["stream_source"] is source.stream_source
        assert result["stream_filters"] == filters

    def test_database_kind_passes_through_contract_docs(self) -> None:
        source = _make_source()
        endpoint = _make_endpoint()
        result = _translate_source_config(
            stream=MagicMock(),
            source=source,
            endpoint=endpoint,
            runtime=_make_runtime("database"),
        )
        assert result["connector_type"] == "database"
        assert result["endpoint_document"] is endpoint
        assert result["stream_source"] is source.stream_source
        assert "stream_filters" not in result

    def test_non_built_in_kind_passes_through_database_shape(self) -> None:
        """Non-built-in connector kinds pass through the same contract-document
        shape as database without raising (#165)."""
        source = _make_source()
        endpoint = _make_endpoint()
        result = _translate_source_config(
            stream=MagicMock(),
            source=source,
            endpoint=endpoint,
            runtime=_make_runtime("nosql"),
        )
        assert result["connector_type"] == "nosql"
        assert result["endpoint_document"] is endpoint
        assert result["stream_source"] is source.stream_source
        assert "stream_filters" not in result

    @pytest.mark.parametrize("kind", ["nosql", "graphql", "file", "sftp", "custom-db"])
    def test_non_built_in_kind_never_raises(self, kind: str) -> None:
        """Regression guard: _translate_source_config must not raise for unknown kinds."""
        result = _translate_source_config(
            stream=MagicMock(),
            source=_make_source(),
            endpoint=_make_endpoint(),
            runtime=_make_runtime(kind),
        )
        assert result["connector_type"] == kind

    def test_base_fields_always_present(self) -> None:
        source = _make_source()
        result = _translate_source_config(
            stream=MagicMock(),
            source=source,
            endpoint=_make_endpoint(),
            runtime=_make_runtime("database"),
        )
        assert result["connector_type"] == "database"
        assert result["endpoint_ref"] == source.endpoint_ref.to_dict()
        assert result["connection_ref"] == "conn-1"
