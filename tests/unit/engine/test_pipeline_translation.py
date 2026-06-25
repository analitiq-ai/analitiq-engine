"""Unit tests for the contract-to-connector translation helpers in src.runner."""

from unittest.mock import MagicMock

import pytest

from src.runner import (
    _build_config_dict,
    _build_destination_config,
    _translate_api_source,
    _translate_database_source,
    _translate_source_config,
)
from src.models.resolved import (
    BatchingConfig,
    PipelineConnections,
    ResolvedDestination,
    ResolvedPipeline,
    ResolvedSource,
    ResolvedStream,
    RuntimeConfig,
)
from src.models.stream import EndpointRef


def _make_endpoint_ref(scope="connector", connection_id="conn", endpoint_id="ep"):
    return EndpointRef(scope=scope, connection_id=connection_id, endpoint_id=endpoint_id)


def _make_runtime(connector_type="database"):
    rt = MagicMock()
    rt.connector_type = connector_type
    return rt


def _make_source(connector_type="database", stream_source=None, endpoint_document=None):
    ref = _make_endpoint_ref()
    rt = _make_runtime(connector_type)
    return ResolvedSource(
        endpoint_ref=ref,
        connection_ref="conn-1",
        runtime=rt,
        endpoint_document=endpoint_document or {"database_object": "orders"},
        stream_source=stream_source or {"replication": {"cursor_field": "updated_at"}},
    )


def _make_destination(write=None):
    ref = _make_endpoint_ref(scope="connection", connection_id="dest", endpoint_id="orders")
    rt = _make_runtime("database")
    return ResolvedDestination(
        endpoint_ref=ref,
        connection_ref="dest-1",
        runtime=rt,
        endpoint_document={"table": "orders"},
        write=write or {"mode": "upsert"},
    )


def _make_stream(
    stream_id="orders", connector_type="database", mapping=None, stream_version=1
):
    src = _make_source(connector_type=connector_type)
    dest = _make_destination()
    return ResolvedStream(
        stream_id=stream_id,
        stream_version=stream_version,
        pipeline_id="test-pipeline",
        display_name=None,
        description=None,
        status="active",
        is_enabled=True,
        tags=[],
        source=src,
        destinations=[dest],
        mapping=mapping or {},
    )


def _make_pipeline(
    pipeline_id="test-pipeline", display_name=None, streams=None, runtime=None
):
    return ResolvedPipeline(
        pipeline_id=pipeline_id,
        name="Test Pipeline",
        display_name=display_name,
        description=None,
        status="active",
        connections=PipelineConnections(source="src-conn", destinations=["dst-conn"]),
        runtime=runtime or RuntimeConfig(),
    )


class TestBuildDestinationConfig:
    def test_explicit_write_mode(self):
        dest = _make_destination(write={"mode": "insert"})
        result = _build_destination_config(dest)
        assert result == {"write_mode": "insert"}

    def test_default_write_mode_when_absent(self):
        dest = _make_destination(write={})
        result = _build_destination_config(dest)
        assert result == {"write_mode": "upsert"}

    def test_truncate_insert_mode(self):
        dest = _make_destination(write={"mode": "truncate_insert"})
        result = _build_destination_config(dest)
        assert result == {"write_mode": "truncate_insert"}


class TestTranslateDatabaseSource:
    def test_attaches_endpoint_and_stream_source(self):
        endpoint = {"database_object": "invoices", "columns": ["id", "amount"]}
        stream_source = {"replication": {"cursor_field": "updated_at"}}
        source = _make_source(stream_source=stream_source, endpoint_document=endpoint)

        result = _translate_database_source(source, endpoint)

        assert result["endpoint_document"] is endpoint
        assert result["stream_source"] is stream_source

    def test_passes_documents_through_verbatim(self):
        endpoint = {"primary_keys": ["id"], "filters": [{"field": "active", "value": True}]}
        source = _make_source(endpoint_document=endpoint)

        result = _translate_database_source(source, endpoint)

        assert result["endpoint_document"]["filters"][0]["field"] == "active"


class TestTranslateApiSource:
    def test_attaches_endpoint_stream_source_and_filters(self):
        filters = [{"field": "status", "op": "eq", "value": "active"}]
        stream_source = {"filters": filters, "replication": {}}
        endpoint = {"operations": {"read": {"request": {"path": "/invoices"}}}}
        source = _make_source(connector_type="api", stream_source=stream_source, endpoint_document=endpoint)
        rt = source.runtime

        result = _translate_api_source(source, endpoint, rt)

        assert result["endpoint_document"] is endpoint
        assert result["stream_source"] is stream_source
        assert result["stream_filters"] == filters

    def test_empty_filters_when_absent(self):
        stream_source = {}
        endpoint = {}
        source = _make_source(connector_type="api", stream_source=stream_source, endpoint_document=endpoint)

        result = _translate_api_source(source, endpoint, source.runtime)

        assert result["stream_filters"] == []

    def test_none_filters_normalised_to_empty_list(self):
        stream_source = {"filters": None}
        source = _make_source(connector_type="api", stream_source=stream_source)

        result = _translate_api_source(source, {}, source.runtime)

        assert result["stream_filters"] == []


class TestTranslateSourceConfig:
    def test_database_kind_adds_endpoint_and_stream_source(self):
        source = _make_source(connector_type="database")
        stream = _make_stream(connector_type="database")
        endpoint = source.endpoint_document

        result = _translate_source_config(
            stream=stream, source=source, endpoint=endpoint, runtime=source.runtime
        )

        assert result["connector_type"] == "database"
        assert result["_resolved_source"] is source
        assert result["connection_ref"] == "conn-1"
        assert result["endpoint_document"] is endpoint
        assert "stream_source" in result

    def test_api_kind_adds_stream_filters(self):
        stream_source = {"filters": [{"field": "x"}]}
        source = _make_source(connector_type="api", stream_source=stream_source)
        stream = _make_stream(connector_type="api")
        endpoint = source.endpoint_document

        result = _translate_source_config(
            stream=stream, source=source, endpoint=endpoint, runtime=source.runtime
        )

        assert result["connector_type"] == "api"
        assert result["stream_filters"] == [{"field": "x"}]

    def test_non_built_in_kind_passes_through_database_shape(self):
        """Non-built-in connector kinds pass through the same contract-document
        shape as database without raising (#165)."""
        source = _make_source(connector_type="nosql")
        stream = _make_stream()
        endpoint = source.endpoint_document

        result = _translate_source_config(
            stream=stream, source=source, endpoint=endpoint, runtime=source.runtime
        )

        assert result["connector_type"] == "nosql"
        assert result["endpoint_document"] is endpoint
        assert result["stream_source"] is source.stream_source
        assert "stream_filters" not in result

    @pytest.mark.parametrize("kind", ["nosql", "graphql", "file", "sftp", "custom-db"])
    def test_non_built_in_kind_never_raises(self, kind):
        """Regression guard: _translate_source_config must not raise for unknown kinds."""
        source = _make_source(connector_type=kind)
        result = _translate_source_config(
            stream=_make_stream(),
            source=source,
            endpoint=source.endpoint_document,
            runtime=source.runtime,
        )
        assert result["connector_type"] == kind

    def test_non_built_in_kind_logs_warning(self, caplog):
        import logging

        source = _make_source(connector_type="nosql")
        with caplog.at_level(logging.WARNING, logger="src.runner"):
            _translate_source_config(
                stream=_make_stream(),
                source=source,
                endpoint=source.endpoint_document,
                runtime=source.runtime,
            )
        assert any("nosql" in r.message for r in caplog.records)

    def test_endpoint_ref_is_serialised(self):
        source = _make_source(connector_type="database")
        stream = _make_stream()

        result = _translate_source_config(
            stream=stream, source=source, endpoint={}, runtime=source.runtime
        )

        assert isinstance(result["endpoint_ref"], dict)
        assert "scope" in result["endpoint_ref"]


class TestBuildConfigDict:
    def test_stream_id_becomes_key_and_name(self):
        pipeline = _make_pipeline()
        stream = _make_stream(stream_id="invoices")

        result = _build_config_dict(pipeline, [stream])

        assert "invoices" in result["streams"]
        assert result["streams"]["invoices"]["name"] == "invoices"

    def test_stream_version_propagated(self):
        pipeline = _make_pipeline()
        stream = _make_stream(stream_id="invoices", stream_version=3)

        result = _build_config_dict(pipeline, [stream])

        assert result["streams"]["invoices"]["stream_version"] == 3

    def test_pipeline_id_and_name_propagated(self):
        pipeline = _make_pipeline(pipeline_id="wise-to-pg", display_name="Wise → PG")

        result = _build_config_dict(pipeline, [])

        assert result["pipeline_id"] == "wise-to-pg"
        assert result["name"] == "Wise → PG"

    def test_pipeline_name_falls_back_to_id_when_display_name_absent(self):
        pipeline = _make_pipeline(pipeline_id="wise-to-pg", display_name=None)

        result = _build_config_dict(pipeline, [])

        assert result["name"] == "wise-to-pg"

    def test_stream_with_no_destinations_raises(self):
        pipeline = _make_pipeline()
        stream = _make_stream()
        stream.destinations.clear()

        with pytest.raises(ValueError, match="has no destinations"):
            _build_config_dict(pipeline, [stream])

    def test_multi_stream_pipeline(self):
        pipeline = _make_pipeline()
        stream_a = _make_stream(stream_id="orders")
        stream_b = _make_stream(stream_id="invoices")

        result = _build_config_dict(pipeline, [stream_a, stream_b])

        assert set(result["streams"].keys()) == {"orders", "invoices"}

    def test_mapping_assignments_translated(self):
        assignment = {
            "target": {"path": "id", "arrow_type": "Int64", "nullable": False},
            "value": {"expression": {"op": "get", "path": "id"}},
        }
        pipeline = _make_pipeline()
        stream = _make_stream(mapping={"assignments": [assignment]})

        result = _build_config_dict(pipeline, [stream])

        assignments = result["streams"]["orders"]["mapping"]["assignments"]
        assert len(assignments) == 1
        assert assignments[0]["target"]["path"] == ["id"]
        assert assignments[0]["value"]["kind"] == "expr"

    def test_runtime_propagated(self):
        pipeline = _make_pipeline(
            runtime=RuntimeConfig(batching=BatchingConfig(batch_size=500))
        )

        result = _build_config_dict(pipeline, [])

        assert result["runtime"]["batching"]["batch_size"] == 500
