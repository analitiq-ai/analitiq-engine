"""Unit tests for resolved-runtime factories.

Drives every factory function with synthetic dict inputs. Each test
asserts either a clean build or a `ResolveError` with the expected
`file:$.path: message` shape.
"""

from __future__ import annotations

import pytest

from src.engine.resolved import (
    ApiReadEndpoint,
    ApiWriteEndpoint,
    CursorPagination,
    DatabaseReadEndpoint,
    DatabaseWriteEndpoint,
    EndpointRef,
    HttpTransport,
    OffsetPagination,
    PagePagination,
    ResolveError,
    SqlAlchemyTransport,
    build_api_read_endpoint,
    build_api_write_endpoint,
    build_connection,
    build_connector,
    build_db_read_endpoint,
    build_db_write_endpoint,
    build_endpoint_ref,
    build_resolved_pipeline,
    build_resolved_stream,
)


# --- EndpointRef ---------------------------------------------------------


def test_endpoint_ref_builds() -> None:
    ref = build_endpoint_ref(
        {"scope": "connector", "connection_id": "c1", "endpoint_id": "e1"},
        source="t.json",
        path="$",
    )
    assert ref == EndpointRef(scope="connector", connection_id="c1", endpoint_id="e1")
    assert str(ref) == "connector:c1/e1"


def test_endpoint_ref_rejects_bad_scope() -> None:
    with pytest.raises(ResolveError) as exc:
        build_endpoint_ref(
            {"scope": "nope", "connection_id": "c1", "endpoint_id": "e1"},
            source="t.json",
            path="$",
        )
    assert "scope" in str(exc.value)


def test_endpoint_ref_requires_endpoint_id() -> None:
    """Critical: this is the new contract field; missing it is the §1.4 canary."""
    with pytest.raises(ResolveError) as exc:
        build_endpoint_ref(
            {"scope": "connector", "connection_id": "c1", "alias": "e1"},
            source="endpoint.json",
            path="$",
        )
    assert "endpoint_id" in str(exc.value)
    assert "endpoint.json" in str(exc.value)


# --- Database endpoints --------------------------------------------------


def _db_read_spec() -> dict:
    return {
        "endpoint_id": "wise_transfers",
        "database_object": {"schema": "public", "name": "wise_transfers", "object_type": "table"},
        "columns": [
            {"name": "id", "arrow_type": "Int64", "native_type": "bigint", "nullable": False, "ordinal_position": 1},
            {"name": "amount", "arrow_type": "Decimal128(18, 2)", "native_type": "numeric(18,2)", "nullable": True, "ordinal_position": 2},
        ],
        "primary_keys": ["id"],
    }


def test_db_read_endpoint_builds() -> None:
    ep = build_db_read_endpoint(_db_read_spec(), source="ep.json")
    assert isinstance(ep, DatabaseReadEndpoint)
    assert ep.database_object.schema == "public"
    assert ep.database_object.name == "wise_transfers"
    assert len(ep.columns) == 2
    assert ep.columns[0].arrow_type == "Int64"
    assert ep.primary_keys == ("id",)


def test_db_write_endpoint_builds_from_same_spec() -> None:
    ep = build_db_write_endpoint(_db_read_spec(), source="ep.json")
    assert isinstance(ep, DatabaseWriteEndpoint)
    assert ep.primary_keys == ("id",)


def test_db_read_endpoint_requires_columns() -> None:
    spec = _db_read_spec()
    del spec["columns"]
    with pytest.raises(ResolveError) as exc:
        build_db_read_endpoint(spec, source="ep.json")
    assert "columns" in str(exc.value)


def test_db_read_endpoint_rejects_column_without_arrow_type() -> None:
    spec = _db_read_spec()
    spec["columns"][1] = {"name": "amount", "native_type": "numeric(18,2)"}
    with pytest.raises(ResolveError) as exc:
        build_db_read_endpoint(spec, source="ep.json")
    assert "$.columns[1]" in str(exc.value)
    assert "arrow_type" in str(exc.value)


# --- API endpoints -------------------------------------------------------


def _api_read_spec() -> dict:
    return {
        "endpoint_id": "transfers",
        "operations": {
            "read": {
                "request": {"method": "GET", "path": "/v1/transfers", "query": {"limit": {"from_param": "limit"}}},
                "params": {
                    "limit": {"in": "query", "type": "integer", "required": False, "controlled_by": "pagination"},
                    "cursor": {"in": "query", "type": "string", "required": False, "format": "date-time"},
                },
                "pagination": {
                    "type": "offset",
                    "limit": {"param": "limit", "default": {"ref": "runtime.batch_size"}, "max": 100},
                    "offset": {"param": "offset", "initial": 0},
                    "stop_when": {"empty": {"ref": "response.records"}},
                },
                "replication": {
                    "supported_methods": ["full_refresh", "incremental"],
                    "cursor_mappings": [
                        {"cursor_field": "created", "param": "createdDateStart", "operator": "gte", "format": "date-time"}
                    ],
                },
                "response": {"records": {"ref": "response.body"}},
            }
        },
    }


def test_api_read_endpoint_builds() -> None:
    ep = build_api_read_endpoint(_api_read_spec(), source="ep.json")
    assert isinstance(ep, ApiReadEndpoint)
    assert ep.request.method == "GET"
    assert ep.request.path == "/v1/transfers"
    assert isinstance(ep.pagination, OffsetPagination)
    assert ep.pagination.limit_param == "limit"
    assert ep.pagination.offset_param == "offset"
    assert ep.replication.supported_methods == ("full_refresh", "incremental")
    assert ep.replication.cursor_mappings[0].cursor_field == "created"
    assert ep.response.records_ref == "response.body"


def test_api_read_endpoint_rejects_unknown_pagination_type() -> None:
    spec = _api_read_spec()
    spec["operations"]["read"]["pagination"]["type"] = "bogus"
    with pytest.raises(ResolveError) as exc:
        build_api_read_endpoint(spec, source="ep.json")
    assert "pagination" in str(exc.value)
    assert "bogus" in str(exc.value)


def test_api_read_endpoint_builds_cursor_pagination() -> None:
    spec = _api_read_spec()
    spec["operations"]["read"]["pagination"] = {
        "type": "cursor",
        "cursor": {
            "param": "next_token",
            "initial": None,
            "next_cursor": {"ref": "response.body.next_page_token"},
        },
        "stop_when": {"empty": {"ref": "response.records"}},
    }
    ep = build_api_read_endpoint(spec, source="ep.json")
    assert isinstance(ep.pagination, CursorPagination)
    assert ep.pagination.cursor_param == "next_token"
    assert ep.pagination.cursor_response_ref == "response.body.next_page_token"


def test_api_read_endpoint_builds_page_pagination() -> None:
    spec = _api_read_spec()
    spec["operations"]["read"]["pagination"] = {
        "type": "page",
        "page": {"param": "page", "initial": 1},
        "size": {"param": "per_page", "default": 50},
    }
    ep = build_api_read_endpoint(spec, source="ep.json")
    assert isinstance(ep.pagination, PagePagination)
    assert ep.pagination.page_param == "page"
    assert ep.pagination.size_param == "per_page"


def test_api_write_endpoint_builds() -> None:
    spec = {
        "endpoint_id": "create_contact",
        "operations": {
            "write": {
                "insert": {
                    "request": {"method": "POST", "path": "/v1/contacts"},
                    "batching": {"max_records": 100},
                }
            }
        },
    }
    ep = build_api_write_endpoint(spec, source="ep.json", write_mode="insert")
    assert isinstance(ep, ApiWriteEndpoint)
    assert ep.write_mode == "insert"
    assert ep.request.method == "POST"
    assert ep.batching is not None
    assert ep.batching.max_records == 100


def test_api_write_endpoint_missing_write_mode_errors() -> None:
    spec = {
        "endpoint_id": "create_contact",
        "operations": {"write": {"insert": {"request": {"method": "POST", "path": "/v1/contacts"}}}},
    }
    with pytest.raises(ResolveError) as exc:
        build_api_write_endpoint(spec, source="ep.json", write_mode="upsert")
    assert "upsert" in str(exc.value)


# --- Connector / Connection ----------------------------------------------


def _connector_db_spec() -> dict:
    return {
        "connector_id": "postgres",
        "kind": "database",
        "display_name": "PostgreSQL",
        "default_transport": "database",
        "transports": {
            "database": {
                "transport_type": "sqlalchemy",
                "driver": "postgresql+asyncpg",
                "dsn": {"template": "postgresql+asyncpg://{u}:{p}@{h}/{d}", "bindings": {}},
            }
        },
    }


def _connector_api_spec() -> dict:
    return {
        "connector_id": "wise",
        "kind": "api",
        "display_name": "Wise",
        "default_transport": "api",
        "transports": {
            "api": {
                "transport_type": "http",
                "base_url": "https://api.wise.com",
                "headers": {"Accept": "application/json"},
                "timeout_seconds": 30,
            }
        },
    }


def test_connector_db_builds() -> None:
    c = build_connector(_connector_db_spec(), source="c.json")
    assert c.kind == "database"
    transport = c.transports["database"]
    assert isinstance(transport, SqlAlchemyTransport)
    assert transport.driver == "postgresql+asyncpg"


def test_connector_api_builds() -> None:
    c = build_connector(_connector_api_spec(), source="c.json")
    assert c.kind == "api"
    transport = c.transports["api"]
    assert isinstance(transport, HttpTransport)
    assert transport.base_url == "https://api.wise.com"


def test_connector_unknown_transport_type_errors() -> None:
    spec = _connector_api_spec()
    spec["transports"]["api"]["transport_type"] = "magic"
    with pytest.raises(ResolveError) as exc:
        build_connector(spec, source="c.json")
    assert "transport_type" in str(exc.value)


def test_connection_builds_and_rejects_connector_mismatch() -> None:
    connector = build_connector(_connector_api_spec(), source="c.json")
    conn = build_connection(
        {"connection_id": "c1", "connector_id": "wise", "parameters": {}, "secret_refs": {}},
        source="conn.json",
        connector=connector,
    )
    assert conn.connector.connector_id == "wise"

    with pytest.raises(ResolveError):
        build_connection(
            {"connection_id": "c1", "connector_id": "OTHER", "parameters": {}, "secret_refs": {}},
            source="conn.json",
            connector=connector,
        )


# --- Stream / Pipeline ---------------------------------------------------


def _stream_spec_incremental() -> dict:
    return {
        "stream_id": "s1",
        "pipeline_id": "p1",
        "display_name": "transfers stream",
        "source": {
            "endpoint_ref": {"scope": "connector", "connection_id": "src-conn", "endpoint_id": "transfers"},
            "filters": [],
            "replication": {"method": "incremental", "cursor_field": "created"},
        },
        "destinations": [
            {
                "endpoint_ref": {"scope": "connection", "connection_id": "dst-conn", "endpoint_id": "wise_transfers"},
                "write": {"mode": "upsert", "conflict_keys": [["id"]]},
            }
        ],
        "mapping": {
            "assignments": [
                {
                    "target": {"path": "id", "arrow_type": "Int64", "nullable": False},
                    "value": {"expression": {"op": "get", "path": "id"}},
                }
            ]
        },
    }


def _resolved_connections() -> dict:
    src_connector = build_connector(_connector_api_spec(), source="c1.json")
    dst_connector = build_connector(_connector_db_spec(), source="c2.json")
    src = build_connection(
        {"connection_id": "src-conn", "connector_id": "wise", "parameters": {}, "secret_refs": {}},
        source="conn-src.json",
        connector=src_connector,
    )
    dst = build_connection(
        {"connection_id": "dst-conn", "connector_id": "postgres", "parameters": {}, "secret_refs": {}},
        source="conn-dst.json",
        connector=dst_connector,
    )
    return {"src-conn": src, "dst-conn": dst}


def test_resolved_stream_builds_incremental_api_to_db() -> None:
    connections = _resolved_connections()
    endpoints = {
        ("connector", "src-conn", "transfers"): {**_api_read_spec(), "__source__": "api-ep.json"},
        ("connection", "dst-conn", "wise_transfers"): {**_db_read_spec(), "__source__": "db-ep.json"},
    }
    stream = build_resolved_stream(
        _stream_spec_incremental(),
        source="stream.json",
        connections=connections,
        endpoints=endpoints,
    )
    assert stream.stream_id == "s1"
    assert isinstance(stream.source.endpoint, ApiReadEndpoint)
    assert stream.source.replication.method == "incremental"
    assert stream.source.replication.cursor_field == "created"
    assert len(stream.destinations) == 1
    assert isinstance(stream.destinations[0].endpoint, DatabaseWriteEndpoint)
    assert stream.destinations[0].write.mode == "upsert"
    assert stream.destinations[0].write.conflict_keys == (("id",),)


def test_resolved_stream_rejects_incremental_without_cursor_field() -> None:
    connections = _resolved_connections()
    endpoints = {
        ("connector", "src-conn", "transfers"): {**_api_read_spec(), "__source__": "api-ep.json"},
        ("connection", "dst-conn", "wise_transfers"): {**_db_read_spec(), "__source__": "db-ep.json"},
    }
    spec = _stream_spec_incremental()
    del spec["source"]["replication"]["cursor_field"]
    with pytest.raises(ResolveError) as exc:
        build_resolved_stream(spec, source="stream.json", connections=connections, endpoints=endpoints)
    assert "cursor_field" in str(exc.value)


def test_resolved_stream_rejects_unknown_connection() -> None:
    connections = _resolved_connections()
    endpoints = {
        ("connector", "src-conn", "transfers"): {**_api_read_spec(), "__source__": "api-ep.json"},
    }
    spec = _stream_spec_incremental()
    spec["source"]["endpoint_ref"]["connection_id"] = "ghost"
    with pytest.raises(ResolveError) as exc:
        build_resolved_stream(spec, source="stream.json", connections=connections, endpoints=endpoints)
    assert "ghost" in str(exc.value)


def test_resolved_stream_rejects_unknown_endpoint() -> None:
    connections = _resolved_connections()
    spec = _stream_spec_incremental()
    with pytest.raises(ResolveError) as exc:
        build_resolved_stream(spec, source="stream.json", connections=connections, endpoints={})
    assert "endpoint not found" in str(exc.value)


def test_resolved_pipeline_builds() -> None:
    connections = _resolved_connections()
    endpoints = {
        ("connector", "src-conn", "transfers"): {**_api_read_spec(), "__source__": "api-ep.json"},
        ("connection", "dst-conn", "wise_transfers"): {**_db_read_spec(), "__source__": "db-ep.json"},
    }
    pipeline_spec = {
        "pipeline_id": "p1",
        "display_name": "Wise to PostgreSQL",
        "status": "active",
        "connections": {"source": "src-conn", "destinations": ["dst-conn"]},
        "streams": ["s1"],
        "schedule": {"type": "manual"},
        "runtime": {
            "buffer_size": 1000,
            "batching": {"batch_size": 100, "max_concurrent_batches": 1},
            "logging": {"log_level": "INFO", "metrics_enabled": True},
            "error_handling": {"strategy": "dlq", "max_retries": 3, "retry_delay_seconds": 5},
        },
        "engine": {"vcpu": 1, "memory": 4096},
    }
    pipeline = build_resolved_pipeline(
        pipeline_spec,
        source="pipeline.json",
        stream_specs=[(_stream_spec_incremental(), "stream.json")],
        connections=connections,
        endpoints=endpoints,
    )
    assert pipeline.pipeline_id == "p1"
    assert pipeline.source_connection_id == "src-conn"
    assert pipeline.destination_connection_ids == ("dst-conn",)
    assert pipeline.runtime.batching.batch_size == 100
    assert pipeline.engine is not None
    assert pipeline.engine.vcpu == 1


def test_resolved_pipeline_requires_runtime() -> None:
    spec = {
        "pipeline_id": "p1",
        "connections": {"source": "a", "destinations": ["b"]},
        "streams": [],
    }
    with pytest.raises(ResolveError) as exc:
        build_resolved_pipeline(
            spec,
            source="pipeline.json",
            stream_specs=[],
            connections={},
            endpoints={},
        )
    assert "runtime" in str(exc.value)
