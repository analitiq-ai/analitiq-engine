"""Programmatic fixture builders for pipeline-shape integration tests.

Each builder materializes a complete, schema-conformant fixture tree
under a ``root`` path. The tests load the result via
``load_resolved_pipeline`` and run it end-to-end through fake source /
gRPC components.

Five pipeline shapes are covered:

* ``api_to_db_incremental`` — API source (cursor pagination), DB sink,
  incremental replication on a timestamp.
* ``db_to_db_incremental`` — DB source (offset pagination), DB sink,
  incremental replication on an id.
* ``db_to_db_full`` — DB → DB full refresh (truncate-insert).
* ``api_to_api`` — API → API write (insert).
* ``multi_stream_one_dest`` — three streams on one API connection
  feeding one DB connection (mixed replication).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2))


# ---------------------------------------------------------------------------
# Connectors
# ---------------------------------------------------------------------------


_DEFAULT_TYPE_MAP = [
    {"match": "exact", "native": "bigint", "canonical": "Int64"},
    {"match": "exact", "native": "text", "canonical": "Utf8"},
    {"match": "exact", "native": "string", "canonical": "Utf8"},
    {"match": "exact", "native": "long", "canonical": "Int64"},
    {"match": "exact", "native": "datetime", "canonical": "Timestamp(MICROSECOND, UTC)"},
]


def _write_type_map(root: Path, slug: str) -> None:
    _write_json(
        root / "connectors" / slug / "definition" / "type-map.json",
        _DEFAULT_TYPE_MAP,  # type: ignore[arg-type]
    )


def _api_connector(root: Path, slug: str, *, base_url: str = "https://example.invalid") -> None:
    _write_json(
        root / "connectors" / slug / "definition" / "connector.json",
        {
            "$schema": "https://schemas.analitiq.ai/connector/latest.json",
            "connector_id": slug,
            "kind": "api",
            "display_name": slug.title(),
            "version": "1.0.0",
            "default_transport": "primary",
            "transports": {
                "primary": {
                    "transport_type": "http",
                    "base_url": base_url,
                    "headers": {"Accept": "application/json"},
                    "timeout_seconds": 30,
                }
            },
            "auth": {"type": "api_key"},
        },
    )
    _write_type_map(root, slug)


def _db_connector(root: Path, slug: str = "postgresql") -> None:
    _write_json(
        root / "connectors" / slug / "definition" / "connector.json",
        {
            "$schema": "https://schemas.analitiq.ai/connector/latest.json",
            "connector_id": slug,
            "kind": "database",
            "display_name": "PostgreSQL",
            "version": "1.0.0",
            "default_transport": "primary",
            "transports": {
                "primary": {
                    "transport_type": "sqlalchemy",
                    "driver": "postgresql",
                    "dsn": {
                        "template": (
                            "postgresql+asyncpg://{username}:{password}"
                            "@{host}:{port}/{database}"
                        ),
                        "bindings": {
                            "username": {"from": "parameters.username"},
                            "host": {"from": "parameters.host"},
                            "port": {"from": "parameters.port"},
                            "database": {"from": "parameters.database"},
                            "password": {"from": "secrets.password"},
                        },
                    },
                }
            },
            "auth": {"type": "basic"},
        },
    )
    _write_type_map(root, slug)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


def _api_read_endpoint(
    root: Path,
    *,
    connector_slug: str,
    endpoint_id: str,
    path: str,
    pagination: dict[str, Any],
    cursor_field: str | None = None,
) -> None:
    spec = {
        "$schema": "https://schemas.analitiq.ai/api-endpoint/latest.json",
        "endpoint_id": endpoint_id,
        "display_name": endpoint_id,
        "operations": {
            "read": {
                "request": {"method": "GET", "path": path},
                "params": {},
                "pagination": pagination,
                "replication": {
                    "supported_methods": ["full_refresh", "incremental"]
                    if cursor_field
                    else ["full_refresh"],
                    "cursor_mappings": (
                        [
                            {
                                "cursor_field": cursor_field,
                                "param": f"{cursor_field}_after",
                                "operator": "gte",
                                "format": "date-time",
                            }
                        ]
                        if cursor_field
                        else []
                    ),
                },
                "response": {
                    "records": {"ref": "response.body"},
                    "schema": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": {"type": "integer", "native_type": "long", "arrow_type": "Int64"},
                                "value": {"type": "string", "native_type": "string", "arrow_type": "Utf8"},
                                **(
                                    {cursor_field: {"type": "string", "native_type": "datetime", "arrow_type": "Timestamp(MICROSECOND, UTC)"}}
                                    if cursor_field
                                    else {}
                                ),
                            },
                            "required": ["id"],
                        },
                    },
                },
            }
        },
    }
    _write_json(
        root / "connectors" / connector_slug / "definition" / "endpoints" / f"{endpoint_id}.json",
        spec,
    )


def _api_write_endpoint(
    root: Path,
    *,
    connector_slug: str,
    endpoint_id: str,
    path: str,
) -> None:
    _write_json(
        root / "connectors" / connector_slug / "definition" / "endpoints" / f"{endpoint_id}.json",
        {
            "$schema": "https://schemas.analitiq.ai/api-endpoint/latest.json",
            "endpoint_id": endpoint_id,
            "display_name": endpoint_id,
            "operations": {
                "write": {
                    "insert": {
                        "request": {"method": "POST", "path": path},
                        "input_schema": {
                            "type": "object",
                            "properties": {
                                "id": {"type": "integer", "arrow_type": "Int64"},
                                "value": {"type": "string", "arrow_type": "Utf8"},
                            },
                        },
                        "batching": {"max_records": 50},
                    }
                }
            },
        },
    )


def _db_endpoint(
    root: Path,
    *,
    connection_id: str,
    endpoint_id: str,
    schema: str,
    table: str,
    columns: list[dict[str, Any]],
    primary_keys: list[str],
) -> None:
    _write_json(
        root / "connections" / connection_id / "definition" / "endpoints" / f"{endpoint_id}.json",
        {
            "$schema": "https://schemas.analitiq.ai/database-endpoint/latest.json",
            "endpoint_id": endpoint_id,
            "database_object": {"schema": schema, "name": table, "object_type": "table"},
            "columns": columns,
            "primary_keys": primary_keys,
        },
    )


# ---------------------------------------------------------------------------
# Connections
# ---------------------------------------------------------------------------


def _api_connection(root: Path, *, connection_id: str, connector_slug: str) -> None:
    _write_json(
        root / "connections" / connection_id / "connection.json",
        {
            "$schema": "https://schemas.analitiq.ai/connection/latest.json",
            "connection_id": connection_id,
            "connector_id": connector_slug,
            "display_name": f"{connector_slug} connection",
            "parameters": {},
            "selections": {},
            "secret_refs": {"api_key": f"connections/{connection_id}/api_key"},
        },
    )
    (root / "connections" / connection_id / ".secrets").mkdir(parents=True, exist_ok=True)
    (root / "connections" / connection_id / ".secrets" / "credentials.json").write_text(
        json.dumps({"api_key": "test-token"})
    )


def _db_connection(
    root: Path,
    *,
    connection_id: str,
    connector_slug: str = "postgresql",
    database: str = "test",
) -> None:
    _write_json(
        root / "connections" / connection_id / "connection.json",
        {
            "$schema": "https://schemas.analitiq.ai/connection/latest.json",
            "connection_id": connection_id,
            "connector_id": connector_slug,
            "display_name": f"{connector_slug} connection",
            "parameters": {
                "host": "localhost",
                "port": 5432,
                "database": database,
                "username": "test",
            },
            "selections": {},
            "secret_refs": {
                "password": f"connections/{connection_id}/password"
            },
        },
    )
    (root / "connections" / connection_id / ".secrets").mkdir(parents=True, exist_ok=True)
    (root / "connections" / connection_id / ".secrets" / "credentials.json").write_text(
        json.dumps({"password": "test-pass"})
    )


# ---------------------------------------------------------------------------
# Pipeline / stream
# ---------------------------------------------------------------------------


def _write_pipeline(
    root: Path,
    *,
    pipeline_id: str,
    display_name: str,
    source_conn: str,
    dest_conns: list[str],
    stream_ids: list[str],
) -> None:
    _write_json(
        root / "pipelines" / "manifest.json",
        {
            "$schema": "https://schemas.analitiq.ai/pipeline-manifest/latest.json",
            "pipelines": [
                {
                    "pipeline_id": pipeline_id,
                    "display_name": display_name,
                    "path": f"{pipeline_id}/pipeline.json",
                    "status": "active",
                }
            ],
        },
    )
    _write_json(
        root / "pipelines" / pipeline_id / "pipeline.json",
        {
            "$schema": "https://schemas.analitiq.ai/pipeline/latest.json",
            "pipeline_id": pipeline_id,
            "display_name": display_name,
            "status": "active",
            "connections": {"source": source_conn, "destinations": dest_conns},
            "streams": stream_ids,
            "schedule": {"type": "manual"},
            "engine": {"vcpu": 1, "memory": 1024},
            "runtime": {
                "buffer_size": 100,
                "batching": {"batch_size": 10, "max_concurrent_batches": 1},
                "logging": {"log_level": "INFO", "metrics_enabled": False},
                "error_handling": {"strategy": "fail_fast", "max_retries": 1, "retry_delay_seconds": 0},
            },
        },
    )


def _api_source_block(
    *,
    source_conn: str,
    endpoint_id: str,
    cursor_field: str | None,
) -> dict[str, Any]:
    block: dict[str, Any] = {
        "endpoint_ref": {
            "scope": "connector",
            "connection_id": source_conn,
            "endpoint_id": endpoint_id,
        },
        "filters": [],
        "primary_keys": ["id"],
        "replication": {"method": "incremental" if cursor_field else "full_refresh"},
    }
    if cursor_field:
        block["replication"]["cursor_field"] = cursor_field
        block["replication"]["safety_window_seconds"] = 0
    return block


def _db_source_block(
    *,
    source_conn: str,
    endpoint_id: str,
    cursor_field: str | None,
) -> dict[str, Any]:
    block: dict[str, Any] = {
        "endpoint_ref": {
            "scope": "connection",
            "connection_id": source_conn,
            "endpoint_id": endpoint_id,
        },
        "filters": [],
        "primary_keys": ["id"],
        "replication": {"method": "incremental" if cursor_field else "full_refresh"},
    }
    if cursor_field:
        block["replication"]["cursor_field"] = cursor_field
    return block


def _db_dest_block(
    *,
    dest_conn: str,
    endpoint_id: str,
    write_mode: str,
    conflict_keys: list[list[str]] | None = None,
) -> dict[str, Any]:
    write: dict[str, Any] = {"mode": write_mode}
    if conflict_keys:
        write["conflict_keys"] = conflict_keys
    return {
        "endpoint_ref": {
            "scope": "connection",
            "connection_id": dest_conn,
            "endpoint_id": endpoint_id,
        },
        "write": write,
    }


def _api_dest_block(
    *,
    dest_conn: str,
    endpoint_id: str,
    write_mode: str = "insert",
) -> dict[str, Any]:
    return {
        "endpoint_ref": {
            "scope": "connector",
            "connection_id": dest_conn,
            "endpoint_id": endpoint_id,
        },
        "write": {"mode": write_mode},
    }


def _write_stream(
    root: Path,
    *,
    pipeline_id: str,
    stream_id: str,
    display_name: str,
    source_block: dict[str, Any],
    dest_blocks: list[dict[str, Any]],
) -> None:
    _write_json(
        root / "pipelines" / pipeline_id / "streams" / f"{stream_id}.json",
        {
            "$schema": "https://schemas.analitiq.ai/stream/latest.json",
            "stream_id": stream_id,
            "display_name": display_name,
            "pipeline_id": pipeline_id,
            "status": "active",
            "source": source_block,
            "destinations": dest_blocks,
            "mapping": {
                "assignments": [
                    {
                        "target": {"path": "id", "arrow_type": "Int64", "nullable": False},
                        "value": {"expression": {"op": "get", "path": "id"}},
                    },
                    {
                        "target": {"path": "value", "arrow_type": "Utf8"},
                        "value": {"expression": {"op": "get", "path": "value"}},
                    },
                ]
            },
        },
    )


# ---------------------------------------------------------------------------
# Public builders — one per shape
# ---------------------------------------------------------------------------


_DEFAULT_COLUMNS = [
    {"name": "id", "type": "bigint", "arrow_type": "Int64", "nullable": False, "ordinal_position": 1},
    {"name": "value", "type": "text", "arrow_type": "Utf8", "nullable": True, "ordinal_position": 2},
]


def build_api_to_db_incremental(root: Path) -> str:
    pipeline_id = "api-to-db-incremental"
    source_conn = "src-api"
    dest_conn = "dest-db"

    _api_connector(root, "wise")
    _db_connector(root)
    _api_connection(root, connection_id=source_conn, connector_slug="wise")
    _db_connection(root, connection_id=dest_conn)
    _api_read_endpoint(
        root,
        connector_slug="wise",
        endpoint_id="events",
        path="/v1/events",
        pagination={
            "type": "cursor",
            "cursor": {
                "param": "cursor",
                "next_cursor": {"ref": "response.body.next_cursor"},
            },
        },
        cursor_field="updated_at",
    )
    _db_endpoint(
        root,
        connection_id=dest_conn,
        endpoint_id="events",
        schema="public",
        table="events",
        columns=_DEFAULT_COLUMNS,
        primary_keys=["id"],
    )
    _write_pipeline(
        root,
        pipeline_id=pipeline_id,
        display_name="API to DB (incremental)",
        source_conn=source_conn,
        dest_conns=[dest_conn],
        stream_ids=["stream-1"],
    )
    _write_stream(
        root,
        pipeline_id=pipeline_id,
        stream_id="stream-1",
        display_name="events",
        source_block=_api_source_block(
            source_conn=source_conn, endpoint_id="events", cursor_field="updated_at"
        ),
        dest_blocks=[
            _db_dest_block(
                dest_conn=dest_conn,
                endpoint_id="events",
                write_mode="upsert",
                conflict_keys=[["id"]],
            )
        ],
    )
    return pipeline_id


def build_db_to_db_incremental(root: Path) -> str:
    pipeline_id = "db-to-db-incremental"
    source_conn = "src-db"
    dest_conn = "dest-db"

    _db_connector(root)
    _db_connection(root, connection_id=source_conn, database="source")
    _db_connection(root, connection_id=dest_conn, database="dest")
    _db_endpoint(
        root,
        connection_id=source_conn,
        endpoint_id="orders",
        schema="public",
        table="orders",
        columns=_DEFAULT_COLUMNS,
        primary_keys=["id"],
    )
    _db_endpoint(
        root,
        connection_id=dest_conn,
        endpoint_id="orders",
        schema="public",
        table="orders",
        columns=_DEFAULT_COLUMNS,
        primary_keys=["id"],
    )
    _write_pipeline(
        root,
        pipeline_id=pipeline_id,
        display_name="DB to DB (incremental)",
        source_conn=source_conn,
        dest_conns=[dest_conn],
        stream_ids=["stream-1"],
    )
    _write_stream(
        root,
        pipeline_id=pipeline_id,
        stream_id="stream-1",
        display_name="orders",
        source_block=_db_source_block(
            source_conn=source_conn, endpoint_id="orders", cursor_field="id"
        ),
        dest_blocks=[
            _db_dest_block(
                dest_conn=dest_conn,
                endpoint_id="orders",
                write_mode="upsert",
                conflict_keys=[["id"]],
            )
        ],
    )
    return pipeline_id


def build_db_to_db_full(root: Path) -> str:
    pipeline_id = "db-to-db-full"
    source_conn = "src-db"
    dest_conn = "dest-db"

    _db_connector(root)
    _db_connection(root, connection_id=source_conn, database="source")
    _db_connection(root, connection_id=dest_conn, database="dest")
    _db_endpoint(
        root,
        connection_id=source_conn,
        endpoint_id="snapshots",
        schema="public",
        table="snapshots",
        columns=_DEFAULT_COLUMNS,
        primary_keys=["id"],
    )
    _db_endpoint(
        root,
        connection_id=dest_conn,
        endpoint_id="snapshots",
        schema="public",
        table="snapshots",
        columns=_DEFAULT_COLUMNS,
        primary_keys=["id"],
    )
    _write_pipeline(
        root,
        pipeline_id=pipeline_id,
        display_name="DB to DB (full refresh)",
        source_conn=source_conn,
        dest_conns=[dest_conn],
        stream_ids=["stream-1"],
    )
    _write_stream(
        root,
        pipeline_id=pipeline_id,
        stream_id="stream-1",
        display_name="snapshots",
        source_block=_db_source_block(
            source_conn=source_conn, endpoint_id="snapshots", cursor_field=None
        ),
        dest_blocks=[
            _db_dest_block(
                dest_conn=dest_conn, endpoint_id="snapshots", write_mode="truncate_insert"
            )
        ],
    )
    return pipeline_id


def build_api_to_api(root: Path) -> str:
    pipeline_id = "api-to-api"
    source_conn = "src-api"
    dest_conn = "dest-api"

    _api_connector(root, "src_api")
    _api_connector(root, "dest_api")
    _api_connection(root, connection_id=source_conn, connector_slug="src_api")
    _api_connection(root, connection_id=dest_conn, connector_slug="dest_api")
    _api_read_endpoint(
        root,
        connector_slug="src_api",
        endpoint_id="items",
        path="/v1/items",
        pagination={
            "type": "offset",
            "limit": {"param": "limit", "default": 100, "max": 100},
            "offset": {"param": "offset", "initial": 0},
        },
    )
    _api_write_endpoint(
        root,
        connector_slug="dest_api",
        endpoint_id="items",
        path="/v1/items",
    )
    _write_pipeline(
        root,
        pipeline_id=pipeline_id,
        display_name="API to API",
        source_conn=source_conn,
        dest_conns=[dest_conn],
        stream_ids=["stream-1"],
    )
    _write_stream(
        root,
        pipeline_id=pipeline_id,
        stream_id="stream-1",
        display_name="items",
        source_block=_api_source_block(
            source_conn=source_conn, endpoint_id="items", cursor_field=None
        ),
        dest_blocks=[_api_dest_block(dest_conn=dest_conn, endpoint_id="items")],
    )
    return pipeline_id


def build_multi_stream_one_dest(root: Path) -> str:
    pipeline_id = "multi-stream-one-dest"
    source_conn = "src-api"
    dest_conn = "dest-db"

    _api_connector(root, "wise")
    _db_connector(root)
    _api_connection(root, connection_id=source_conn, connector_slug="wise")
    _db_connection(root, connection_id=dest_conn)

    # Three API endpoints, three DB target tables.
    for endpoint_id in ("alpha", "beta", "gamma"):
        _api_read_endpoint(
            root,
            connector_slug="wise",
            endpoint_id=endpoint_id,
            path=f"/v1/{endpoint_id}",
            pagination={
                "type": "offset",
                "limit": {"param": "limit", "default": 50, "max": 100},
                "offset": {"param": "offset", "initial": 0},
            },
            cursor_field="updated_at" if endpoint_id != "gamma" else None,
        )
        _db_endpoint(
            root,
            connection_id=dest_conn,
            endpoint_id=endpoint_id,
            schema="public",
            table=endpoint_id,
            columns=_DEFAULT_COLUMNS,
            primary_keys=["id"],
        )

    _write_pipeline(
        root,
        pipeline_id=pipeline_id,
        display_name="One API to one DB (3 streams)",
        source_conn=source_conn,
        dest_conns=[dest_conn],
        stream_ids=["stream-alpha", "stream-beta", "stream-gamma"],
    )
    for endpoint_id in ("alpha", "beta", "gamma"):
        cursor = "updated_at" if endpoint_id != "gamma" else None
        _write_stream(
            root,
            pipeline_id=pipeline_id,
            stream_id=f"stream-{endpoint_id}",
            display_name=endpoint_id,
            source_block=_api_source_block(
                source_conn=source_conn, endpoint_id=endpoint_id, cursor_field=cursor
            ),
            dest_blocks=[
                _db_dest_block(
                    dest_conn=dest_conn,
                    endpoint_id=endpoint_id,
                    write_mode="upsert" if cursor else "truncate_insert",
                    conflict_keys=[["id"]] if cursor else None,
                )
            ],
        )
    return pipeline_id


BUILDERS = {
    "api_to_db_incremental": build_api_to_db_incremental,
    "db_to_db_incremental": build_db_to_db_incremental,
    "db_to_db_full": build_db_to_db_full,
    "api_to_api": build_api_to_api,
    "multi_stream_one_dest": build_multi_stream_one_dest,
}
