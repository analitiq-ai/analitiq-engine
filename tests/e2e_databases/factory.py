"""Generate connection / endpoint / pipeline / stream JSON for one test run.

Every artifact is JSON-Schema validated against the live Analitiq
contract before it is written to disk, so a malformed test config fails
inside the test process instead of inside the engine container.

All output lands under ``workspace/`` (gitignored).
"""
from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Literal, Mapping

from src.config.schema_validator import (
    validate as validate_artifact,  # type: ignore[import-not-found]
)
from tests.e2e_databases.databases._base import ColumnSpec, DatabaseSpec
from tests.e2e_databases.seeds import CURSOR_COLUMN, PRIMARY_KEY_COLUMN, SEED_TABLE_NAME

logger = logging.getLogger(__name__)

FRAMEWORK_ROOT = Path(__file__).resolve().parent
WORKSPACE = FRAMEWORK_ROOT / "workspace"
CONNECTIONS_DIR = WORKSPACE / "connections"
PIPELINES_DIR = WORKSPACE / "pipelines"
STATE_DIR = WORKSPACE / "state"
DEADLETTER_DIR = WORKSPACE / "deadletter"

_NAMESPACE = uuid.UUID("00000000-0000-4000-8000-e2e2e2e2e2e2")

ReplicationMode = Literal["full_refresh", "incremental"]
WriteMode = Literal["insert", "upsert"]


@dataclass(frozen=True)
class GeneratedPair:
    pipeline_id: str
    stream_id: str
    source_connection_id: str
    destination_connection_id: str
    source_endpoint_id: str
    destination_endpoint_id: str


def _det_uuid(*parts: str) -> str:
    name = "::".join(parts)
    return str(uuid.uuid5(_NAMESPACE, name))


def _conn_id(spec: DatabaseSpec, role: str) -> str:
    return _det_uuid("connection", spec.slug, role)


def _pipeline_id(
    source: DatabaseSpec, destination: DatabaseSpec, mode: ReplicationMode
) -> str:
    return _det_uuid("pipeline", source.slug, destination.slug, mode)


def _stream_id(
    source: DatabaseSpec, destination: DatabaseSpec, mode: ReplicationMode
) -> str:
    return _det_uuid("stream", source.slug, destination.slug, mode)


def reset_workspace() -> None:
    """Wipe the generated configs and runtime state for a clean run.

    Runtime state is cleared too: an incremental run leaves a cursor
    bookmark under ``state/{pipeline_id}/``, and re-running the same pair
    would otherwise resume from it and read zero rows. Connector
    definitions live outside ``workspace/`` and are never touched.
    """
    for directory in (CONNECTIONS_DIR, PIPELINES_DIR, STATE_DIR, DEADLETTER_DIR):
        if directory.exists():
            for child in directory.iterdir():
                _rmtree(child)
        directory.mkdir(parents=True, exist_ok=True)
    # Manifest must always exist or the engine fails to discover any pipeline.
    _write_json(PIPELINES_DIR / "manifest.json", {"pipelines": []})


def _rmtree(path: Path) -> None:
    if path.is_dir():
        for child in path.iterdir():
            _rmtree(child)
        path.rmdir()
    else:
        path.unlink()


def _write_json(path: Path, payload: Mapping) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str))


def _column_payload(col: ColumnSpec) -> Dict:
    return {
        "name": col.name,
        "native_type": col.native_type,
        "arrow_type": col.arrow_type,
        "nullable": col.nullable,
    }


def _write_connection_files(spec: DatabaseSpec, role: str) -> str:
    """Write connection.json + .secrets/credentials.json + endpoint JSON. Return connection_id."""
    connection_id = _conn_id(spec, role)
    desc = spec.connection(role)

    connection_dir = CONNECTIONS_DIR / connection_id
    connection_dir.mkdir(parents=True, exist_ok=True)
    (connection_dir / ".secrets").mkdir(exist_ok=True)

    parameters: Dict[str, object] = {
        "host": desc.engine_host,
        "port": desc.engine_port,
        "database": desc.database,
        "username": desc.username,
    }
    parameters.update(desc.extra_parameters)

    secret_refs = {"password": f"connections/{connection_id}/password"}

    connection_payload = {
        "$schema": "https://schemas.analitiq.work/connection/latest.json",
        "connection_id": connection_id,
        "connector_id": spec.dip_connector_id,
        "display_name": f"E2E {spec.slug} ({role})",
        "parameters": parameters,
        "secret_refs": secret_refs,
        "discovered": {},
        "selections": {},
        "tags": ["e2e", spec.slug, role],
    }
    validate_artifact(
        "connection", connection_payload, source=f"<e2e {spec.slug}/{role}>"
    )
    _write_json(connection_dir / "connection.json", connection_payload)

    credentials: Dict[str, str] = {"password": desc.password}
    credentials.update(desc.extra_secrets)
    _write_json(connection_dir / ".secrets" / "credentials.json", credentials)

    endpoint_id = _endpoint_id_for(spec, role)
    endpoint_payload = {
        "$schema": "https://schemas.analitiq.work/database-endpoint/latest.json",
        "endpoint_id": endpoint_id,
        "database_object": {
            "name": SEED_TABLE_NAME,
            "object_type": "table",
            **({"schema": desc.schema} if desc.schema else {}),
        },
        "columns": [_column_payload(c) for c in spec.columns()],
        "primary_keys": [PRIMARY_KEY_COLUMN],
    }
    validate_artifact(
        "database-endpoint", endpoint_payload, source=f"<e2e {spec.slug} endpoint>"
    )
    endpoint_dir = connection_dir / "definition" / "endpoints"
    endpoint_dir.mkdir(parents=True, exist_ok=True)
    _write_json(endpoint_dir / f"{endpoint_id}.json", endpoint_payload)

    return connection_id


def _endpoint_id_for(spec: DatabaseSpec, role: str) -> str:
    return f"{spec.slug}__{role}__{SEED_TABLE_NAME}"


def _write_pipeline_files(
    source: DatabaseSpec,
    destination: DatabaseSpec,
    mode: ReplicationMode,
    write_mode: WriteMode,
    source_connection_id: str,
    destination_connection_id: str,
) -> GeneratedPair:
    pipeline_id = _pipeline_id(source, destination, mode)
    stream_id = _stream_id(source, destination, mode)
    source_endpoint_id = _endpoint_id_for(source, "source")
    destination_endpoint_id = _endpoint_id_for(destination, "destination")

    pipeline_dir = PIPELINES_DIR / pipeline_id
    (pipeline_dir / "streams").mkdir(parents=True, exist_ok=True)

    pipeline_payload = {
        "$schema": "https://schemas.analitiq.work/pipeline/latest.json",
        "pipeline_id": pipeline_id,
        "display_name": f"E2E {source.slug} -> {destination.slug} ({mode})",
        "status": "active",
        "connections": {
            "source": source_connection_id,
            "destinations": [destination_connection_id],
        },
        "streams": [stream_id],
        "schedule": {"type": "manual", "timezone": "UTC"},
        "runtime": {
            "buffer_size": 1000,
            "batching": {"batch_size": 100, "max_concurrent_batches": 1},
            "logging": {"log_level": "INFO", "metrics_enabled": True},
            "error_handling": {
                "strategy": "fail",
                "max_retries": 0,
                "retry_delay_seconds": 0,
            },
        },
        "engine": {"vcpu": 1, "memory": 1024},
        "tags": ["e2e", source.slug, destination.slug, mode],
    }
    validate_artifact(
        "pipeline", pipeline_payload, source=f"<e2e pipeline {pipeline_id}>"
    )
    _write_json(pipeline_dir / "pipeline.json", pipeline_payload)

    stream_payload = _build_stream_payload(
        source=source,
        destination=destination,
        mode=mode,
        write_mode=write_mode,
        pipeline_id=pipeline_id,
        stream_id=stream_id,
        source_connection_id=source_connection_id,
        destination_connection_id=destination_connection_id,
        source_endpoint_id=source_endpoint_id,
        destination_endpoint_id=destination_endpoint_id,
    )
    validate_artifact("stream", stream_payload, source=f"<e2e stream {stream_id}>")
    _write_json(pipeline_dir / "streams" / f"{stream_id}.json", stream_payload)

    _add_to_manifest(pipeline_id, pipeline_payload["display_name"], [stream_id])

    return GeneratedPair(
        pipeline_id=pipeline_id,
        stream_id=stream_id,
        source_connection_id=source_connection_id,
        destination_connection_id=destination_connection_id,
        source_endpoint_id=source_endpoint_id,
        destination_endpoint_id=destination_endpoint_id,
    )


def _build_stream_payload(
    *,
    source: DatabaseSpec,
    destination: DatabaseSpec,
    mode: ReplicationMode,
    write_mode: WriteMode,
    pipeline_id: str,
    stream_id: str,
    source_connection_id: str,
    destination_connection_id: str,
    source_endpoint_id: str,
    destination_endpoint_id: str,
) -> Dict:
    source_columns = {c.name: c for c in source.columns()}
    destination_columns = destination.columns()
    assignments: List[Dict] = []
    for dest_col in destination_columns:
        src_col = source_columns.get(dest_col.name)
        if src_col is None:
            raise ValueError(
                f"Destination column {dest_col.name!r} has no matching source column "
                f"in {source.slug}"
            )
        assignments.append(
            {
                "value": {"expression": {"op": "get", "path": src_col.name}},
                "target": {
                    "arrow_type": dest_col.arrow_type,
                    "nullable": dest_col.nullable,
                    "path": dest_col.name,
                },
            }
        )

    source_block: Dict[str, object] = {
        "endpoint_ref": {
            "scope": "connection",
            "connection_id": source_connection_id,
            "endpoint_id": source_endpoint_id,
        },
        "primary_keys": [PRIMARY_KEY_COLUMN],
    }
    if mode == "incremental":
        source_block["replication"] = {
            "method": "incremental",
            "cursor_field": CURSOR_COLUMN,
            "safety_window_seconds": 0,
        }
    else:
        source_block["replication"] = {"method": "full_refresh"}

    write_block: Dict[str, object] = {"mode": write_mode}
    if write_mode == "upsert":
        write_block["conflict_keys"] = [[PRIMARY_KEY_COLUMN]]

    return {
        "$schema": "https://schemas.analitiq.work/stream/latest.json",
        "stream_id": stream_id,
        "pipeline_id": pipeline_id,
        "display_name": f"{source.slug}->{destination.slug} {mode}",
        "status": "active",
        "source": source_block,
        "destinations": [
            {
                "endpoint_ref": {
                    "scope": "connection",
                    "connection_id": destination_connection_id,
                    "endpoint_id": destination_endpoint_id,
                },
                "write": write_block,
            }
        ],
        "mapping": {"assignments": assignments},
        "tags": ["e2e"],
    }


def _add_to_manifest(pipeline_id: str, display_name: str, streams: List[str]) -> None:
    manifest_path = PIPELINES_DIR / "manifest.json"
    manifest = (
        json.loads(manifest_path.read_text())
        if manifest_path.is_file()
        else {"pipelines": []}
    )
    manifest["pipelines"] = [
        p for p in manifest.get("pipelines", []) if p.get("pipeline_id") != pipeline_id
    ]
    manifest["pipelines"].append(
        {
            "pipeline_id": pipeline_id,
            "display_name": display_name,
            "path": f"{pipeline_id}/pipeline.json",
            "status": "active",
            "streams": streams,
        }
    )
    _write_json(manifest_path, manifest)


def build_pair(
    source: DatabaseSpec,
    destination: DatabaseSpec,
    mode: ReplicationMode,
    write_mode: WriteMode = "insert",
) -> GeneratedPair:
    """Create every config artifact for one pair and write it to ``workspace/``."""
    reset_workspace()
    src_conn_id = _write_connection_files(source, "source")
    dst_conn_id = _write_connection_files(destination, "destination")
    return _write_pipeline_files(
        source=source,
        destination=destination,
        mode=mode,
        write_mode=write_mode,
        source_connection_id=src_conn_id,
        destination_connection_id=dst_conn_id,
    )
