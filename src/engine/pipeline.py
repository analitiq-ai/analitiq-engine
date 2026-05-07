"""Pipeline management and configuration.

This module bridges the new contract-shaped output of
:class:`PipelineConfigPrep` to the runtime-side ``StreamingEngine`` and
its source/destination connectors. The engine and connectors still
consume a flat per-stream config dict, so this module is the single
seam where the structured contract documents are translated.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

from .engine import StreamingEngine
from ..models.stream import EndpointRef
from ..shared.connection_runtime import ConnectionRuntime

logger = logging.getLogger(__name__)


class Pipeline:
    """Compose the runtime config dict and start the streaming engine."""

    def __init__(
        self,
        pipeline_config: Dict[str, Any],
        stream_configs: Optional[List[Dict[str, Any]]] = None,
        resolved_connections: Optional[Dict[str, ConnectionRuntime]] = None,
        resolved_endpoints: Optional[Dict[Any, Dict[str, Any]]] = None,
        connectors: Optional[List[Dict[str, Any]]] = None,
        state_dir: Optional[str] = None,
    ):
        load_dotenv()

        self.pipeline_config = pipeline_config
        self.stream_configs = stream_configs or []
        self.resolved_connections = resolved_connections or {}
        self.resolved_endpoints = resolved_endpoints or {}
        self.connectors = connectors or []

        pipeline_id = pipeline_config["pipeline_id"]
        project_root = Path(__file__).parent.parent.parent
        self.state_dir = state_dir or str(project_root / "state")
        self.dlq_dir = str(project_root / "deadletter" / pipeline_id)
        self._ensure_directories()

        runtime = pipeline_config.get("runtime") or {}
        batching = runtime.get("batching") or {"batch_size": 100, "max_concurrent_batches": 3}
        error_handling = runtime.get("error_handling") or {
            "max_retries": 3,
            "retry_delay_seconds": 5,
        }
        self.engine = StreamingEngine(
            pipeline_id=pipeline_id,
            batch_size=batching.get("batch_size", 100),
            max_concurrent_batches=batching.get("max_concurrent_batches", 3),
            buffer_size=runtime.get("buffer_size", 5000),
            dlq_path=self.dlq_dir,
            max_retries=error_handling.get("max_retries", 3),
            # ``retry_delay_seconds`` is the new contract name; older
            # engine code reads ``retry_delay``.
            retry_delay=error_handling.get(
                "retry_delay_seconds",
                error_handling.get("retry_delay", 5),
            ),
        )

    def _ensure_directories(self) -> None:
        Path(self.state_dir).mkdir(parents=True, exist_ok=True)
        Path(self.dlq_dir).mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Contract -> legacy connector config translation
    # ------------------------------------------------------------------

    def _build_config_dict(self) -> Dict[str, Any]:
        connections = self.pipeline_config.get("connections", {})
        streams: Dict[str, Dict[str, Any]] = {}

        for stream in self.stream_configs:
            source = stream["source"]
            destinations = stream.get("destinations") or []
            if not destinations:
                raise ValueError(
                    f"Stream {stream['stream_id']!r} has no destinations"
                )
            dest = destinations[0]

            source_runtime: ConnectionRuntime = source["_runtime"]
            dest_runtime: ConnectionRuntime = dest["_runtime"]
            source_endpoint = source["_endpoint"]
            dest_endpoint = dest["_endpoint"]

            source_config = _translate_source_config(
                stream=stream,
                source=source,
                endpoint=source_endpoint,
                runtime=source_runtime,
            )
            dest_config = _translate_destination_config(
                stream=stream,
                destination=dest,
                endpoint=dest_endpoint,
                runtime=dest_runtime,
            )

            mapping = stream.get("mapping") or {}
            mapping_config = {
                "assignments": [
                    _translate_assignment(a)
                    for a in (mapping.get("assignments") or [])
                ]
            }

            streams[stream["stream_id"]] = {
                "name": stream["stream_id"],
                "source": source_config,
                "destination": dest_config,
                "mapping": mapping_config,
            }

        first_stream = streams[next(iter(streams))] if streams else None
        pipeline_source = first_stream["source"] if first_stream else {}
        pipeline_dest = first_stream["destination"] if first_stream else {}

        return {
            "pipeline_id": self.pipeline_config["pipeline_id"],
            "name": self.pipeline_config.get("name") or self.pipeline_config.get("alias", ""),
            "version": self.pipeline_config.get("version", 1),
            "connections": connections,
            "resolved_connections": self.resolved_connections,
            "source": pipeline_source,
            "destination": pipeline_dest,
            "streams": streams,
            "runtime": self.pipeline_config.get("runtime") or {},
            "connectors": self.connectors,
        }

    async def run(self) -> None:
        config_dict = self._build_config_dict()
        try:
            await self.engine.stream_data(config_dict)
            logger.info(
                "Pipeline %s completed successfully",
                self.pipeline_config["pipeline_id"],
            )
        except Exception:
            logger.exception(
                "Pipeline %s failed", self.pipeline_config["pipeline_id"]
            )
            raise

    def get_metrics(self) -> Dict[str, Any]:
        return self.engine.get_metrics()


# ---------------------------------------------------------------------------
# Translation helpers
# ---------------------------------------------------------------------------


def _resolve_value_expression(node: Any, runtime: ConnectionRuntime) -> Any:
    """Resolve a value expression (``{ref}``/``{template}``/``{literal}``).

    Used for endpoint param defaults that pull from
    ``connection.selections.*`` etc. Plain scalars pass through; dict
    expressions are resolved through a fresh
    :class:`ResolutionContext` built from the runtime's connection JSON.
    """
    if not isinstance(node, dict):
        return node
    if "literal" in node:
        return node["literal"]
    if "ref" in node:
        path = node["ref"]
        scopes = _runtime_scopes(runtime)
        return _walk_dotted(scopes, path)
    if "template" in node:
        # Best-effort: resolve ${scope.path} placeholders against the
        # available scopes. The runtime side covers the common case
        # (string concatenation of scalar values).
        template = node["template"]
        scopes = _runtime_scopes(runtime)
        return _expand_template(template, scopes)
    return node


def _runtime_scopes(runtime: ConnectionRuntime) -> Dict[str, Any]:
    raw = runtime.raw_config
    return {
        "connection": {
            "parameters": dict(raw.get("parameters") or {}),
            "selections": dict(raw.get("selections") or {}),
            "discovered": dict(raw.get("discovered") or {}),
        },
    }


def _walk_dotted(scopes: Dict[str, Any], path: str) -> Any:
    cursor: Any = scopes
    for segment in path.split("."):
        if not isinstance(cursor, dict) or segment not in cursor:
            return None
        cursor = cursor[segment]
    return cursor


def _expand_template(template: str, scopes: Dict[str, Any]) -> str:
    out: List[str] = []
    i = 0
    while i < len(template):
        j = template.find("${", i)
        if j < 0:
            out.append(template[i:])
            break
        out.append(template[i:j])
        k = template.find("}", j + 2)
        if k < 0:
            out.append(template[i:])
            break
        path = template[j + 2 : k]
        value = _walk_dotted(scopes, path)
        out.append("" if value is None else str(value))
        i = k + 1
    return "".join(out)


def _translate_source_config(
    *,
    stream: Dict[str, Any],
    source: Dict[str, Any],
    endpoint: Dict[str, Any],
    runtime: ConnectionRuntime,
) -> Dict[str, Any]:
    """Build the legacy source config dict from contract-shaped inputs."""
    kind = endpoint.get("kind")
    replication = source.get("replication") or {}
    method = replication.get("method", "full_refresh")
    cursor_field = replication.get("cursor_field")
    safety_window = replication.get("safety_window_seconds")
    tie_breakers = replication.get("tie_breaker_fields") or []

    base: Dict[str, Any] = {
        "connector_type": runtime.connector_type,
        "driver": runtime.driver,
        "_runtime": runtime,
        "endpoint_ref": source["endpoint_ref"],
        "connection_ref": source["connection_ref"],
        "parameters": dict(runtime.raw_config.get("parameters") or {}),
        # Legacy connectors expect ``replication_method`` to be either
        # ``"incremental"`` or ``"full"``. The contract uses
        # ``"full_refresh"`` for the latter.
        "replication_method": "full" if method == "full_refresh" else method,
        "cursor_field": cursor_field,
        "cursor_mode": "inclusive",
        "safety_window_seconds": safety_window,
        "tie_breaker_fields": tie_breakers,
    }

    if kind == "database":
        base.update(_translate_database_source(source, endpoint))
    elif kind == "api":
        base.update(_translate_api_source(source, endpoint, runtime))
    else:
        raise ValueError(
            f"Unsupported source endpoint kind: {kind!r}; expected 'api' or 'database'"
        )

    base["host"] = _connection_host(runtime)
    return base


def _translate_destination_config(
    *,
    stream: Dict[str, Any],
    destination: Dict[str, Any],
    endpoint: Dict[str, Any],
    runtime: ConnectionRuntime,
) -> Dict[str, Any]:
    write = destination.get("write") or {}
    execution = destination.get("execution") or {}
    kind = endpoint.get("kind")

    base: Dict[str, Any] = {
        "connector_type": runtime.connector_type,
        "driver": runtime.driver,
        "_runtime": runtime,
        "endpoint_ref": destination["endpoint_ref"],
        "connection_ref": destination["connection_ref"],
        "parameters": dict(runtime.raw_config.get("parameters") or {}),
        "refresh_mode": write.get("mode", "upsert"),
        # ``write_mode`` is the legacy key the gRPC client reads when
        # building the SchemaMessage. Keep both for back-compat.
        "write_mode": write.get("mode", "upsert"),
        "conflict_keys": write.get("conflict_keys") or [],
        # The destination handler needs the full endpoint document to
        # build its SQLAlchemy table (column metadata, native types,
        # primary keys). Pass it through verbatim under the legacy key
        # the gRPC client uses.
        "endpoint_schema": dict(endpoint),
        "primary_key": endpoint.get("primary_keys") or [],
    }

    if execution.get("batch_size") is not None:
        base["batch_size"] = int(execution["batch_size"])
    if execution.get("max_concurrent_batches") is not None:
        base["max_concurrent_batches"] = int(execution["max_concurrent_batches"])

    if kind == "database":
        base.update(_translate_database_destination(endpoint))
    elif kind == "api":
        base.update(_translate_api_destination(endpoint))
    else:
        raise ValueError(
            f"Unsupported destination endpoint kind: {kind!r}; expected 'api' or 'database'"
        )

    base["host"] = _connection_host(runtime)
    return base


# ---- database endpoints ---------------------------------------------------


def _database_endpoint_string(endpoint: Dict[str, Any]) -> tuple[Optional[str], str, str]:
    obj = endpoint.get("database_object") or {}
    name = obj.get("name")
    if not isinstance(name, str) or not name:
        raise ValueError(
            f"database endpoint {endpoint.get('alias')!r} missing database_object.name"
        )
    schema = obj.get("schema")
    endpoint_str = f"{schema}/{name}" if schema else name
    return schema, name, endpoint_str


def _translate_database_source(
    source: Dict[str, Any], endpoint: Dict[str, Any]
) -> Dict[str, Any]:
    schema, table, endpoint_str = _database_endpoint_string(endpoint)
    columns = endpoint.get("columns") or []
    selected = source.get("selected_columns")
    if selected:
        column_names = list(selected)
    else:
        column_names = [c["name"] for c in columns]

    filters = []
    for f in source.get("filters") or []:
        filters.append(
            {
                "field": f["field"],
                # Legacy filter dict uses ``op``; contract uses ``operator``.
                "op": f.get("operator", "eq"),
                "value": f.get("value"),
            }
        )

    replication = source.get("replication") or {}
    db_pagination = source.get("database_pagination") or {}

    return {
        "endpoint": endpoint_str,
        "schema": schema or "public",
        "table_name": table,
        "columns": column_names,
        "filters": filters,
        "primary_key": endpoint.get("primary_keys") or [],
        # ``replication_key`` is a database-source legacy alias for the
        # cursor field. The connector uses it to build incremental
        # ``WHERE`` predicates.
        "replication_key": replication.get("cursor_field"),
        # Database pagination (offset by default) — the legacy connector
        # uses ``batch_size`` to drive LIMIT/OFFSET pagination.
        "database_pagination": db_pagination or {"type": "offset"},
    }


def _translate_database_destination(endpoint: Dict[str, Any]) -> Dict[str, Any]:
    schema, table, endpoint_str = _database_endpoint_string(endpoint)
    columns = endpoint.get("columns") or []
    primary_keys = endpoint.get("primary_keys") or []

    # Surface column metadata in the form the destination handler uses to
    # build DDL and conflict resolution.
    return {
        "endpoint": endpoint_str,
        "schema": schema or "public",
        "table_name": table,
        "columns": columns,
        "column_names": [c["name"] for c in columns],
        "primary_key": primary_keys,
    }


# ---- api endpoints --------------------------------------------------------


def _translate_api_source(
    source: Dict[str, Any],
    endpoint: Dict[str, Any],
    runtime: ConnectionRuntime,
) -> Dict[str, Any]:
    operations = endpoint.get("operations") or {}
    read = operations.get("read") or {}
    request = read.get("request") or {}
    params = read.get("params") or {}
    pagination = read.get("pagination") or {}
    replication = read.get("replication") or {}
    response = read.get("response") or {}

    method = request.get("method", "GET")
    path = request.get("path")
    if not isinstance(path, str) or not path:
        raise ValueError(
            f"api endpoint {endpoint.get('alias')!r}: operations.read.request.path is required"
        )

    # Build per-param resolved defaults so the connector's filter loop
    # can apply connection-scoped values (e.g. ``connection.selections.profile_id``).
    resolved_filters: Dict[str, Dict[str, Any]] = {}
    for name, decl in params.items():
        if not isinstance(decl, dict):
            continue
        controlled = decl.get("controlled_by")
        # Replication- and pagination-controlled params are filled in by
        # the connector loop, not here.
        if controlled in ("replication", "pagination"):
            continue
        default_value = None
        if "default" in decl:
            default_value = _resolve_value_expression(decl["default"], runtime)
        # Stream filters override defaults via the same dict shape.
        resolved_filters[name] = {
            "type": decl.get("type", "string"),
            "value": default_value,
            "default": default_value,
            "required": decl.get("required", False),
            "operators": decl.get("operators") or [],
        }

    # Apply stream-supplied filters by API parameter name.
    for f in source.get("filters") or []:
        target = f.get("field")
        if not target:
            continue
        entry = resolved_filters.setdefault(target, {"type": "string", "required": False})
        entry["value"] = f.get("value")

    # Replication mapping: cursor_field (record path) -> param (request key).
    replication_filter_mapping: Dict[str, str] = {}
    for mapping in replication.get("cursor_mappings") or []:
        cursor_field = mapping.get("cursor_field")
        param_name = mapping.get("param") or mapping.get("start_param")
        if cursor_field and param_name:
            replication_filter_mapping[cursor_field] = param_name
            # Ensure the param exists in the filters dict so the
            # connector's incremental setup can write into it.
            resolved_filters.setdefault(
                param_name,
                {"type": "string", "required": False, "value": None},
            )

    # Pagination block — translated to the legacy shape the connector
    # consumes.
    pagination_legacy: Dict[str, Any] = {}
    p_type = pagination.get("type")
    if p_type:
        pagination_legacy["type"] = p_type
        params_block = pagination_legacy.setdefault("params", {})
        if p_type == "offset":
            params_block["offset_param"] = pagination.get("offset", {}).get("param", "offset")
            params_block["limit_param"] = pagination.get("limit", {}).get("param", "limit")
        elif p_type == "page":
            params_block["page_param"] = pagination.get("page", {}).get("param", "page")
            params_block["limit_param"] = pagination.get("limit", {}).get("param", "limit")
            pagination_legacy["start_page"] = pagination.get("page", {}).get("initial", 1)
        elif p_type == "cursor":
            params_block["cursor_param"] = pagination.get("cursor", {}).get("param", "cursor")
            params_block["limit_param"] = pagination.get("limit", {}).get("param", "limit")
        elif p_type == "keyset":
            params_block["keyset_param"] = pagination.get("keyset", {}).get("param", "after")
            params_block["limit_param"] = pagination.get("limit", {}).get("param", "limit")
        elif p_type == "link":
            # Link pagination is driven by response.next_url; legacy
            # connector falls back to single-request loop for now.
            pass

    # Records extraction — legacy connector uses ``data_field`` (string
    # key into the JSON body, or absent for root array).
    records_ref = (response.get("records") or {}).get("ref", "")
    if records_ref.startswith("response.body."):
        data_field = records_ref[len("response.body."):]
    else:
        data_field = None

    return {
        "endpoint": path,
        "method": method,
        "filters": resolved_filters,
        "replication_filter_mapping": replication_filter_mapping,
        "pagination": pagination_legacy,
        "data_field": data_field,
        # Pass through declared params so any future extension can read
        # them without re-parsing the endpoint document.
        "params": params,
    }


def _translate_api_destination(endpoint: Dict[str, Any]) -> Dict[str, Any]:
    operations = endpoint.get("operations") or {}
    write_modes = operations.get("write") or {}
    return {
        "operations": {"write": write_modes},
        "endpoint_path": (
            (operations.get("read") or {}).get("request", {}).get("path", "")
        ),
    }


# ---- generic helpers ------------------------------------------------------


# ---------------------------------------------------------------------------
# Mapping translation (contract assignment shape -> AssignmentTransformer shape)
# ---------------------------------------------------------------------------


def _translate_assignment(assignment: Dict[str, Any]) -> Dict[str, Any]:
    """Translate a contract-shaped assignment to the legacy transformer shape.

    The contract authors targets with a dotted ``path`` string and an
    Apache Arrow type label. The transformer's expected shape uses a list
    path and a tagged ``value.kind`` (``"expr"`` or ``"const"``); this
    function only reshapes those structural differences. It does NOT map
    Arrow types to anything else — native ↔ Arrow translation is
    connector/connection-owned (via each artifact's ``type-map.json``)
    and is applied by the destination handler at write time.

    Contract shape:
        {
          "target": {"path": "id", "arrow_type": "Int64", "nullable": false},
          "value":  {"expression": {"op": "get", "path": "id"}}
        }

    Transformer shape:
        {
          "target": {"path": ["id"], "arrow_type": "Int64", "nullable": false},
          "value":  {"kind": "expr", "expr": {"op": "get", "path": ["id"]}}
        }
    """
    raw_target = assignment.get("target") or {}
    raw_value = assignment.get("value") or {}

    target_path_raw = raw_target.get("path", "")
    if isinstance(target_path_raw, str):
        target_path = [seg for seg in target_path_raw.split(".") if seg]
    elif isinstance(target_path_raw, list):
        target_path = list(target_path_raw)
    else:
        target_path = []

    # Pass the contract-declared fields through unchanged. The transformer
    # treats unknown ``type`` values as passthrough (no JSON coercion),
    # which is the right behavior for Arrow-typed values headed to the
    # destination's Arrow-based schema contract.
    target = dict(raw_target)
    target["path"] = target_path

    value: Dict[str, Any]
    if "expression" in raw_value:
        expression = dict(raw_value["expression"])
        expr_path = expression.get("path")
        if isinstance(expr_path, str):
            expression["path"] = [seg for seg in expr_path.split(".") if seg]
        value = {"kind": "expr", "expr": expression}
    elif "constant" in raw_value:
        # Constant carries its own ``arrow_type``; preserve it verbatim
        # for the destination to interpret via its type-map.
        value = {"kind": "const", "const": dict(raw_value["constant"] or {})}
    else:
        value = dict(raw_value)

    out: Dict[str, Any] = {"target": target, "value": value}
    if "validate" in assignment:
        out["validate"] = assignment["validate"]
    return out


def _connection_host(runtime: ConnectionRuntime) -> Optional[str]:
    raw = runtime.raw_config
    params = raw.get("parameters") or {}
    if isinstance(params, dict) and isinstance(params.get("host"), str):
        return params["host"]
    connector = runtime.connector_definition or {}
    transports = connector.get("transports") or {}
    default_ref = connector.get("default_transport")
    if default_ref and default_ref in transports:
        base_url = transports[default_ref].get("base_url")
        if isinstance(base_url, str):
            return base_url
    legacy = raw.get("host")
    return legacy if isinstance(legacy, str) else None
