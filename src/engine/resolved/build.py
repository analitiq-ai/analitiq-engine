"""Factories that lift raw JSON specs into resolved-runtime objects.

All inputs are plain dicts (loaded from disk or fetched from the backend);
all outputs are the frozen dataclasses defined in `types.py`. Every
function raises `ResolveError` with `file:$.path` context on the first
violation — no silent defaults, no legacy-key fallbacks.

Schema versions assumed:
    pipeline v6, stream v8, connection v6, connector v7,
    api-endpoint v7, database-endpoint v7
"""

from __future__ import annotations

from typing import Any

from .errors import ResolveError, require, require_str
from .types import (
    ApiReadEndpoint,
    ApiWriteEndpoint,
    Assignment,
    AssignmentTarget,
    BatchingPolicy,
    Column,
    CursorMapping,
    CursorPagination,
    DatabaseObject,
    DatabaseReadEndpoint,
    DatabaseWriteEndpoint,
    EndpointRef,
    EngineSizing,
    ErrorHandlingPolicy,
    ExecutionConfig,
    Filter,
    HttpRequest,
    HttpTransport,
    KeysetPagination,
    LinkPagination,
    LoggingPolicy,
    OffsetPagination,
    PagePagination,
    PaginationSpec,
    ParamSpec,
    ReadEndpoint,
    Replication,
    ReplicationConfig,
    ResolvedConnection,
    ResolvedConnector,
    ResolvedDestination,
    ResolvedPipeline,
    ResolvedSource,
    ResolvedStream,
    ResponseExtraction,
    Schedule,
    SqlAlchemyTransport,
    StreamMapping,
    RuntimeConfig,
    TransportSpec,
    WriteBatching,
    WriteEndpoint,
    WriteSpec,
)


# --- Endpoint reference ---------------------------------------------------


def build_endpoint_ref(spec: Any, *, source: str, path: str) -> EndpointRef:
    if not isinstance(spec, dict):
        raise ResolveError(source, path, "endpoint_ref must be an object")
    scope = require_str(spec, "scope", source=source, path=path)
    if scope not in ("connector", "connection"):
        raise ResolveError(source, f"{path}.scope", f"must be 'connector' or 'connection', got {scope!r}")
    connection_id = require_str(spec, "connection_id", source=source, path=path)
    endpoint_id = require_str(spec, "endpoint_id", source=source, path=path)
    return EndpointRef(scope=scope, connection_id=connection_id, endpoint_id=endpoint_id)


# --- Database endpoints ---------------------------------------------------


def _build_column(spec: dict, *, source: str, path: str) -> Column:
    if not isinstance(spec, dict):
        raise ResolveError(source, path, "column must be an object")
    return Column(
        name=require_str(spec, "name", source=source, path=path),
        arrow_type=require_str(spec, "arrow_type", source=source, path=path),
        native_type=spec.get("native_type"),
        nullable=bool(spec.get("nullable", True)),
        default=spec.get("default"),
        ordinal_position=spec.get("ordinal_position"),
    )


def _build_database_object(spec: dict, *, source: str, path: str) -> DatabaseObject:
    if not isinstance(spec, dict):
        raise ResolveError(source, path, "database_object must be an object")
    return DatabaseObject(
        schema=require_str(spec, "schema", source=source, path=path),
        name=require_str(spec, "name", source=source, path=path),
        object_type=spec.get("object_type", "table"),
    )


def build_db_read_endpoint(spec: dict, *, source: str) -> DatabaseReadEndpoint:
    endpoint_id = require_str(spec, "endpoint_id", source=source, path="$")
    db_obj_raw = require(spec, "database_object", source=source, path="$")
    cols_raw = require(spec, "columns", source=source, path="$")
    if not isinstance(cols_raw, list):
        raise ResolveError(source, "$.columns", "must be an array")
    columns = tuple(
        _build_column(c, source=source, path=f"$.columns[{i}]")
        for i, c in enumerate(cols_raw)
    )
    pks = spec.get("primary_keys") or []
    if not isinstance(pks, list):
        raise ResolveError(source, "$.primary_keys", "must be an array of strings")
    return DatabaseReadEndpoint(
        endpoint_id=endpoint_id,
        database_object=_build_database_object(db_obj_raw, source=source, path="$.database_object"),
        columns=columns,
        primary_keys=tuple(str(k) for k in pks),
    )


def build_db_write_endpoint(spec: dict, *, source: str) -> DatabaseWriteEndpoint:
    db_read = build_db_read_endpoint(spec, source=source)
    return DatabaseWriteEndpoint(
        endpoint_id=db_read.endpoint_id,
        database_object=db_read.database_object,
        columns=db_read.columns,
        primary_keys=db_read.primary_keys,
    )


# --- API endpoints --------------------------------------------------------


def _build_http_request(spec: dict, *, source: str, path: str) -> HttpRequest:
    if not isinstance(spec, dict):
        raise ResolveError(source, path, "request must be an object")
    return HttpRequest(
        method=require_str(spec, "method", source=source, path=path),
        path=require_str(spec, "path", source=source, path=path),
        query=dict(spec.get("query") or {}),
        headers=dict(spec.get("headers") or {}),
        path_params=dict(spec.get("path_params") or {}),
        body=spec.get("body"),
    )


def _build_param(name: str, spec: dict, *, source: str, path: str) -> ParamSpec:
    if not isinstance(spec, dict):
        raise ResolveError(source, path, f"param {name!r} must be an object")
    return ParamSpec(
        name=name,
        location=require_str(spec, "in", source=source, path=path),
        type=require_str(spec, "type", source=source, path=path),
        required=bool(spec.get("required", False)),
        default=spec.get("default"),
        controlled_by=spec.get("controlled_by"),
        operators=tuple(spec.get("operators") or ()),
        format=spec.get("format"),
    )


def _build_pagination(spec: Any, *, source: str, path: str) -> PaginationSpec | None:
    if spec is None:
        return None
    if not isinstance(spec, dict):
        raise ResolveError(source, path, "pagination must be an object")
    pag_type = require_str(spec, "type", source=source, path=path)
    stop_when_empty = None
    sw = spec.get("stop_when")
    if isinstance(sw, dict):
        empty = sw.get("empty")
        if isinstance(empty, dict):
            stop_when_empty = empty.get("ref")
        elif isinstance(empty, str):
            stop_when_empty = empty

    if pag_type == "offset":
        limit = spec.get("limit") or {}
        offset = spec.get("offset") or {}
        if not isinstance(limit, dict) or not isinstance(offset, dict):
            raise ResolveError(source, path, "offset pagination requires 'limit' and 'offset' objects")
        return OffsetPagination(
            limit_param=require_str(limit, "param", source=source, path=f"{path}.limit"),
            offset_param=require_str(offset, "param", source=source, path=f"{path}.offset"),
            offset_initial=int(offset.get("initial", 0)),
            limit_default=limit.get("default"),
            limit_max=limit.get("max"),
            stop_when_empty_ref=stop_when_empty,
        )
    if pag_type == "cursor":
        cursor = spec.get("cursor") or {}
        if not isinstance(cursor, dict):
            raise ResolveError(source, path, "cursor pagination requires 'cursor' object")
        next_cursor = cursor.get("next_cursor") or {}
        return CursorPagination(
            cursor_param=require_str(cursor, "param", source=source, path=f"{path}.cursor"),
            cursor_response_ref=require_str(next_cursor, "ref", source=source, path=f"{path}.cursor.next_cursor"),
            initial_cursor=cursor.get("initial"),
            stop_when_empty_ref=stop_when_empty,
        )
    if pag_type == "page":
        page = spec.get("page") or {}
        size = spec.get("size") or {}
        if not isinstance(page, dict):
            raise ResolveError(source, path, "page pagination requires 'page' object")
        return PagePagination(
            page_param=require_str(page, "param", source=source, path=f"{path}.page"),
            page_initial=int(page.get("initial", 1)),
            size_param=size.get("param") if isinstance(size, dict) else None,
            size_default=size.get("default") if isinstance(size, dict) else None,
            stop_when_empty_ref=stop_when_empty,
        )
    if pag_type == "keyset":
        keyset = spec.get("keyset") or {}
        if not isinstance(keyset, dict):
            raise ResolveError(source, path, "keyset pagination requires 'keyset' object")
        return KeysetPagination(
            key_field=require_str(keyset, "key_field", source=source, path=f"{path}.keyset"),
            key_param=require_str(keyset, "param", source=source, path=f"{path}.keyset"),
            direction=str(keyset.get("direction", "asc")),
            stop_when_empty_ref=stop_when_empty,
        )
    if pag_type == "link":
        link = spec.get("link") or {}
        if not isinstance(link, dict):
            raise ResolveError(source, path, "link pagination requires 'link' object")
        next_link = link.get("next") or {}
        return LinkPagination(
            next_link_ref=require_str(next_link, "ref", source=source, path=f"{path}.link.next"),
            stop_when_empty_ref=stop_when_empty,
        )
    raise ResolveError(source, f"{path}.type", f"unknown pagination type {pag_type!r}")


def _build_replication(spec: Any, *, source: str, path: str) -> Replication:
    if spec is None:
        return Replication(supported_methods=(), cursor_mappings=())
    if not isinstance(spec, dict):
        raise ResolveError(source, path, "replication must be an object")
    methods = tuple(spec.get("supported_methods") or ())
    mappings_raw = spec.get("cursor_mappings") or ()
    mappings: list[CursorMapping] = []
    for i, m in enumerate(mappings_raw):
        if not isinstance(m, dict):
            raise ResolveError(source, f"{path}.cursor_mappings[{i}]", "must be an object")
        mappings.append(
            CursorMapping(
                cursor_field=require_str(m, "cursor_field", source=source, path=f"{path}.cursor_mappings[{i}]"),
                param=require_str(m, "param", source=source, path=f"{path}.cursor_mappings[{i}]"),
                operator=require_str(m, "operator", source=source, path=f"{path}.cursor_mappings[{i}]"),
                format=m.get("format"),
            )
        )
    return Replication(supported_methods=methods, cursor_mappings=tuple(mappings))


def _build_response(spec: Any, *, source: str, path: str) -> ResponseExtraction:
    if not isinstance(spec, dict):
        raise ResolveError(source, path, "response must be an object")
    records = spec.get("records") or {}
    if not isinstance(records, dict):
        raise ResolveError(source, f"{path}.records", "must be an object")
    return ResponseExtraction(
        records_ref=require_str(records, "ref", source=source, path=f"{path}.records"),
        metadata_ref=(spec.get("metadata") or {}).get("ref") if isinstance(spec.get("metadata"), dict) else None,
        schema=spec.get("schema"),
    )


def build_api_read_endpoint(spec: dict, *, source: str) -> ApiReadEndpoint:
    endpoint_id = require_str(spec, "endpoint_id", source=source, path="$")
    ops = require(spec, "operations", source=source, path="$")
    if not isinstance(ops, dict) or "read" not in ops:
        raise ResolveError(source, "$.operations.read", "missing read operation")
    read = ops["read"]
    request = _build_http_request(require(read, "request", source=source, path="$.operations.read"), source=source, path="$.operations.read.request")
    params_raw = read.get("params") or {}
    params = {
        name: _build_param(name, p, source=source, path=f"$.operations.read.params.{name}")
        for name, p in params_raw.items()
    }
    pagination = _build_pagination(read.get("pagination"), source=source, path="$.operations.read.pagination")
    replication = _build_replication(read.get("replication"), source=source, path="$.operations.read.replication")
    response = _build_response(read.get("response"), source=source, path="$.operations.read.response")
    return ApiReadEndpoint(
        endpoint_id=endpoint_id,
        request=request,
        params=params,
        pagination=pagination,
        replication=replication,
        response=response,
    )


def build_api_write_endpoint(spec: dict, *, source: str, write_mode: str) -> ApiWriteEndpoint:
    endpoint_id = require_str(spec, "endpoint_id", source=source, path="$")
    ops = require(spec, "operations", source=source, path="$")
    if not isinstance(ops, dict):
        raise ResolveError(source, "$.operations", "must be an object")
    write_ops = ops.get("write")
    if not isinstance(write_ops, dict) or write_mode not in write_ops:
        raise ResolveError(source, f"$.operations.write.{write_mode}", "missing write op for this mode")
    w = write_ops[write_mode]
    request = _build_http_request(require(w, "request", source=source, path=f"$.operations.write.{write_mode}"), source=source, path=f"$.operations.write.{write_mode}.request")
    batching_raw = w.get("batching")
    batching = WriteBatching(max_records=batching_raw.get("max_records")) if isinstance(batching_raw, dict) else None
    return ApiWriteEndpoint(
        endpoint_id=endpoint_id,
        write_mode=write_mode,
        request=request,
        input_schema=w.get("input_schema") or w.get("schema"),
        batching=batching,
    )


# Dispatch on connector kind + scope. Determines which endpoint factory runs.

def build_read_endpoint(spec: dict, *, source: str, connector_kind: str) -> ReadEndpoint:
    if connector_kind == "database":
        return build_db_read_endpoint(spec, source=source)
    if connector_kind in ("api", "stdout", "file"):
        return build_api_read_endpoint(spec, source=source)
    raise ResolveError(source, "$", f"unknown connector kind {connector_kind!r}")


def build_write_endpoint(spec: dict, *, source: str, connector_kind: str, write_mode: str) -> WriteEndpoint:
    if connector_kind == "database":
        return build_db_write_endpoint(spec, source=source)
    if connector_kind in ("api", "stdout", "file"):
        return build_api_write_endpoint(spec, source=source, write_mode=write_mode)
    raise ResolveError(source, "$", f"unknown connector kind {connector_kind!r}")


# --- Connector / Connection ----------------------------------------------


def _build_transport(name: str, spec: dict, *, source: str) -> TransportSpec:
    path = f"$.transports.{name}"
    if not isinstance(spec, dict):
        raise ResolveError(source, path, "transport must be an object")
    t_type = require_str(spec, "transport_type", source=source, path=path)
    if t_type == "http":
        return HttpTransport(
            base_url=require(spec, "base_url", source=source, path=path),
            headers=dict(spec.get("headers") or {}),
            timeout_seconds=spec.get("timeout_seconds"),
        )
    if t_type == "sqlalchemy":
        dsn = require(spec, "dsn", source=source, path=path)
        if not isinstance(dsn, dict):
            raise ResolveError(source, f"{path}.dsn", "must be an object")
        return SqlAlchemyTransport(
            driver=require_str(spec, "driver", source=source, path=path),
            dsn_template=require_str(dsn, "template", source=source, path=f"{path}.dsn"),
            dsn_bindings=dict(dsn.get("bindings") or {}),
            tls=spec.get("tls"),
        )
    raise ResolveError(source, f"{path}.transport_type", f"unknown transport_type {t_type!r}")


def build_connector(spec: dict, *, source: str) -> ResolvedConnector:
    transports_raw = require(spec, "transports", source=source, path="$")
    if not isinstance(transports_raw, dict) or not transports_raw:
        raise ResolveError(source, "$.transports", "must be a non-empty object")
    transports = {
        name: _build_transport(name, t, source=source) for name, t in transports_raw.items()
    }
    return ResolvedConnector(
        connector_id=require_str(spec, "connector_id", source=source, path="$"),
        kind=require_str(spec, "kind", source=source, path="$"),
        display_name=require_str(spec, "display_name", source=source, path="$"),
        default_transport=require_str(spec, "default_transport", source=source, path="$"),
        transports=transports,
    )


def build_connection(
    spec: dict,
    *,
    source: str,
    connector: ResolvedConnector,
) -> ResolvedConnection:
    connection_id = require_str(spec, "connection_id", source=source, path="$")
    connector_id = require_str(spec, "connector_id", source=source, path="$")
    if connector_id != connector.connector_id:
        raise ResolveError(
            source,
            "$.connector_id",
            f"connector mismatch: connection says {connector_id!r} but resolved connector is {connector.connector_id!r}",
        )
    return ResolvedConnection(
        connection_id=connection_id,
        connector=connector,
        parameters=dict(spec.get("parameters") or {}),
        selections=dict(spec.get("selections") or {}),
        secret_refs=dict(spec.get("secret_refs") or {}),
    )


# --- Stream / Pipeline ----------------------------------------------------


def _build_replication_config(spec: Any, *, source: str, path: str) -> ReplicationConfig:
    if not isinstance(spec, dict):
        raise ResolveError(source, path, "replication must be an object")
    method = require_str(spec, "method", source=source, path=path)
    if method not in ("full_refresh", "incremental"):
        raise ResolveError(source, f"{path}.method", f"must be 'full_refresh' or 'incremental', got {method!r}")
    cursor_field = spec.get("cursor_field")
    if method == "incremental" and not cursor_field:
        raise ResolveError(source, f"{path}.cursor_field", "incremental replication requires cursor_field")
    tie_breakers_raw = spec.get("tie_breaker_fields")
    tie_breakers: tuple[tuple[str, ...], ...] | None = None
    if tie_breakers_raw is not None:
        tie_breakers = tuple(
            tuple(group) if isinstance(group, list) else (group,) for group in tie_breakers_raw
        )
    return ReplicationConfig(
        method=method,
        cursor_field=cursor_field,
        safety_window_seconds=spec.get("safety_window_seconds"),
        tie_breaker_fields=tie_breakers,
    )


def _build_filters(spec: Any, *, source: str, path: str) -> tuple[Filter, ...]:
    if spec is None:
        return ()
    if not isinstance(spec, list):
        raise ResolveError(source, path, "filters must be an array")
    out: list[Filter] = []
    for i, f in enumerate(spec):
        if not isinstance(f, dict):
            raise ResolveError(source, f"{path}[{i}]", "filter must be an object")
        out.append(
            Filter(
                field=require_str(f, "field", source=source, path=f"{path}[{i}]"),
                op=require_str(f, "op", source=source, path=f"{path}[{i}]"),
                value=f.get("value"),
            )
        )
    return tuple(out)


def _build_write_spec(spec: Any, *, source: str, path: str) -> WriteSpec:
    if not isinstance(spec, dict):
        raise ResolveError(source, path, "write must be an object")
    mode = require_str(spec, "mode", source=source, path=path)
    if mode not in ("insert", "upsert", "update", "truncate_insert"):
        raise ResolveError(source, f"{path}.mode", f"unknown write mode {mode!r}")
    cks_raw = spec.get("conflict_keys")
    if cks_raw is None:
        return WriteSpec(mode=mode, conflict_keys=None)
    if not isinstance(cks_raw, list):
        raise ResolveError(source, f"{path}.conflict_keys", "must be an array of arrays")
    cks = tuple(tuple(g) if isinstance(g, list) else (g,) for g in cks_raw)
    return WriteSpec(mode=mode, conflict_keys=cks)


def _build_execution(spec: Any) -> ExecutionConfig:
    if not isinstance(spec, dict):
        return ExecutionConfig(batch_size=None, max_concurrent_batches=None)
    return ExecutionConfig(
        batch_size=spec.get("batch_size"),
        max_concurrent_batches=spec.get("max_concurrent_batches"),
    )


def _build_mapping(spec: Any, *, source: str, path: str) -> StreamMapping | None:
    if spec is None:
        return None
    if not isinstance(spec, dict):
        raise ResolveError(source, path, "mapping must be an object")
    assignments_raw = spec.get("assignments") or []
    if not isinstance(assignments_raw, list):
        raise ResolveError(source, f"{path}.assignments", "must be an array")
    out: list[Assignment] = []
    for i, a in enumerate(assignments_raw):
        if not isinstance(a, dict):
            raise ResolveError(source, f"{path}.assignments[{i}]", "assignment must be an object")
        target_raw = require(a, "target", source=source, path=f"{path}.assignments[{i}]")
        if not isinstance(target_raw, dict):
            raise ResolveError(source, f"{path}.assignments[{i}].target", "must be an object")
        target = AssignmentTarget(
            path=require_str(target_raw, "path", source=source, path=f"{path}.assignments[{i}].target"),
            arrow_type=require_str(target_raw, "arrow_type", source=source, path=f"{path}.assignments[{i}].target"),
            nullable=bool(target_raw.get("nullable", True)),
        )
        value_raw = require(a, "value", source=source, path=f"{path}.assignments[{i}]")
        if not isinstance(value_raw, dict):
            raise ResolveError(source, f"{path}.assignments[{i}].value", "must be an object")
        if "expression" not in value_raw and "constant" not in value_raw:
            raise ResolveError(
                source,
                f"{path}.assignments[{i}].value",
                "must contain 'expression' or 'constant'",
            )
        out.append(Assignment(target=target, value=dict(value_raw), validate=a.get("validate")))
    return StreamMapping(assignments=tuple(out))


def build_resolved_stream(
    stream_spec: dict,
    *,
    source: str,
    connections: dict[str, ResolvedConnection],
    endpoints: dict[tuple[str, str, str], dict],
) -> ResolvedStream:
    """Build a single resolved stream.

    `connections` is keyed by `connection_id`. `endpoints` is keyed by
    `(scope, connection_id, endpoint_id)` and holds the raw endpoint JSON
    + the path it was loaded from so error messages point at the file.
    """

    stream_id = require_str(stream_spec, "stream_id", source=source, path="$")
    pipeline_id = require_str(stream_spec, "pipeline_id", source=source, path="$")

    src_raw = require(stream_spec, "source", source=source, path="$")
    if not isinstance(src_raw, dict):
        raise ResolveError(source, "$.source", "must be an object")
    src_ref = build_endpoint_ref(require(src_raw, "endpoint_ref", source=source, path="$.source"), source=source, path="$.source.endpoint_ref")
    src_connection = connections.get(src_ref.connection_id)
    if src_connection is None:
        raise ResolveError(source, "$.source.endpoint_ref.connection_id", f"unknown connection {src_ref.connection_id!r}")
    src_endpoint_raw = endpoints.get((src_ref.scope, src_ref.connection_id, src_ref.endpoint_id))
    if src_endpoint_raw is None:
        raise ResolveError(source, "$.source.endpoint_ref", f"endpoint not found: {src_ref}")
    src_endpoint_source = src_endpoint_raw.get("__source__", str(src_ref))
    src_endpoint = build_read_endpoint(
        src_endpoint_raw,
        source=src_endpoint_source,
        connector_kind=src_connection.connector.kind,
    )
    replication = _build_replication_config(require(src_raw, "replication", source=source, path="$.source"), source=source, path="$.source.replication")
    filters = _build_filters(src_raw.get("filters"), source=source, path="$.source.filters")
    pks_raw = src_raw.get("primary_keys")
    if pks_raw is None:
        pks = tuple(src_endpoint.primary_keys) if isinstance(src_endpoint, DatabaseReadEndpoint) else ()
    else:
        if not isinstance(pks_raw, list):
            raise ResolveError(source, "$.source.primary_keys", "must be an array of strings")
        pks = tuple(str(k) for k in pks_raw)
    resolved_source = ResolvedSource(
        connection=src_connection,
        endpoint=src_endpoint,
        endpoint_ref=src_ref,
        replication=replication,
        filters=filters,
        primary_keys=pks,
        parameters=dict(src_raw.get("parameters") or {}),
    )

    dests_raw = require(stream_spec, "destinations", source=source, path="$")
    if not isinstance(dests_raw, list) or not dests_raw:
        raise ResolveError(source, "$.destinations", "must be a non-empty array")
    destinations: list[ResolvedDestination] = []
    for i, d in enumerate(dests_raw):
        if not isinstance(d, dict):
            raise ResolveError(source, f"$.destinations[{i}]", "destination must be an object")
        dref = build_endpoint_ref(require(d, "endpoint_ref", source=source, path=f"$.destinations[{i}]"), source=source, path=f"$.destinations[{i}].endpoint_ref")
        dconn = connections.get(dref.connection_id)
        if dconn is None:
            raise ResolveError(source, f"$.destinations[{i}].endpoint_ref.connection_id", f"unknown connection {dref.connection_id!r}")
        dep_raw = endpoints.get((dref.scope, dref.connection_id, dref.endpoint_id))
        if dep_raw is None:
            raise ResolveError(source, f"$.destinations[{i}].endpoint_ref", f"endpoint not found: {dref}")
        write_spec = _build_write_spec(require(d, "write", source=source, path=f"$.destinations[{i}]"), source=source, path=f"$.destinations[{i}].write")
        dep_source = dep_raw.get("__source__", str(dref))
        dep_endpoint = build_write_endpoint(
            dep_raw,
            source=dep_source,
            connector_kind=dconn.connector.kind,
            write_mode=write_spec.mode,
        )
        destinations.append(
            ResolvedDestination(
                connection=dconn,
                endpoint=dep_endpoint,
                endpoint_ref=dref,
                write=write_spec,
                execution=_build_execution(d.get("execution")),
            )
        )

    return ResolvedStream(
        stream_id=stream_id,
        display_name=stream_spec.get("display_name") or stream_id,
        pipeline_id=pipeline_id,
        status=stream_spec.get("status", "active"),
        source=resolved_source,
        destinations=tuple(destinations),
        mapping=_build_mapping(stream_spec.get("mapping"), source=source, path="$.mapping"),
    )


def _build_schedule(spec: Any, *, source: str, path: str) -> Schedule | None:
    if spec is None:
        return None
    if not isinstance(spec, dict):
        raise ResolveError(source, path, "schedule must be an object")
    return Schedule(
        type=require_str(spec, "type", source=source, path=path),
        cron=spec.get("cron"),
        interval_minutes=spec.get("interval_minutes"),
        timezone=spec.get("timezone"),
    )


def _build_runtime(spec: Any, *, source: str, path: str) -> RuntimeConfig:
    if not isinstance(spec, dict):
        raise ResolveError(source, path, "runtime must be an object")
    batching_raw = spec.get("batching") or {}
    logging_raw = spec.get("logging") or {}
    err_raw = spec.get("error_handling") or {}
    return RuntimeConfig(
        buffer_size=int(spec.get("buffer_size", 1000)),
        batching=BatchingPolicy(
            batch_size=int(batching_raw.get("batch_size", 100)),
            max_concurrent_batches=int(batching_raw.get("max_concurrent_batches", 1)),
        ),
        logging=LoggingPolicy(
            log_level=str(logging_raw.get("log_level", "INFO")),
            metrics_enabled=bool(logging_raw.get("metrics_enabled", False)),
        ),
        error_handling=ErrorHandlingPolicy(
            strategy=str(err_raw.get("strategy", "dlq")),
            max_retries=int(err_raw.get("max_retries", 3)),
            retry_delay_seconds=int(err_raw.get("retry_delay_seconds", err_raw.get("retry_delay", 5))),
        ),
    )


def _build_engine_sizing(spec: Any) -> EngineSizing | None:
    if not isinstance(spec, dict):
        return None
    return EngineSizing(vcpu=spec.get("vcpu"), memory=spec.get("memory"))


def build_resolved_pipeline(
    pipeline_spec: dict,
    *,
    source: str,
    stream_specs: list[tuple[dict, str]],  # (raw_stream, path)
    connections: dict[str, ResolvedConnection],
    endpoints: dict[tuple[str, str, str], dict],
) -> ResolvedPipeline:
    pipeline_id = require_str(pipeline_spec, "pipeline_id", source=source, path="$")
    conns = require(pipeline_spec, "connections", source=source, path="$")
    if not isinstance(conns, dict):
        raise ResolveError(source, "$.connections", "must be an object")
    src_id = require_str(conns, "source", source=source, path="$.connections")
    dest_ids_raw = require(conns, "destinations", source=source, path="$.connections")
    if not isinstance(dest_ids_raw, list) or not dest_ids_raw:
        raise ResolveError(source, "$.connections.destinations", "must be a non-empty array")
    streams = tuple(
        build_resolved_stream(s, source=p, connections=connections, endpoints=endpoints)
        for s, p in stream_specs
    )
    return ResolvedPipeline(
        pipeline_id=pipeline_id,
        display_name=pipeline_spec.get("display_name") or pipeline_id,
        status=pipeline_spec.get("status", "active"),
        schedule=_build_schedule(pipeline_spec.get("schedule"), source=source, path="$.schedule"),
        runtime=_build_runtime(require(pipeline_spec, "runtime", source=source, path="$"), source=source, path="$.runtime"),
        engine=_build_engine_sizing(pipeline_spec.get("engine")),
        streams=streams,
        source_connection_id=src_id,
        destination_connection_ids=tuple(str(d) for d in dest_ids_raw),
    )
