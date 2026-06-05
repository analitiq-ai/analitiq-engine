"""Pipeline management and configuration.

This module bridges the contract-shaped output of
:class:`PipelineConfigPrep` to the runtime-side ``StreamingEngine`` and
its source/destination connectors. The engine and connectors consume a
flat per-stream config dict; this module is the single seam where the
structured contract documents are translated to that shape.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

from .engine import StreamingEngine
from ..models.stream import EndpointRef
from ..models.resolved import ResolvedPipeline, ResolvedStream
from cdk.connection_runtime import ConnectionRuntime

logger = logging.getLogger(__name__)


class Pipeline:
    """Compose the typed resolved config and start the streaming engine."""

    def __init__(
        self,
        pipeline_config: ResolvedPipeline,
        stream_configs: Optional[List[ResolvedStream]] = None,
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

        pipeline_id = pipeline_config.pipeline_id
        project_root = Path(__file__).parent.parent.parent
        self.state_dir = state_dir or str(project_root / "state")
        self.dlq_dir = str(project_root / "deadletter" / pipeline_id)
        self._ensure_directories()

        runtime = pipeline_config.runtime
        batching = runtime.get("batching") or {"batch_size": 1000, "max_concurrent_batches": 3}
        error_handling = runtime.get("error_handling") or {
            "max_retries": 3,
            "retry_delay_seconds": 5,
        }
        self.engine = StreamingEngine(
            pipeline_id=pipeline_id,
            batch_size=batching.get("batch_size", 1000),
            max_concurrent_batches=batching.get("max_concurrent_batches", 3),
            buffer_size=runtime.get("buffer_size", 5000),
            dlq_path=self.dlq_dir,
            max_retries=error_handling.get("max_retries", 3),
            retry_delay=error_handling.get("retry_delay_seconds", 5),
        )

    def _ensure_directories(self) -> None:
        Path(self.state_dir).mkdir(parents=True, exist_ok=True)
        Path(self.dlq_dir).mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Contract -> connector config translation
    # ------------------------------------------------------------------

    def _build_config_dict(self) -> Dict[str, Any]:
        streams: Dict[str, Dict[str, Any]] = {}

        for stream in self.stream_configs:
            if not stream.destinations:
                raise ValueError(
                    f"Stream {stream.stream_id!r} has no destinations"
                )
            dest = stream.primary_destination()

            # Source connectors run in this process and need the runtime
            # handle + resolved endpoint document. Destination handlers
            # run in the destination container and load both for
            # themselves via PipelineConfigPrep + set_stream_endpoints, so
            # only the engine-facing summary (write mode) is built here.
            source_config = _translate_source_config(
                stream=stream,
                source=stream.source,
                endpoint=stream.source.endpoint_document,
                runtime=stream.source.runtime,
            )
            dest_config = _build_destination_config(dest)

            mapping = stream.mapping or {}
            mapping_config = {
                "assignments": [
                    _translate_assignment(a)
                    for a in (mapping.get("assignments") or [])
                ]
            }

            streams[stream.stream_id] = {
                "name": stream.stream_id,
                "source": source_config,
                "destination": dest_config,
                "mapping": mapping_config,
            }

        return {
            "pipeline_id": self.pipeline_config.pipeline_id,
            "name": self.pipeline_config.display_name or self.pipeline_config.pipeline_id,
            "streams": streams,
            "runtime": self.pipeline_config.runtime,
        }

    async def run(self) -> None:
        config_dict = self._build_config_dict()
        try:
            await self.engine.stream_data(config_dict)
            logger.info(
                "Pipeline %s completed successfully",
                self.pipeline_config.pipeline_id,
            )
        except Exception:
            logger.exception(
                "Pipeline %s failed", self.pipeline_config.pipeline_id
            )
            raise

    def get_metrics(self) -> Dict[str, Any]:
        return self.engine.get_metrics()


# ---------------------------------------------------------------------------
# Translation helpers
# ---------------------------------------------------------------------------


def _translate_source_config(
    *,
    stream: ResolvedStream,
    source: "ResolvedSource",
    endpoint: Dict[str, Any],
    runtime: ConnectionRuntime,
) -> Dict[str, Any]:
    """Attach the contract documents to the source-side runtime payload.

    The connectors (API + database) read replication, filters, columns,
    pagination, etc. directly off the contract ``endpoint_document`` and
    ``stream_source`` dicts. The translator only injects the runtime
    handle and the connector type discriminator so the engine knows which
    connector class to instantiate.
    """
    _ = stream  # signature parity with the destination translator
    kind = runtime.connector_type
    base: Dict[str, Any] = {
        "connector_type": kind,
        "_resolved_source": source,
        "endpoint_ref": source.endpoint_ref.to_dict(),
        "connection_ref": source.connection_ref,
    }
    if kind == "database":
        base.update(_translate_database_source(source, endpoint))
    elif kind == "api":
        base.update(_translate_api_source(source, endpoint, runtime))
    else:
        raise ValueError(
            f"Unsupported source connector kind: {kind!r}; expected 'api' or 'database'"
        )
    return base


def _build_destination_config(
    destination: "ResolvedDestination",
) -> Dict[str, Any]:
    """Engine-facing destination dict.

    The engine only needs the write mode (forwarded to the gRPC
    ``SchemaMessage``). Everything else about the destination — table
    name, columns, primary keys, conflict keys, batching — is consumed
    by the destination container, which loads the contract endpoint
    document via :class:`PipelineConfigPrep`.
    """
    return {"write_mode": destination.write.get("mode", "upsert")}


def _translate_database_source(
    source: "ResolvedSource", endpoint: Dict[str, Any]
) -> Dict[str, Any]:
    """Pass the contract documents through to :class:`GenericSQLConnector`.

    The connector consumes ``database_object``, ``columns``,
    ``primary_keys``, plus the stream's ``selected_columns``, ``filters``,
    and ``replication`` block directly. This seam only attaches the
    documents to the source config dict.
    """
    return {
        "endpoint_document": endpoint,
        "stream_source": source.stream_source,
    }


def _translate_api_source(
    source: "ResolvedSource",
    endpoint: Dict[str, Any],
    runtime: ConnectionRuntime,
) -> Dict[str, Any]:
    """Pass the contract documents through to :class:`APIConnector`.

    The connector consumes ``operations.read.{request,params,pagination,
    response,replication}`` directly and resolves value expressions
    against the connection runtime; this seam only attaches the
    documents to the source config dict.
    """
    _ = runtime  # signature parity with the database path
    return {
        "endpoint_document": endpoint,
        "stream_source": source.stream_source,
        "stream_filters": list(source.stream_source.get("filters") or []),
    }


# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Mapping translation (contract assignment shape -> AssignmentTransformer shape)
# ---------------------------------------------------------------------------


def _translate_assignment(assignment: Dict[str, Any]) -> Dict[str, Any]:
    """Translate a contract-shaped assignment to the transformer's shape.

    The contract authors targets with a dotted ``path`` string and an
    Apache Arrow type label. The transformer's expected shape uses a list
    path and a tagged ``value.kind`` (``"expr"`` or ``"const"``); this
    function only reshapes those structural differences. It does NOT map
    Arrow types to anything else — native ↔ Arrow translation is
    connector/connection-owned (via each artifact's ``type-map-read.json``)
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


