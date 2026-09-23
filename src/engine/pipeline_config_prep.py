"""Pipeline configuration preparation.

Loads pipeline, stream, connection, connector, and endpoint artifacts
from the on-disk modular layout and assembles the in-memory config the
runtime consumes.

Layout (rooted at the project containing ``pipelines/manifest.json``):

    pipelines/manifest.json
    pipelines/<pipeline_id>/pipeline.json
    pipelines/<pipeline_id>/streams/<stream_id>.json
    connections/<connection_id>/connection.json
    connections/<connection_id>/.secrets/credentials.json
    connections/<connection_id>/definition/endpoints/<endpoint_id>.json
        (private endpoints)
    connections/<connection_id>/definition/type-map.json (optional)
    connectors/<connector_id>/definition/connector.json
    connectors/<connector_id>/definition/type-map.json
    connectors/<connector_id>/definition/endpoints/<endpoint_id>.json (public endpoints)

Identity is ``*_id`` throughout. Cross-document references carry the id
that matches the on-disk directory name:

    pipeline.connections.source       -> "<connection_id>"
    pipeline.connections.destinations -> ["<connection_id>", ...]
    pipeline.streams                  -> ["<stream_id>", ...]
    stream.pipeline_id                -> "<pipeline_id>"
    stream.source.endpoint_ref        -> {scope, connection_id, endpoint_id}
    stream.destinations[].endpoint_ref-> {scope, connection_id, endpoint_id}
        (connection-scoped refs carry database_object; endpoint_id is
         server-derived from it)

Before anything is built, the pipeline and the packages it references are
read into one workspace request and gated on the published validator's
verdict (:mod:`src.config.run_workspace`). The engine checks no document
itself: its typed models are parsed from the texts that verdict passed.
"""

from __future__ import annotations

import json
import logging
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from analitiq.contracts.connection import ConnectionInput
from analitiq.contracts.connection_package import ConnectionPackage
from analitiq.contracts.connector import Connector
from analitiq.contracts.connector_package import ConnectorPackage
from analitiq.contracts.pipeline_package import PipelinePackage
from analitiq.contracts.pipelines.config import PipelineInput
from analitiq.contracts.pipelines.config import Runtime as ContractRuntime
from analitiq.contracts.stream import (
    ApiStreamDestination,
    DatabaseStreamDestination,
    EndpointRef,
    IncrementalReplication,
    StreamInput,
    StreamSource,
)
from pydantic import TypeAdapter

from cdk.connection_runtime import ConnectionRuntime
from cdk.declarations import parse_declared_concurrency, parse_declared_error_map
from cdk.secrets import SchemeSecretsResolver, SecretsResolver
from cdk.sql.capabilities import parse_declared_capabilities
from cdk.type_map import TypeMapper
from cdk.type_map.loader import parse_type_mapper
from src.config import settings
from src.config.endpoint_resolver import (
    ConnectionLookup,
    endpoint_ref_label,
    resolve_endpoint_path,
)
from src.config.run_workspace import RunWorkspace, gate_run, read_run_workspace
from src.config.utils import author_set, load_json_file
from src.engine.mapping import MappingDocument
from src.models.resolved import (
    BatchingConfig,
    EndpointDocument,
    ErrorHandlingConfig,
    LoggingConfig,
    PipelineConnections,
    ReplicationConfig,
    ResolvedDestination,
    ResolvedPipeline,
    ResolvedSource,
    ResolvedStream,
    RuntimeConfig,
    dump_authored,
)

logger = logging.getLogger(__name__)

#: A stream side as the contract declares it: the source block, or one of the
#: kind-tagged destination blocks. Each carries the ``endpoint_ref`` the side
#: resolves through.
_StreamSide = StreamSource | ApiStreamDestination | DatabaseStreamDestination

# The two endpoint models each pin their own ``$schema`` URL, so the union
# selects the document's model without a kind table.
_CONNECTOR_DOCUMENT: TypeAdapter[Connector] = TypeAdapter(Connector)
_ENDPOINT_DOCUMENT: TypeAdapter[EndpointDocument] = TypeAdapter(EndpointDocument)


def _parse_runtime_config(contract: ContractRuntime) -> RuntimeConfig:
    """Build the engine's :class:`RuntimeConfig` from a pipeline's runtime block.

    Reads the validated contract model and forwards only the fields the author
    explicitly set; every omitted key falls through to the engine's own default
    (sourced from :mod:`src.config.settings`, env-overridable). The engine
    deliberately keeps its own defaults -- e.g. error strategy ``fail`` -- rather
    than the contract's (``dlq``), so it must forward author-set values only, not
    the contract's defaults. Precedence: pipeline config > env var > engine default.
    """
    batching = contract.batching
    error_handling = contract.error_handling
    # ``metrics_enabled`` is deliberately not read: the engine emits its run
    # metrics unconditionally, so honouring the flag would mean dropping the
    # record of a run that failed.
    logging_block = contract.logging
    return RuntimeConfig(
        batching=BatchingConfig(**author_set(batching, batch_size=batching.batch_size)),
        error_handling=ErrorHandlingConfig(
            **author_set(
                error_handling,
                strategy=error_handling.strategy,
                max_retries=error_handling.max_retries,
                retry_delay_seconds=error_handling.retry_delay_seconds,
            )
        ),
        logging=LoggingConfig(
            **author_set(logging_block, log_level=logging_block.log_level)
        ),
        **author_set(contract, buffer_size=contract.buffer_size),
    )


def _parse_replication(source: StreamSource) -> ReplicationConfig | None:
    """Build the engine's :class:`ReplicationConfig` from a stream's source block.

    Returns ``None`` when no replication policy is present (full-refresh sources
    may omit it). ``method`` selects the variant of the contract's
    method-discriminated replication union, where ``cursor_field`` exists on the
    incremental branch only -- the full-refresh branch forbids it, so the engine
    reads ``None`` for it there. ``tie_breaker_fields`` is shared by both and
    carries through as ``None`` when absent (the engine has no settings default
    for these, so no author-intent filtering is needed).
    """
    contract = source.replication
    if contract is None:
        return None
    return ReplicationConfig(
        method=contract.method,
        cursor_field=(
            contract.cursor_field
            if isinstance(contract, IncrementalReplication)
            else None
        ),
        tie_breaker_fields=contract.tie_breaker_fields,
    )


# A stream reference in the pipeline's ``streams`` list may carry a trailing
# version suffix (e.g. ``{uuid}_v2``). The stream document's own ``stream_id``
# is always bare, so the suffix is split off for the index lookup and the
# integer rides onto every emitted checkpoint line. The engine never acts on
# the version; it is metadata the deployment uses to scope the durable cursor.
_STREAM_VERSION_RE = re.compile(r"_v(\d+)$")


def _split_stream_ref(ref: str) -> tuple[str, int]:
    """Split a stream reference into ``(bare_stream_id, version)``.

    A bare reference (no ``_v{n}`` suffix) is version 1: a stream that was
    never edited, or any locally hand-authored config with no versioning
    concept. Resolution and cursor keying use the bare id regardless.
    """
    match = _STREAM_VERSION_RE.search(ref)
    if not match:
        return ref, 1
    return ref[: match.start()], int(match.group(1))


# ---------------------------------------------------------------------------
# Dataclasses for internal state
# ---------------------------------------------------------------------------


@dataclass
class _ConnectionRecord:
    """One entry in the on-disk connection index, keyed by ``connection_id``."""

    connection_id: str  # directory name under connections/
    connector_id: str
    document: ConnectionInput


@dataclass
class _StreamRecord:
    """One entry in the on-disk stream index, keyed by ``stream_id``."""

    stream_id: str
    document: StreamInput


# ---------------------------------------------------------------------------
# PipelineConfigPrep
# ---------------------------------------------------------------------------


class PipelineConfigPrep:
    """Loads, validates, and assembles a pipeline's runtime configuration."""

    def __init__(self) -> None:
        """Resolve project paths and the PIPELINE_ID this run executes."""
        self._paths = self._discover_paths()

        self.pipeline_id_input = os.getenv("PIPELINE_ID", "")
        if not self.pipeline_id_input:
            raise RuntimeError("PIPELINE_ID environment variable is required")

        # The run's workspace, read and gated in create_config().
        self._workspace: RunWorkspace | None = None

        # Indexes built once per create_config() call, keyed by id.
        self._connection_records: dict[str, _ConnectionRecord] = {}  # by connection_id
        self._stream_records: dict[str, _StreamRecord] = {}  # by stream_id

        # Resolved artifacts
        self._resolved_connections: dict[
            str, ConnectionRuntime
        ] = {}  # by connection_id
        self._resolved_endpoints: dict[EndpointRef, EndpointDocument] = {}
        self._loaded_connectors: dict[str, Connector] = {}  # by connector_id
        self._connector_type_mappers: dict[str, TypeMapper | None] = {}
        self._connection_type_mappers: dict[str, TypeMapper | None] = {}

        logger.info(
            "PipelineConfigPrep initialized: PIPELINE_ID=%s, paths=%s",
            self.pipeline_id_input,
            {k: str(v) for k, v in self._paths.items()},
        )

    # ------------------------------------------------------------------
    # Project layout
    # ------------------------------------------------------------------

    @staticmethod
    def _discover_paths() -> dict[str, Path]:
        """Walk up from CWD until ``pipelines/manifest.json`` is found."""
        current = Path.cwd()
        for _ in range(10):
            candidate = current / "pipelines" / "manifest.json"
            if candidate.exists():
                return {
                    "root": current,
                    "manifest": candidate,
                    "connectors": current / "connectors",
                    "connections": current / "connections",
                    "pipelines": current / "pipelines",
                }
            if current.parent == current:
                break
            current = current.parent
        raise RuntimeError(
            "Could not find pipelines/manifest.json in current or parent "
            "directories. Run from the project root."
        )

    # ------------------------------------------------------------------
    # Manifest + the run's workspace
    # ------------------------------------------------------------------

    def _pipeline_directory(self) -> str:
        """Locate the workspace directory of the pipeline PIPELINE_ID names.

        The manifest only locates the pipeline; whether it may run is the
        verdict's call, from the pipeline document's own ``status``. The
        manifest is read before the verdict grades it, so it is read
        leniently: a manifest that does not parse, or locates nothing for
        PIPELINE_ID, refuses the run here; every other defect in it is the
        verdict's to report.
        """
        manifest_path = self._paths["manifest"]
        if not manifest_path.is_file():
            raise FileNotFoundError(f"Pipeline manifest not found: {manifest_path}")
        manifest = load_json_file(manifest_path)
        entries = manifest.get("pipelines") if isinstance(manifest, Mapping) else None
        for entry in entries if isinstance(entries, list) else []:
            if (
                isinstance(entry, Mapping)
                and entry.get("pipeline_id") == self.pipeline_id_input
                and isinstance(entry.get("path"), str)
            ):
                path = self._paths["pipelines"] / entry["path"]
                return f"{path.parent.relative_to(self._paths['root']).as_posix()}/"
        raise ValueError(
            f"Pipeline id {self.pipeline_id_input!r} is not located by "
            f"{manifest_path}"
        )

    def _gated_workspace(self) -> RunWorkspace:
        if self._workspace is None:
            raise RuntimeError("The run's workspace is read in create_config()")
        return self._workspace

    # ------------------------------------------------------------------
    # Typed models, parsed from the texts the verdict passed
    # ------------------------------------------------------------------

    def _index_connections(self) -> None:
        workspace = self._gated_workspace()
        self._connection_records.clear()
        for directory_id, directory in workspace.connection_directories.items():
            key = directory + ConnectionPackage.ROOT
            document = ConnectionInput.model_validate_json(workspace.text(key))
            self._connection_records[directory_id] = _ConnectionRecord(
                connection_id=directory_id,
                connector_id=document.connector_id,
                document=document,
            )
        logger.info("Indexed %d connection(s)", len(self._connection_records))

    def _index_streams(self) -> None:
        """Index every stream of the pipeline's package by its bare ``stream_id``.

        The verdict refuses a stream with no ``stream_id`` and two streams
        sharing one, so each passed document has exactly one index entry.
        """
        workspace = self._gated_workspace()
        self._stream_records.clear()
        for text in workspace.texts(workspace.pipeline_directory, "stream").values():
            document = StreamInput.model_validate_json(text)
            if document.stream_id:
                # Key by the version-stripped base id so the index shares one
                # key space with pipeline.streams lookup (which strips ``_v{n}``).
                self._stream_records[
                    _split_stream_ref(document.stream_id)[0]
                ] = _StreamRecord(stream_id=document.stream_id, document=document)
        logger.info("Indexed %d stream(s)", len(self._stream_records))

    def _connection_lookup(self) -> ConnectionLookup:
        return ConnectionLookup(
            directory_by_id={
                cid: rec.connection_id for cid, rec in self._connection_records.items()
            },
            connector_id_by_id={
                cid: rec.connector_id for cid, rec in self._connection_records.items()
            },
        )

    def _type_mapper(self, directory: str, label: str) -> TypeMapper | None:
        """Return the package's type map, or ``None`` when it carries none."""
        maps = self._gated_workspace().texts(directory, "type-map")
        if not maps:
            logger.info("No type-map for %s", label)
            return None
        ((key, text),) = maps.items()
        return parse_type_mapper(label, json.loads(text), source=key)

    # ------------------------------------------------------------------
    # Connector + connection materialization (in-memory only)
    # ------------------------------------------------------------------

    def _load_connector(self, connector_id: str) -> Connector:
        if connector_id in self._loaded_connectors:
            return self._loaded_connectors[connector_id]
        workspace = self._gated_workspace()
        directory = workspace.connector_directories[connector_id]
        document = _CONNECTOR_DOCUMENT.validate_json(
            workspace.text(directory + ConnectorPackage.ROOT)
        )
        self._loaded_connectors[connector_id] = document
        self._connector_type_mappers[connector_id] = self._type_mapper(
            directory, connector_id
        )
        return document

    def _connection_type_mapper(self, connection_id: str) -> TypeMapper | None:
        if connection_id not in self._connection_type_mappers:
            directory = self._gated_workspace().connection_directories[connection_id]
            self._connection_type_mappers[connection_id] = self._type_mapper(
                directory, f"connection:{connection_id}"
            )
        return self._connection_type_mappers[connection_id]

    def _create_secrets_resolver(self, directory: str) -> SecretsResolver:
        connection_dir = self._paths["connections"] / directory
        return SchemeSecretsResolver(
            connection_dir,
            s3_endpoint_url=settings.s3_secrets_endpoint_url(),
            s3_region=settings.s3_secrets_region(),
        )

    def _resolve_connection_by_id(self, connection_id: str) -> ConnectionRuntime:
        """Materialize (or return cached) ConnectionRuntime for a ``connection_id``."""
        record = self._connection_records.get(connection_id)
        if record is None:
            raise ValueError(
                f"Connection id {connection_id!r} is not present under "
                f"{self._paths['connections']}; "
                f"known: {sorted(self._connection_records)}"
            )
        if connection_id in self._resolved_connections:
            return self._resolved_connections[connection_id]

        connector = self._load_connector(record.connector_id)
        # kind is a closed-enum discriminator validated by the connector
        # contract in _load_connector; whether that kind is runnable is the
        # worker registry's job (ConnectorNotRegisteredError). Config prep
        # neither re-checks the shape nor hard-codes a kind set.
        runtime = ConnectionRuntime(
            connection=record.document,
            connection_id=connection_id,
            connector_id=record.connector_id,
            connector_type=connector.kind.value,
            resolver=self._create_secrets_resolver(connection_id),
            connector=connector,
            connector_type_mapper=self._connector_type_mappers.get(record.connector_id),
            connection_type_mapper=self._connection_type_mapper(connection_id),
        )
        # Parse the declared blocks (sql_capabilities, issue #390; error_map
        # and concurrency, issue #401) on the trusted side, at config load: a
        # malformed declaration fails here as a config error, never inside a
        # spawned worker where a dead pre-serve process would surface as a
        # connect failure instead. None (no block) is legal; needed-but-
        # undeclared facts refuse at their consumer sites.
        source = f"connector {record.connector_id!r}"
        parse_declared_capabilities(runtime.declared_sql_capabilities, source=source)
        parse_declared_error_map(runtime.declared_error_map, source=source)
        parse_declared_concurrency(runtime.declared_concurrency, source=source)
        self._resolved_connections[connection_id] = runtime
        logger.info(
            "Resolved connection: connection_id=%s connector=%s",
            connection_id,
            record.connector_id,
        )
        return runtime

    # ------------------------------------------------------------------
    # Endpoint resolution
    # ------------------------------------------------------------------

    def _resolve_endpoint(self, ref: EndpointRef) -> EndpointDocument:
        """Resolve one endpoint reference to its typed contract document."""
        if ref in self._resolved_endpoints:
            return self._resolved_endpoints[ref]
        # A contract model's own str dumps every field; error text and logs
        # name the endpoint by its on-disk handle instead.
        label = endpoint_ref_label(ref)
        path = resolve_endpoint_path(ref, self._paths, self._connection_lookup())
        key = path.relative_to(self._paths["root"]).as_posix()
        model = _ENDPOINT_DOCUMENT.validate_json(self._gated_workspace().text(key))
        self._resolved_endpoints[ref] = model
        logger.info("Resolved endpoint: %s", label)
        return model

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def create_config(
        self,
    ) -> tuple[
        ResolvedPipeline,
        list[ResolvedStream],
        dict[str, ConnectionRuntime],
        dict[EndpointRef, EndpointDocument],
        list[Connector],
    ]:
        """Load and return the validated, resolved pipeline configuration.

        Returns a tuple of:

        * ``pipeline``: :class:`ResolvedPipeline` with pipeline-level config.
        * ``streams``: list of :class:`ResolvedStream` with typed
          source/destinations — ``ConnectionRuntime`` and the resolved
          endpoint document (a typed contract model) live as explicit
          fields, not dict keys.
        * ``resolved_connections``: dict keyed by ``connection_id`` of
          :class:`ConnectionRuntime` (one per saved connection used by
          the pipeline).
        * ``resolved_endpoints``: dict keyed by :class:`EndpointRef` of
          typed endpoint documents.
        * ``connectors``: list of connector documents loaded.
        """
        workspace = read_run_workspace(self._paths, self._pipeline_directory())
        gate_run(workspace)
        self._workspace = workspace

        pipeline_doc = PipelineInput.model_validate_json(
            workspace.text(workspace.pipeline_directory + PipelinePackage.ROOT)
        )
        # The verdict requires it (RULE-PIPE-018); ResolvedPipeline refuses an
        # empty one, so the cast narrows the type without a second check.
        pipeline_id = cast(str, pipeline_doc.pipeline_id)

        source_id = pipeline_doc.connections.source
        # The pipeline contract requires >= 1 destination, so dest_ids is
        # non-empty here.
        dest_ids = list(pipeline_doc.connections.destinations)

        self._index_connections()
        self._index_streams()

        self._resolve_connection_by_id(source_id)
        for dest_id in dest_ids:
            self._resolve_connection_by_id(dest_id)

        # Stream configs. A reference may carry a ``_v{n}`` version suffix; the
        # bare id resolves the stream record (referential soundness is already
        # guaranteed by the verdict) and the version rides onto the emitted
        # checkpoint line.
        stream_configs: list[ResolvedStream] = [
            self._build_stream_config(self._stream_records[bare_id], stream_version)
            for bare_id, stream_version in (
                _split_stream_ref(ref) for ref in pipeline_doc.streams
            )
        ]
        display_name = pipeline_doc.display_name
        pipeline = ResolvedPipeline(
            pipeline_id=pipeline_id,
            name=display_name or pipeline_id,
            display_name=display_name,
            connections=PipelineConnections(
                source=source_id,
                destinations=dest_ids,
            ),
            runtime=_parse_runtime_config(pipeline_doc.runtime),
        )

        connectors = list(self._loaded_connectors.values())
        logger.info(
            "Configuration assembled: pipeline=%s, streams=%d, connections=%d, "
            "endpoints=%d, connectors=%d",
            pipeline_id,
            len(stream_configs),
            len(self._resolved_connections),
            len(self._resolved_endpoints),
            len(connectors),
        )
        return (
            pipeline,
            stream_configs,
            dict(self._resolved_connections),
            dict(self._resolved_endpoints),
            connectors,
        )

    # ------------------------------------------------------------------
    # Stream config construction
    # ------------------------------------------------------------------

    def _resolve_endpoint_block(
        self, block: _StreamSide
    ) -> tuple[EndpointRef, ConnectionRuntime, EndpointDocument]:
        """Resolve one stream side's ``endpoint_ref`` into its parts.

        The ref is contract-validated with the stream and referentially
        checked by the workspace verdict; this resolves it to the connection
        runtime and the endpoint document it points at.
        """
        endpoint_ref = block.endpoint_ref
        runtime = self._resolve_connection_by_id(endpoint_ref.connection_id)
        endpoint = self._resolve_endpoint(endpoint_ref)
        return endpoint_ref, runtime, endpoint

    def _build_stream_config(
        self, record: _StreamRecord, stream_version: int
    ) -> ResolvedStream:
        """Translate a saved stream document into a typed :class:`ResolvedStream`."""
        document = record.document
        stream_id = record.stream_id

        # ---- source ----
        source = document.source
        (
            source_endpoint_ref,
            source_runtime,
            source_endpoint,
        ) = self._resolve_endpoint_block(source)

        resolved_source = ResolvedSource(
            endpoint_ref=source_endpoint_ref,
            connection_ref=source_runtime.connection_id,
            runtime=source_runtime,
            endpoint_document=source_endpoint,
            stream_source=source,
            replication=_parse_replication(source),
            primary_keys=list(source.primary_keys or []),
        )

        # ---- destinations ----
        resolved_destinations: list[ResolvedDestination] = []
        for destination in document.destinations:
            (
                dest_endpoint_ref,
                dest_runtime,
                dest_endpoint,
            ) = self._resolve_endpoint_block(destination)

            resolved_destinations.append(
                ResolvedDestination(
                    endpoint_ref=dest_endpoint_ref,
                    connection_ref=dest_runtime.connection_id,
                    runtime=dest_runtime,
                    endpoint_document=dest_endpoint,
                    write=destination.write,
                )
            )

        # The mapping crosses as the authored document: the engine's
        # MappingDocument is its own reading of the contract's mapping
        # grammar (see src.engine.mapping), parsed from the authored JSON.
        mapping = document.mapping
        return ResolvedStream(
            stream_id=stream_id,
            stream_version=stream_version,
            source=resolved_source,
            destinations=resolved_destinations,
            mapping=MappingDocument.parse(
                dump_authored(mapping) if mapping is not None else {}
            ),
        )

    # ------------------------------------------------------------------
    # Convenience accessors
    # ------------------------------------------------------------------

    def get_resolved_connection(self, connection_id: str) -> ConnectionRuntime:
        if connection_id not in self._resolved_connections:
            raise KeyError(
                f"Connection {connection_id!r} not resolved; "
                f"known: {sorted(self._resolved_connections)}"
            )
        return self._resolved_connections[connection_id]

    def get_connectors(self) -> list[Connector]:
        return list(self._loaded_connectors.values())

    def get_connector_for_connection(self, connection_id: str) -> Connector:
        record = self._connection_records.get(connection_id)
        if record is None:
            raise KeyError(
                f"Connection id {connection_id!r} not indexed; "
                f"known: {sorted(self._connection_records)}"
            )
        return self._loaded_connectors[record.connector_id]
