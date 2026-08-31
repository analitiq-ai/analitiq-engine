"""Typed resolved-runtime boundary between PipelineConfigPrep and the engine.

These dataclasses own all raw-JSON access.  ``PipelineConfigPrep`` builds
them once; ``StreamingEngine`` consumes typed attributes throughout (via the
translation helpers in ``src.runner``), and only at the ``WorkerReadable``
boundary are the contract documents serialised back to JSON-safe dicts
(:func:`dump_endpoint_document`).

``ConnectionRuntime`` and the contract documents (endpoint document, stream
source, destination write block) live as explicit typed fields rather than
``_runtime`` / ``_endpoint`` magic dict keys. :func:`dump_authored` is where
the engine serialises a document again (the connection runtime's worker
payload is the other): the worker bootstrap and the published bundle
validator parse it back as the same contract model, and the stream mapping
is parsed by the engine's own ``MappingDocument``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Annotated, Any, get_args, get_origin

from analitiq.contracts.connection import ConnectionInput
from analitiq.contracts.pipelines.config import ErrorHandling as ContractErrorHandling
from analitiq.contracts.pipelines.config import PipelineInput
from analitiq.contracts.stream import (
    ApiWrite,
    DatabaseConflictKeyedWrite,
    DatabaseKeylessWrite,
    EndpointRef,
    Replication,
    StreamInput,
    StreamMapping,
    StreamSource,
)

from cdk.connection_runtime import ConnectionRuntime
from src.config import settings
from src.config.schema_validator import EndpointDocument
from src.engine.mapping import MappingDocument
from src.models.state import ReplicationConfig as StateReplicationConfig
from src.shared.contract_literals import contract_literals
from src.shared.logging_level import require_log_level


def with_effective_safety_window(stream_source: dict[str, Any]) -> dict[str, Any]:
    """Return *stream_source* with the safety window filled on an incremental read.

    The safety window is operational policy: it is how far back a stored
    cursor is rewound to cover clock skew and late-arriving rows, and a
    connector never declares it. Filling it here rather than in a connector
    keeps one owner for the number and lets the connector treat an absent
    value as the wiring defect it is, instead of inventing a default of its
    own — which is how the engine came to hold three copies of it.
    """
    replication = stream_source.get("replication")
    if not isinstance(replication, dict):
        return stream_source
    if replication.get("method") != "incremental":
        return stream_source
    if replication.get("safety_window_seconds") is not None:
        return stream_source
    filled = dict(stream_source)
    filled["replication"] = {
        **replication,
        "safety_window_seconds": StateReplicationConfig.safety_window_seconds,
    }
    return filled


#: The authored documents serialised again for a reader that parses them
#: back: the endpoint document and stream source the worker bootstrap
#: carries to the connector, the run bundle the published validator checks,
#: and the stream mapping the engine's own
#: :class:`~src.engine.mapping.MappingDocument` reads (its assignment
#: targets, constants and validation rules are the contract's models,
#: parsed from the authored JSON).
AuthoredDocument = (
    EndpointDocument
    | StreamSource
    | PipelineInput
    | StreamInput
    | ConnectionInput
    | StreamMapping
)


def dump_authored(document: AuthoredDocument) -> dict[str, Any]:
    """Serialise an authored document for a reader that parses it back.

    The one dump for every document that crosses a boundary whole. The
    reader parses the JSON back -- into the same contract model, or, for the
    mapping, into the engine's ``MappingDocument`` reading of that grammar --
    so ``by_alias`` restores the contract's field names (``$schema``,
    ``schema``) and ``exclude_unset`` keeps the author's omissions omitted,
    never baking the model's defaults into the wire shape.
    """
    return document.model_dump(mode="json", by_alias=True, exclude_unset=True)


def dump_endpoint_document(document: EndpointDocument) -> dict[str, Any]:
    """Serialise a typed endpoint document back to its authored JSON shape."""
    return dump_authored(document)


def dump_endpoint_ref(ref: EndpointRef) -> dict[str, Any]:
    """Serialise an endpoint reference back to its authored JSON shape.

    ``by_alias`` restores the contract's wire names (``database_object.schema``
    is the ``schema_`` attribute). ``exclude_none`` rather than
    ``exclude_unset``: a connection ref's ``endpoint_id`` is derived by the
    contract validator, not authored, so ``exclude_unset`` would drop the very
    handle the worker resolves the endpoint by -- while the optional locator
    fields must stay out, because the contract rejects an explicit null there.
    """
    return ref.model_dump(mode="json", by_alias=True, exclude_none=True)


def _variant_literals(annotation: Any, field_name: str) -> frozenset[str]:
    """Read *field_name*'s vocabulary across every variant of a union annotation.

    Accepts the union bare or wrapped in ``Annotated`` (the contract's
    discriminated unions carry a ``Field(discriminator=...)``); the wrapper is
    stripped explicitly rather than by unpacking ``get_args``, so an annotation
    that stops being a union reaches the error below instead of failing on a
    bare unpack that names neither the contract nor the cause.
    """
    if get_origin(annotation) is Annotated:
        annotation = get_args(annotation)[0]
    variants = get_args(annotation)
    if not variants:
        raise RuntimeError(
            f"{annotation!r} is no longer a union of contract variants; this "
            "reader must follow it"
        )
    return frozenset().union(
        *(contract_literals(variant, field_name) for variant in variants)
    )


_VALID_REPLICATION_METHODS = _variant_literals(Replication, "method")


@dataclass(frozen=True)
class ReplicationConfig:
    """Source replication policy, typed against the published stream contract.

    ``safety_window_seconds`` is intentionally not carried: it travels to the
    connector inside the ``stream_source`` wire document (see
    :func:`with_effective_safety_window`) and no engine decision reads it, so
    there is nothing to type here.
    """

    method: str
    cursor_field: str | None = None
    tie_breaker_fields: list[str] | None = None

    def __post_init__(self) -> None:
        if self.method not in _VALID_REPLICATION_METHODS:
            raise ValueError(
                f"Unknown replication method {self.method!r}; "
                f"expected one of {sorted(_VALID_REPLICATION_METHODS)}"
            )
        # The contract carries cursor_field as a string on its incremental
        # replication variant and forbids it on full_refresh, so this engine
        # view holds a string or None. Fail loud at this boundary if anything
        # else slips through (e.g. a legacy list), rather than letting it
        # reach compute_max_cursor as an opaque TypeError.
        if self.cursor_field is not None and not isinstance(self.cursor_field, str):
            raise ValueError(
                "cursor_field must be a string or None; the contract forbids a "
                f"list, got {type(self.cursor_field).__name__}"
            )


@dataclass
class ResolvedSource:
    """Source side of a resolved stream — runtime object and contract docs.

    ``replication`` and ``primary_keys`` are the engine-internal typed view of
    the source-read policy (read off ``stream_source``); the contract
    ``stream_source`` block still travels to the connector whole.
    """

    endpoint_ref: EndpointRef
    connection_ref: str
    runtime: ConnectionRuntime
    endpoint_document: EndpointDocument
    stream_source: StreamSource
    replication: ReplicationConfig | None = None
    primary_keys: list[str] = field(default_factory=list)

    def to_source_config(self) -> dict[str, Any]:
        """JSON-safe source config dict for the worker bootstrap.

        Returns only the contract documents (the endpoint document dumped
        back to its authored JSON shape); the ``ConnectionRuntime`` object
        travels as a separate argument to ``build_bootstrap`` and is never
        embedded in the JSON payload.
        """
        return {
            "endpoint_ref": dump_endpoint_ref(self.endpoint_ref),
            "connection_ref": self.connection_ref,
            "endpoint_document": dump_endpoint_document(self.endpoint_document),
            "stream_source": with_effective_safety_window(
                dump_authored(self.stream_source)
            ),
        }


#: A destination's authored ``write`` block, in every shape the stream
#: contract declares it: the API shape, and the database shapes with and
#: without conflict keys.
DestinationWrite = ApiWrite | DatabaseKeylessWrite | DatabaseConflictKeyedWrite


def write_conflict_keys(write: DestinationWrite) -> list[str]:
    """Return the conflict keys a write block authors; empty for every other shape.

    Only a database upsert carries them (the contract requires them there and
    forbids them elsewhere); an API upsert takes its natural key from the
    endpoint document instead, so it answers empty here.
    """
    if isinstance(write, DatabaseConflictKeyedWrite):
        return list(write.conflict_keys)
    return []


@dataclass
class ResolvedDestination:
    """Destination side of a resolved stream — runtime object and contract docs."""

    endpoint_ref: EndpointRef
    connection_ref: str
    runtime: ConnectionRuntime
    endpoint_document: EndpointDocument
    write: DestinationWrite


@dataclass
class ResolvedStream:
    """Fully resolved stream — typed source/destinations and mapping.

    Carries what the run reads and nothing else: a stream's display name,
    description, status and tags are authoring metadata the engine never
    acts on (the bundle validator requires an active pipeline with at least
    one active stream; per-stream status is not acted on by the engine).
    """

    stream_id: str
    stream_version: int
    source: ResolvedSource
    destinations: list[ResolvedDestination]
    mapping: MappingDocument

    def __post_init__(self) -> None:
        if not self.stream_id:
            raise ValueError("ResolvedStream.stream_id cannot be empty")

    def primary_destination(self) -> ResolvedDestination:
        if not self.destinations:
            raise ValueError(f"Stream {self.stream_id!r} has no destinations")
        return self.destinations[0]


@dataclass(frozen=True)
class BatchingConfig:
    """Batch sizing for the engine's producer/consumer loop.

    Defaults come from :mod:`src.config.settings` (env-overridable) and apply
    only when a pipeline's ``runtime.batching`` block omits the key.
    """

    batch_size: int = field(default_factory=settings.default_batch_size)

    def __post_init__(self) -> None:
        if self.batch_size <= 0:
            raise ValueError(f"batch_size must be positive, got {self.batch_size}")


# The published pipeline contract's error-handling strategy enum, read from the
# contract rather than narrowed to what the engine branches on, so a
# contract-valid pipeline is never rejected at this boundary. The engine's own
# *default* is separate and deliberately differs from the contract's.
_VALID_ERROR_STRATEGIES = contract_literals(ContractErrorHandling, "strategy")


@dataclass(frozen=True)
class ErrorHandlingConfig:
    """Fault-handling policy for a pipeline run.

    Defaults come from :mod:`src.config.settings` (env-overridable) and apply
    only when a pipeline's ``runtime.error_handling`` block omits the key.
    """

    strategy: str = field(default_factory=settings.default_error_strategy)
    max_retries: int = field(default_factory=settings.default_max_retries)
    retry_delay_seconds: int = field(
        default_factory=settings.default_retry_delay_seconds
    )

    def __post_init__(self) -> None:
        if self.strategy not in _VALID_ERROR_STRATEGIES:
            raise ValueError(
                f"Unknown error strategy {self.strategy!r}; "
                f"expected one of {sorted(_VALID_ERROR_STRATEGIES)}"
            )
        if self.max_retries < 0:
            raise ValueError(
                f"max_retries must be non-negative, got {self.max_retries}"
            )
        if self.retry_delay_seconds < 0:
            raise ValueError(
                "retry_delay_seconds must be non-negative, got "
                f"{self.retry_delay_seconds}"
            )


@dataclass(frozen=True)
class LoggingConfig:
    """Verbosity for a pipeline run.

    The default comes from :mod:`src.config.settings` (``LOG_LEVEL``) and
    applies only when a pipeline's ``runtime.logging`` block omits the key, so
    logging keeps the runtime block's one precedence rule rather than becoming
    a special case: pipeline config > env var > engine default.

    The contract's ``metrics_enabled`` is deliberately not carried, and the
    engine reads it nowhere. Metrics are the run's own accounting that the
    control plane reads, so a pipeline document able to switch them off would
    produce runs indistinguishable from unrecorded ones; disabling them is a
    deployment decision, never a tenant-authored one.
    """

    log_level: str = field(default_factory=settings.log_level)

    def __post_init__(self) -> None:
        # The same check the process start applies to ``LOG_LEVEL``: one
        # vocabulary, whichever end the value came from.
        require_log_level(self.log_level)


@dataclass(frozen=True)
class RuntimeConfig:
    """Pipeline runtime tuning.

    ``batching`` / ``error_handling`` / ``logging`` are typed sub-configs
    (closed, known key sets) so consumers read attributes instead of
    ``dict.get(...)`` with per-call-site defaults -- the defaults live once, in
    :mod:`src.config.settings`.
    """

    batching: BatchingConfig = field(default_factory=BatchingConfig)
    error_handling: ErrorHandlingConfig = field(default_factory=ErrorHandlingConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    buffer_size: int = field(default_factory=settings.default_buffer_size)

    def __post_init__(self) -> None:
        if self.buffer_size <= 0:
            raise ValueError(f"buffer_size must be positive, got {self.buffer_size}")


@dataclass(frozen=True)
class PipelineConnections:
    """Connection-id wiring: source connection and destination connections."""

    source: str
    destinations: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.source:
            raise ValueError("PipelineConnections.source cannot be empty")


@dataclass
class ResolvedPipeline:
    """Resolved pipeline-level configuration.

    Carries what the run reads and nothing else. A pipeline's schedule and
    engine sizing are control-plane facts (scheduler hint, vCPU/memory) the
    engine never acts on, and its description, status and tags are authoring
    metadata: none of them is carried, so none is read.
    """

    pipeline_id: str
    name: str
    display_name: str | None
    connections: PipelineConnections
    runtime: RuntimeConfig = field(default_factory=RuntimeConfig)

    def __post_init__(self) -> None:
        if not self.pipeline_id:
            raise ValueError("ResolvedPipeline.pipeline_id cannot be empty")
