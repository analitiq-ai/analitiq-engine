"""Typed resolved-runtime boundary between PipelineConfigPrep and the engine.

These dataclasses own all raw-JSON access.  ``PipelineConfigPrep`` builds
them once; ``StreamingEngine`` consumes typed attributes throughout (via the
translation helpers in ``src.runner``), and only at the ``WorkerReadable``
boundary are the contract documents serialised back to JSON-safe dicts
(:func:`dump_endpoint_document`).

``ConnectionRuntime`` and the resolved endpoint document (a typed contract
model) live as explicit typed fields rather than ``_runtime`` /
``_endpoint`` magic dict keys.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Annotated, Any, get_args, get_origin

from analitiq.contracts.pipelines.config import ErrorHandling as ContractErrorHandling
from analitiq.contracts.stream import Replication
from pydantic import BaseModel

from cdk.connection_runtime import ConnectionRuntime
from src.config import settings
from src.config.schema_validator import EndpointDocument
from src.engine.mapping import MappingDocument
from src.models.state import ReplicationConfig as StateReplicationConfig
from src.models.stream import EndpointRef


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


def dump_endpoint_document(document: EndpointDocument) -> dict[str, Any]:
    """Serialise a typed endpoint document back to its authored JSON shape.

    This is the one dump used everywhere a contract endpoint document
    crosses the engine boundary (worker bootstrap, CDK handlers).
    ``by_alias`` restores contract field names (``$schema``, ``schema``);
    ``exclude_unset`` keeps fields the author omitted out of the payload,
    so the dumped document round-trips the authored one instead of baking
    the model's defaults into the wire shape.
    """
    return document.model_dump(mode="json", by_alias=True, exclude_unset=True)


def _contract_literals(model: type[BaseModel], field_name: str) -> frozenset[str]:
    """Read one contract model field's ``Literal`` vocabulary.

    Restating a contract enum in engine code is how a contract-valid document
    starts being rejected: the contract gains a value, the copy does not, and
    nothing fails until an author hits it. Reading the annotation keeps one
    source.

    Every shape this cannot read raises here, naming the model and the field.
    These run at import, so the alternative to a loud failure is an engine that
    starts up and rejects documents the contract permits.
    """
    fields = getattr(model, "model_fields", None)
    if fields is None or field_name not in fields:
        raise RuntimeError(
            f"{model!r} does not declare a {field_name!r} field; the contract "
            "changed shape and this reader must follow it"
        )
    values = get_args(fields[field_name].annotation)
    if not values or not all(isinstance(value, str) for value in values):
        raise RuntimeError(
            f"{model.__name__}.{field_name} is not a Literal of strings; the "
            "contract changed shape and this reader must follow it"
        )
    return frozenset(values)


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
        *(_contract_literals(variant, field_name) for variant in variants)
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
    the source-read policy (parsed from ``stream_source``); the raw
    ``stream_source`` document still travels to the connector unchanged.
    """

    endpoint_ref: EndpointRef
    connection_ref: str
    runtime: ConnectionRuntime
    endpoint_document: EndpointDocument
    stream_source: dict[str, Any]
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
            "endpoint_ref": self.endpoint_ref.to_dict(),
            "connection_ref": self.connection_ref,
            "endpoint_document": dump_endpoint_document(self.endpoint_document),
            "stream_source": with_effective_safety_window(self.stream_source),
        }


@dataclass
class ResolvedDestination:
    """Destination side of a resolved stream — runtime object and contract docs."""

    endpoint_ref: EndpointRef
    connection_ref: str
    runtime: ConnectionRuntime
    endpoint_document: EndpointDocument
    write: dict[str, Any]


@dataclass
class ResolvedStream:
    """Fully resolved stream — typed source/destinations and metadata."""

    stream_id: str
    stream_version: int
    pipeline_id: str | None
    display_name: str | None
    description: str | None
    status: str
    is_enabled: bool
    tags: list[str]
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
_VALID_ERROR_STRATEGIES = _contract_literals(ContractErrorHandling, "strategy")


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
class RuntimeConfig:
    """Pipeline runtime tuning.

    ``batching`` / ``error_handling`` are typed sub-configs (closed, known
    key sets) so consumers read attributes instead of ``dict.get(...)`` with
    per-call-site defaults -- the defaults live once, in
    :mod:`src.config.settings`.
    """

    batching: BatchingConfig = field(default_factory=BatchingConfig)
    error_handling: ErrorHandlingConfig = field(default_factory=ErrorHandlingConfig)
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

    ``schedule`` and ``engine_config`` stay raw dicts on purpose: they are
    opaque control-plane passthroughs (scheduler hint, vCPU/memory sizing)
    that the engine never reads, so there is no structure to type.
    """

    pipeline_id: str
    name: str
    display_name: str | None
    description: str | None
    status: str
    connections: PipelineConnections
    tags: list[str] = field(default_factory=list)
    schedule: dict[str, Any] = field(default_factory=dict)
    engine_config: dict[str, Any] = field(default_factory=dict)
    runtime: RuntimeConfig = field(default_factory=RuntimeConfig)

    def __post_init__(self) -> None:
        if not self.pipeline_id:
            raise ValueError("ResolvedPipeline.pipeline_id cannot be empty")
