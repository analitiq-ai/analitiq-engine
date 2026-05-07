"""Dataclass models for Stream configuration.

Engine-side runtime view of streams, mirroring the published stream
schema at ``https://schemas.analitiq.work/stream/latest.json``.

The canonical artifact identity is the versioned UUID emitted by the
saved-document services. References between artifacts always carry that
versioned UUID; aliases are display-only.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class ReplicationMethod(str, Enum):
    """Replication methods for data sync."""

    FULL_REFRESH = "full_refresh"
    INCREMENTAL = "incremental"


class WriteMode(str, Enum):
    """Destination write modes (database)."""

    INSERT = "insert"
    UPSERT = "upsert"


class TargetType(str, Enum):
    """Target field types for type-safe mapping."""

    STRING = "string"
    INTEGER = "integer"
    DECIMAL = "decimal"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    OBJECT = "object"
    ARRAY = "array"


class ExpressionOp(str, Enum):
    """Stream mapping expression operations.

    The published schema currently allows only ``get``; the broader
    runtime tolerates the historical operator set so existing
    transformation code keeps working until it is migrated.
    """

    GET = "get"


class ValueKind(str, Enum):
    """Value assignment kind."""

    EXPRESSION = "expression"
    CONSTANT = "constant"


class ValidationType(str, Enum):
    """Validation rule types."""

    REQUIRED = "required"
    NOT_NULL = "not_null"
    MIN_LENGTH = "min_length"
    MAX_LENGTH = "max_length"
    PATTERN = "pattern"
    RANGE = "range"
    IN_LIST = "in_list"


def _serialize(obj: Any) -> Any:
    """Recursively serialize dataclass instances, enums, and containers."""
    if isinstance(obj, Enum):
        return obj.value
    if hasattr(obj, "__dataclass_fields__"):
        return {k: _serialize(v) for k, v in asdict(obj).items()}
    if isinstance(obj, list):
        return [_serialize(item) for item in obj]
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    return obj


# ---------------------------------------------------------------------------
# Endpoint reference
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EndpointRef:
    """Structured reference to an endpoint definition.

    ``connection_id`` is a versioned connection UUID
    (``00000000-..._v1``). ``alias`` is the endpoint slug declared inside
    the endpoint document. Resolution rules:

    - ``scope="connector"``: load the connection by ``connection_id``,
      read its ``connector_alias``, then load
      ``connectors/<connector_alias>/definition/endpoints/<alias>.json``.
    - ``scope="connection"``: load the connection by ``connection_id``
      and load
      ``connections/<connection_dir>/definition/endpoints/<alias>.json``.

    Frozen so instances are hashable and usable as dict keys.
    """

    scope: str
    connection_id: str
    alias: str

    _VALID_SCOPES = ("connector", "connection")

    def __post_init__(self) -> None:
        if self.scope not in self._VALID_SCOPES:
            raise ValueError(
                f"EndpointRef.scope must be one of {self._VALID_SCOPES}, got {self.scope!r}"
            )
        if not self.connection_id:
            raise ValueError("EndpointRef.connection_id cannot be empty")
        if not self.alias:
            raise ValueError("EndpointRef.alias cannot be empty")

    def __str__(self) -> str:
        return f"{self.scope}:{self.connection_id}/{self.alias}"

    @classmethod
    def from_dict(cls, data: Any) -> "EndpointRef":
        """Validate and construct from a dict (or pass-through if already typed)."""
        if isinstance(data, EndpointRef):
            return data
        if not isinstance(data, dict):
            raise TypeError(
                "endpoint_ref must be an object with keys "
                "{'scope','connection_id','alias'}, got "
                f"{type(data).__name__}"
            )
        allowed = {"scope", "connection_id", "alias"}
        unknown = set(data.keys()) - allowed - {"x-" + k for k in data.keys() if k.startswith("x-")}
        unknown = {k for k in unknown if not k.startswith("x-")}
        if unknown:
            raise ValueError(
                f"endpoint_ref has unknown keys {sorted(unknown)}; allowed: {sorted(allowed)}"
            )
        missing = allowed - set(data.keys())
        if missing:
            raise ValueError(
                f"endpoint_ref is missing required keys {sorted(missing)}"
            )
        return cls(
            scope=data["scope"],
            connection_id=data["connection_id"],
            alias=data["alias"],
        )

    def to_dict(self) -> Dict[str, str]:
        return {
            "scope": self.scope,
            "connection_id": self.connection_id,
            "alias": self.alias,
        }


# ---------------------------------------------------------------------------
# Mapping primitives
# ---------------------------------------------------------------------------


@dataclass
class GetExpression:
    """``{"op": "get", "path": "..."}`` source-field reference."""

    op: ExpressionOp = ExpressionOp.GET
    path: str = ""


@dataclass
class ConstantValue:
    """Typed constant assignment."""

    arrow_type: str = "Utf8"
    value: Any = None


@dataclass
class AssignmentTarget:
    """Target field specification for a stream mapping assignment."""

    path: str = ""
    arrow_type: str = "Utf8"
    native_type: Optional[str] = None
    nullable: bool = True


@dataclass
class AssignmentValue:
    """Value specification (exactly one of ``expression`` or ``constant``)."""

    expression: Optional[Dict[str, Any]] = None
    constant: Optional[ConstantValue] = None


@dataclass
class ValidationRule:
    """Single record-validation rule."""

    type: ValidationType = ValidationType.NOT_NULL
    field: str = ""
    value: Any = None
    message: Optional[str] = None


@dataclass
class ValidationConfig:
    """Validation block (assignments).rules / ``validate`` block."""

    rules: List[ValidationRule] = field(default_factory=list)
    error_handling: Optional[Dict[str, Any]] = None


@dataclass
class Assignment:
    """Single field assignment rule."""

    target: AssignmentTarget = field(default_factory=AssignmentTarget)
    value: AssignmentValue = field(default_factory=AssignmentValue)
    validate: Optional[ValidationConfig] = None

    def model_dump(self) -> Dict[str, Any]:
        return _serialize(self)


@dataclass
class MappingConfig:
    """Stream mapping configuration."""

    assignments: List[Assignment] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Source / destination
# ---------------------------------------------------------------------------


@dataclass
class ReplicationConfig:
    """Source replication policy."""

    method: ReplicationMethod = ReplicationMethod.FULL_REFRESH
    cursor_field: Optional[str] = None
    safety_window_seconds: Optional[int] = None
    tie_breaker_fields: Optional[List[str]] = None


@dataclass
class StreamFilter:
    """Stream-supplied read predicate."""

    field: str = ""
    operator: str = "eq"
    value: Any = None


@dataclass
class DatabasePagination:
    """Database read-page configuration (offset or keyset)."""

    type: str = "offset"
    page_size: Optional[int] = None
    order_by_field: Optional[str] = None


@dataclass
class SourceConfig:
    """Stream source configuration."""

    endpoint_ref: Optional[EndpointRef] = None
    selected_columns: Optional[List[str]] = None
    filters: List[StreamFilter] = field(default_factory=list)
    replication: ReplicationConfig = field(default_factory=ReplicationConfig)
    database_pagination: Optional[DatabasePagination] = None
    primary_keys: Optional[List[str]] = None


@dataclass
class WriteConfig:
    """Destination write behavior."""

    mode: WriteMode = WriteMode.INSERT
    conflict_keys: Optional[List[List[str]]] = None


@dataclass
class ExecutionConfig:
    """Per-stream destination execution overrides."""

    batch_size: Optional[int] = None
    max_concurrent_batches: Optional[int] = None


@dataclass
class DestinationConfig:
    """Stream destination configuration."""

    endpoint_ref: Optional[EndpointRef] = None
    write: WriteConfig = field(default_factory=WriteConfig)
    execution: Optional[ExecutionConfig] = None


@dataclass
class StreamConfig:
    """Complete Stream configuration model.

    Mirrors the persisted stream document. Server-managed fields
    (``stream_id``, ``version``, ``stream_schema_version``, ``org_id``,
    ``created_at``, ``updated_at``) are present in the persisted document
    and exposed here for convenience; engine code uses them only for
    identity, idempotency, and observability.
    """

    stream_id: str = ""
    version: int = 1
    stream_schema_version: int = 1
    pipeline_id: str = ""
    alias: str = ""
    display_name: Optional[str] = None
    description: Optional[str] = None
    status: str = "draft"
    tags: Optional[List[str]] = None
    org_id: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    source: SourceConfig = field(default_factory=SourceConfig)
    destinations: List[DestinationConfig] = field(default_factory=list)
    mapping: MappingConfig = field(default_factory=MappingConfig)

    def get_primary_destination(self) -> DestinationConfig:
        if not self.destinations:
            raise ValueError(
                f"Stream {self.stream_id!r} has no destinations configured"
            )
        return self.destinations[0]
