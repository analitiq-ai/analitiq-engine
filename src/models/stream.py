"""Dataclass models for Stream configuration (STREAM.yaml specification)."""

from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Dict, List, Optional


class ReplicationMethod(str, Enum):
    """Replication methods for data sync."""
    INCREMENTAL = "incremental"
    FULL = "full"


class CursorMode(str, Enum):
    """Cursor boundary semantics."""
    INCLUSIVE = "inclusive"
    EXCLUSIVE = "exclusive"


class WriteMode(str, Enum):
    """Destination write modes."""
    INSERT = "insert"
    UPSERT = "upsert"
    UPDATE = "update"


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
    """Expression AST operation types."""
    GET = "get"
    CONST = "const"
    PIPE = "pipe"
    FN = "fn"
    IF = "if"
    EQ = "eq"
    NEQ = "neq"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
    AND = "and"
    OR = "or"
    NOT = "not"
    IN = "in"
    CONCAT = "concat"
    COALESCE = "coalesce"


class ValueKind(str, Enum):
    """Value assignment kind."""
    CONST = "const"
    EXPR = "expr"


class ValidationType(str, Enum):
    """Validation rule types."""
    NOT_NULL = "not_null"
    REQUIRED = "required"
    MIN_LENGTH = "min_length"
    MAX_LENGTH = "max_length"
    PATTERN = "pattern"
    RANGE = "range"
    IN_LIST = "in_list"


def _serialize(obj: Any) -> Any:
    """Recursively serialize dataclass instances, enums, and containers to plain dicts/values."""
    if isinstance(obj, Enum):
        return obj.value
    if hasattr(obj, "__dataclass_fields__"):
        return {k: _serialize(v) for k, v in asdict(obj).items()}
    if isinstance(obj, list):
        return [_serialize(item) for item in obj]
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    return obj


@dataclass
class GenericTypeMapping:
    """Mapping of a field to its generic type representation."""
    generic_type: TargetType


@dataclass
class DestinationTypeMapping:
    """Mapping of a field to its destination-specific type."""
    destination_type: str
    nullable: bool = True


# Expression AST Models

@dataclass
class ExpressionNode:
    """Base expression AST node."""
    op: ExpressionOp


@dataclass
class GetExpression(ExpressionNode):
    """Get field value from source record."""
    path: List[str] = field(default_factory=list)


@dataclass
class ConstValue:
    """Typed constant value."""
    type: str
    value: Any = None


@dataclass
class ConstExpression(ExpressionNode):
    """Constant value expression."""
    value: Any = None


@dataclass
class FnExpression(ExpressionNode):
    """Function call expression."""
    name: str = ""
    version: int = 1
    args: List[Any] = field(default_factory=list)


@dataclass
class PipeExpression(ExpressionNode):
    """Pipeline of expressions (compose left-to-right)."""
    args: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class IfExpression(ExpressionNode):
    """Conditional expression."""
    args: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class ComparisonExpression(ExpressionNode):
    """Comparison expression (eq, neq, gt, gte, lt, lte)."""
    args: List[Dict[str, Any]] = field(default_factory=list)


# Validation Models

@dataclass
class ValidationRule:
    """Single validation rule."""
    type: ValidationType
    value: Any = None
    message: Optional[str] = None


@dataclass
class ValidationConfig:
    """Validation configuration for an assignment."""
    rules: List[ValidationRule] = field(default_factory=list)
    on_error: str = "dlq"


# Assignment (Mapping) Models

@dataclass
class AssignmentTarget:
    """Target field specification for an assignment."""
    path: List[str] = field(default_factory=list)
    type: TargetType = TargetType.STRING
    dest_type: Optional[str] = None
    nullable: bool = True


@dataclass
class AssignmentValue:
    """Value specification for an assignment (const or expr)."""
    kind: ValueKind = ValueKind.EXPR
    const: Optional[ConstValue] = None
    expr: Optional[Dict[str, Any]] = None


@dataclass
class Assignment:
    """Single field assignment rule."""
    target: AssignmentTarget = field(default_factory=AssignmentTarget)
    value: AssignmentValue = field(default_factory=AssignmentValue)
    validation: Optional[ValidationConfig] = None

    def model_dump(self) -> Dict[str, Any]:
        """Serialize to dict. Uses 'validate' key for validation (matches data_transformer expectations)."""
        result = _serialize(self)
        # Rename 'validation' -> 'validate' for backward compat with data_transformer
        val = result.pop("validation", None)
        if val is not None:
            result["validate"] = val
        return result


@dataclass
class MappingConfig:
    """Complete mapping configuration for a stream."""
    assignments: List[Assignment] = field(default_factory=list)
    source_schema_id: Optional[str] = None
    target_schema_id: Optional[str] = None
    defaults: Optional[Dict[str, Any]] = None
    assignments_hash: Optional[str] = None
    source_to_generic: Optional[Dict[str, GenericTypeMapping]] = None
    generic_to_destination: Optional[Dict[str, Dict[str, DestinationTypeMapping]]] = None
    type_mapping_assignments_hash: Optional[str] = None


# Source/Destination Configuration Models

@dataclass
class ReplicationConfig:
    """Source replication configuration."""
    method: ReplicationMethod = ReplicationMethod.INCREMENTAL
    cursor_field: List[str] = field(default_factory=list)
    safety_window_seconds: Optional[int] = None
    tie_breaker_fields: Optional[List[List[str]]] = None


@dataclass
class SourceConfig:
    """Stream source configuration."""
    connection_ref: str = ""
    endpoint_id: str = ""
    primary_key: List[str] = field(default_factory=list)
    replication: ReplicationConfig = field(default_factory=ReplicationConfig)
    source_schema_fingerprint: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None


@dataclass
class IdempotencyKeyConfig:
    """Idempotency key configuration for safe retries."""
    expr: Dict[str, Any] = field(default_factory=dict)


@dataclass
class WriteModeConfig:
    """Destination write mode configuration."""
    mode: WriteMode = WriteMode.UPSERT
    conflict_keys: Optional[List[List[str]]] = None
    idempotency_key: Optional[IdempotencyKeyConfig] = None


@dataclass
class DestinationBatchingConfig:
    """Destination batching configuration."""
    supported: bool = False
    size: int = 1


@dataclass
class DestinationConfig:
    """Stream destination configuration."""
    connection_ref: str = ""
    endpoint_id: str = ""
    write: WriteModeConfig = field(default_factory=WriteModeConfig)
    target_schema_fingerprint: Optional[str] = None
    batching: Optional[DestinationBatchingConfig] = None


@dataclass
class StreamEngineConfig:
    """Stream-specific engine config overrides."""
    error_handling: Optional[Dict[str, Any]] = None
    rate_limits: Optional[Dict[str, Any]] = None


@dataclass
class StreamConfig:
    """Complete Stream configuration model based on STREAM.yaml specification."""
    stream_id: str = ""
    pipeline_id: str = ""
    source: SourceConfig = field(default_factory=SourceConfig)
    destinations: List[DestinationConfig] = field(default_factory=list)
    version: int = 1
    status: str = "draft"
    is_enabled: bool = True
    mapping: MappingConfig = field(default_factory=MappingConfig)
    tags: Optional[List[str]] = None
    runtime: Optional[StreamEngineConfig] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    def get_primary_destination(self) -> DestinationConfig:
        """Get the primary (first) destination."""
        return self.destinations[0]
