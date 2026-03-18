"""Pydantic models for Stream configuration (STREAM.yaml specification)."""

from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


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


class GenericTypeMapping(BaseModel):
    """Mapping of a field to its generic type representation."""
    model_config = ConfigDict(extra="forbid")

    generic_type: TargetType = Field(
        ...,
        description="Generic type name (string, integer, decimal, boolean, datetime, date, object, array)"
    )


class DestinationTypeMapping(BaseModel):
    """Mapping of a field to its destination-specific type."""
    model_config = ConfigDict(extra="forbid")

    destination_type: str = Field(
        ...,
        description="Native SQL/destination type string (e.g., 'BIGINT', 'VARCHAR(255)')"
    )
    nullable: bool = Field(
        True,
        description="Whether the field is nullable at the destination"
    )

    @field_validator("destination_type")
    @classmethod
    def validate_destination_type(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("destination_type cannot be empty")
        return v.strip()


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


# Expression AST Models

class ExpressionNode(BaseModel):
    """Base expression AST node."""
    model_config = ConfigDict(extra="allow")

    op: ExpressionOp = Field(..., description="Operation type")


class GetExpression(ExpressionNode):
    """Get field value from source record."""
    op: Literal[ExpressionOp.GET] = ExpressionOp.GET
    path: List[str] = Field(..., description="Token array path to field")


class ConstValue(BaseModel):
    """Typed constant value."""
    model_config = ConfigDict(extra="forbid")

    type: str = Field(..., description="Value type (string, integer, object, etc.)")
    value: Any = Field(..., description="The constant value")


class ConstExpression(ExpressionNode):
    """Constant value expression."""
    op: Literal[ExpressionOp.CONST] = ExpressionOp.CONST
    value: Any = Field(..., description="Constant value")


class FnExpression(ExpressionNode):
    """Function call expression."""
    op: Literal[ExpressionOp.FN] = ExpressionOp.FN
    name: str = Field(..., description="Function name from catalog")
    version: int = Field(1, ge=1, description="Function version")
    args: List[Any] = Field(default_factory=list, description="Function arguments")


class PipeExpression(ExpressionNode):
    """Pipeline of expressions (compose left-to-right)."""
    op: Literal[ExpressionOp.PIPE] = ExpressionOp.PIPE
    args: List[Dict[str, Any]] = Field(..., min_length=1, description="Expression nodes to pipe")


class IfExpression(ExpressionNode):
    """Conditional expression."""
    op: Literal[ExpressionOp.IF] = ExpressionOp.IF
    args: List[Dict[str, Any]] = Field(
        ...,
        min_length=3,
        max_length=3,
        description="[condition, then_expr, else_expr]"
    )


class ComparisonExpression(ExpressionNode):
    """Comparison expression (eq, neq, gt, gte, lt, lte)."""
    args: List[Dict[str, Any]] = Field(..., min_length=2, max_length=2, description="Two operands to compare")


# Validation Models

class ValidationRule(BaseModel):
    """Single validation rule."""
    model_config = ConfigDict(extra="allow")

    type: ValidationType = Field(..., description="Validation rule type")
    value: Optional[Any] = Field(None, description="Rule parameter value")
    message: Optional[str] = Field(None, description="Custom error message")


class ValidationConfig(BaseModel):
    """Validation configuration for an assignment."""
    model_config = ConfigDict(extra="forbid")

    rules: List[ValidationRule] = Field(default_factory=list, description="Validation rules")
    on_error: str = Field("dlq", description="Action on validation failure")


# Assignment (Mapping) Models

class AssignmentTarget(BaseModel):
    """Target field specification for an assignment."""
    model_config = ConfigDict(extra="forbid")

    path: List[str] = Field(..., min_length=1, description="Token array path to target field")
    type: TargetType = Field(..., description="Target field type (generic)")
    dest_type: Optional[str] = Field(
        None,
        description="Destination-specific SQL type override (optional)"
    )
    nullable: bool = Field(True, description="Whether null values are allowed")


class AssignmentValue(BaseModel):
    """Value specification for an assignment (const or expr)."""
    model_config = ConfigDict(extra="allow")

    kind: ValueKind = Field(..., description="Value kind (const or expr)")
    const: Optional[ConstValue] = Field(None, description="Constant value (when kind=const)")
    expr: Optional[Dict[str, Any]] = Field(None, description="Expression AST (when kind=expr)")

    @model_validator(mode="after")
    def validate_value_kind(self) -> "AssignmentValue":
        if self.kind == ValueKind.CONST and self.const is None:
            raise ValueError("const value required when kind is 'const'")
        if self.kind == ValueKind.EXPR and self.expr is None:
            raise ValueError("expr required when kind is 'expr'")
        return self


class Assignment(BaseModel):
    """
    Single field assignment rule.

    Maps a target field to either a constant value or an expression result.
    Optionally includes validation rules.
    """
    model_config = ConfigDict(extra="forbid")

    target: AssignmentTarget = Field(..., description="Target field specification")
    value: AssignmentValue = Field(..., description="Value specification")
    validation: Optional[ValidationConfig] = Field(
        None,
        alias="validate",
        description="Validation configuration"
    )


class MappingConfig(BaseModel):
    """
    Complete mapping configuration for a stream.

    Contains an ordered list of assignments that build the destination payload.
    """
    model_config = ConfigDict(extra="forbid")

    assignments: List[Assignment] = Field(default_factory=list, description="Ordered assignment rules")
    # Optional fields (not always present in cloud)
    source_schema_id: Optional[str] = Field(None, description="Source schema identifier")
    target_schema_id: Optional[str] = Field(None, description="Target schema identifier")
    defaults: Optional[Dict[str, Any]] = Field(None, description="Default values for mapping")
    assignments_hash: Optional[str] = Field(None, description="Hash of assignments for cache invalidation")

    source_to_generic: Optional[Dict[str, GenericTypeMapping]] = Field(
        None,
        description="Mapping from source field paths (dot-joined) to generic types"
    )
    generic_to_destination: Optional[Dict[str, Dict[str, DestinationTypeMapping]]] = Field(
        None,
        description="Mapping from generic to destination types, keyed by connection_ref"
    )
    type_mapping_assignments_hash: Optional[str] = Field(
        None,
        description="Hash tracking when type mappings were generated"
    )


# Source/Destination Configuration Models

class ReplicationConfig(BaseModel):
    """Source replication configuration."""
    model_config = ConfigDict(extra="forbid")

    method: ReplicationMethod = Field(ReplicationMethod.INCREMENTAL, description="Replication method")
    cursor_field: List[str] = Field(default_factory=list, description="Token array path to cursor field")
    safety_window_seconds: Optional[int] = Field(None, ge=0, description="Safety window for late-arriving data")
    tie_breaker_fields: Optional[List[List[str]]] = Field(None, description="Tie-breaker field paths")


class SourceConfig(BaseModel):
    """Stream source configuration."""
    model_config = ConfigDict(extra="forbid")

    connection_ref: str = Field(..., description="Connection alias reference from pipeline")
    endpoint_id: str = Field(..., description="Source endpoint UUID")
    primary_key: List[str] = Field(default_factory=list, description="Primary key field names")
    replication: ReplicationConfig = Field(
        default_factory=ReplicationConfig,
        description="Replication configuration"
    )
    # Optional fields (not always present in cloud)
    source_schema_fingerprint: Optional[str] = Field(
        None,
        description="Deterministic hash of source schema (sha256:hex)"
    )
    parameters: Optional[Dict[str, Any]] = Field(
        None,
        description="Connector-specific parameters/filters"
    )


class IdempotencyKeyConfig(BaseModel):
    """Idempotency key configuration for safe retries."""
    model_config = ConfigDict(extra="allow")

    expr: Dict[str, Any] = Field(..., description="Expression AST to compute idempotency key")


class WriteModeConfig(BaseModel):
    """Destination write mode configuration."""
    model_config = ConfigDict(extra="forbid")

    mode: WriteMode = Field(WriteMode.UPSERT, description="Write mode")
    # Optional fields (not always present in cloud)
    conflict_keys: Optional[List[List[str]]] = Field(None, description="Conflict resolution key paths")
    idempotency_key: Optional[IdempotencyKeyConfig] = Field(None, description="Idempotency key for retries")


class DestinationBatchingConfig(BaseModel):
    """Destination batching configuration."""
    model_config = ConfigDict(extra="forbid")

    supported: bool = Field(False, description="Whether batching is supported")
    size: int = Field(1, ge=1, description="Batch size")


class DestinationConfig(BaseModel):
    """Stream destination configuration."""
    model_config = ConfigDict(extra="forbid")

    connection_ref: str = Field(..., description="Connection alias reference from pipeline")
    endpoint_id: str = Field(..., description="Destination endpoint UUID")
    write: WriteModeConfig = Field(default_factory=WriteModeConfig, description="Write mode configuration")
    # Optional fields (not always present in cloud)
    target_schema_fingerprint: Optional[str] = Field(
        None,
        description="Deterministic hash of target schema (sha256:hex)"
    )
    batching: Optional[DestinationBatchingConfig] = Field(
        None,
        description="Batching configuration"
    )


class StreamEngineConfig(BaseModel):
    """Stream-specific engine config overrides."""
    model_config = ConfigDict(extra="allow")

    error_handling: Optional[Dict[str, Any]] = Field(None, description="Error handling overrides")
    rate_limits: Optional[Dict[str, Any]] = Field(None, description="Rate limit overrides")


class StreamConfig(BaseModel):
    """
    Complete Stream configuration model based on STREAM.yaml specification.

    Represents a single data stream within a pipeline, including:
    - Stream metadata (id, status, enabled state)
    - Source configuration (connection, endpoint, replication)
    - Destinations (multi-destination support)
    - Mapping (assignment-based field transformations)
    - Engine config overrides
    """
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    version: int = Field(1, description="Stream configuration version")
    stream_id: str = Field(..., description="Unique stream identifier")
    pipeline_id: str = Field(..., description="Parent pipeline identifier")
    org_id: str = Field("", description="Org identifier")
    status: str = Field("draft", description="Stream status (draft, active, paused, etc.)")
    is_enabled: bool = Field(True, description="Whether the stream is enabled")

    # Source configuration
    source: SourceConfig = Field(..., description="Source configuration")

    # Multi-destination support
    destinations: List[DestinationConfig] = Field(
        ...,
        min_length=1,
        description="Destination configurations (at least one required)"
    )

    # Mapping configuration
    mapping: MappingConfig = Field(default_factory=MappingConfig, description="Field mapping configuration")

    # Optional fields (not always present in cloud)
    tags: Optional[List[str]] = Field(None, description="Stream tags for categorization")
    engine_config: Optional[StreamEngineConfig] = Field(None, description="Stream-specific engine config overrides")

    # Timestamps
    created_at: Optional[str] = Field(None, description="Stream creation timestamp")
    updated_at: Optional[str] = Field(None, description="Stream last update timestamp")

    @field_validator("source")
    @classmethod
    def validate_source(cls, v: SourceConfig) -> SourceConfig:
        """Validate source configuration."""
        if not v.connection_ref:
            raise ValueError("source.connection_ref is required")
        if not v.endpoint_id:
            raise ValueError("source.endpoint_id is required")
        return v

    @field_validator("destinations")
    @classmethod
    def validate_destinations(cls, v: List[DestinationConfig]) -> List[DestinationConfig]:
        """Validate destination configurations."""
        endpoint_ids = set()
        for dest in v:
            if dest.endpoint_id in endpoint_ids:
                raise ValueError(f"Duplicate destination endpoint_id: {dest.endpoint_id}")
            endpoint_ids.add(dest.endpoint_id)
        return v

    def get_primary_destination(self) -> DestinationConfig:
        """Get the primary (first) destination."""
        return self.destinations[0]


class StreamConfigLegacy(BaseModel):
    """
    Legacy stream configuration for backwards compatibility.
    Maps old structure (embedded in pipeline) to new StreamConfig model.
    """
    model_config = ConfigDict(extra="allow")

    # Stream metadata
    name: str = Field(..., description="Stream name")
    description: Optional[str] = Field(None, description="Stream description")

    # Source config (old format)
    source: Dict[str, Any] = Field(..., description="Legacy source configuration")

    # Destination config (old format - single destination)
    destination: Dict[str, Any] = Field(..., description="Legacy destination configuration")

    # Mapping config (old format)
    mapping: Optional[Dict[str, Any]] = Field(None, description="Legacy mapping configuration")

    def to_new_format(
        self,
        stream_id: str,
        pipeline_id: str,
        org_id: str,
        source_connection_ref: str,
        dest_connection_ref: str
    ) -> StreamConfig:
        """Convert legacy stream config to new format."""
        # Build source config
        source_legacy = self.source
        source = SourceConfig(
            connection_ref=source_connection_ref,
            endpoint_id=source_legacy.get("endpoint_id", ""),
            primary_key=source_legacy.get("primary_key", []),
            replication=ReplicationConfig(
                method=ReplicationMethod(source_legacy.get("replication_method", "incremental")),
                cursor_field=[source_legacy["cursor_field"]] if source_legacy.get("cursor_field") else [],
                # cursor_mode removed - always use inclusive
                safety_window_seconds=source_legacy.get("safety_window_seconds", 120),
                tie_breaker_fields=(
                    [[f] for f in source_legacy["tie_breaker_fields"]]
                    if source_legacy.get("tie_breaker_fields")
                    else None
                ),
            ),
        )

        # Build destination config
        dest_legacy = self.destination
        destination = DestinationConfig(
            connection_ref=dest_connection_ref,
            endpoint_id=dest_legacy.get("endpoint_id", ""),
            write=WriteModeConfig(
                mode=WriteMode(dest_legacy.get("refresh_mode", "upsert")),
            ),
            batching=DestinationBatchingConfig(
                supported=dest_legacy.get("batch_support", False),
                size=dest_legacy.get("batch_size", 1),
            ),
        )

        # Convert mapping to new assignment format
        assignments = []
        if self.mapping:
            # Convert field_mappings
            for source_field, field_config in self.mapping.get("field_mappings", {}).items():
                if isinstance(field_config, dict):
                    target_field = field_config.get("target", source_field)
                    transformations = field_config.get("transformations", [])

                    # Build expression
                    path = source_field.split(".")
                    if transformations:
                        # Create pipe expression with transforms
                        pipe_args: List[Dict[str, Any]] = [{"op": "get", "path": path}]
                        for transform in transformations:
                            if isinstance(transform, str):
                                pipe_args.append({"op": "fn", "name": transform, "version": 1, "args": []})
                        expr: Dict[str, Any] = {"op": "pipe", "args": pipe_args}
                    else:
                        expr = {"op": "get", "path": path}

                    # Build validation if present
                    validate = None
                    if field_config.get("validation"):
                        validate = ValidationConfig(
                            rules=[ValidationRule(**r) for r in field_config["validation"].get("rules", [])],
                            on_error=field_config["validation"].get("error_action", "dlq"),
                        )

                    assignments.append(Assignment(
                        target=AssignmentTarget(
                            path=[target_field],
                            type=TargetType.STRING,  # Default, would need schema for accurate type
                            nullable=validate is None,
                        ),
                        value=AssignmentValue(kind=ValueKind.EXPR, expr=expr),
                        validate=validate,
                    ))
                else:
                    # Simple string mapping
                    assignments.append(Assignment(
                        target=AssignmentTarget(path=[field_config], type=TargetType.STRING, nullable=True),
                        value=AssignmentValue(
                            kind=ValueKind.EXPR,
                            expr={"op": "get", "path": source_field.split(".")}
                        ),
                    ))

            # Convert computed_fields
            for field_name, computed_config in self.mapping.get("computed_fields", {}).items():
                if isinstance(computed_config, dict):
                    expr_value = computed_config.get("expression", "")
                    # Try to parse as JSON for object constants
                    try:
                        import json
                        parsed = json.loads(expr_value)
                        const_value = ConstValue(type="object", value=parsed)
                    except (json.JSONDecodeError, TypeError):
                        const_value = ConstValue(type="string", value=expr_value)

                    assignments.append(Assignment(
                        target=AssignmentTarget(path=[field_name], type=TargetType.STRING, nullable=False),
                        value=AssignmentValue(kind=ValueKind.CONST, const=const_value),
                    ))

        mapping = MappingConfig(assignments=assignments)

        return StreamConfig(
            version="1.0",
            stream_id=stream_id,
            pipeline_id=pipeline_id,
            org_id=org_id,
            is_enabled=True,
            tags=[],
            source=source,
            destinations=[destination],
            mapping=mapping,
        )
