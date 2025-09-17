"""Pydantic models for transformation definitions."""

from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, field_validator


class TransformationType(str, Enum):
    """Available transformation types."""

    # String transformations
    STRIP = "strip"
    UPPER = "upper"
    LOWER = "lower"
    TRUNCATE = "truncate"
    REGEX_REPLACE = "regex_replace"

    # Numeric transformations
    ABS = "abs"
    TO_INT = "to_int"
    TO_FLOAT = "to_float"
    NUMBER_FORMAT = "number_format"

    # Boolean transformations
    BOOLEAN = "boolean"

    # Date/Time transformations
    ISO_TO_DATE = "iso_to_date"
    ISO_TO_DATETIME = "iso_to_datetime"
    ISO_TO_TIMESTAMP = "iso_to_timestamp"
    ISO_STRING_TO_DATETIME = "iso_string_to_datetime"
    NOW = "now"

    # Utility transformations
    UUID = "uuid"
    MD5 = "md5"
    COALESCE = "coalesce"


class TransformationConfig(BaseModel):
    """Configuration for a single transformation."""
    
    type: TransformationType = Field(
        ...,
        description="Type of transformation to apply"
    )
    
    parameters: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Parameters for the transformation"
    )


class FieldMappingConfig(BaseModel):
    """Configuration for mapping a source field to target field."""
    
    target: str = Field(
        ...,
        description="Target field name in destination"
    )
    
    transformations: Optional[List[Union[str, TransformationConfig]]] = Field(
        default_factory=list,
        description="List of transformations to apply to the field value"
    )
    
    validation: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Validation rules for the transformed value"
    )
    
    @field_validator('transformations', mode='before')
    @classmethod
    def normalize_transformations(cls, v):
        """Normalize transformation definitions to consistent format."""
        if not v:
            return []

        normalized = []
        for transform in v:
            if isinstance(transform, str):
                # Convert string to TransformationConfig
                try:
                    transform_type = TransformationType(transform)
                    normalized.append(TransformationConfig(type=transform_type))
                except ValueError:
                    raise ValueError(f"Unknown transformation type: {transform}")
            elif isinstance(transform, dict):
                # Convert dict to TransformationConfig
                normalized.append(TransformationConfig(**transform))
            elif isinstance(transform, TransformationConfig):
                normalized.append(transform)
            else:
                raise ValueError(f"Invalid transformation format: {transform}")

        return normalized


class ComputedFieldConfig(BaseModel):
    """Configuration for computed fields."""
    
    expression: str = Field(
        ...,
        description="Expression to compute the field value"
    )
    
    validation: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Validation rules for the computed value"
    )


class MappingConfig(BaseModel):
    """Configuration for field mappings and transformations."""
    
    field_mappings: Dict[str, FieldMappingConfig] = Field(
        default_factory=dict,
        description="Mapping from source fields to target fields with transformations"
    )
    
    computed_fields: Dict[str, ComputedFieldConfig] = Field(
        default_factory=dict,
        description="Computed fields with expressions"
    )
    
    default_error_action: str = Field(
        default="fail",
        description="Default action when transformation/validation fails",
        pattern="^(fail|skip|dlq)$"
    )
    
    class Config:
        """Pydantic model configuration."""
        extra = "forbid"
        validate_assignment = True


class TransformationResult(BaseModel):
    """Result of applying transformations to a record."""
    
    success: bool = Field(
        ...,
        description="Whether all transformations succeeded"
    )
    
    transformed_record: Dict[str, Any] = Field(
        default_factory=dict,
        description="The transformed record"
    )
    
    errors: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="List of transformation/validation errors"
    )
    
    skipped_fields: List[str] = Field(
        default_factory=list,
        description="Fields that were skipped due to errors"
    )
    
    dlq_fields: List[str] = Field(
        default_factory=list,
        description="Fields that were sent to DLQ due to errors"
    )


class TransformationStats(BaseModel):
    """Statistics for transformation operations."""
    
    total_records: int = Field(
        default=0,
        description="Total number of records processed"
    )
    
    successful_records: int = Field(
        default=0,
        description="Number of records successfully transformed"
    )
    
    failed_records: int = Field(
        default=0,
        description="Number of records that failed transformation"
    )
    
    skipped_fields: Dict[str, int] = Field(
        default_factory=dict,
        description="Count of skipped fields by field name"
    )
    
    dlq_fields: Dict[str, int] = Field(
        default_factory=dict,
        description="Count of DLQ fields by field name"
    )
    
    transformation_errors: Dict[str, int] = Field(
        default_factory=dict,
        description="Count of transformation errors by type"
    )
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total_records == 0:
            return 0.0
        return (self.successful_records / self.total_records) * 100.0
    
    @property
    def failure_rate(self) -> float:
        """Calculate failure rate as percentage."""
        if self.total_records == 0:
            return 0.0
        return (self.failed_records / self.total_records) * 100.0