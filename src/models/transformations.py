"""Pydantic models for transformation definitions."""

from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


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


