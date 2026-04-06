"""Pydantic models for API connector validation."""

from typing import Any, Dict, List, Optional, Literal
from pydantic import BaseModel, Field, field_validator, model_validator


class RateLimitConfig(BaseModel):
    """Rate limiting configuration."""
    
    max_requests: int = Field(100, gt=0, description="Maximum requests per time window")
    time_window: int = Field(60, gt=0, description="Time window in seconds")



class FilterConfig(BaseModel):
    """API filter configuration."""
    
    type: str = Field(..., description="Filter data type")
    value: Optional[Any] = Field(None, description="Filter value")
    default: Optional[Any] = Field(None, description="Default filter value")
    required: bool = Field(False, description="Whether filter is required")
    description: Optional[str] = Field(None, description="Filter description")
    
    @model_validator(mode='after')
    def validate_filter_has_value(self):
        """Ensure required filters have a value."""
        if self.required and self.value is None and self.default is None:
            raise ValueError("Required filter must have either 'value' or 'default'")
        return self


class APIReadConfig(BaseModel):
    """API read configuration with validation."""
    
    endpoint: str = Field(..., description="API endpoint path")
    method: Literal["GET", "POST", "PUT", "PATCH"] = Field("GET", description="HTTP method")
    pagination: Optional[Dict[str, Any]] = Field(None, description="Pagination configuration")
    filters: Dict[str, FilterConfig] = Field(default_factory=dict, description="Request filters")
    data_field: str = Field("data", description="Response field containing records")
    pipeline_config: Optional[Dict[str, Any]] = Field(None, description="Pipeline configuration")
    
    @field_validator("endpoint")
    @classmethod
    def validate_endpoint(cls, v):
        """Validate endpoint path."""
        if not v or not v.strip():
            raise ValueError("endpoint cannot be empty")
        
        # Ensure endpoint starts with /
        endpoint = v.strip()
        if not endpoint.startswith("/"):
            endpoint = "/" + endpoint
            
        return endpoint
    


class HTTPResponse(BaseModel):
    """Safe HTTP response wrapper."""
    
    status: int = Field(..., ge=100, le=599, description="HTTP status code")
    headers: Dict[str, str] = Field(default_factory=dict, description="Response headers")
    data: Any = Field(None, description="Response data")
    
    @field_validator("headers")
    @classmethod
    def validate_headers(cls, v):
        """Validate response headers."""
        validated = {}
        for name, value in v.items():
            if isinstance(name, str) and isinstance(value, str):
                validated[name.lower()] = value
        return validated



class RecordBatch(BaseModel):
    """Validated batch of records."""
    
    records: List[Dict[str, Any]] = Field(..., description="List of records")
    batch_id: Optional[int] = Field(None, description="Batch identifier")
    total_count: Optional[int] = Field(None, description="Total available records")
    has_more: bool = Field(False, description="Whether more records are available")
    next_cursor: Optional[str] = Field(None, description="Cursor for next batch")
    
    @field_validator("records")
    @classmethod
    def validate_records(cls, v):
        """Validate records list."""
        if not isinstance(v, list):
            raise ValueError("Records must be a list")
        
        # Validate each record is a dictionary
        for i, record in enumerate(v):
            if not isinstance(record, dict):
                raise ValueError(f"Record at index {i} must be a dictionary")
                
        return v
    
    @model_validator(mode='after')
    def validate_batch_consistency(self):
        """Validate batch metadata is consistent."""
        if self.total_count is not None and self.total_count < len(self.records):
            raise ValueError("total_count cannot be less than actual record count")

        if self.has_more and not self.next_cursor:
            # Warning rather than error - some APIs don't provide cursors
            pass

        return self


