"""Pydantic models for API connector validation."""

from datetime import datetime
from typing import Any, Dict, List, Optional, Union, Literal
from urllib.parse import urlparse

from pydantic import BaseModel, Field, field_validator, model_validator


class RateLimitConfig(BaseModel):
    """Rate limiting configuration."""
    
    max_requests: int = Field(100, gt=0, description="Maximum requests per time window")
    time_window: int = Field(60, gt=0, description="Time window in seconds")


class PaginationParams(BaseModel):
    """Pagination parameters configuration."""
    
    limit_param: Optional[str] = Field(None, description="Parameter name for page size")
    max_limit: Optional[int] = Field(None, gt=0, description="Maximum allowed page size")
    page_param: Optional[str] = Field(None, description="Parameter name for page number")
    offset_param: Optional[str] = Field(None, description="Parameter name for offset")
    cursor_param: Optional[str] = Field(None, description="Parameter name for cursor")
    next_cursor_field: Optional[str] = Field(None, description="Response field containing next cursor")
    uses_link_header: bool = Field(False, description="Whether API uses Link header for pagination")


class TimeWindowParams(BaseModel):
    """Time window parameters for filtering."""
    
    start_param: Optional[str] = Field(None, description="Start time parameter name")
    end_param: Optional[str] = Field(None, description="End time parameter name")


class PaginationConfig(BaseModel):
    """Pagination configuration with validation."""
    
    type: Literal["offset", "cursor", "page", "time", "link"] = Field(..., description="Pagination type")
    params: PaginationParams = Field(default_factory=PaginationParams, description="Pagination parameters")


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


class APIConnectionConfig(BaseModel):
    """API connection configuration with validation."""

    host: str = Field(..., description="Host URL for the API")
    headers: Dict[str, str] = Field(default_factory=dict, description="HTTP headers")
    timeout: int = Field(30, gt=0, le=300, description="Request timeout in seconds")
    max_connections: int = Field(10, gt=0, le=100, description="Maximum concurrent connections")
    max_connections_per_host: int = Field(2, gt=0, le=50, description="Maximum connections per host")
    rate_limit: Optional[RateLimitConfig] = Field(None, description="Rate limiting configuration")
    
    @field_validator("host")
    @classmethod
    def validate_host(cls, v):
        """Validate host URL format."""
        if not v:
            raise ValueError("host cannot be empty")

        parsed = urlparse(v)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError("host must be a valid URL with scheme and host")

        if parsed.scheme not in ["http", "https"]:
            raise ValueError("host must use http or https scheme")

        return v
    
    @field_validator("headers")
    @classmethod
    def validate_headers(cls, v):
        """Validate HTTP headers."""
        validated_headers = {}
        for name, value in v.items():
            if not isinstance(name, str) or not name.strip():
                raise ValueError("Header names must be non-empty strings")
            if not isinstance(value, str):
                raise ValueError(f"Header value for '{name}' must be a string")
            validated_headers[name.strip()] = value.strip()
        return validated_headers


class APIReadConfig(BaseModel):
    """API read configuration with validation."""
    
    endpoint: str = Field(..., description="API endpoint path")
    method: Literal["GET", "POST", "PUT", "PATCH"] = Field("GET", description="HTTP method")
    pagination: Optional[PaginationConfig] = Field(None, description="Pagination configuration")
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
    
    @model_validator(mode='after')
    def validate_pagination_params(self):
        """Validate pagination parameters are consistent with type."""
        if not self.pagination:
            return self
            
        pag_type = self.pagination.type
        params = self.pagination.params
        
        if pag_type == "offset" and not params.offset_param:
            raise ValueError("Offset pagination requires 'offset_param'")
        
        if pag_type == "cursor" and not params.cursor_param:
            raise ValueError("Cursor pagination requires 'cursor_param'")
            
        if pag_type == "page" and not params.page_param:
            raise ValueError("Page pagination requires 'page_param'")
            
        return self


class APIWriteConfig(BaseModel):
    """API write configuration with validation."""
    
    endpoint: str = Field(..., description="API endpoint path")
    method: Literal["POST", "PUT", "PATCH"] = Field("POST", description="HTTP method")
    batch_support: bool = Field(False, description="Whether endpoint supports batch writes")
    batch_size: int = Field(1, gt=0, le=10000, description="Maximum batch size")
    idempotency_header: Optional[str] = Field(None, description="Idempotency header name")
    
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


class APIRequestParams(BaseModel):
    """Validated API request parameters."""
    
    url: str = Field(..., description="Full request URL")
    method: str = Field(..., description="HTTP method")
    params: Dict[str, Any] = Field(default_factory=dict, description="Query parameters")
    headers: Dict[str, str] = Field(default_factory=dict, description="Request headers")
    json_data: Optional[Dict[str, Any]] = Field(None, description="JSON request body")
    
    @field_validator("url")
    @classmethod
    def validate_url(cls, v):
        """Validate request URL."""
        parsed = urlparse(v)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError("Invalid URL format")
        return v
    
    @field_validator("method")
    @classmethod
    def validate_method(cls, v):
        """Validate HTTP method."""
        if v not in ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]:
            raise ValueError(f"Unsupported HTTP method: {v}")
        return v.upper()
    
    @field_validator("params")
    @classmethod
    def validate_params(cls, v):
        """Validate query parameters."""
        validated = {}
        for key, value in v.items():
            if not isinstance(key, str):
                raise ValueError("Parameter keys must be strings")
            # Convert values to strings for URL encoding
            if value is not None:
                validated[key] = str(value)
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


class EndpointConfig(BaseModel):
    """API endpoint configuration."""

    endpoint: str = Field(..., description="API endpoint path")
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"] = Field("GET", description="HTTP method")
    endpoint_schema: Optional[Dict[str, Any]] = Field(None, description="JSON Schema for endpoint data")

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


class HostConfig(BaseModel):
    """API host configuration."""

    host: str = Field(..., description="Host URL for the API")
    headers: Dict[str, str] = Field(default_factory=dict, description="HTTP headers")
    rate_limit: Optional[RateLimitConfig] = Field(None, description="Rate limiting configuration")

    @field_validator("host")
    @classmethod
    def validate_host(cls, v):
        """Validate host URL format."""
        if not v:
            raise ValueError("host cannot be empty")

        parsed = urlparse(v)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError("host must be a valid URL with scheme and host")

        if parsed.scheme not in ["http", "https"]:
            raise ValueError("host must use http or https scheme")

        return v


class APIConfig(BaseModel):
    """Complete API configuration combining host and endpoint."""

    host: HostConfig = Field(..., description="Host configuration")
    endpoint: EndpointConfig = Field(..., description="Endpoint configuration")
    connection: Optional[APIConnectionConfig] = Field(None, description="Connection configuration")
    read_config: Optional[APIReadConfig] = Field(None, description="Read-specific configuration")
    write_config: Optional[APIWriteConfig] = Field(None, description="Write-specific configuration")