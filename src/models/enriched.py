"""Enriched configuration models for engine consumption.

These models encapsulate the merged config (connection + endpoint + stream settings)
and use inheritance to avoid duplicating common stream fields.
"""

from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field

from .api import RateLimitConfig


# Base classes with common stream fields

class BaseSourceStreamConfig(BaseModel):
    """Common stream settings for all source types."""
    model_config = ConfigDict(extra="allow")

    connection_ref: str = Field(..., description="Connection alias reference from pipeline")
    endpoint_id: str = Field(..., description="Source endpoint identifier")
    replication_method: str = Field("incremental", description="Replication method")
    cursor_field: Optional[str] = Field(None, description="Field to track progress")
    safety_window_seconds: Optional[int] = Field(None, ge=0, description="Safety window for late-arriving data")
    primary_key: List[str] = Field(default_factory=list, description="Primary key field names")


class BaseDestinationStreamConfig(BaseModel):
    """Common stream settings for all destination types."""
    model_config = ConfigDict(extra="allow")

    connection_ref: str = Field(..., description="Connection alias reference from pipeline")
    endpoint_id: str = Field(..., description="Destination endpoint identifier")
    refresh_mode: str = Field("upsert", description="Write mode (insert, upsert, update)")
    batch_support: bool = Field(False, description="Whether batching is supported")
    batch_size: int = Field(1, ge=1, description="Batch size for processing")


# API-specific configs

class EnrichedAPISourceConfig(BaseSourceStreamConfig):
    """Complete source config for API connector.

    Merges connection config (host, headers, etc.), endpoint config (endpoint path,
    method, pagination), and stream settings (replication, cursor field, etc.).
    """
    connector_type: Literal["api"] = Field("api", description="Connector type discriminator")
    host: str = Field(..., description="API base URL")
    headers: Dict[str, str] = Field(default_factory=dict, description="HTTP headers")
    timeout: int = Field(30, ge=1, le=300, description="Request timeout in seconds")
    max_connections: int = Field(10, ge=1, le=100, description="Maximum concurrent connections")
    rate_limit: Optional[RateLimitConfig] = Field(None, description="Rate limiting configuration")

    # Endpoint fields
    endpoint: str = Field(..., description="API endpoint path")
    method: str = Field("GET", description="HTTP method")
    pagination: Optional[Dict[str, Any]] = Field(None, description="Pagination configuration")
    replication_filter_mapping: Optional[Dict[str, str]] = Field(
        None, description="Maps cursor field to API query parameter"
    )


class EnrichedAPIDestinationConfig(BaseDestinationStreamConfig):
    """Complete destination config for API connector."""
    connector_type: Literal["api"] = Field("api", description="Connector type discriminator")
    host: str = Field(..., description="API base URL")
    headers: Dict[str, str] = Field(default_factory=dict, description="HTTP headers")
    timeout: int = Field(30, ge=1, le=300, description="Request timeout in seconds")

    # Endpoint fields
    endpoint: str = Field(..., description="API endpoint path")
    method: str = Field("POST", description="HTTP method")


# Database-specific configs

class EnrichedDatabaseSourceConfig(BaseSourceStreamConfig):
    """Complete source config for database connector.

    Merges connection config (driver, host, credentials), endpoint config (schema,
    table), and stream settings (replication, cursor field, etc.).
    """
    connector_type: Literal["database"] = Field("database", description="Connector type discriminator")
    driver: str = Field(..., description="Database driver (postgresql, mysql, etc.)")
    host: str = Field(..., description="Database host")
    port: int = Field(..., description="Database port")
    database: str = Field(..., description="Database name")
    username: str = Field(..., description="Database username")
    password: str = Field(..., description="Database password")

    # Endpoint fields
    schema_name: str = Field("public", alias="schema", description="Database schema name")
    table: str = Field(..., description="Database table name")


class EnrichedDatabaseDestinationConfig(BaseDestinationStreamConfig):
    """Complete destination config for database connector."""
    connector_type: Literal["database"] = Field("database", description="Connector type discriminator")
    driver: str = Field(..., description="Database driver (postgresql, mysql, etc.)")
    host: str = Field(..., description="Database host")
    port: int = Field(..., description="Database port")
    database: str = Field(..., description="Database name")
    username: str = Field(..., description="Database username")
    password: str = Field(..., description="Database password")

    # Endpoint fields
    schema_name: str = Field("public", alias="schema", description="Database schema name")
    table: str = Field(..., description="Database table name")
    write_mode: str = Field("insert", description="Write mode (insert, upsert)")
    primary_key: List[str] = Field(default_factory=list, description="Primary key columns for upsert")


# Union types for type hints
EnrichedSourceConfig = Union[EnrichedAPISourceConfig, EnrichedDatabaseSourceConfig]
EnrichedDestinationConfig = Union[EnrichedAPIDestinationConfig, EnrichedDatabaseDestinationConfig]
