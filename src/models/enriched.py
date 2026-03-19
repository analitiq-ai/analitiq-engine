"""Enriched configuration models for engine consumption.

These models validate the merged connection + endpoint config shape.
Stream-level fields (replication_method, cursor_field, etc.) are passed
through as plain dict entries — they are NOT part of these models.
"""

from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Field

from .api import RateLimitConfig


class DatabaseConnectionParameters(BaseModel):
    """Connector-specific parameters for database connections."""

    port: int = Field(..., description="Database port")
    database: str = Field(..., description="Database name")
    username: str = Field(..., description="Database username")
    password: str = Field(..., description="Database password")
    ssl_mode: Optional[str] = Field(None, description="SSL mode")
    connection_pool: Optional[Dict[str, Any]] = Field(None, description="Pool settings")


class APIConnectionParameters(BaseModel):
    """Connector-specific parameters for API connections."""

    headers: Dict[str, str] = Field(default_factory=dict, description="HTTP headers")
    timeout: int = Field(30, ge=1, le=300, description="Request timeout in seconds")
    max_connections: int = Field(10, ge=1, le=100, description="Maximum concurrent connections")
    max_connections_per_host: int = Field(2, ge=1, le=50, description="Maximum connections per host")
    rate_limit: Optional[RateLimitConfig] = Field(None, description="Rate limiting configuration")


class EnrichedDatabaseConfig(BaseModel):
    """Connection + endpoint config for database connectors."""

    connector_type: Literal["database"] = Field("database", description="Connector type discriminator")
    driver: str = Field(..., description="Database driver from connector metadata")
    host: str = Field(..., description="Database host")
    parameters: DatabaseConnectionParameters

    # Endpoint fields
    schema_name: str = Field("public", alias="schema", description="Database schema name")
    table: str = Field(..., description="Database table name")


class EnrichedAPIConfig(BaseModel):
    """Connection + endpoint config for API connectors."""

    connector_type: Literal["api"] = Field("api", description="Connector type discriminator")
    host: str = Field(..., description="API base URL")
    parameters: APIConnectionParameters

    # Endpoint fields
    endpoint: str = Field(..., description="API endpoint path")
    method: str = Field("GET", description="HTTP method")
    pagination: Optional[Dict[str, Any]] = Field(None, description="Pagination configuration")
    replication_filter_mapping: Optional[Dict[str, str]] = Field(
        None, description="Maps cursor field to API query parameter"
    )


# Union types for type hints
EnrichedSourceConfig = Union[EnrichedAPIConfig, EnrichedDatabaseConfig]
EnrichedDestinationConfig = Union[EnrichedAPIConfig, EnrichedDatabaseConfig]
