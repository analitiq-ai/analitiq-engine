"""Enriched configuration models for engine consumption.

These models validate the merged connection + endpoint config shape.
Stream-level fields (replication_method, cursor_field, etc.) are passed
through as plain dict entries — they are NOT part of these models.
"""

from typing import Any, Dict, Literal, Optional, Union

from pydantic import BaseModel, Field


class EnrichedDatabaseConfig(BaseModel):
    """Connection + endpoint config for database connectors."""

    connector_type: Literal["database"] = Field("database", description="Connector type discriminator")
    driver: str = Field(..., description="Database driver from connector metadata")
    host: str = Field(..., description="Database host")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Connection parameters")

    # Endpoint fields
    schema_name: str = Field("public", alias="schema", description="Database schema name")
    table: str = Field(..., description="Database table name")


class EnrichedAPIConfig(BaseModel):
    """Connection + endpoint config for API connectors."""

    connector_type: Literal["api"] = Field("api", description="Connector type discriminator")
    host: str = Field(..., description="API base URL")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Connection parameters")

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
