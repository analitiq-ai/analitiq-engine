"""Enriched configuration models for engine consumption.

These models validate the merged connection + endpoint config shape.
Stream-level fields (replication_method, cursor_field, etc.) are passed
through as plain dict entries — they are NOT part of these models.
"""

from typing import Any, Dict, Literal, Optional, Union

from pydantic import BaseModel, Field


class EnrichedDatabaseConfig(BaseModel):
    """Connection + endpoint config for database connectors.

    ``driver`` and ``host`` are now derived at materialize time from the
    connector's ``transports.<ref>`` block and the connection's
    ``parameters``; they remain on this model only so legacy validation
    can pass through and downstream code can read them when a value is
    available.
    """

    connector_type: Literal["database"] = Field("database", description="Connector type discriminator")
    driver: Optional[str] = Field(None, description="Base SQL dialect derived from the connector transport")
    host: Optional[str] = Field(None, description="Database host (from connection.parameters.host)")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Connection parameters")

    # Endpoint fields
    schema_name: str = Field("public", alias="schema", description="Database schema name")
    table: str = Field(..., description="Database table name")


class EnrichedAPIConfig(BaseModel):
    """Connection + endpoint config for API connectors.

    ``host`` is the connector's transport base URL when known statically;
    for connectors that template their base URL (e.g. region- or
    tenant-specific), it is left ``None`` and the engine consumes the
    materialized base URL from :class:`ConnectionRuntime` at request time.
    """

    connector_type: Literal["api"] = Field("api", description="Connector type discriminator")
    host: Optional[str] = Field(None, description="API base URL (literal transport.base_url, if any)")
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
