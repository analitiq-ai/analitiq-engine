"""
Configuration validators for Analitiq Stream.
"""

from typing import Any


def validate_pipeline_config(config: dict[str, Any]) -> dict[str, Any]:
    """Validate a lightweight pipeline configuration file.

    The pipeline file must contain 'pipeline' and 'streams' keys.
    The pipeline must have 'connections' with 'source' and 'destinations'.
    """
    if "pipeline" not in config:
        raise ValueError("Pipeline config missing required key: 'pipeline'")
    if "streams" not in config:
        raise ValueError("Pipeline config missing required key: 'streams'")

    pipeline = config["pipeline"]
    connections = pipeline.get("connections", {})
    if not connections.get("source"):
        raise ValueError("Pipeline must define connections.source")
    if not connections.get("destinations"):
        raise ValueError("Pipeline must define connections.destinations")

    stream_ids = pipeline.get("streams", [])
    if not stream_ids:
        raise ValueError("Pipeline must list at least one stream ID in 'streams'")

    return config


def validate_connection_config(config: dict[str, Any]) -> dict[str, Any]:
    """Validate a connection configuration file.

    The connection must have a 'connector_slug' to link to a connector.
    """
    if "connector_slug" not in config:
        raise ValueError("Connection config missing required key: 'connector_slug'")
    return config
