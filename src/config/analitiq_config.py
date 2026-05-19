"""Configuration validators for Analitiq Stream.

Shapes match the published contracts:
- pipeline: https://schemas.analitiq.ai/pipeline/latest.json (v6+, flat document)
- connection: https://schemas.analitiq.ai/connection/latest.json (v6+, flat document)
"""

from typing import Any


def validate_pipeline_config(config: dict[str, Any]) -> dict[str, Any]:
    """Validate a pipeline configuration file (flat, per published contract).

    The pipeline document is flat — no ``{"pipeline": {...}}`` wrapper. It must
    carry ``connections.source`` and a non-empty ``connections.destinations``,
    plus a non-empty ``streams`` list.
    """
    connections = config.get("connections")
    if not isinstance(connections, dict):
        raise ValueError("Pipeline config missing required key: 'connections'")
    if not connections.get("source"):
        raise ValueError("Pipeline must define connections.source")
    if not connections.get("destinations"):
        raise ValueError("Pipeline must define connections.destinations")

    stream_ids = config.get("streams") or []
    if not stream_ids:
        raise ValueError("Pipeline must list at least one stream ID in 'streams'")

    return config


def validate_connection_config(config: dict[str, Any]) -> dict[str, Any]:
    """Validate a connection configuration file (flat, per published contract).

    The connection must carry ``connector_id`` linking it to a connector.
    """
    if "connector_id" not in config:
        raise ValueError("Connection config missing required key: 'connector_id'")
    return config
