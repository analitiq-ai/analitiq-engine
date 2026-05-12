"""Configuration validators.

The bespoke dict-based validators that lived here have been replaced by
JSON Schema validation against the published Analitiq contract schemas
(:mod:`src.config.schema_validator`). The two functions below remain as
thin shims so older imports continue to work; new code should call
:func:`src.config.schema_validator.validate` directly.
"""

from __future__ import annotations

from typing import Any, Dict

from src.config.schema_validator import validate


def validate_pipeline_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Validate a pipeline document against the published pipeline schema."""
    validate("pipeline", config, source="<pipeline>")
    return config


def validate_connection_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Validate a connection document against the published connection schema."""
    validate("connection", config, source="<connection>")
    return config
