"""Backward compatibility alias for src.source.connectors.

This module is deprecated. Please use src.source.connectors instead.
"""
import warnings

warnings.warn(
    "src.connectors is deprecated, use src.source.connectors instead",
    DeprecationWarning,
    stacklevel=2
)

# Re-export everything from source.connectors for backwards compatibility
from ..source.connectors import *
from ..source.connectors import __all__
