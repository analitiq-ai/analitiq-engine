"""Backward compatibility alias for src.engine.

This module is deprecated. Please use src.engine instead.
"""
import warnings

warnings.warn(
    "src.core is deprecated, use src.engine instead",
    DeprecationWarning,
    stacklevel=2
)

# Re-export everything from engine for backwards compatibility
from ..engine import *
from ..engine import __all__
