"""Backward compatibility alias for src.state.

This module is deprecated. Please use src.state instead.
"""
import warnings

warnings.warn(
    "src.fault_tolerance is deprecated, use src.state instead",
    DeprecationWarning,
    stacklevel=2
)

# Re-export everything from state for backwards compatibility
from ..state import *
from ..state import __all__
