"""Shared engine utilities (engine-only; not part of the CDK).

The connector plumbing that used to live here — ``ConnectionRuntime``, the
transport factory, the rate limiter, the SQL/identifier helpers — moved into the
``cdk`` package (ADR §4). What remains is engine-only and is imported directly
from its module: the run-id lifecycle (this file), plus ``dict_path`` and
``http_utils``.
"""

from .run_id import (
    get_or_generate_run_id,
    get_run_id,
    initialize_run_id,
)

__all__ = [
    "get_run_id",
    "get_or_generate_run_id",
    "initialize_run_id",
]
