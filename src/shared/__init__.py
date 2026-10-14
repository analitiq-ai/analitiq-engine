"""Shared engine utilities (engine-only; not part of the CDK).

The connector plumbing that used to live here — ``ConnectionRuntime``, the
transport factory, the rate limiter, the SQL/identifier helpers, the HTTP and
dict-path helpers the API connectors used — moved into the ``cdk`` package
(see ``docs/architecture/connector-module-architecture.md``, "Overview").
What remains is engine-only: the run-id lifecycle (``run_id``) and log-level
setup (``logging_setup``).
"""

from .run_id import get_or_generate_run_id, get_run_id, initialize_run_id

__all__ = [
    "get_run_id",
    "get_or_generate_run_id",
    "initialize_run_id",
]
