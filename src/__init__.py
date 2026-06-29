"""
Analitiq Stream - A fault-tolerant data streaming framework for Python 3.11+.

A high-performance, async-first framework for streaming data between various sources
and destinations with comprehensive fault tolerance and observability.
"""

__version__ = "0.1.0"
__author__ = "Analitiq Core Team"

# Aliased to a private name so it is not re-exported as a public package
# attribute (see tests/.../test_package_init.py::test_no_unexpected_exports).
import os as _os

# Disable gRPC's fork handlers before any submodule below imports grpc and
# registers them. The engine forks worker subprocesses (worker/spawn.py) while
# holding open gRPC channels to the destination; gRPC's pthread_atfork child
# handler then runs in the fork and aborts on the epoll1 poller
# (ev_epoll1_linux.cc "Check failed: next_worker->state == KICKED"). Every
# subprocess here is fork+exec and never touches gRPC before exec, so the fork
# handlers are pure overhead. This must run before the imports below pull in
# grpc; setdefault keeps any explicit override.
_os.environ.setdefault("GRPC_ENABLE_FORK_SUPPORT", "0")

from .engine.engine import StreamingEngine  # noqa: E402
from .source.connectors.api import APIConnector  # noqa: E402
from .source.connectors.base import BaseConnector  # noqa: E402
from .state.circuit_breaker import CircuitBreaker  # noqa: E402
from .state.dead_letter_queue import DeadLetterQueue  # noqa: E402
from .state.retry_handler import RetryHandler  # noqa: E402

__all__ = [
    "StreamingEngine",
    "RetryHandler",
    "CircuitBreaker",
    "DeadLetterQueue",
    "BaseConnector",
    "APIConnector",
]
