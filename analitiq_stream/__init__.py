"""
Analitiq Stream - A fault-tolerant data streaming framework for Python 3.11+

A high-performance, async-first framework for streaming data between various sources
and destinations with comprehensive fault tolerance and observability.
"""

__version__ = "0.1.0"
__author__ = "Analitiq Core Team"

from .connectors.api import APIConnector
from .connectors.base import BaseConnector
from .connectors.database import DatabaseConnector
from .core.credentials import CredentialsManager, credentials_manager
from .core.engine import StreamingEngine
from .core.pipeline import Pipeline
from .fault_tolerance.circuit_breaker import CircuitBreaker
from .fault_tolerance.dead_letter_queue import DeadLetterQueue
from .fault_tolerance.retry_handler import RetryHandler
from .fault_tolerance.state_manager import StateManager
from .schema.schema_manager import SchemaManager

__all__ = [
    "StreamingEngine",
    "Pipeline",
    "credentials_manager",
    "CredentialsManager",
    "StateManager",
    "RetryHandler",
    "CircuitBreaker",
    "DeadLetterQueue",
    "SchemaManager",
    "BaseConnector",
    "DatabaseConnector",
    "APIConnector",
]
