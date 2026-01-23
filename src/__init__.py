"""
Analitiq Stream - A fault-tolerant data streaming framework for Python 3.11+

A high-performance, async-first framework for streaming data between various sources
and destinations with comprehensive fault tolerance and observability.
"""

__version__ = "0.1.0"
__author__ = "Analitiq Core Team"

from .source.connectors.api import APIConnector
from .source.connectors.base import BaseConnector
from .source.connectors.database import DatabaseConnector
from .engine.credentials import CredentialsManager, credentials_manager
from .engine.engine import StreamingEngine
from .engine.pipeline import Pipeline
from .state.circuit_breaker import CircuitBreaker
from .state.dead_letter_queue import DeadLetterQueue
from .state.retry_handler import RetryHandler
from .schema.schema_manager import SchemaManager

__all__ = [
    "StreamingEngine",
    "Pipeline",
    "credentials_manager",
    "CredentialsManager",
    "RetryHandler",
    "CircuitBreaker",
    "DeadLetterQueue",
    "SchemaManager",
    "BaseConnector",
    "DatabaseConnector",
    "APIConnector",
]
