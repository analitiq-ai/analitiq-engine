"""
Secrets management module with late-binding support.

This module provides a pluggable secrets resolution system that supports:
- Local file-based secrets (for development)
- AWS S3-based secrets (for cloud deployments)
- In-memory secrets (for testing)

Secrets are resolved at connection time (late-binding), not at config load time,
providing better security by minimizing the time secrets are in memory.
"""

from src.secrets.protocol import SecretsResolver
from src.secrets.exceptions import (
    SecretNotFoundError,
    SecretAccessDeniedError,
    SecretResolutionError,
    PlaceholderExpansionError,
)
from src.secrets.resolvers.local import LocalFileSecretsResolver
from src.secrets.resolvers.memory import InMemorySecretsResolver
from src.secrets.resolvers.s3 import S3SecretsResolver

__all__ = [
    # Protocol
    "SecretsResolver",
    # Exceptions
    "SecretNotFoundError",
    "SecretAccessDeniedError",
    "SecretResolutionError",
    "PlaceholderExpansionError",
    # Resolvers
    "LocalFileSecretsResolver",
    "InMemorySecretsResolver",
    "S3SecretsResolver",
]
