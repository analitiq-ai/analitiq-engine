"""
Secrets management module with late-binding support.

This module provides a pluggable secrets resolution system that supports:
- Local file-based secrets (for development)
- In-memory secrets (for testing)

Secrets are resolved at connection time (late-binding), not at config load time,
providing better security by minimizing the time secrets are in memory.
"""

from cdk.secrets.protocol import SecretsResolver
from cdk.secrets.exceptions import (
    SecretNotFoundError,
    SecretAccessDeniedError,
    SecretResolutionError,
    PlaceholderExpansionError,
)
from cdk.secrets.resolvers.local import LocalFileSecretsResolver
from cdk.secrets.resolvers.memory import InMemorySecretsResolver

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
]
