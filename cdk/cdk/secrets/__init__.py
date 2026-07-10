"""
Secrets management module with late-binding support.

Resolution is driven by a connection's ``secret_refs`` map: each value is a
scheme-prefixed locator that names its source. The scheme-dispatch resolver
sources each value by scheme (``env:`` / ``file:`` / ``sidecar:`` built-in,
``s3://`` under the ``[s3]`` extra); an in-memory resolver serves tests.

Secrets are resolved at connection time (late-binding), not at config load time,
providing better security by minimizing the time secrets are in memory.
"""

from cdk.secrets.exceptions import (
    PlaceholderExpansionError,
    SecretAccessDeniedError,
    SecretNotFoundError,
    SecretResolutionError,
    UnsupportedSecretRefScheme,
)
from cdk.secrets.protocol import SecretsResolver
from cdk.secrets.resolvers.memory import InMemorySecretsResolver
from cdk.secrets.resolvers.scheme import SchemeSecretsResolver

__all__ = [
    # Protocol
    "SecretsResolver",
    # Exceptions
    "SecretNotFoundError",
    "SecretAccessDeniedError",
    "SecretResolutionError",
    "PlaceholderExpansionError",
    "UnsupportedSecretRefScheme",
    # Resolvers
    "SchemeSecretsResolver",
    "InMemorySecretsResolver",
]
