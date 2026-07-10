"""
SecretsResolver protocol definition.

Defines the interface that all secrets resolvers must implement.
"""

from abc import ABC, abstractmethod
from collections.abc import Mapping
from types import TracebackType


class SecretsResolver(ABC):
    """
    Abstract base class for secrets resolution.

    Resolution is driven by the connection's ``secret_refs`` map: each value is
    a scheme-prefixed locator that names where its secret lives. A resolver
    turns that map into ``{name: value}``, sourcing each value by its scheme
    (environment variable, local file, sidecar file, S3, ...).

    Implementations should:
    - Return exactly the declared refs, resolved to their string values
    - Raise SecretNotFoundError when a declared ref's source is missing
    - Raise SecretAccessDeniedError for permission issues
    - Raise SecretResolutionError (or a subclass) for other failures

    A resolver never falls back to an empty or silent secret: an unresolvable
    ref is an error, surfaced here.
    """

    @abstractmethod
    async def resolve(
        self,
        connection_id: str,
        secret_refs: Mapping[str, str],
    ) -> dict[str, str]:
        """
        Resolve a connection's declared secret references.

        Args:
            connection_id: Identifier for the connection (used for diagnostics)
            secret_refs: Map of ref name -> scheme-prefixed locator, from the
                connection's ``secret_refs`` block

        Returns:
            Dictionary mapping each ref name to its resolved value

        Raises:
            SecretNotFoundError: If a declared ref's source cannot be found
            SecretAccessDeniedError: If access to a secret is denied
            SecretResolutionError: For other resolution failures
        """
        ...

    @abstractmethod
    async def close(self) -> None:
        """
        Clean up any resources held by the resolver.

        Should be called when the resolver is no longer needed.
        """
        ...

    async def __aenter__(self) -> "SecretsResolver":
        """Async context manager entry."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Async context manager exit."""
        await self.close()
