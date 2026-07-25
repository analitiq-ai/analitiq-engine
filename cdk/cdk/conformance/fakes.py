"""In-memory stand-ins the suite drives the CDK contract surface with.

Small on purpose: only what the suite needs to exercise the connector
through public CDK entry points, with no engine, broker, or secret
store present.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from cdk.secrets.protocol import SecretsResolver


class MemoryCheckpointStore:
    """A :class:`~cdk.types.CheckpointStore` holding cursors in a dict.

    The live tier reads through the connector's real read path, which
    persists cursors through this store; a test seeds it to exercise
    resume and inspects it to certify cursor advancement.
    """

    def __init__(self) -> None:
        self.cursors: dict[str, dict[str, Any]] = {}

    @staticmethod
    def _key(stream_name: str, partition: dict[str, Any] | None) -> str:
        return f"{stream_name}|{sorted((partition or {}).items())!r}"

    async def get_cursor(
        self, stream_name: str, partition: dict[str, Any] | None = None
    ) -> dict[str, Any] | None:
        """Return the saved cursor for *stream_name*, if any."""
        return self.cursors.get(self._key(stream_name, partition))

    async def save_cursor(
        self,
        stream_name: str,
        partition: dict[str, Any] | None,
        cursor: dict[str, Any],
    ) -> None:
        """Persist *cursor* for *stream_name*."""
        self.cursors[self._key(stream_name, partition)] = cursor


class NoSecretsResolver(SecretsResolver):
    """A resolver for runtimes that never materialize a transport.

    The suite's definition-only runtimes (used to derive the driver
    string) declare no secrets; declaring any is a wiring defect, so
    this resolver refuses rather than inventing values.
    """

    async def resolve(
        self,
        connection_id: str,
        secret_refs: Mapping[str, str],
    ) -> dict[str, str]:
        """Return no secrets; refuse if any were declared."""
        if secret_refs:
            raise RuntimeError(
                f"connection {connection_id!r} declares secret_refs "
                f"{sorted(secret_refs)}, but this conformance runtime "
                f"resolves no secrets"
            )
        return {}

    async def close(self) -> None:
        """Nothing to release."""
