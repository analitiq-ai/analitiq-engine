"""A registered destination kind with no storage backend says so (issue #424).

``s3`` is a planned destination kind: the worker registry routes it to
``GenericFileConnector`` exactly like ``file``, but no S3 storage backend
exists yet. The operator must learn that from one message naming the kind and
the missing backend -- not from a lookup failure deep in a storage registry
they have no reason to know exists.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from cdk.connection_runtime import ConnectionRuntime
from cdk.file import StorageBackendNotBuiltError
from cdk.file.generic import GenericFileConnector
from cdk.file.local_backend import LocalFileStorage
from src.worker import build_worker_registries


def _runtime(kind: str, config: dict[str, object]) -> ConnectionRuntime:
    return ConnectionRuntime(
        raw_config=config,
        connection_id=f"conn-{kind}",
        connector_id=kind,
        connector_type=kind,
        driver=None,
        resolver=AsyncMock(resolve=AsyncMock(return_value={})),
    )


class TestPlannedKindWithoutBackend:
    @pytest.mark.asyncio
    async def test_s3_destination_refuses_to_connect(self):
        """The kind the worker registry hands the file handler is refused at
        connect, through the same registry a pipeline resolves it with."""
        _, registry = build_worker_registries()
        handler = registry.create("s3", "my-bucket")
        assert isinstance(handler, GenericFileConnector)

        with pytest.raises(StorageBackendNotBuiltError) as excinfo:
            await handler.connect(_runtime("s3", {"prefix": "data/"}))

        message = str(excinfo.value)
        assert "'s3'" in message
        assert "s3 storage backend" in message
        assert "does not exist yet" in message
        assert "planned, not" in message
        assert "misconfigured" in message
        # The one built backend is named, so the message is actionable.
        assert "file (LocalFileStorage)" in message

    @pytest.mark.asyncio
    async def test_refusal_precedes_acquiring_the_runtime(self):
        """The kind alone decides the backend, so the refusal lands before
        connect() takes shared ownership of the runtime or materializes its
        config: nothing is held or opened for a kind that cannot write.

        It does not spare the secret store -- the engine shell resolves the
        connection into the worker's launch bootstrap before the worker
        process exists, so the credentials were fetched long before connect().
        """
        runtime = _runtime("s3", {"prefix": "data/"})
        handler = GenericFileConnector()

        with pytest.raises(StorageBackendNotBuiltError):
            await handler.connect(runtime)

        assert runtime._ref_count == 0
        assert runtime._materialized is False
        assert runtime._resolved_config is None

    @pytest.mark.asyncio
    async def test_file_destination_still_connects_through_local_storage(
        self, tmp_path
    ):
        """The refusal is scoped to the unbuilt kind: ``file`` still resolves
        the local filesystem backend and connects."""
        _, registry = build_worker_registries()
        handler = registry.create("file", "csvbox")

        await handler.connect(_runtime("file", {"path": str(tmp_path)}))
        try:
            assert isinstance(handler._storage, LocalFileStorage)
            assert await handler.health_check() is True
        finally:
            await handler.disconnect()
