"""Tests that file and stream handlers do not retain secrets after connect()."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.shared.connection_runtime import ConnectionRuntime
from src.destination.connectors.file import FileDestinationHandler
from src.destination.connectors.stream import StreamDestinationHandler


def _make_file_runtime(*, raw_config=None):
    """Create a file-type ConnectionRuntime with mock resolver."""
    config = raw_config or {
        "path": "/tmp/output",
        "prefix": "data/",
        "connector_type": "file",
        "file_format": "jsonl",
        "formatter_config": {},
        "path_template": None,
        "secret_field": "${MY_SECRET}",
    }
    return ConnectionRuntime(
        raw_config=config,
        connection_id="conn-file-test",
        connector_type="file",
        driver=None,
        resolver=AsyncMock(resolve=AsyncMock(return_value={"MY_SECRET": "top-secret"})),
    )


class TestFileHandlerSecretRetention:
    """Verify FileDestinationHandler does not retain secrets on self._config."""

    @pytest.mark.asyncio
    async def test_config_contains_only_path_and_prefix(self):
        runtime = _make_file_runtime()
        handler = FileDestinationHandler()

        mock_storage = AsyncMock()
        mock_storage.health_check = AsyncMock(return_value=True)
        mock_manifest = AsyncMock()
        mock_manifest.load = AsyncMock()

        with (
            patch("src.destination.connectors.file.get_storage_backend", return_value=mock_storage),
            patch("src.destination.connectors.file.ManifestTracker", return_value=mock_manifest),
        ):
            await handler.connect(runtime)

        assert set(handler._config.keys()) == {"path", "prefix"}
        assert handler._config["path"] == "/tmp/output"
        assert handler._config["prefix"] == "data/"

    @pytest.mark.asyncio
    async def test_secret_fields_not_in_config(self):
        runtime = _make_file_runtime()
        handler = FileDestinationHandler()

        mock_storage = AsyncMock()
        mock_manifest = AsyncMock()
        mock_manifest.load = AsyncMock()

        with (
            patch("src.destination.connectors.file.get_storage_backend", return_value=mock_storage),
            patch("src.destination.connectors.file.ManifestTracker", return_value=mock_manifest),
        ):
            await handler.connect(runtime)

        assert "secret_field" not in handler._config
        assert "MY_SECRET" not in str(handler._config.values())

    @pytest.mark.asyncio
    async def test_runtime_resolved_config_scrubbed_after_connect(self):
        runtime = _make_file_runtime()
        handler = FileDestinationHandler()

        mock_storage = AsyncMock()
        mock_manifest = AsyncMock()
        mock_manifest.load = AsyncMock()

        with (
            patch("src.destination.connectors.file.get_storage_backend", return_value=mock_storage),
            patch("src.destination.connectors.file.ManifestTracker", return_value=mock_manifest),
        ):
            await handler.connect(runtime)

        assert runtime._resolved_config is None


    @pytest.mark.asyncio
    async def test_secrets_scrubbed_on_connect_failure(self):
        runtime = _make_file_runtime()
        handler = FileDestinationHandler()

        mock_storage = AsyncMock()
        mock_storage.connect.side_effect = ValueError("path invalid")

        with patch("src.destination.connectors.file.get_storage_backend", return_value=mock_storage):
            with pytest.raises(ValueError, match="path invalid"):
                await handler.connect(runtime)

        # Secrets must be scrubbed even on failure
        assert runtime._resolved_config is None

    @pytest.mark.asyncio
    async def test_write_batch_uses_reduced_config(self):
        runtime = _make_file_runtime()
        handler = FileDestinationHandler()

        mock_storage = AsyncMock()
        mock_storage.build_path.return_value = "/tmp/output/stream-1/0.jsonl"
        mock_storage.write_file.return_value = "/tmp/output/stream-1/0.jsonl"
        mock_manifest = AsyncMock()
        mock_manifest.load = AsyncMock()
        mock_manifest.check_committed = AsyncMock(return_value=None)
        mock_manifest.record_commit = AsyncMock()
        mock_formatter = MagicMock()
        mock_formatter.serialize_batch.return_value = b'{"id": 1}\n'
        mock_formatter.file_extension = ".jsonl"
        mock_formatter.content_type = "application/jsonl"

        with (
            patch("src.destination.connectors.file.get_storage_backend", return_value=mock_storage),
            patch("src.destination.connectors.file.ManifestTracker", return_value=mock_manifest),
            patch("src.destination.connectors.file.get_formatter", return_value=mock_formatter),
        ):
            await handler.connect(runtime)

            from src.grpc.generated.analitiq.v1 import Cursor
            result = await handler.write_batch(
                run_id="run-1",
                stream_id="stream-1",
                batch_seq=0,
                records=[{"id": 1}],
                record_ids=["r1"],
                cursor=Cursor(token=b"cursor-0"),
            )

        assert result.success
        # Verify build_path was called with path from the reduced config
        mock_storage.build_path.assert_called_once()
        call_kwargs = mock_storage.build_path.call_args
        assert call_kwargs[1]["base_path"] == "/tmp/output"


class TestStreamHandlerSecretRetention:
    """Verify StreamDestinationHandler does not retain secrets on self._config."""

    @pytest.mark.asyncio
    async def test_config_is_empty_after_connect(self):
        runtime = ConnectionRuntime(
            raw_config={
                "file_format": "jsonl",
                "formatter_config": {},
                "api_key": "${KEY}",
            },
            connection_id="conn-stream-test",
            connector_type="stdout",
            driver=None,
            resolver=AsyncMock(resolve=AsyncMock(return_value={"KEY": "secret-key"})),
        )
        handler = StreamDestinationHandler()
        await handler.connect(runtime)

        assert handler._config == {}

    @pytest.mark.asyncio
    async def test_runtime_resolved_config_scrubbed_after_connect(self):
        runtime = ConnectionRuntime(
            raw_config={"file_format": "jsonl"},
            connection_id="conn-stream-test",
            connector_type="stdout",
            driver=None,
            resolver=AsyncMock(resolve=AsyncMock(return_value={})),
        )
        handler = StreamDestinationHandler()
        await handler.connect(runtime)

        assert runtime._resolved_config is None

    @pytest.mark.asyncio
    async def test_secrets_scrubbed_on_connect_failure(self):
        runtime = ConnectionRuntime(
            raw_config={
                "file_format": "unsupported_format_xyz",
                "formatter_config": {},
            },
            connection_id="conn-stream-fail",
            connector_type="stdout",
            driver=None,
            resolver=AsyncMock(resolve=AsyncMock(return_value={})),
        )
        handler = StreamDestinationHandler()

        with patch("src.destination.connectors.stream.get_formatter", side_effect=ValueError("unknown format")):
            with pytest.raises(ValueError, match="unknown format"):
                await handler.connect(runtime)

        # Secrets must be scrubbed even on failure
        assert runtime._resolved_config is None
