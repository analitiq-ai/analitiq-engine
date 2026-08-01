"""The file and stdout connectors keep no secret on self._config after connect()."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cdk.connection_runtime import ConnectionRuntime
from cdk.file.generic import GenericFileConnector
from cdk.stdout.generic import GenericStdoutConnector

# A fixed, timezone-aware emit instant for write_batch/send_batch calls; the
# engine stamps this per batch (issue #353). Value is arbitrary for sinks
# that ignore it.
_EMITTED_AT = datetime(2026, 7, 21, 9, 0, 0, tzinfo=timezone.utc)


def _make_file_runtime(*, raw_config=None):
    """Create a file-type ConnectionRuntime with mock resolver."""
    config = raw_config or {
        "path": "/tmp/output",
        "prefix": "data/",
        "file_format": "jsonl",
        "formatter_config": {},
        "path_template": None,
        "secret_field": "${MY_SECRET}",
    }
    return ConnectionRuntime(
        raw_config=config,
        connection_id="conn-file-test",
        connector_id="test-connector",
        connector_type="file",
        driver=None,
        resolver=AsyncMock(resolve=AsyncMock(return_value={"MY_SECRET": "top-secret"})),
    )


class TestFileConnectorSecretRetention:
    """Verify GenericFileConnector does not retain secrets on self._config."""

    @pytest.mark.asyncio
    async def test_config_contains_only_path_and_prefix(self):
        runtime = _make_file_runtime()
        handler = GenericFileConnector()

        mock_storage = AsyncMock()
        mock_storage.health_check = AsyncMock(return_value=True)

        with patch(
            "cdk.file.generic.get_storage_backend",
            return_value=mock_storage,
        ):
            await handler.connect(runtime)

        # Prove the double was the thing that ran: a patch target that no
        # longer names the lookup site would leave the real backend writing
        # to the real filesystem, and every assertion below would still pass.
        mock_storage.connect.assert_awaited_once()
        assert set(handler._config.keys()) == {"path", "prefix"}
        assert handler._config["path"] == "/tmp/output"
        assert handler._config["prefix"] == "data/"

    @pytest.mark.asyncio
    async def test_secret_fields_not_in_config(self):
        runtime = _make_file_runtime()
        handler = GenericFileConnector()

        mock_storage = AsyncMock()

        with patch(
            "cdk.file.generic.get_storage_backend",
            return_value=mock_storage,
        ):
            await handler.connect(runtime)

        mock_storage.connect.assert_awaited_once()
        assert "secret_field" not in handler._config
        assert "MY_SECRET" not in str(handler._config.values())

    @pytest.mark.asyncio
    async def test_runtime_resolved_config_scrubbed_after_connect(self):
        runtime = _make_file_runtime()
        handler = GenericFileConnector()

        mock_storage = AsyncMock()

        with patch(
            "cdk.file.generic.get_storage_backend",
            return_value=mock_storage,
        ):
            await handler.connect(runtime)

        mock_storage.connect.assert_awaited_once()
        assert runtime._resolved_config is None

    @pytest.mark.asyncio
    async def test_secrets_scrubbed_on_connect_failure(self):
        runtime = _make_file_runtime()
        handler = GenericFileConnector()

        mock_storage = AsyncMock()
        mock_storage.connect.side_effect = ValueError("path invalid")

        with patch(
            "cdk.file.generic.get_storage_backend",
            return_value=mock_storage,
        ), pytest.raises(ValueError, match="path invalid"):
            await handler.connect(runtime)

        # Secrets must be scrubbed even on failure
        assert runtime._resolved_config is None

    @pytest.mark.asyncio
    async def test_write_batch_uses_reduced_config(self):
        runtime = _make_file_runtime()
        handler = GenericFileConnector()

        mock_storage = AsyncMock()
        mock_storage.build_path.return_value = "/tmp/output/stream-1/0.jsonl"
        mock_storage.write_file.return_value = "/tmp/output/stream-1/0.jsonl"
        mock_formatter = MagicMock()
        mock_formatter.serialize_batch.return_value = b'{"id": 1}\n'
        mock_formatter.file_extension = ".jsonl"
        mock_formatter.content_type = "application/jsonl"

        with (
            patch(
                "cdk.file.generic.get_storage_backend",
                return_value=mock_storage,
            ),
            patch(
                "cdk.file.generic.get_formatter",
                return_value=mock_formatter,
            ),
        ):
            await handler.connect(runtime)

            import pyarrow as pa

            from cdk.types import Cursor

            result = await handler.write_batch(
                run_id="run-1",
                stream_id="stream-1",
                batch_seq=0,
                record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
                record_ids=["r1"],
                cursor=Cursor(token=b"cursor-0"),
                emitted_at=_EMITTED_AT,
            )

        assert result.success
        # Verify build_path was called with path from the reduced config
        mock_storage.build_path.assert_called_once()
        call_kwargs = mock_storage.build_path.call_args
        assert call_kwargs[1]["base_path"] == "/tmp/output"


class TestStdoutConnectorSecretRetention:
    """Verify GenericStdoutConnector does not retain secrets on self._config."""

    @pytest.mark.asyncio
    async def test_config_is_empty_after_connect(self):
        runtime = ConnectionRuntime(
            raw_config={
                "file_format": "jsonl",
                "formatter_config": {},
                "api_key": "${KEY}",
            },
            connection_id="conn-stream-test",
            connector_id="test-connector",
            connector_type="stdout",
            driver=None,
            resolver=AsyncMock(resolve=AsyncMock(return_value={"KEY": "secret-key"})),
        )
        handler = GenericStdoutConnector()
        await handler.connect(runtime)

        assert handler._config == {}

    @pytest.mark.asyncio
    async def test_runtime_resolved_config_scrubbed_after_connect(self):
        runtime = ConnectionRuntime(
            raw_config={"file_format": "jsonl"},
            connection_id="conn-stream-test",
            connector_id="test-connector",
            connector_type="stdout",
            driver=None,
            resolver=AsyncMock(resolve=AsyncMock(return_value={})),
        )
        handler = GenericStdoutConnector()
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
            connector_id="test-connector",
            connector_type="stdout",
            driver=None,
            resolver=AsyncMock(resolve=AsyncMock(return_value={})),
        )
        handler = GenericStdoutConnector()

        with patch(
            "cdk.stdout.generic.get_formatter",
            side_effect=ValueError("unknown format"),
        ), pytest.raises(ValueError, match="unknown format"):
            await handler.connect(runtime)

        # Secrets must be scrubbed even on failure
        assert runtime._resolved_config is None
