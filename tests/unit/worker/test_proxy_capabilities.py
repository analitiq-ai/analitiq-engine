"""The proxy mirrors the worker's advertised capabilities, adding none.

These are plain property reads on a constructed proxy: the capabilities
object is assigned directly, so nothing here connects to a worker.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from cdk.types import WriteMode
from src.worker.proxy import WorkerProxyHandler

pytestmark = pytest.mark.unit


def _proxy() -> WorkerProxyHandler:
    return WorkerProxyHandler(
        connectors_dir=Path("/nonexistent/connectors"),
        connections_dir=Path("/nonexistent/connections"),
    )


def test_capability_passthrough_with_fallbacks():
    proxy = _proxy()
    # Before connect: safe defaults, no crash.
    assert proxy.connector_type == "unknown"
    assert proxy.supports_upsert is False
    caps = MagicMock(
        connector_type="database",
        supports_transactions=True,
        supports_upsert=True,
        supports_bulk_load=True,
        max_batch_size=500,
        max_batch_bytes=0,
    )
    proxy._capabilities = caps
    assert proxy.connector_type == "database"
    assert proxy.supports_upsert is True
    assert proxy.max_batch_size == 500
    # Zero means "worker did not declare" -- fall back to the base default.
    assert proxy.max_batch_bytes == 8 * 1024 * 1024


def test_auto_create_and_truncate_forwarded_from_capabilities():
    proxy = _proxy()
    # Before connect: both default False, no crash.
    assert proxy.supports_auto_create is False
    assert proxy.supports_truncate is False
    # supports_truncate is derived from the advertised write-mode list, not a
    # dedicated bool -- mirror exactly what the worker advertised.
    caps = MagicMock(
        supports_auto_create=True,
        supported_write_modes=[
            WriteMode.WRITE_MODE_INSERT,
            WriteMode.WRITE_MODE_TRUNCATE_INSERT,
        ],
    )
    proxy._capabilities = caps
    assert proxy.supports_auto_create is True
    assert proxy.supports_truncate is True


def test_truncate_absent_and_auto_create_false_when_not_advertised():
    proxy = _proxy()
    caps = MagicMock(
        supports_auto_create=False,
        supported_write_modes=[WriteMode.WRITE_MODE_INSERT],
    )
    proxy._capabilities = caps
    assert proxy.supports_auto_create is False
    assert proxy.supports_truncate is False


def test_insert_mirrored_from_the_worker_write_mode_list():
    # Inheriting the base's unconditional True would re-advertise a mode the
    # worker's schema handshake refuses (issue #388): an unmigrated SQLAlchemy
    # worker omits INSERT when its stage capabilities are missing, and the
    # proxy must not add it back.
    proxy = _proxy()
    assert proxy.supports_insert is False  # before connect
    proxy._capabilities = MagicMock(
        supported_write_modes=[WriteMode.WRITE_MODE_UPSERT],
    )
    assert proxy.supports_insert is False
    proxy._capabilities = MagicMock(
        supported_write_modes=[WriteMode.WRITE_MODE_INSERT],
    )
    assert proxy.supports_insert is True
