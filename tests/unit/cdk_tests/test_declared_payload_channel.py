"""The #401 declared blocks cross the trusted/worker boundary verbatim.

``error_map`` and ``concurrency`` ride the resolved worker payload exactly
like the #390 ``sql_capabilities`` block: folded in by ``resolve_spec`` on
the trusted side, restored by ``from_resolved_payload`` in the worker — the
worker never reads ``connector.json``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from cdk.connection_runtime import ConnectionRuntime
from cdk.declarations import parse_declared_concurrency, parse_declared_error_map

ERROR_MAP = {
    "sqlstate": {"08": "unreachable"},
    "exception": {"OperationalError": "transient"},
}
CONCURRENCY = {"max_connections": 4}


def _trusted_runtime(definition):
    return ConnectionRuntime(
        raw_config={"path": "/tmp/out"},
        connection_id="db-1",
        connector_id="demo",
        connector_type="database",
        resolver=AsyncMock(resolve=AsyncMock(return_value={})),
        connector_definition=definition,
    )


@pytest.mark.asyncio
async def test_declared_blocks_ride_resolve_spec_and_rebuild():
    runtime = _trusted_runtime(
        {
            "connector_id": "demo",
            "error_map": ERROR_MAP,
            "concurrency": CONCURRENCY,
        }
    )
    assert runtime.declared_error_map == ERROR_MAP
    assert runtime.declared_concurrency == CONCURRENCY

    payload = await runtime.resolve_spec()
    assert payload["error_map"] == ERROR_MAP
    assert payload["concurrency"] == CONCURRENCY

    worker_runtime = ConnectionRuntime.from_resolved_payload(payload)
    assert worker_runtime.declared_error_map == ERROR_MAP
    assert worker_runtime.declared_concurrency == CONCURRENCY
    # Parsed worker-side exactly as the trusted side would parse it.
    error_map = parse_declared_error_map(worker_runtime.declared_error_map)
    assert error_map is not None and error_map.sqlstate["08"] == "unreachable"
    assert parse_declared_concurrency(worker_runtime.declared_concurrency) == 4


@pytest.mark.asyncio
async def test_undeclared_stays_undeclared_across_the_boundary():
    runtime = _trusted_runtime({"connector_id": "demo"})
    assert runtime.declared_error_map is None
    assert runtime.declared_concurrency is None
    payload = await runtime.resolve_spec()
    assert payload["error_map"] is None
    assert payload["concurrency"] is None
    worker_runtime = ConnectionRuntime.from_resolved_payload(payload)
    assert worker_runtime.declared_error_map is None
    assert worker_runtime.declared_concurrency is None


def test_restored_blocks_do_not_share_state_with_the_payload():
    runtime = _trusted_runtime({"connector_id": "demo", "error_map": ERROR_MAP})
    copied = runtime.declared_error_map
    assert copied is not None
    copied["sqlstate"]["08"] = "mutated"
    # The runtime's own copy is isolated from the caller's mutation.
    assert runtime.declared_error_map == ERROR_MAP
