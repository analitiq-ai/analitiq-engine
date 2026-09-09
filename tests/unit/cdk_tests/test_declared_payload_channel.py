"""The #401 declared blocks cross the trusted/worker boundary verbatim.

``error_map`` and ``concurrency`` ride the resolved worker payload exactly
like the #390 ``sql_capabilities`` block: folded in by ``resolve_spec`` on
the trusted side, restored by ``from_resolved_payload`` in the worker — the
worker never reads ``connector.json``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from contract_documents import connection_document, connector_document

from cdk.connection_runtime import ConnectionRuntime
from cdk.declarations import parse_declared_concurrency

# The vendored ``analitiq.contracts`` package (claude-code-plugins#91's job to
# update, per issue #513's Decisions) still validates ``error_map`` against
# the pre-#513 fixed-family shape -- ``connector_document`` below goes
# through that Pydantic contract, so this file's fixture stays in that shape.
# This test proves the wire only (the block rides resolve_spec / rebuild
# verbatim, whatever it contains); the new key_attrs/codes shape and its
# parsing are exhaustively covered, unconstrained by the vendored contract,
# in test_declarations.py.
ERROR_MAP = {
    "sqlstate": {"08": "unreachable"},
    "exception": {"OperationalError": "transient"},
}
CONCURRENCY = {"max_connections": 4}


def _trusted_runtime(**declared):
    return ConnectionRuntime(
        connection=connection_document(),
        connection_id="db-1",
        connector_id="demo",
        connector_type="database",
        resolver=AsyncMock(resolve=AsyncMock(return_value={})),
        connector=connector_document("database", **declared),
    )


@pytest.mark.asyncio
async def test_declared_blocks_ride_resolve_spec_and_rebuild():
    runtime = _trusted_runtime(error_map=ERROR_MAP, concurrency=CONCURRENCY)
    assert runtime.declared_error_map == ERROR_MAP
    assert runtime.declared_concurrency == CONCURRENCY

    payload = await runtime.resolve_spec()
    assert payload["error_map"] == ERROR_MAP
    assert payload["concurrency"] == CONCURRENCY

    worker_runtime = ConnectionRuntime.from_resolved_payload(payload)
    assert worker_runtime.declared_error_map == ERROR_MAP
    assert worker_runtime.declared_concurrency == CONCURRENCY
    assert parse_declared_concurrency(worker_runtime.declared_concurrency) == 4


@pytest.mark.asyncio
async def test_undeclared_stays_undeclared_across_the_boundary():
    runtime = _trusted_runtime()
    assert runtime.declared_error_map is None
    assert runtime.declared_concurrency is None
    payload = await runtime.resolve_spec()
    assert payload["error_map"] is None
    assert payload["concurrency"] is None
    worker_runtime = ConnectionRuntime.from_resolved_payload(payload)
    assert worker_runtime.declared_error_map is None
    assert worker_runtime.declared_concurrency is None


def test_restored_blocks_do_not_share_state_with_the_payload():
    runtime = _trusted_runtime(error_map=ERROR_MAP)
    copied = runtime.declared_error_map
    assert copied is not None
    copied["sqlstate"]["08"] = "mutated"
    # The runtime's own copy is isolated from the caller's mutation.
    assert runtime.declared_error_map == ERROR_MAP
