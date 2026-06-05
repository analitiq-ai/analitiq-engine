"""``materialize_runtime`` balances the ref-count on a materialize failure.

A failed connection attempt must not leave a shared ``ConnectionRuntime`` with
an elevated ref count — that would keep its transport/session from ever
disposing. The helper acquires, then releases the ref if ``materialize()``
raises, preserving the original exception type for the caller's classification.
"""

from __future__ import annotations

import pytest

from cdk.connection_runtime import (
    DETERMINISTIC_CONNECT_ERRORS,
    ConnectionRuntime,
    materialize_runtime,
)
from cdk.secrets.resolvers.memory import InMemorySecretsResolver


def _runtime() -> ConnectionRuntime:
    return ConnectionRuntime(
        raw_config={"host": "h", "parameters": {}},
        connection_id="c",
        connector_id="test-connector",
        connector_type="api",
        driver=None,
        resolver=InMemorySecretsResolver({}),
    )


class TestMaterializeRuntime:
    @pytest.mark.asyncio
    async def test_releases_ref_when_materialize_fails(self):
        runtime = _runtime()

        async def _boom(*_a, **_k):
            raise RuntimeError("materialize boom")

        runtime.materialize = _boom  # type: ignore[method-assign]
        assert runtime._ref_count == 0

        with pytest.raises(RuntimeError, match="materialize boom"):
            await materialize_runtime(runtime)

        # acquire() rolled back -> no leaked reference.
        assert runtime._ref_count == 0

    @pytest.mark.asyncio
    async def test_preserves_original_exception_type(self):
        runtime = _runtime()

        class Boom(Exception):
            pass

        async def _boom(*_a, **_k):
            raise Boom("deterministic")

        runtime.materialize = _boom  # type: ignore[method-assign]

        with pytest.raises(Boom):
            await materialize_runtime(runtime)
        assert runtime._ref_count == 0

    def test_value_error_not_in_deterministic_errors(self):
        # ValueError is too broad — third-party or internal raises must NOT be
        # silently classified as deterministic config errors.
        assert ValueError not in DETERMINISTIC_CONNECT_ERRORS

    @pytest.mark.asyncio
    async def test_holds_ref_on_success(self):
        runtime = _runtime()

        async def _ok(*_a, **_k):
            runtime._materialized = True

        runtime.materialize = _ok  # type: ignore[method-assign]

        await materialize_runtime(runtime)
        assert runtime._ref_count == 1  # held for the caller's read/write

        await runtime.close()
        assert runtime._ref_count == 0
