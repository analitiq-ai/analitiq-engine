"""Engine-mode shutdown gates ledger pruning on a fully successful run.

``PipelineRunner.run()`` returns ``True`` for both a fully "success" run and a
"partial" one (some streams failed, or records were dead-lettered/skipped) --
the latter still exits 0. Pruning the destination idempotency ledger must key
off the stricter ``runner.status == "success"``, never the exit-code bool:
pruning after a partial run would corrupt a resume with the same RUN_ID, which
relies on the ledger to skip already-committed batches.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from src.main import run_engine_mode


class _StubRunner:
    """Stand-in for PipelineRunner with a fixed terminal status."""

    def __init__(self, *, returns: bool, status: str) -> None:
        self._returns = returns
        self.status = status

    async def run(self) -> bool:
        return self._returns


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "returns, status, expected_prune",
    [
        (True, "success", True),
        (True, "partial", False),
        (False, "failed", False),
    ],
)
async def test_prunes_only_on_full_success(
    returns: bool, status: str, expected_prune: bool
) -> None:
    runner = _StubRunner(returns=returns, status=status)
    with patch("src.runner.PipelineRunner", return_value=runner), patch(
        "src.main._send_shutdown_to_destination", new=AsyncMock()
    ) as send_shutdown:
        result = await run_engine_mode()

    assert result is returns
    send_shutdown.assert_awaited_once_with(expected_prune)
