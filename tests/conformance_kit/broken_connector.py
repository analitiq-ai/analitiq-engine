"""A deliberately broken connector for the failing-direction acceptance run.

Importable by the subprocess tier-1 run (a fixture module, not a test
module): it overrides a private facade internal, which the kit must turn
into a red run.
"""

from __future__ import annotations

from typing import Any

from .reference_connector import ReferenceConnector


class PrivateOverrideConnector(ReferenceConnector):
    """Overrides a private ``GenericSQLConnector`` internal (forbidden)."""

    def _prepare_write_batch(self, state: Any, record_batch: Any) -> Any:
        return record_batch
