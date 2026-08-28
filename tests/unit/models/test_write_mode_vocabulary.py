"""The destination write-mode vocabulary is one set, restated across the wire.

``truncate_insert`` shipped across the CDK, the proto enum and the SQL write
path while the engine kept its own two-mode ``WriteMode`` enum, so a stream
declaring it was rejected at destination startup by the very check meant to
catch typos (issue #435). Nothing failed, because no test tied the
restatements to each other.

That engine enum is gone: ``src/main.py`` now checks ``write.mode`` against the
contract's own ``WRITE_MODES``, so there is no second vocabulary left to drift
from it. What remains restated is the proto enum the mode travels on, and this
test makes that restatement answer to the contract.
"""

from __future__ import annotations

import pytest
from analitiq.contracts.endpoints import WriteMode as ContractWriteMode

from cdk.types import WriteMode as ProtoWriteMode
from src.grpc.client import DestinationGRPCClient

#: The modes a contract-valid destination document may declare.
CONTRACT_MODES = frozenset(ContractWriteMode.__args__)


@pytest.mark.parametrize("mode", sorted(CONTRACT_MODES))
def test_schema_message_translates_every_contract_mode(mode: str) -> None:
    """The client's string -> proto lookup covers the whole vocabulary; a gap
    there raises ``Unknown write_mode`` on a document the contract accepts."""
    client = DestinationGRPCClient()
    schema_msg = client._build_schema_message(mode, {"write_mode": mode})
    assert schema_msg.write_mode == getattr(
        ProtoWriteMode, f"WRITE_MODE_{mode.upper()}"
    )
