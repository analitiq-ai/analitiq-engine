"""The destination write-mode vocabulary is one set, restated in three places.

``truncate_insert`` shipped across the CDK, the proto enum and the SQL write
path while the engine's own :class:`src.models.WriteMode` still listed two
modes, so a stream declaring it was rejected at destination startup by the very
check meant to catch typos (issue #435). Nothing failed, because no test tied
the restatements to each other.

The published contract is the authority here: it declares the modes a document
may carry. These tests make every engine-side restatement answer to it, so a
mode added to or dropped from the contract fails a test rather than a run.
"""

from __future__ import annotations

import pytest
from analitiq.contracts.endpoints import WriteMode as ContractWriteMode

from cdk.types import WriteMode as ProtoWriteMode
from src.grpc.client import DestinationGRPCClient
from src.models import WriteMode

#: The modes a contract-valid destination document may declare.
CONTRACT_MODES = frozenset(ContractWriteMode.__args__)


def test_engine_enum_matches_the_contract_vocabulary() -> None:
    """``src/main.py`` rejects an unknown ``write.mode`` against this enum, so a
    mode missing here is a contract-valid stream refused at startup."""
    assert {mode.value for mode in WriteMode} == CONTRACT_MODES


@pytest.mark.parametrize("mode", sorted(CONTRACT_MODES))
def test_every_contract_mode_has_a_wire_value(mode: str) -> None:
    """The proto enum carries what the contract permits, so a valid mode always
    survives the engine -> destination hop."""
    assert hasattr(ProtoWriteMode, f"WRITE_MODE_{mode.upper()}")


@pytest.mark.parametrize("mode", sorted(CONTRACT_MODES))
def test_schema_message_translates_every_contract_mode(mode: str) -> None:
    """The client's string -> proto lookup covers the whole vocabulary; a gap
    there raises ``Unknown write_mode`` on a document the contract accepts."""
    client = DestinationGRPCClient()
    schema_msg = client._build_schema_message(mode, {"write_mode": mode})
    assert schema_msg.write_mode == getattr(ProtoWriteMode, f"WRITE_MODE_{mode.upper()}")
