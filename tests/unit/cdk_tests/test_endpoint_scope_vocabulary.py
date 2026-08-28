"""The CDK's endpoint-scope vocabulary is the contract's, restated as an enum.

``cdk.types.EndpointScope`` names the two endpoint-ref scopes so the CDK can
dispatch on a member and check a raw ``scope`` string at one conversion. The
contract declares the same two, as a ``Literal`` on each variant of its
endpoint-ref union. Two spellings of one vocabulary drift in silence: a scope
the contract adds is a contract-valid endpoint_ref the CDK refuses at
``EndpointScope(value)``, and a scope it renames is a type-mapper lookup that
fails on every stream -- neither shows up until a run does.

The vocabulary is read off the models through ``_variant_literals``, the
engine's one reader of a contract Literal, rather than by a second walk of the
same annotation.
"""

from __future__ import annotations

import pytest
from analitiq.contracts.stream import EndpointRef

from cdk.types import EndpointScope
from src.models.resolved import _variant_literals

pytestmark = pytest.mark.unit

#: The scopes a contract-valid endpoint_ref may carry, unioned over both
#: variants of the contract's discriminated union.
CONTRACT_SCOPES = _variant_literals(EndpointRef, "scope")


def test_the_enum_carries_exactly_the_contract_scopes() -> None:
    assert {scope.value for scope in EndpointScope} == CONTRACT_SCOPES, (
        "cdk.types.EndpointScope has drifted from the contract's endpoint-ref "
        f"scope literal: the enum spells {sorted(s.value for s in EndpointScope)}, "
        f"the contract {sorted(CONTRACT_SCOPES)}. Every CDK caller converts a "
        "raw scope string through this enum, so a member the contract dropped "
        "is dead dispatch and a scope it added is refused as unknown"
    )
