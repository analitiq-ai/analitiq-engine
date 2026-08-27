"""A defective api request block refuses one stream, not the whole gRPC stream.

``build_write_plan`` resolves the endpoint's declared params, headers,
query and path bindings at the schema handshake. Every way that resolution
can fail is a defect in ONE stream's endpoint document, so it belongs on
that stream's ``SchemaAck`` with a config-defect category.

Before the resolution boundary was classified, each defect left the build
wearing whatever the resolver happened to raise: an unknown derived
function as ``TransportSpecError``, a derived function handed the wrong
type as ``TypeError``. The servicer's clauses match
``(UnmappedTypeError, InvalidTypeMapError)`` and
``(KeyError, TypeError, ValueError)`` -- ``TransportSpecError`` matched
neither, so ``StreamRecords`` re-raised and every other stream sharing the
connection died with one stream's typo.

A defect the published schema can decide from the document alone is
refused earlier, by the parse at registration -- but it lands on the same
ack. Registration runs before the gRPC server exists, so it records the
parse failure against its stream instead of raising, and the handshake is
where that stream hears about it while its neighbours sync. The two sets
are kept apart below so neither quietly stops being tested: one proves the
contract still decides these documents, the other proves the decision
reaches one ack rather than the process exit code.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from analitiq.contracts.endpoints import ApiEndpointDoc
from cdk_tests.api.fakes import FakeSession, runtime_with
from pydantic import ValidationError

from cdk.api import GenericAPIConnector
from cdk.types import FailureCategory
from src.destination.server import DestinationServicer
from src.grpc.generated.analitiq.v1 import SchemaMessage, WriteMode

pytestmark = pytest.mark.unit

#: The wire carries an int; the CDK-native enum is built inside the
#: servicer. Read from the generated enum, never hand-pinned -- the
#: generated module exists so no test carries the number.
_INSERT = WriteMode.WRITE_MODE_INSERT


def _document(
    *,
    params: dict[str, Any] | None = None,
    headers: dict[str, Any] | None = None,
    query: dict[str, Any] | None = None,
    path: str = "/items",
    path_params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    request: dict[str, Any] = {
        "method": "POST",
        "path": path,
        "body": {"item": {"from_input": "record"}},
    }
    if headers is not None:
        request["headers"] = headers
    if query is not None:
        request["query"] = query
    if path_params is not None:
        request["path_params"] = path_params
    block: dict[str, Any] = {
        "request": request,
        "input": {
            "schema": {
                "type": "object",
                "properties": {
                    "id": {
                        "type": "integer",
                        "native_type": "integer",
                        "arrow_type": "Int64",
                    }
                },
            }
        },
    }
    if params is not None:
        block["params"] = params
    return {
        "$schema": "https://schemas.analitiq.ai/api-endpoint/latest.json",
        "endpoint_id": "items",
        "operations": {"write": {"insert": block}},
    }


async def _connector(documents: dict[str, dict[str, Any]]) -> GenericAPIConnector:
    connector = GenericAPIConnector()
    connector.set_stream_endpoints(documents)
    await connector.connect(runtime_with(FakeSession()))
    return connector


async def _ack_for(connector: GenericAPIConnector, stream_id: str) -> Any:
    return await DestinationServicer(connector, MagicMock())._handle_schema_message(
        SchemaMessage(
            stream_id=stream_id,
            version=1,
            write_mode=_INSERT,
            ack_timeout_seconds=30,
        )
    )


async def _ack(document: dict[str, Any]) -> Any:
    return await _ack_for(await _connector({"items": document}), "items")


#: Defects no document can be judged on alone: each one turns on which
#: derived functions the run registered and what type they accept, which is a
#: runtime fact. These are the ones a handshake still has to answer for.
_DEFECTS: dict[str, dict[str, Any]] = {
    "a query value calling a function nobody registered": {
        "query": {"q": {"function": "nope", "input": {"literal": 1}}}
    },
    "a query value handing a function the wrong type": {
        "query": {"q": {"function": "base64_encode", "input": {"literal": 5}}}
    },
    "a param default calling a function nobody registered": {
        "params": {
            "tok": {
                "in": "query",
                "type": "string",
                "required": True,
                "default": {"function": "nope", "input": {"literal": 1}},
            }
        },
        "query": {"token": {"from_param": "tok"}},
    },
}

#: Defects the contract decides from the document alone. They never reach a
#: handshake: parsing the document is what refuses them, so their coverage is
#: that the published schema still catches each one.
_STATIC_DEFECTS: dict[str, dict[str, Any]] = {
    "a header with conflicting expression markers": {
        "headers": {"X-T": {"ref": "connection.parameters.a", "template": "b"}}
    },
    "a header reading a scope that does not exist": {
        "headers": {"X-T": {"ref": "nosuchscope.a"}}
    },
    "a param default reading a scope that does not exist": {
        "params": {
            "tok": {
                "in": "query",
                "type": "string",
                "required": True,
                "default": {"ref": "nosuchscope.tok"},
            }
        },
        "query": {"token": {"from_param": "tok"}},
    },
    "a path placeholder nothing binds": {
        "path": "/items/{id}",
        "path_params": {"id": {"ref": "connection.parameters.absent"}},
    },
}


@pytest.mark.asyncio
@pytest.mark.parametrize("defect", sorted(_DEFECTS), ids=sorted(_DEFECTS))
class TestOneStreamsDocumentDefectStaysOnItsOwnAck:
    async def test_the_handshake_answers_instead_of_raising(self, defect: str) -> None:
        # The whole point: the servicer returns an ack. A raise here means
        # the RPC dies and every other stream on the connection dies with
        # it, over a defect in one stream's document.
        ack = await _ack(_document(**_DEFECTS[defect]))
        assert not ack.accepted
        assert ack.stream_id == "items"

    async def test_the_ack_declares_who_owns_it(self, defect: str) -> None:
        # The engine classifies from the category, never from the wording.
        ack = await _ack(_document(**_DEFECTS[defect]))
        assert ack.failure_category == FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT
        assert ack.message


@pytest.mark.parametrize("defect", sorted(_STATIC_DEFECTS), ids=sorted(_STATIC_DEFECTS))
class TestADocumentDefectTheContractDecidesStillLandsOnOneAck:
    def test_the_published_schema_refuses_the_document(self, defect: str) -> None:
        # Nothing about these needs a run: the ack path above is the
        # residue left once the contract has had its say, and it stays
        # honest only while the contract keeps catching these four.
        with pytest.raises(ValidationError):
            ApiEndpointDoc.model_validate(_document(**_STATIC_DEFECTS[defect]))

    @pytest.mark.asyncio
    async def test_registration_does_not_kill_the_worker(self, defect: str) -> None:
        # set_stream_endpoints runs from the worker entry point, before
        # DestinationGRPCServer is constructed. A raise there is a process
        # exit and the engine sees a dead worker -- not one rejected
        # stream -- so the refusal has to wait for an ack to ride on.
        connector = await _connector(
            {"items": _document(**_STATIC_DEFECTS[defect]), "other": _document()}
        )
        ack = await _ack_for(connector, "items")
        assert not ack.accepted
        assert ack.stream_id == "items"
        assert ack.failure_category == FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT
        # The parse failure itself, not the "you never registered it"
        # message the same missing entry would otherwise produce.
        assert "ApiEndpointDoc" in ack.message
        assert "call set_stream_endpoints" not in ack.message

    @pytest.mark.asyncio
    async def test_the_other_streams_on_the_worker_still_configure(
        self, defect: str
    ) -> None:
        connector = await _connector(
            {"items": _document(**_STATIC_DEFECTS[defect]), "other": _document()}
        )
        assert (await _ack_for(connector, "other")).accepted
