"""A defective api request block refuses one stream, not the whole gRPC stream.

``build_write_plan`` resolves the endpoint's declared params, headers,
query and path bindings at the schema handshake. Every way that resolution
can fail is a defect in ONE stream's endpoint document, so it belongs on
that stream's ``SchemaAck`` with a config-defect category.

Before the resolution boundary was classified, each defect left the build
wearing whatever the resolver happened to raise: conflicting expression
markers and an unknown derived function as ``TransportSpecError``, a
derived function handed the wrong type as ``TypeError``. The servicer's
clauses match ``(UnmappedTypeError, InvalidTypeMapError)`` and
``(KeyError, TypeError, ValueError)`` -- ``TransportSpecError`` matched
neither, so ``StreamRecords`` re-raised and every other stream sharing the
connection died with one stream's typo.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from cdk_tests.api.fakes import FakeSession, runtime_with

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


async def _ack(document: dict[str, Any]) -> Any:
    connector = GenericAPIConnector()
    connector.set_stream_endpoints({"items": document})
    await connector.connect(runtime_with(FakeSession()))
    return await DestinationServicer(connector, MagicMock())._handle_schema_message(
        SchemaMessage(
            stream_id="items",
            version=1,
            write_mode=_INSERT,
            ack_timeout_seconds=30,
        )
    )


_DEFECTS: dict[str, dict[str, Any]] = {
    "a header with conflicting expression markers": {
        "headers": {"X-T": {"ref": "connection.parameters.a", "template": "b"}}
    },
    "a query value calling a function nobody registered": {
        "query": {"q": {"function": "nope", "input": {"literal": 1}}}
    },
    "a header reading a scope that does not exist": {
        "headers": {"X-T": {"ref": "nosuchscope.a"}}
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
