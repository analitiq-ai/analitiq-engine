"""Which transport an operation dispatches through, in either role.

The failure this closes is silent in the worst direction: a read declaring
``transport_ref`` used to go out on the default origin with the default
headers, the provider answered 200, and the run was green with the wrong
data from the wrong host. So every test here asks the same question of the
recorded call -- which SESSION took it, and what URL did it get -- rather
than of the plan that produced it.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pyarrow as pa
import pytest

from cdk.api import GenericAPIConnector
from cdk.exceptions import ReadError, TransportSpecError
from cdk.types import Cursor, SchemaSpec, WriteMode

from .fakes import (
    BASE_URL,
    FakeCheckpoint,
    FakeResponse,
    FakeSession,
    endpoint_document,
    runtime_with,
    stream_source,
)

pytestmark = pytest.mark.unit

#: The second origin one system serves from -- the file-download shape the
#: single-origin pin refused while the connector declares both.
FILES_URL = "https://files.example.test/v2"


def _document(transport_ref: str | None = None) -> dict[str, Any]:
    request: dict[str, Any] = {"method": "GET", "path": "/items"}
    if transport_ref is not None:
        request["transport_ref"] = transport_ref
    return endpoint_document(request=request)


async def _read(runtime: Any, document: dict[str, Any]) -> list[Any]:
    connector = GenericAPIConnector()
    return [
        batch
        async for batch in connector.read_batches(
            runtime,
            {"endpoint_document": document, "stream_source": stream_source()},
            checkpoint=FakeCheckpoint(),
            stream_name="items",
            batch_size=100,
        )
    ]


def _page() -> FakeResponse:
    return FakeResponse(body={"records": [{"id": 1, "name": "n1"}]})


class TestReadDispatch:
    async def test_a_read_naming_a_transport_goes_out_on_that_one(self) -> None:
        default, files = FakeSession([]), FakeSession([_page()])
        runtime = runtime_with(default, transports={"files": (files, FILES_URL)})

        await _read(runtime, _document("files"))

        assert default.calls == [], "the default session must not have been used"
        assert files.calls[0]["url"] == f"{FILES_URL}/items"

    async def test_a_read_naming_nothing_goes_out_on_the_default(self) -> None:
        default, files = FakeSession([_page()]), FakeSession([])
        runtime = runtime_with(default, transports={"files": (files, FILES_URL)})

        await _read(runtime, _document())

        assert files.calls == []
        assert default.calls[0]["url"] == f"{BASE_URL}/items"

    async def test_naming_the_default_transport_reuses_its_session(self) -> None:
        """The shape every shipped connector has: the ref spelled out.

        A second sender over the same transport would open a second
        connection pool to the origin the connection already holds one to.
        """
        default = FakeSession([_page()])
        connector = GenericAPIConnector()
        runtime = runtime_with(default)
        async for _batch in connector.read_batches(
            runtime,
            {"endpoint_document": _document("api"), "stream_source": stream_source()},
            checkpoint=FakeCheckpoint(),
            stream_name="items",
            batch_size=100,
        ):
            pass
        assert default.calls[0]["url"] == f"{BASE_URL}/items"

    async def test_a_transport_the_run_did_not_resolve_is_refused_by_name(self) -> None:
        """Never the default instead: that is the silent failure this ends."""
        default = FakeSession([])
        runtime = runtime_with(default)

        with pytest.raises(ReadError, match="no resolved transport 'oauth'"):
            await _read(runtime, _document("oauth"))
        assert default.calls == [], "the request must not fall back to the default"


class TestWriteDispatch:
    """The write path expresses the same selection, and lands the same way."""

    @staticmethod
    def _write_document(transport_ref: str | None = None) -> dict[str, Any]:
        request: dict[str, Any] = {
            "method": "POST",
            "path": "/items",
            "body": {"item": {"from_input": "record"}},
        }
        if transport_ref is not None:
            request["transport_ref"] = transport_ref
        return {
            "$schema": "https://schemas.analitiq.ai/api-endpoint/latest.json",
            "endpoint_id": "items",
            "operations": {
                "write": {
                    "insert": {
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
                }
            },
        }

    async def _write(self, runtime: Any, document: dict[str, Any]) -> None:
        connector = GenericAPIConnector()
        connector.set_stream_endpoints({"items": document})
        await connector.connect(runtime)
        accepted = await connector.configure_schema(
            SchemaSpec(
                stream_id="items",
                version=1,
                write_mode=WriteMode.WRITE_MODE_INSERT,
                ack_timeout_seconds=30,
            )
        )
        assert accepted, connector.last_schema_rejection
        await connector.write_batch(
            run_id="run-1",
            stream_id="items",
            batch_seq=1,
            record_batch=pa.RecordBatch.from_pylist([{"id": 1}]),
            record_ids=["r0"],
            cursor=Cursor(b"c"),
            emitted_at=datetime(2026, 7, 31, tzinfo=timezone.utc),
        )

    async def test_a_write_naming_a_transport_goes_out_on_that_one(self) -> None:
        default, files = FakeSession([]), FakeSession([FakeResponse(body={})])
        runtime = runtime_with(default, transports={"files": (files, FILES_URL)})

        await self._write(runtime, self._write_document("files"))

        assert default.calls == [], "the default session must not have been used"
        assert files.calls[0]["url"] == f"{FILES_URL}/items"

    async def test_a_write_naming_nothing_goes_out_on_the_default(self) -> None:
        default, files = FakeSession([FakeResponse(body={})]), FakeSession([])
        runtime = runtime_with(default, transports={"files": (files, FILES_URL)})

        await self._write(runtime, self._write_document())

        assert files.calls == []
        assert default.calls[0]["url"] == f"{BASE_URL}/items"


class TestContainment:
    async def test_a_next_link_onto_a_second_declared_transport_is_followed(
        self,
    ) -> None:
        """One system, two origins -- the shape the single-origin pin refused."""
        default = FakeSession(
            [
                FakeResponse(
                    body={
                        "records": [{"id": 1, "name": "n1"}],
                        "links": {"next": f"{FILES_URL}/items?page=2"},
                    }
                ),
                FakeResponse(body={"records": [], "links": {"next": None}}),
            ]
        )
        runtime = runtime_with(
            default, transports={"files": (FakeSession([]), FILES_URL)}
        )
        document = endpoint_document(
            request={"method": "GET", "path": "/items"},
            pagination={
                "type": "link",
                "link": {"next_url": {"ref": "response.body.links.next"}},
            },
        )

        await _read(runtime, document)

        # The link was followed rather than refused: the traversal asked for
        # a second page, on the origin the second transport declares.
        assert default.calls[1]["url"] == f"{FILES_URL}/items?page=2"

    async def test_a_next_link_off_every_declared_origin_is_still_refused(
        self,
    ) -> None:
        default = FakeSession(
            [
                FakeResponse(
                    body={
                        "records": [{"id": 1, "name": "n1"}],
                        "links": {"next": "https://evil.test/steal"},
                    }
                )
            ]
        )
        runtime = runtime_with(
            default, transports={"files": (FakeSession([]), FILES_URL)}
        )
        document = endpoint_document(
            request={"method": "GET", "path": "/items"},
            pagination={
                "type": "link",
                "link": {"next_url": {"ref": "response.body.links.next"}},
            },
        )

        with pytest.raises(ReadError, match="leaves the connection's declared origins"):
            await _read(runtime, document)


class TestTheRuntimeRefusesWhatItCannotOpen:
    async def test_a_ref_outside_the_resolved_set_names_the_set(self) -> None:
        runtime = runtime_with(FakeSession([]))
        with pytest.raises(TransportSpecError, match=r"this run resolved \['api'\]"):
            await runtime.http_transport("oauth")

    async def test_the_declared_origins_are_every_resolved_transports(self) -> None:
        runtime = runtime_with(
            FakeSession([]), transports={"files": (FakeSession([]), FILES_URL)}
        )
        assert runtime.declared_origins == {BASE_URL, FILES_URL}
