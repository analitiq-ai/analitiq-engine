"""Which transport an operation dispatches through, in either role.

The failure this closes is silent in the worst direction: a read declaring
``transport_ref`` used to go out on the default origin with the default
headers, the provider answered 200, and the run was green with the wrong
data from the wrong host. So every test here asks the same question of the
recorded call -- which SESSION took it, and what URL did it get -- rather
than of the plan that produced it.
"""

from __future__ import annotations

import asyncio
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
    @staticmethod
    def _linked(next_url: str | None) -> FakeResponse:
        return FakeResponse(
            body={"records": [{"id": 1, "name": "n1"}], "links": {"next": next_url}}
        )

    @staticmethod
    def _link_document() -> dict[str, Any]:
        return endpoint_document(
            request={"method": "GET", "path": "/items"},
            pagination={
                "type": "link",
                "link": {"next_url": {"ref": "response.body.links.next"}},
            },
        )

    async def test_a_link_off_the_reads_own_transport_is_refused(self) -> None:
        """Every page of a read goes out on the transport the read opened.

        The alternative -- switching transport mid-traversal -- has no
        answer for what the ENDPOINT declared: its `request.headers` are
        the endpoint's, bound once, and there is nothing correct to send
        the second host. Containment is per-transport for that reason.
        """
        default = FakeSession([self._linked(f"{FILES_URL}/items?page=2")])
        runtime = runtime_with(
            default, transports={"files": (FakeSession([]), FILES_URL)}
        )

        with pytest.raises(ReadError, match="leaves its transport's origin"):
            await _read(runtime, self._link_document())

    async def test_a_read_on_a_named_transport_is_contained_to_ITS_origin(
        self,
    ) -> None:
        """Per-transport, not per-connection: the default's origin is not its."""
        default = FakeSession([])
        files = FakeSession([self._linked(f"{BASE_URL}/items?page=2")])
        runtime = runtime_with(default, transports={"files": (files, FILES_URL)})
        document = self._link_document()
        document["operations"]["read"]["request"]["transport_ref"] = "files"

        with pytest.raises(ReadError, match="leaves its transport's origin"):
            await _read(runtime, document)
        assert default.calls == [], "the read must not reach the default's origin"

    async def test_a_link_within_the_reads_own_transport_is_followed(self) -> None:
        files = FakeSession(
            [self._linked(f"{FILES_URL}/items?page=2"), self._linked(None)]
        )
        runtime = runtime_with(
            FakeSession([]), transports={"files": (files, FILES_URL)}
        )
        document = self._link_document()
        document["operations"]["read"]["request"]["transport_ref"] = "files"

        await _read(runtime, document)

        assert files.calls[1]["url"] == f"{FILES_URL}/items?page=2"


class TestAWriteTransportIsJudgedAtTheHandshake:
    async def test_a_non_http_write_transport_refuses_the_schema(self) -> None:
        """Not at the first non-empty batch, after the stream was accepted."""
        connector = GenericAPIConnector()
        runtime = runtime_with(FakeSession([]))
        # A declared transport of another kind: an api operation has no
        # session to open from it.
        runtime._transport_specs["warehouse"] = {
            "transport_type": "sqlalchemy",
            "dsn": "postgresql+asyncpg://h/d",
        }
        connector.set_stream_endpoints(
            {"items": TestWriteDispatch._write_document("warehouse")}
        )
        await connector.connect(runtime)

        accepted = await connector.configure_schema(
            SchemaSpec(
                stream_id="items",
                version=1,
                write_mode=WriteMode.WRITE_MODE_INSERT,
                ack_timeout_seconds=30,
            )
        )
        assert not accepted
        assert "sqlalchemy" in (connector.last_schema_rejection or "")


class TestReadinessFollowsTheWrites:
    async def test_health_probes_the_transport_the_configured_stream_uses(
        self,
    ) -> None:
        """SERVING must not mean "the default answered" once streams are set.

        The records go where the write plans dispatch, so a probe of the
        default alone reports ready while the host that will receive them
        is down.
        """
        default = FakeSession([])
        # The retry client re-attempts a 500, so the host stays down for
        # every attempt it makes.
        files = FakeSession([FakeResponse(status=500, body={}) for _ in range(5)])
        connector = GenericAPIConnector()
        connector.set_stream_endpoints(
            {"items": TestWriteDispatch._write_document("files")}
        )
        runtime = runtime_with(default, transports={"files": (files, FILES_URL)})
        await connector.connect(runtime)
        assert await connector.configure_schema(
            SchemaSpec(
                stream_id="items",
                version=1,
                write_mode=WriteMode.WRITE_MODE_INSERT,
                ack_timeout_seconds=30,
            )
        ), connector.last_schema_rejection

        assert await connector.health_check() is False
        assert default.calls == [], "the default is not what this stream writes to"

    async def test_with_no_streams_configured_the_default_is_probed(self) -> None:
        """What a source has, and a destination before its first handshake."""
        default = FakeSession([FakeResponse(status=404, body={})])
        connector = GenericAPIConnector()
        await connector.connect(runtime_with(default))

        assert await connector.health_check() is True, "404 means it answered"
        assert default.calls[0]["url"] == BASE_URL


class TestOpeningATransportIsSerialised:
    async def test_concurrent_first_use_opens_one_sender_and_one_session(
        self,
    ) -> None:
        """Two streams reaching a named transport together must not race.

        The loser of an unguarded race is a live session and retry client
        nothing holds a reference to -- never closed, and pacing outside
        the rate limiter the winner shares.
        """
        connector = GenericAPIConnector()
        files = FakeSession([])
        runtime = runtime_with(
            FakeSession([]), transports={"files": (files, FILES_URL)}
        )
        await connector.connect(runtime)

        opened = await asyncio.gather(
            *(connector._dispatch_through("files") for _ in range(8))
        )

        assert len({id(d) for d in opened}) == 1, "one dispatch, shared by all"
        assert len(connector._dispatches) == 2, "the default and 'files', once each"


class TestTheRuntimeRefusesWhatItCannotOpen:
    async def test_a_ref_outside_the_resolved_set_names_the_set(self) -> None:
        runtime = runtime_with(FakeSession([]))
        with pytest.raises(TransportSpecError, match=r"this run resolved \['api'\]"):
            await runtime.http_transport("oauth")

    def test_header_names_are_read_per_transport_without_opening_one(self) -> None:
        """What an operation's own headers are judged against, and whose."""
        files = FakeSession([])
        files.headers["X-Files-Key"] = "k"
        default = FakeSession([])
        default.headers["Authorization"] = "Bearer t"
        runtime = runtime_with(default, transports={"files": (files, FILES_URL)})

        assert runtime.transport_header_names() == {"authorization"}
        assert runtime.transport_header_names("files") == {"x-files-key"}
