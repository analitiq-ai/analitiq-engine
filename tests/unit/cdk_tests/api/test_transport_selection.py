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

    async def test_a_link_onto_a_second_transport_is_fetched_through_it(
        self,
    ) -> None:
        """One system, two origins -- and the page goes out on the right one.

        Widening containment to the declared set is what lets this link be
        followed at all. Fetching it through the transport the read
        STARTED on would hand the api credential to the documents host and
        send none of the headers that host was declared with -- the leak
        the origin guard exists to prevent, walked through the widened
        gate.
        """
        default = FakeSession([self._linked(f"{FILES_URL}/items?page=2")])
        files = FakeSession([self._linked(None)])
        default.headers["Authorization"] = "Bearer api-token"
        files.headers["Authorization"] = "Bearer files-token"
        runtime = runtime_with(default, transports={"files": (files, FILES_URL)})

        await _read(runtime, self._link_document())

        assert len(default.calls) == 1, "the read's own request, and no more"
        assert files.calls[0]["url"] == f"{FILES_URL}/items?page=2"

    async def test_a_link_back_onto_the_reads_own_origin_stays_put(self) -> None:
        """Not a change of transport: the read continuing where it already is."""
        default = FakeSession(
            [self._linked(f"{BASE_URL}/items?page=2"), self._linked(None)]
        )
        runtime = runtime_with(
            default, transports={"files": (FakeSession([]), FILES_URL)}
        )

        await _read(runtime, self._link_document())

        assert default.calls[1]["url"] == f"{BASE_URL}/items?page=2"

    async def test_a_link_onto_an_origin_two_transports_serve_is_refused(
        self,
    ) -> None:
        """Nothing in the link says which transport's credentials to use."""
        default = FakeSession([self._linked("https://twin.example.test/v2/items")])
        runtime = runtime_with(
            default,
            transports={
                "one": (FakeSession([]), "https://twin.example.test/a"),
                "two": (FakeSession([]), "https://twin.example.test/b"),
            },
        )

        with pytest.raises(ReadError, match="2 declared transports serve"):
            await _read(runtime, self._link_document())

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


class TestHeadersAreJudgedAgainstTheTransportInUse:
    """An operation's headers answer to the transport it goes out on.

    Judging them against the default's names lets an endpoint on a named
    transport shadow that transport's credential, and refuses one for a
    header the transport it uses never sends. Both directions are the same
    defect: a fact about transport B read off transport A.
    """

    @staticmethod
    def _shadowing(transport_ref: str) -> dict[str, Any]:
        return endpoint_document(
            request={
                "method": "GET",
                "path": "/items",
                "transport_ref": transport_ref,
                "headers": {"Authorization": {"from_param": "token"}},
            },
            params={
                "token": {
                    "in": "header",
                    "type": "string",
                    "required": True,
                    "default": {"literal": "Bearer attacker"},
                }
            },
        )

    async def test_shadowing_the_named_transports_own_credential_is_refused(
        self,
    ) -> None:
        default, files = FakeSession([]), FakeSession([])
        files.headers["Authorization"] = "Bearer files-token"
        runtime = runtime_with(default, transports={"files": (files, FILES_URL)})

        with pytest.raises(ReadError, match="request.headers declares"):
            await _read(runtime, self._shadowing("files"))
        assert files.calls == [], "the request must not have gone out"

    async def test_a_header_only_the_default_sends_does_not_refuse_a_read_elsewhere(
        self,
    ) -> None:
        """The other direction: A's names must not judge an endpoint on B."""
        default, files = FakeSession([]), FakeSession([_page()])
        default.headers["Authorization"] = "Bearer api-token"
        runtime = runtime_with(default, transports={"files": (files, FILES_URL)})

        await _read(runtime, self._shadowing("files"))

        assert files.calls[0]["headers"]["Authorization"] == "Bearer attacker"


class TestHeadersMustBeSafeOnEveryTransportAPageMayUse:
    """A read's header map is bound once and sent on every page.

    A followed link can move a page onto another transport, so a name that
    is free on the opening transport and owned by the one a link lands on
    would silently override that transport's credential at the second
    origin.
    """

    @staticmethod
    def _document(*, paginated: bool) -> dict[str, Any]:
        pagination = (
            {"type": "link", "link": {"next_url": {"ref": "response.body.links.next"}}}
            if paginated
            else None
        )
        return endpoint_document(
            request={
                "method": "GET",
                "path": "/items",
                "headers": {"X-Files-Key": {"literal": "endpoint"}},
            },
            pagination=pagination,
        )

    async def test_a_link_read_may_not_shadow_a_continuation_transports_header(
        self,
    ) -> None:
        default, files = FakeSession([]), FakeSession([])
        files.headers["X-Files-Key"] = "connection"
        runtime = runtime_with(default, transports={"files": (files, FILES_URL)})

        with pytest.raises(ReadError, match="request.headers declares"):
            await _read(runtime, self._document(paginated=True))

    async def test_a_read_that_cannot_follow_links_keeps_the_header(self) -> None:
        """Its pages never leave the transport it opened, so nothing collides."""
        default, files = FakeSession([_page()]), FakeSession([])
        files.headers["X-Files-Key"] = "connection"
        runtime = runtime_with(default, transports={"files": (files, FILES_URL)})

        await _read(runtime, self._document(paginated=False))

        assert default.calls[0]["headers"]["X-Files-Key"] == "endpoint"


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

    async def test_the_base_urls_are_keyed_by_ref_so_a_link_can_select_one(
        self,
    ) -> None:
        runtime = runtime_with(
            FakeSession([]), transports={"files": (FakeSession([]), FILES_URL)}
        )
        assert runtime.http_transport_base_urls == {"api": BASE_URL, "files": FILES_URL}

    def test_header_names_are_read_per_transport_without_opening_one(self) -> None:
        """What an operation's own headers are judged against, and whose."""
        files = FakeSession([])
        files.headers["X-Files-Key"] = "k"
        default = FakeSession([])
        default.headers["Authorization"] = "Bearer t"
        runtime = runtime_with(default, transports={"files": (files, FILES_URL)})

        assert runtime.transport_header_names() == {"authorization"}
        assert runtime.transport_header_names("files") == {"x-files-key"}
