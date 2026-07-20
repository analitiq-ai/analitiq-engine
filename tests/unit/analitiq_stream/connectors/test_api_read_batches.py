"""Behavioural tests for :meth:`src.source.connectors.api.APIConnector.read_batches`.

The connector consumes the contract endpoint document directly. These
tests drive ``read_batches`` against a mocked aiohttp session and a
pre-materialized ``ConnectionRuntime`` to verify:

* request URL composition (base + endpoint path),
* every contract pagination strategy (``offset``, ``page``, ``cursor``,
  ``link``, ``keyset``) is executable, advances the way its block
  declares, and terminates on its declared ``stop_when`` predicate,
* the declared page size honours ``limit.default`` / ``limit.max``,
* incremental replication reads cursor from state manager and applies
  the safety window to the outgoing request params,
* non-200 responses raise :class:`ReadError`,
* missing ``endpoint_document`` raises :class:`ReadError`.

``read_batches`` now owns the connection lifecycle: it is handed the runtime
and connects/disconnects internally, so these tests pass the runtime directly
rather than calling ``connect()`` first.

No live HTTP. The session is a ``MagicMock`` with ``request`` returning
an async context manager that yields a stub response.
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pyarrow as pa
import pytest
from multidict import CIMultiDict

from cdk.connection_runtime import ConnectionRuntime
from cdk.secrets import InMemorySecretsResolver
from src.source.connectors.api import APIConnector
from src.source.connectors.base import ReadError, TransientReadError

# ---------------------------------------------------------------------------
# Fakes for the aiohttp session + state manager
# ---------------------------------------------------------------------------


class _FakeResponse:
    """Drop-in for the ``aiohttp`` response object used by the connector."""

    def __init__(
        self, *, status: int, body: Any, headers: dict[str, str] | None = None
    ):
        self.status = status
        self._body = body
        # The connector publishes response headers into the `response`
        # resolution scope, so link pagination can follow a `Link` header.
        self.headers = headers or {}

    async def __aenter__(self) -> _FakeResponse:
        return self

    async def __aexit__(self, *_exc) -> None:
        return None

    async def json(self, *, loads: Any = json.loads) -> Any:
        # aiohttp decodes the raw body text through ``loads``; model that so the
        # connector's custom decoder (e.g. parse_float=Decimal) actually runs.
        raw = self._body if isinstance(self._body, str) else json.dumps(self._body)
        return loads(raw)

    async def text(self) -> str:
        return json.dumps(self._body) if not isinstance(self._body, str) else self._body


class _FakeSession:
    """Records each ``request(...)`` call and returns the next queued response.

    The connector uses ``session.request(method, url, params=...)`` as an
    async context manager. We mirror that shape and capture each call so
    tests can assert on the URL + params actually sent.
    """

    def __init__(self, responses: list[_FakeResponse]):
        self._responses = list(responses)
        self.calls: list[tuple[str, str, dict[str, Any]]] = []
        # JSON body per request (None when the connector sent none),
        # parallel to ``calls``.
        self.bodies: list[Any] = []

    def request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any],
        json: Any = None,
        allow_redirects: bool = True,
    ):
        # The connector turns redirect-following off and handles 3xx itself, so
        # the fake has to accept the flag the real session takes.
        assert allow_redirects is False, "the connector must not follow redirects"
        self.calls.append((method, url, dict(params)))
        self.bodies.append(json)
        if not self._responses:
            raise AssertionError(f"unexpected extra request: {method} {url} {params}")
        return self._responses.pop(0)


def _runtime_with_session(
    session: _FakeSession, *, parameters: dict[str, Any] | None = None
) -> ConnectionRuntime:
    """Build a ``ConnectionRuntime`` whose transport is already
    materialized with ``session`` so ``connect()`` adopts it.
    """
    runtime = ConnectionRuntime(
        raw_config={"host": "https://api.example.test", "parameters": parameters or {}},
        connection_id="test-conn",
        connector_id="test-connector",
        connector_type="api",
        driver=None,
        resolver=InMemorySecretsResolver({}),
    )
    runtime._session = session
    runtime._base_url = "https://api.example.test"
    runtime._materialized = True
    return runtime


def _endpoint_doc_with_records(
    pagination: dict[str, Any] | None = None,
    replication: dict[str, Any] | None = None,
    extra_record_properties: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    request_body: Any = None,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    record_properties: dict[str, Any] = {
        "id": {"type": "integer", "arrow_type": "Int64"},
        "name": {"type": "string", "arrow_type": "Utf8"},
    }
    record_properties.update(extra_record_properties or {})
    read_block: dict[str, Any] = {
        "request": {"method": "GET", "path": "/items"},
        "response": {
            "schema": {
                "type": "object",
                "properties": {
                    "records": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": record_properties,
                        },
                    },
                },
            },
            "records": {"ref": "response.body.records"},
        },
    }
    if request_body is not None:
        read_block["request"]["body"] = request_body
    if params:
        read_block["params"] = params
    if metadata:
        read_block["response"]["metadata"] = metadata
    if pagination:
        read_block["pagination"] = pagination
    if replication:
        read_block["replication"] = replication
    return {
        "$schema": "https://schemas.analitiq.ai/api-endpoint/latest.json",
        "endpoint_id": "items",
        "operations": {"read": read_block},
    }


def _stream_source(
    *,
    replication_method: str = "full_refresh",
    cursor_field: str | None = None,
    safety_window: int | None = None,
) -> dict[str, Any]:
    block: dict[str, Any] = {
        "endpoint_ref": {
            "scope": "connector",
            "connection_id": "test-conn",
            "endpoint_id": "items",
        },
        "primary_keys": ["id"],
        "replication": {"method": replication_method},
    }
    if cursor_field:
        block["replication"]["cursor_field"] = cursor_field
    if safety_window is not None:
        block["replication"]["safety_window_seconds"] = safety_window
    return block


async def _consume(
    connector: APIConnector,
    runtime: ConnectionRuntime,
    *,
    config: dict[str, Any],
    state_manager: Any,
    stream_name: str,
    partition: dict[str, Any] | None = None,
    batch_size: int = 1000,
) -> list[pa.RecordBatch]:
    """Drive ``read_batches`` with the runtime it now owns.

    ``read_batches`` connects and disconnects internally, so callers pass the
    runtime directly (no prior ``connect()``); ``state_manager`` is forwarded as
    the ``checkpoint`` argument.
    """
    batches: list[pa.RecordBatch] = []
    async for batch in connector.read_batches(
        runtime,
        config,
        checkpoint=state_manager,
        stream_name=stream_name,
        partition=partition,
        batch_size=batch_size,
    ):
        batches.append(batch)
    return batches


# ---------------------------------------------------------------------------
# Happy path — no pagination
# ---------------------------------------------------------------------------


class TestReadBatchesNoPagination:
    @pytest.mark.asyncio
    async def test_single_request_yields_one_arrow_batch(self):
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={
                        "records": [
                            {"id": 1, "name": "alpha"},
                            {"id": 2, "name": "beta"},
                        ]
                    },
                ),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        batches = await _consume(
            connector,
            runtime,
            config={
                "endpoint_document": _endpoint_doc_with_records(),
                "stream_source": _stream_source(),
            },
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=100,
        )

        assert len(batches) == 1
        assert batches[0].num_rows == 2
        # Pin the Arrow dtypes from the endpoint schema — a regression that
        # silently downgraded ``id`` to a string would still pass the
        # ``to_pylist()`` value check below.
        assert batches[0].schema.field("id").type == pa.int64()
        assert batches[0].schema.field("name").type == pa.string()
        assert batches[0].column("id").to_pylist() == [1, 2]
        assert batches[0].column("name").to_pylist() == ["alpha", "beta"]
        assert len(session.calls) == 1
        method, url, params = session.calls[0]
        assert method == "GET"
        assert url == "https://api.example.test/items"


# ---------------------------------------------------------------------------
# Alternate records.ref shapes (#95)
# ---------------------------------------------------------------------------


def _endpoint_doc_with_ref(
    records_ref: str, response_schema: dict[str, Any]
) -> dict[str, Any]:
    """Endpoint document with an explicit ``records.ref`` + response schema."""
    return {
        "$schema": "https://schemas.analitiq.ai/api-endpoint/latest.json",
        "endpoint_id": "items",
        "operations": {
            "read": {
                "request": {"method": "GET", "path": "/items"},
                "response": {
                    "schema": response_schema,
                    "records": {"ref": records_ref},
                },
            },
        },
    }


_RECORD_ITEMS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "id": {"type": "integer", "arrow_type": "Int64"},
        "name": {"type": "string", "arrow_type": "Utf8"},
    },
}


class TestReadBatchesRecordsRefShapes:
    @pytest.mark.asyncio
    async def test_response_body_ref_reads_bare_array_body(self):
        # ``records.ref = "response.body"``: the response body IS the
        # record array, no wrapper field.
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body=[{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}],
                ),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_ref(
            "response.body",
            {"type": "array", "items": _RECORD_ITEMS_SCHEMA},
        )
        batches = await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=100,
        )

        assert len(batches) == 1
        assert batches[0].num_rows == 2
        assert batches[0].schema.field("id").type == pa.int64()
        assert batches[0].column("name").to_pylist() == ["alpha", "beta"]

    @pytest.mark.asyncio
    async def test_deep_ref_walks_nested_path(self):
        # ``records.ref = "response.body.data.items"``: both the schema
        # walk (data -> items) and the payload walk must follow the path.
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={"data": {"items": [{"id": 7, "name": "deep"}]}},
                ),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_ref(
            "response.body.data.items",
            {
                "type": "object",
                "properties": {
                    "data": {
                        "type": "object",
                        "properties": {
                            "items": {
                                "type": "array",
                                "items": _RECORD_ITEMS_SCHEMA,
                            },
                        },
                    },
                },
            },
        )
        batches = await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=100,
        )

        assert len(batches) == 1
        assert batches[0].column("id").to_pylist() == [7]
        assert batches[0].column("name").to_pylist() == ["deep"]

    @pytest.mark.asyncio
    async def test_ref_to_undeclared_field_raises_listing_available(self):
        # The ref points at a field the response schema does not declare:
        # fail before any HTTP request, naming the bad field and listing
        # what IS declared so the author can fix the endpoint document.
        session = _FakeSession([])
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records()
        endpoint["operations"]["read"]["response"]["records"][
            "ref"
        ] = "response.body.does_not_exist"
        with pytest.raises(
            ReadError,
            match=r"does_not_exist.*not declared under properties.*"
            r"available: \['records'\]",
        ):
            await _consume(
                connector,
                runtime,
                config={
                    "endpoint_document": endpoint,
                    "stream_source": _stream_source(),
                },
                state_manager=MagicMock(),
                stream_name="items",
                batch_size=100,
            )
        assert session.calls == []


# ---------------------------------------------------------------------------
# Offset pagination
# ---------------------------------------------------------------------------


class TestReadBatchesOffsetPagination:
    @pytest.mark.asyncio
    async def test_offset_loop_advances_by_effective_limit_and_stops_on_predicate(self):
        # batch_size=2; first page = 2 rows (full), second = 1 row. The stop
        # predicate — not a hardcoded short-page rule — ends the read.
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={"records": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]},
                ),
                _FakeResponse(status=200, body={"records": [{"id": 3, "name": "c"}]}),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "offset",
                "offset": {"param": "offset", "initial": 0},
                "limit": {"param": "limit"},
                "stop_when": {
                    "lt": [
                        {"ref": "response.record_count"},
                        {"ref": "runtime.batch_size"},
                    ]
                },
            },
        )
        batches = await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=2,
        )

        assert len(batches) == 2
        assert [b.num_rows for b in batches] == [2, 1]
        offsets = [c[2].get("offset") for c in session.calls]
        limits = [c[2].get("limit") for c in session.calls]
        assert offsets == [0, 2]
        assert limits == [2, 2]

    @pytest.mark.asyncio
    async def test_offset_honours_declared_increment_by(self):
        # A provider that counts offsets in pages, not records: increment_by
        # overrides the "advance by the effective limit" default.
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={"records": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]},
                ),
                _FakeResponse(status=200, body={"records": []}),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "offset",
                "offset": {"param": "start", "initial": 5, "increment_by": 1},
                "limit": {"param": "limit"},
                "stop_when": {"empty": {"ref": "response.body.records"}},
            },
        )
        await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=2,
        )

        assert [c[2].get("start") for c in session.calls] == [5, 6]

    @pytest.mark.asyncio
    async def test_limit_default_expression_is_clamped_to_declared_max(self):
        # The sevdesk/wise shape: default tracks the engine batch size, max is
        # the provider ceiling and wins.
        session = _FakeSession(
            [_FakeResponse(status=200, body={"records": [{"id": 1, "name": "a"}]})]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "offset",
                "offset": {"param": "offset", "initial": 0},
                "limit": {
                    "param": "limit",
                    "default": {"ref": "runtime.batch_size"},
                    "max": 100,
                },
                "stop_when": {
                    "lt": [{"ref": "response.record_count"}, {"literal": 100}]
                },
            },
        )
        await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=5000,
        )

        assert session.calls[0][2]["limit"] == 100

    @pytest.mark.asyncio
    async def test_pagination_without_stop_when_raises_read_error(self):
        # `stop_when` is contract-required on every strategy. Without it the
        # loop has no declared end, so it fails loud rather than guessing.
        session = _FakeSession(
            [_FakeResponse(status=200, body={"records": [{"id": 1, "name": "a"}]})]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "offset",
                "offset": {"param": "offset", "initial": 0},
                "limit": {"param": "limit"},
            },
        )
        with pytest.raises(ReadError, match="stop_when"):
            await _consume(
                connector,
                runtime,
                config={
                    "endpoint_document": endpoint,
                    "stream_source": _stream_source(),
                },
                state_manager=MagicMock(),
                stream_name="items",
                batch_size=2,
            )


# ---------------------------------------------------------------------------
# Page pagination
# ---------------------------------------------------------------------------


class TestReadBatchesPagePagination:
    @pytest.mark.asyncio
    async def test_page_loop_increments_page_and_stops_on_predicate(self):
        # Both pages are full; only the declared `has_more` flag ends the read,
        # which the old short-page heuristic could not express.
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={
                        "records": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}],
                        "has_more": True,
                    },
                ),
                _FakeResponse(
                    status=200,
                    body={
                        "records": [{"id": 3, "name": "c"}, {"id": 4, "name": "d"}],
                        "has_more": False,
                    },
                ),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "page",
                "page": {"param": "page", "initial": 1},
                "limit": {"param": "limit"},
                "stop_when": {
                    "eq": [{"ref": "response.body.has_more"}, {"literal": False}]
                },
            },
        )
        batches = await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=2,
        )

        assert [b.num_rows for b in batches] == [2, 2]
        assert [c[2].get("page") for c in session.calls] == [1, 2]

    @pytest.mark.asyncio
    async def test_full_last_page_is_not_truncated_without_a_limit_param(self):
        # Regression for the old `len(records) < batch_size` rule: with no
        # limit param the provider picks the page size, so a first page that
        # simply is not batch_size long used to end the read after one page.
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={"records": [{"id": 1, "name": "a"}], "has_more": True},
                ),
                _FakeResponse(
                    status=200,
                    body={"records": [{"id": 2, "name": "b"}], "has_more": False},
                ),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "page",
                "page": {"param": "page", "initial": 1},
                "stop_when": {
                    "eq": [{"ref": "response.body.has_more"}, {"literal": False}]
                },
            },
        )
        batches = await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=1000,
        )

        assert [b.num_rows for b in batches] == [1, 1]


# ---------------------------------------------------------------------------
# Cursor pagination
# ---------------------------------------------------------------------------


class TestReadBatchesCursorPagination:
    @pytest.mark.asyncio
    async def test_cursor_reads_the_declared_next_cursor_expression(self):
        # The declared field is `meta.continuation`, NOT `next_cursor`. The
        # body also carries a decoy literally named `next_cursor` — the old
        # hardcoded fallback read that one regardless of the declaration.
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={
                        "records": [{"id": 1, "name": "a"}],
                        "meta": {"continuation": "token-2"},
                        "next_cursor": "DECOY",
                    },
                ),
                _FakeResponse(
                    status=200,
                    body={
                        "records": [{"id": 2, "name": "b"}],
                        "meta": {},
                        "next_cursor": "DECOY",
                    },
                ),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "cursor",
                "cursor": {
                    "param": "page_token",
                    "next_cursor": {"ref": "response.body.meta.continuation"},
                },
                "limit": {"param": "limit"},
                "stop_when": {"missing": {"ref": "response.body.meta.continuation"}},
            },
        )
        batches = await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=10,
        )

        assert [b.num_rows for b in batches] == [1, 1]
        # First request has no token; second carries the declared field's
        # value, never the decoy.
        assert "page_token" not in session.calls[0][2]
        assert session.calls[1][2]["page_token"] == "token-2"

    @pytest.mark.asyncio
    async def test_cursor_without_next_cursor_raises_read_error(self):
        session = _FakeSession(
            [_FakeResponse(status=200, body={"records": [{"id": 1, "name": "a"}]})]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "cursor",
                "cursor": {"param": "page_token"},
                "stop_when": {"missing": {"ref": "response.body.next"}},
            },
        )
        with pytest.raises(ReadError, match="cursor.next_cursor"):
            await _consume(
                connector,
                runtime,
                config={
                    "endpoint_document": endpoint,
                    "stream_source": _stream_source(),
                },
                state_manager=MagicMock(),
                stream_name="items",
                batch_size=10,
            )


# ---------------------------------------------------------------------------
# Link pagination
# ---------------------------------------------------------------------------


class TestReadBatchesLinkPagination:
    @pytest.mark.asyncio
    async def test_link_follows_next_url_from_the_body(self):
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={
                        "records": [{"id": 1, "name": "a"}],
                        "next": "https://api.example.test/items?cursor=abc&limit=50",
                    },
                ),
                _FakeResponse(status=200, body={"records": [{"id": 2, "name": "b"}]}),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "link",
                "link": {"next_url": {"ref": "response.body.next"}},
                "stop_when": {"missing": {"ref": "response.body.next"}},
            },
        )
        batches = await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=10,
        )

        assert [b.num_rows for b in batches] == [1, 1]
        assert session.calls[0][1] == "https://api.example.test/items"
        # The follow-up goes to the provider's absolute URL, and the declared
        # params are not re-appended — the link already carries the query.
        assert session.calls[1][1] == (
            "https://api.example.test/items?cursor=abc&limit=50"
        )
        assert session.calls[1][2] == {}

    @pytest.mark.asyncio
    async def test_link_follows_next_url_from_a_response_header(self):
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={"records": [{"id": 1, "name": "a"}]},
                    headers={"X-Next-Page": "https://api.example.test/items?page=2"},
                ),
                _FakeResponse(status=200, body={"records": [{"id": 2, "name": "b"}]}),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "link",
                "link": {"next_url": {"ref": "response.headers.X-Next-Page"}},
                "stop_when": {"missing": {"ref": "response.headers.X-Next-Page"}},
            },
        )
        batches = await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=10,
        )

        assert [b.num_rows for b in batches] == [1, 1]
        assert session.calls[1][1] == "https://api.example.test/items?page=2"

    @pytest.mark.asyncio
    async def test_link_without_next_url_raises_read_error(self):
        session = _FakeSession(
            [_FakeResponse(status=200, body={"records": [{"id": 1, "name": "a"}]})]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "link",
                "link": {},
                "stop_when": {"missing": {"ref": "response.body.next"}},
            },
        )
        with pytest.raises(ReadError, match="link.next_url"):
            await _consume(
                connector,
                runtime,
                config={
                    "endpoint_document": endpoint,
                    "stream_source": _stream_source(),
                },
                state_manager=MagicMock(),
                stream_name="items",
                batch_size=10,
            )


# ---------------------------------------------------------------------------
# Keyset pagination
# ---------------------------------------------------------------------------


class TestReadBatchesKeysetPagination:
    @pytest.mark.asyncio
    async def test_keyset_advances_key_from_last_record_and_stops_on_predicate(self):
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={"records": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]},
                ),
                _FakeResponse(status=200, body={"records": [{"id": 3, "name": "c"}]}),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "keyset",
                "keyset": {"param": "after_id", "order_by_field": "id"},
                "limit": {"param": "limit"},
                "stop_when": {"lt": [{"ref": "response.record_count"}, {"literal": 2}]},
            },
        )
        batches = await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=2,
        )

        assert [b.num_rows for b in batches] == [2, 1]
        # First request carries no key; second carries the last id of page 1.
        # An int key stays native (yarl renders it faithfully); only Decimals
        # are stringified at the query boundary.
        assert "after_id" not in session.calls[0][2]
        assert session.calls[1][2]["after_id"] == 2

    @pytest.mark.asyncio
    async def test_keyset_initial_seeds_the_first_request(self):
        session = _FakeSession(
            [_FakeResponse(status=200, body={"records": [{"id": 8, "name": "h"}]})]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "keyset",
                "keyset": {
                    "param": "after_id",
                    "order_by_field": "id",
                    "initial": 7,
                },
                "limit": {"param": "limit"},
                "stop_when": {"lt": [{"ref": "response.record_count"}, {"literal": 2}]},
            },
        )
        await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=2,
        )

        assert session.calls[0][2]["after_id"] == 7

    @pytest.mark.asyncio
    async def test_keyset_order_by_field_walks_a_dotted_record_path(self):
        # The contract types order_by_field as a dotted record path, so the key
        # can live on a nested object rather than at the record's top level.
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={
                        "records": [
                            {"id": 1, "name": "a", "meta": {"seq": 11}},
                            {"id": 2, "name": "b", "meta": {"seq": 12}},
                        ]
                    },
                ),
                _FakeResponse(status=200, body={"records": [{"id": 3, "name": "c"}]}),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "keyset",
                "keyset": {"param": "after", "order_by_field": "meta.seq"},
                "limit": {"param": "limit"},
                "stop_when": {"lt": [{"ref": "response.record_count"}, {"literal": 2}]},
            },
            extra_record_properties={
                "meta": {
                    "type": "object",
                    "arrow_type": "Object",
                    "properties": {"seq": {"type": "integer", "arrow_type": "Int64"}},
                }
            },
        )
        await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=2,
        )

        assert session.calls[1][2]["after"] == 12

    @pytest.mark.asyncio
    async def test_keyset_without_order_by_field_raises_read_error(self):
        session = _FakeSession(
            [
                _FakeResponse(status=200, body={"records": [{"id": 1, "name": "a"}]}),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "keyset",
                "keyset": {"param": "after_id"},
                "stop_when": {"empty": {"ref": "response.body.records"}},
            },
        )
        with pytest.raises(ReadError, match="keyset.order_by_field"):
            await _consume(
                connector,
                runtime,
                config={
                    "endpoint_document": endpoint,
                    "stream_source": _stream_source(),
                },
                state_manager=MagicMock(),
                stream_name="items",
                batch_size=2,
            )


# ---------------------------------------------------------------------------
# Incremental replication
# ---------------------------------------------------------------------------


class TestReadBatchesIncrementalReplication:
    @pytest.mark.asyncio
    async def test_incremental_cursor_flows_into_params_with_safety_window(self):
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={"records": [{"id": 1, "updated_at": "2024-01-01T12:00:30Z"}]},
                ),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        # Prior cursor: 2024-01-01T12:00:00Z; safety window 60s -> 11:59:00Z
        state_manager = MagicMock()
        state_manager.get_cursor = AsyncMock(
            return_value={"cursor": "2024-01-01T12:00:00Z"}
        )
        state_manager.save_cursor = AsyncMock()

        endpoint = _endpoint_doc_with_records(
            replication={
                "cursor_mappings": [
                    {"cursor_field": "updated_at", "param": "since"},
                ],
            },
        )
        await _consume(
            connector,
            runtime,
            config={
                "endpoint_document": endpoint,
                "stream_source": _stream_source(
                    replication_method="incremental",
                    cursor_field="updated_at",
                    safety_window=60,
                ),
            },
            state_manager=state_manager,
            stream_name="items",
            partition={},
            batch_size=10,
        )

        assert len(session.calls) == 1
        params = session.calls[0][2]
        assert params["since"] == "2024-01-01T11:59:00Z"
        # The batch's last record advances the cursor through the minimal
        # CheckpointStore contract (the same shape the SQL connector saves).
        state_manager.save_cursor.assert_awaited_once_with(
            "items", {}, {"cursor": "2024-01-01T12:00:30Z"}
        )

    @pytest.mark.asyncio
    async def test_first_run_with_no_prior_cursor_skips_filter(self):
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={"records": [{"id": 1, "updated_at": "2024-01-01T00:00:00Z"}]},
                ),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        state_manager = MagicMock()
        state_manager.get_cursor = AsyncMock(return_value=None)
        state_manager.save_cursor = AsyncMock()

        endpoint = _endpoint_doc_with_records(
            replication={
                "cursor_mappings": [{"cursor_field": "updated_at", "param": "since"}]
            },
        )
        await _consume(
            connector,
            runtime,
            config={
                "endpoint_document": endpoint,
                "stream_source": _stream_source(
                    replication_method="incremental",
                    cursor_field="updated_at",
                    safety_window=60,
                ),
            },
            state_manager=state_manager,
            stream_name="items",
            partition={},
            batch_size=10,
        )

        # No cursor -> no ``since`` filter on the first request.
        assert "since" not in session.calls[0][2]


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------


class TestReadBatchesErrorPaths:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", [429, 500, 503])
    async def test_transient_status_raises_retryable_error(self, status):
        # Rate limits and upstream outages heal on retry — they must NOT
        # raise ReadError, which the worker classifies as fatal.
        session = _FakeSession(
            [
                _FakeResponse(status=status, body="server exploded"),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        with pytest.raises(TransientReadError, match=f"status {status}"):
            await _consume(
                connector,
                runtime,
                config={
                    "endpoint_document": _endpoint_doc_with_records(),
                    "stream_source": _stream_source(),
                },
                state_manager=MagicMock(),
                stream_name="items",
                batch_size=10,
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", [400, 401, 403, 404])
    async def test_deterministic_status_raises_read_error(self, status):
        session = _FakeSession(
            [
                _FakeResponse(status=status, body="bad request"),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        with pytest.raises(ReadError, match=f"status {status}"):
            await _consume(
                connector,
                runtime,
                config={
                    "endpoint_document": _endpoint_doc_with_records(),
                    "stream_source": _stream_source(),
                },
                state_manager=MagicMock(),
                stream_name="items",
                batch_size=10,
            )

    @pytest.mark.asyncio
    async def test_missing_endpoint_document_raises(self):
        session = _FakeSession([])
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        with pytest.raises(ReadError, match="missing 'endpoint_document'"):
            await _consume(
                connector,
                runtime,
                config={"stream_source": _stream_source()},
                state_manager=MagicMock(),
                stream_name="items",
                batch_size=10,
            )

    @pytest.mark.asyncio
    async def test_missing_stream_source_endpoint_ref_raises(self):
        session = _FakeSession([])
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        with pytest.raises(ReadError, match="stream_source missing 'endpoint_ref'"):
            await _consume(
                connector,
                runtime,
                config={
                    "endpoint_document": _endpoint_doc_with_records(),
                    "stream_source": {"primary_keys": ["id"]},
                },
                state_manager=MagicMock(),
                stream_name="items",
                batch_size=10,
            )


# ---------------------------------------------------------------------------
# Connection lifecycle — read_batches owns acquire/release
# ---------------------------------------------------------------------------


class TestReadBatchesLifecycle:
    @pytest.mark.asyncio
    async def test_runtime_released_when_consumer_stops_early(self):
        # The runtime is acquired on entry (ref_count 1) and released in the
        # generator's finally when the consumer closes it mid-stream.
        session = _FakeSession(
            [_FakeResponse(status=200, body={"records": [{"id": 1, "name": "a"}]})]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        agen = connector.read_batches(
            runtime,
            config={
                "endpoint_document": _endpoint_doc_with_records(),
                "stream_source": _stream_source(),
            },
            checkpoint=MagicMock(),
            stream_name="items",
            batch_size=10,
        )
        first = await agen.__anext__()
        assert first.num_rows == 1
        assert runtime._ref_count == 1  # acquired on connect()
        assert connector.is_connected is True

        await agen.aclose()  # consumer stops early -> finally disconnects

        assert runtime._ref_count == 0
        assert connector.is_connected is False

    @pytest.mark.asyncio
    async def test_runtime_released_when_connect_fails(self):
        # connect() runs inside read_batches' try, so a materialize failure
        # still reaches disconnect() and releases the acquired ref.
        from src.source.connectors.base import ConnectionError as ApiConnectionError

        runtime = _runtime_with_session(_FakeSession([]))

        async def _boom(*_args, **_kwargs):
            raise RuntimeError("materialize boom")

        runtime.materialize = _boom  # type: ignore[method-assign]

        connector = APIConnector("test")
        with pytest.raises(ApiConnectionError):
            await _consume(
                connector,
                runtime,
                config={
                    "endpoint_document": _endpoint_doc_with_records(),
                    "stream_source": _stream_source(),
                },
                state_manager=MagicMock(),
                stream_name="items",
                batch_size=10,
            )

        assert runtime._ref_count == 0  # acquired then released in finally
        assert connector.is_connected is False


# ---------------------------------------------------------------------------
# Declared params: value-expression defaults (#166)
# ---------------------------------------------------------------------------


class TestReadBatchesParamDefaults:
    @pytest.mark.asyncio
    async def test_function_form_default_resolves(self):
        # The pre-#166 lightweight resolver only knew literal/ref/template;
        # a function-form default fell through as the raw expression dict.
        import base64

        session = _FakeSession(
            [_FakeResponse(status=200, body={"records": [{"id": 1, "name": "a"}]})]
        )
        runtime = _runtime_with_session(session, parameters={"api_token": "tok-123"})
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records()
        endpoint["operations"]["read"]["params"] = {
            "auth": {
                "in": "query",
                "type": "string",
                "required": True,
                "default": {
                    "function": "base64_encode",
                    "input": {"ref": "connection.parameters.api_token"},
                },
            },
        }
        await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=10,
        )

        params = session.calls[0][2]
        assert params["auth"] == base64.b64encode(b"tok-123").decode("ascii")

    @pytest.mark.asyncio
    async def test_unresolved_default_omits_param(self):
        # Contract rule 7: a default that cannot resolve omits the
        # parameter instead of sending the raw expression structure.
        session = _FakeSession(
            [_FakeResponse(status=200, body={"records": [{"id": 1, "name": "a"}]})]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records()
        endpoint["operations"]["read"]["params"] = {
            "team": {
                "in": "query",
                "type": "string",
                "required": False,
                "default": {"ref": "connection.parameters.missing_team"},
            },
        }
        await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=10,
        )

        assert "team" not in session.calls[0][2]

    @pytest.mark.asyncio
    async def test_runtime_batch_size_ref_resolves_to_effective_page_size(self):
        # ``runtime.batch_size`` is the effective page size driving the
        # pagination loops — the ``batch_size`` argument, not a config key.
        session = _FakeSession(
            [_FakeResponse(status=200, body={"records": [{"id": 1, "name": "a"}]})]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records()
        endpoint["operations"]["read"]["params"] = {
            "page_size": {
                "in": "query",
                "type": "integer",
                "required": False,
                "default": {"ref": "runtime.batch_size"},
            },
        }
        await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=250,
        )

        assert session.calls[0][2]["page_size"] == 250

    @pytest.mark.asyncio
    async def test_template_default_with_missing_placeholder_is_kept_partial(self):
        # Plain template defaults resolve leniently: the missing placeholder
        # renders empty and the partially-resolved param still goes out.
        session = _FakeSession(
            [_FakeResponse(status=200, body={"records": [{"id": 1, "name": "a"}]})]
        )
        runtime = _runtime_with_session(session, parameters={"org": "acme"})
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records()
        endpoint["operations"]["read"]["params"] = {
            "scope": {
                "in": "query",
                "type": "string",
                "required": False,
                "default": {
                    "template": (
                        "${connection.parameters.org}/" "${connection.parameters.gone}"
                    )
                },
            },
        }
        await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=10,
        )

        assert session.calls[0][2]["scope"] == "acme/"


# ---------------------------------------------------------------------------
# Declared read request body (#166)
# ---------------------------------------------------------------------------


class TestReadBatchesRequestBody:
    @pytest.mark.asyncio
    async def test_declared_body_is_resolved_and_sent(self):
        # POST-read endpoints declare ``request.body``; expression nodes
        # inside it resolve against the connection scopes, unresolved
        # fields are omitted, and the result rides as the JSON body.
        session = _FakeSession(
            [_FakeResponse(status=200, body={"records": [{"id": 1, "name": "a"}]})]
        )
        runtime = _runtime_with_session(session, parameters={"region": "eu"})
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records()
        endpoint["operations"]["read"]["request"] = {
            "method": "POST",
            "path": "/items/search",
            "body": {
                "region": {"ref": "connection.parameters.region"},
                "missing": {"ref": "connection.parameters.not_there"},
                "static": "all",
            },
        }
        await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=10,
        )

        method, url, _ = session.calls[0]
        assert method == "POST"
        assert url == "https://api.example.test/items/search"
        assert session.bodies[0] == {"region": "eu", "static": "all"}

    @pytest.mark.asyncio
    async def test_no_declared_body_sends_none(self):
        session = _FakeSession(
            [_FakeResponse(status=200, body={"records": [{"id": 1, "name": "a"}]})]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        await _consume(
            connector,
            runtime,
            config={
                "endpoint_document": _endpoint_doc_with_records(),
                "stream_source": _stream_source(),
            },
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=10,
        )

        assert session.bodies == [None]

    @pytest.mark.asyncio
    async def test_body_is_sent_on_every_page(self):
        # Pagination loops must carry the body on each page request, not
        # just the first.
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={"records": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]},
                ),
                _FakeResponse(status=200, body={"records": [{"id": 3, "name": "c"}]}),
            ]
        )
        runtime = _runtime_with_session(session, parameters={"region": "eu"})
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "offset",
                "offset": {"param": "offset", "initial": 0},
                "limit": {"param": "limit"},
                "stop_when": {
                    "lt": [
                        {"ref": "response.record_count"},
                        {"ref": "runtime.batch_size"},
                    ]
                },
            },
        )
        endpoint["operations"]["read"]["request"] = {
            "method": "POST",
            "path": "/items/search",
            "body": {"region": {"ref": "connection.parameters.region"}},
        }
        await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=2,
        )

        assert session.bodies == [{"region": "eu"}, {"region": "eu"}]

    @pytest.mark.asyncio
    async def test_from_param_binds_resolved_param_into_body(self):
        # Bodies may mix literals with {"from_param": ...} per the contract;
        # a param declared ``in: body`` lands in the body via its binding
        # and stays out of the query string.
        session = _FakeSession(
            [_FakeResponse(status=200, body={"records": [{"id": 1, "name": "a"}]})]
        )
        runtime = _runtime_with_session(session, parameters={"team_id": "t-9"})
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records()
        endpoint["operations"]["read"]["params"] = {
            "team": {
                "in": "body",
                "type": "string",
                "required": True,
                "default": {"ref": "connection.parameters.team_id"},
            },
            "verbose": {
                "in": "query",
                "type": "boolean",
                "required": False,
                "default": {"literal": True},
            },
        }
        endpoint["operations"]["read"]["request"] = {
            "method": "POST",
            "path": "/items/search",
            "body": {"filter": {"team": {"from_param": "team"}}},
        }
        await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=10,
        )

        params = session.calls[0][2]
        assert params == {"verbose": True}  # in: body param not in query
        assert session.bodies[0] == {"filter": {"team": "t-9"}}

    @pytest.mark.asyncio
    async def test_from_param_for_missing_param_drops_field(self):
        # A from_param naming a param with no resolved value binds None,
        # which the expression pass omits — never the raw binding dict.
        session = _FakeSession(
            [_FakeResponse(status=200, body={"records": [{"id": 1, "name": "a"}]})]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records()
        endpoint["operations"]["read"]["request"] = {
            "method": "POST",
            "path": "/items/search",
            "body": {"q": {"from_param": "undeclared"}, "static": "all"},
        }
        await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=10,
        )

        assert session.bodies[0] == {"static": "all"}

    @pytest.mark.asyncio
    async def test_replication_param_binds_into_body(self):
        # Incremental POST-search endpoints carry the cursor filter in the
        # body: the replication-derived param value must reach from_param.
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={"records": [{"id": 1, "updated_at": "2024-01-01T12:00:30Z"}]},
                ),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        state_manager = MagicMock()
        state_manager.get_cursor = AsyncMock(
            return_value={"cursor": "2024-01-01T12:00:00Z"}
        )
        state_manager.save_cursor = AsyncMock()

        endpoint = _endpoint_doc_with_records(
            replication={
                "cursor_mappings": [
                    {"cursor_field": "updated_at", "param": "since"},
                ],
            },
        )
        endpoint["operations"]["read"]["request"] = {
            "method": "POST",
            "path": "/items/search",
            "body": {"updated_after": {"from_param": "since"}},
        }
        await _consume(
            connector,
            runtime,
            config={
                "endpoint_document": endpoint,
                "stream_source": _stream_source(
                    replication_method="incremental",
                    cursor_field="updated_at",
                    safety_window=60,
                ),
            },
            state_manager=state_manager,
            stream_name="items",
            partition={},
            batch_size=10,
        )

        assert session.bodies[0] == {"updated_after": "2024-01-01T11:59:00Z"}

    @pytest.mark.asyncio
    async def test_body_paginated_endpoint_advances_offset_in_body(self):
        # Pagination params declared ``in: body`` ride inside the body via
        # from_param — rebuilt per page so the offset actually advances —
        # and never appear in the query string.
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={"records": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]},
                ),
                _FakeResponse(status=200, body={"records": [{"id": 3, "name": "c"}]}),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "offset",
                "offset": {"param": "offset", "initial": 0},
                "limit": {"param": "limit"},
                "stop_when": {
                    "lt": [
                        {"ref": "response.record_count"},
                        {"ref": "runtime.batch_size"},
                    ]
                },
            },
        )
        endpoint["operations"]["read"]["params"] = {
            "offset": {
                "in": "body",
                "type": "integer",
                "required": True,
                "controlled_by": "pagination",
            },
            "limit": {
                "in": "body",
                "type": "integer",
                "required": True,
                "controlled_by": "pagination",
            },
        }
        endpoint["operations"]["read"]["request"] = {
            "method": "POST",
            "path": "/items/search",
            "body": {
                "paging": {
                    "offset": {"from_param": "offset"},
                    "limit": {"from_param": "limit"},
                },
            },
        }
        await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=2,
        )

        # Query strings carry neither pagination param.
        assert [c[2] for c in session.calls] == [{}, {}]
        # The body advances per page.
        assert session.bodies == [
            {"paging": {"offset": 0, "limit": 2}},
            {"paging": {"offset": 2, "limit": 2}},
        ]


# ---------------------------------------------------------------------------
# Decimal precision at the JSON boundary (#288)
# ---------------------------------------------------------------------------


_HIGH_PRECISION_RECORDS_SCHEMA: dict[str, Any] = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            # 18 significant digits -- more than a float64 can represent, so a
            # default json.loads would round it before Arrow ever sees it.
            "amount": {"type": "number", "arrow_type": "Decimal128(20,8)"},
            "rate": {"type": "number", "arrow_type": "Float64"},
        },
    },
}


class TestReadBatchesDecimalPrecision:
    @pytest.mark.asyncio
    async def test_decimal_column_keeps_exact_source_digits(self):
        # Body is delivered as raw JSON text (not a pre-parsed dict) so the
        # connector's decoder governs how the numeric token is parsed -- the
        # whole point of the fix is to parse it losslessly.
        body = '[{"amount": 1234567890.12345678, "rate": 3.14159265358979}]'
        session = _FakeSession([_FakeResponse(status=200, body=body)])
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_ref(
            "response.body", _HIGH_PRECISION_RECORDS_SCHEMA
        )
        batches = await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=100,
        )

        assert len(batches) == 1
        amount = batches[0].column("amount").to_pylist()[0]
        # Every digit survives the boundary -- the value the default parser
        # would have rounded to 1234567890.1234567.
        assert amount == Decimal("1234567890.12345678")
        # The Float64 column still narrows to a double without erroring on the
        # Decimal the lossless parse produced.
        assert batches[0].schema.field("rate").type == pa.float64()
        assert batches[0].column("rate").to_pylist()[0] == pytest.approx(
            3.14159265358979
        )

    @pytest.mark.asyncio
    async def test_keyset_decimal_key_sent_with_full_precision(self):
        # A fractional keyset key parses to Decimal; it must reach the next
        # request as its full-precision string. yarl would otherwise truncate
        # the Decimal to its integer part and silently skip rows.
        page1 = '{"records": [{"score": 10.5}, {"score": 1234567890.12345678}]}'
        page2 = '{"records": [{"score": 1234567890.99999999}]}'
        session = _FakeSession(
            [
                _FakeResponse(status=200, body=page1),
                _FakeResponse(status=200, body=page2),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_ref(
            "response.body.records",
            {
                "type": "object",
                "properties": {
                    "records": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "score": {"type": "number", "arrow_type": "Float64"},
                            },
                        },
                    },
                },
            },
        )
        endpoint["operations"]["read"]["pagination"] = {
            "type": "keyset",
            "keyset": {"param": "after", "order_by_field": "score"},
            "limit": {"param": "limit"},
            "stop_when": {
                "lt": [{"ref": "response.record_count"}, {"ref": "runtime.batch_size"}]
            },
        }
        await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=2,
        )

        assert "after" not in session.calls[0][2]
        assert session.calls[1][2]["after"] == "1234567890.12345678"

    @pytest.mark.asyncio
    async def test_keyset_body_param_keeps_native_numeric_type(self):
        # A keyset param declared ``in: body`` must reach the JSON body as its
        # native type. Stringifying it (to dodge yarl in the query) would send
        # an integer key as a string, which a numeric body schema can reject.
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={"records": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]},
                ),
                _FakeResponse(status=200, body={"records": [{"id": 3, "name": "c"}]}),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "keyset",
                "keyset": {"param": "after", "order_by_field": "id"},
                "limit": {"param": "limit"},
                "stop_when": {
                    "lt": [
                        {"ref": "response.record_count"},
                        {"ref": "runtime.batch_size"},
                    ]
                },
            },
        )
        endpoint["operations"]["read"]["params"] = {
            "after": {"in": "body", "type": "integer", "required": False},
        }
        endpoint["operations"]["read"]["request"] = {
            "method": "POST",
            "path": "/items/search",
            "body": {"after": {"from_param": "after"}},
        }
        await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=2,
        )

        # Body param stays out of the query and keeps its native int type.
        assert "after" not in session.calls[1][2]
        assert session.bodies[1] == {"after": 2}

    @pytest.mark.asyncio
    async def test_keyset_body_param_narrows_fractional_decimal_to_float(self):
        # A fractional keyset key parses to Decimal. In the body it must become
        # a float (a JSON number) -- stdlib json.dumps, which aiohttp uses for
        # the body, cannot serialize a Decimal and would raise on page 2.
        page1 = '{"records": [{"score": 1.5}, {"score": 2.5}]}'
        page2 = '{"records": [{"score": 3.5}]}'
        session = _FakeSession(
            [
                _FakeResponse(status=200, body=page1),
                _FakeResponse(status=200, body=page2),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_ref(
            "response.body.records",
            {
                "type": "object",
                "properties": {
                    "records": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "score": {"type": "number", "arrow_type": "Float64"},
                            },
                        },
                    },
                },
            },
        )
        endpoint["operations"]["read"]["params"] = {
            "after": {"in": "body", "type": "number", "required": False},
        }
        endpoint["operations"]["read"]["request"] = {
            "method": "POST",
            "path": "/items/search",
            "body": {"after": {"from_param": "after"}},
        }
        endpoint["operations"]["read"]["pagination"] = {
            "type": "keyset",
            "keyset": {"param": "after", "order_by_field": "score"},
            "limit": {"param": "limit"},
            "stop_when": {
                "lt": [{"ref": "response.record_count"}, {"ref": "runtime.batch_size"}]
            },
        }
        await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=2,
        )

        body = session.bodies[1]
        assert body == {"after": 2.5}
        assert isinstance(body["after"], float)


# ---------------------------------------------------------------------------
# Review regressions: every case below silently lost records before the fix
# ---------------------------------------------------------------------------


class TestPaginationSilentTruncationRegressions:
    @pytest.mark.asyncio
    async def test_offset_steps_by_records_returned_not_the_requested_size(self):
        """A provider that ignores the requested page size must not skip records.

        The endpoint declares no limit param, so the provider picks the page
        size (2 here) while the engine's batch_size is 10. Stepping by the
        batch size would request offsets 0, 10, 20 and punch a hole through
        records 3-10 with no error at all.
        """
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={"records": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]},
                ),
                _FakeResponse(
                    status=200,
                    body={"records": [{"id": 3, "name": "c"}, {"id": 4, "name": "d"}]},
                ),
                _FakeResponse(status=200, body={"records": []}),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "offset",
                "offset": {"param": "offset", "initial": 0},
                "stop_when": {"empty": {"ref": "response.body.records"}},
            },
        )
        batches = await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=10,
        )

        assert [c[2].get("offset") for c in session.calls] == [0, 2, 4]
        assert [r["id"] for b in batches for r in b.to_pylist()] == [1, 2, 3, 4]

    @pytest.mark.asyncio
    async def test_offset_declared_increment_by_still_wins(self):
        # A page-counting provider declares increment_by explicitly; the
        # records-returned default must not override that.
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={"records": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]},
                ),
                _FakeResponse(status=200, body={"records": []}),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "offset",
                "offset": {"param": "page", "initial": 0, "increment_by": 1},
                "stop_when": {"empty": {"ref": "response.body.records"}},
            },
        )
        await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=10,
        )

        assert [c[2].get("page") for c in session.calls] == [0, 1]

    @pytest.mark.parametrize("strategy", ["offset", "page"])
    @pytest.mark.asyncio
    async def test_zero_increment_by_is_rejected(self, strategy):
        # A zero step re-requests the same page forever, re-emitting the same
        # records to the destination on every iteration.
        session = _FakeSession(
            [_FakeResponse(status=200, body={"records": [{"id": 1, "name": "a"}]})]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": strategy,
                strategy: {"param": "p", "initial": 0, "increment_by": 0},
                "stop_when": {"empty": {"ref": "response.body.records"}},
            },
        )
        with pytest.raises(ReadError, match="must be >= 1"):
            await _consume(
                connector,
                runtime,
                config={
                    "endpoint_document": endpoint,
                    "stream_source": _stream_source(),
                },
                state_manager=MagicMock(),
                stream_name="items",
                batch_size=10,
            )

    @pytest.mark.asyncio
    async def test_link_follows_a_header_whose_casing_differs_from_the_declaration(
        self,
    ):
        """HTTP header names are case-insensitive; the response scope must be too.

        The endpoint declares `response.headers.Link`; the provider sends
        `link`. A case-sensitive copy makes the ref unresolvable, `missing`
        returns True, and the read ends after page 1 with no error.
        """
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={"records": [{"id": 1, "name": "a"}]},
                    headers=CIMultiDict({"link": "https://api.example.test/items?p=2"}),
                ),
                _FakeResponse(status=200, body={"records": [{"id": 2, "name": "b"}]}),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "link",
                "link": {"next_url": {"ref": "response.headers.Link"}},
                "stop_when": {"missing": {"ref": "response.headers.Link"}},
            },
        )
        batches = await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=10,
        )

        assert [b.num_rows for b in batches] == [1, 1]
        assert session.calls[1][1] == "https://api.example.test/items?p=2"

    @pytest.mark.asyncio
    async def test_stop_when_reads_unresolved_metadata_as_missing(self):
        """`missing` on a metadata key must agree with the same ref written direct.

        Resolving metadata key-by-key leaves an unresolved expression in the
        scope as a present key holding None, so `missing` answers False and the
        declared stop condition never fires — the loop then runs until the
        provider happens to return an empty page.
        """
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={
                        "records": [{"id": 1, "name": "a"}],
                        "paging": {"next": "p2"},
                    },
                ),
                _FakeResponse(
                    status=200,
                    body={"records": [{"id": 2, "name": "b"}], "paging": {}},
                ),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "page",
                "page": {"param": "page", "initial": 1},
                "stop_when": {"missing": {"ref": "response.metadata.next_page"}},
            },
            metadata={"next_page": {"ref": "response.body.paging.next"}},
        )
        batches = await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=10,
        )

        # Page 2's `paging.next` is gone, so the predicate stops the read
        # there rather than requesting a third page the fake would reject.
        assert [b.num_rows for b in batches] == [1, 1]

    @pytest.mark.asyncio
    async def test_metadata_resolves_into_the_response_scope(self):
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={"records": [{"id": 1, "name": "a"}], "total": 1},
                ),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "page",
                "page": {"param": "page", "initial": 1},
                "stop_when": {
                    "gte": [
                        {"ref": "response.record_count"},
                        {"ref": "response.metadata.total"},
                    ]
                },
            },
            metadata={"total": {"ref": "response.body.total"}},
        )
        batches = await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=10,
        )

        assert [b.num_rows for b in batches] == [1]

    @pytest.mark.asyncio
    async def test_keyset_order_by_field_absent_from_records_raises(self):
        """A mis-declared order_by_field truncated every read to one page.

        `walk_path` cannot tell "field absent" from "field null", so a typo
        used to end the read silently and permanently.
        """
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={"records": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]},
                ),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "keyset",
                "keyset": {"param": "after", "order_by_field": "ident"},
                "stop_when": {"empty": {"ref": "response.body.records"}},
            },
        )
        with pytest.raises(ReadError, match="carries no 'ident' field"):
            await _consume(
                connector,
                runtime,
                config={
                    "endpoint_document": endpoint,
                    "stream_source": _stream_source(),
                },
                state_manager=MagicMock(),
                stream_name="items",
                batch_size=10,
            )

    @pytest.mark.asyncio
    async def test_keyset_null_order_key_ends_the_read_without_raising(self):
        # Present but null is the provider having no further key, not a
        # mis-declaration — it ends the read rather than failing.
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={
                        "records": [
                            {"id": 1, "name": "a"},
                            {"id": None, "name": "b"},
                        ]
                    },
                ),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "keyset",
                "keyset": {"param": "after", "order_by_field": "id"},
                "stop_when": {"empty": {"ref": "response.body.records"}},
            },
        )
        batches = await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=10,
        )

        assert [b.num_rows for b in batches] == [2]

    @pytest.mark.asyncio
    async def test_declared_but_unresolvable_initial_raises(self):
        # Substituting a default would start the read somewhere the endpoint
        # never declared, changing which records are returned invisibly.
        session = _FakeSession(
            [_FakeResponse(status=200, body={"records": [{"id": 1, "name": "a"}]})]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "page",
                "page": {"param": "page", "initial": {"ref": "state.last_page"}},
                "stop_when": {"empty": {"ref": "response.body.records"}},
            },
        )
        with pytest.raises(ReadError, match="declared but resolved to no value"):
            await _consume(
                connector,
                runtime,
                config={
                    "endpoint_document": endpoint,
                    "stream_source": _stream_source(),
                },
                state_manager=MagicMock(),
                stream_name="items",
                batch_size=10,
            )

    @pytest.mark.asyncio
    async def test_empty_first_page_is_reported(self, caplog):
        # A records.ref that addresses nothing and a genuinely empty source
        # are indistinguishable from the outside; the log must name both.
        session = _FakeSession(
            [_FakeResponse(status=200, body={"records": None, "error": "quota"})]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "offset",
                "offset": {"param": "offset", "initial": 0},
                "stop_when": {"empty": {"ref": "response.body.records"}},
            },
        )
        with caplog.at_level("WARNING"):
            batches = await _consume(
                connector,
                runtime,
                config={
                    "endpoint_document": endpoint,
                    "stream_source": _stream_source(),
                },
                state_manager=MagicMock(),
                stream_name="items",
                batch_size=10,
            )

        assert batches == []
        assert "first page carried no records" in caplog.text
        assert "response.body.records" in caplog.text

    @pytest.mark.asyncio
    async def test_stop_when_sees_the_clamped_page_size_not_the_batch_size(self):
        """`runtime.batch_size` must report the page size actually requested.

        `limit.max` clamps the engine's batch_size of 10 down to 2, so every
        full page holds 2 records. The standard stop idiom asks "did this page
        come back short of what we asked for". Against the unclamped batch
        size that reads 2 < 10 on the very first page and truncates the stream
        to a fifth of its records, silently.
        """
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={"records": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]},
                ),
                _FakeResponse(
                    status=200,
                    body={"records": [{"id": 3, "name": "c"}, {"id": 4, "name": "d"}]},
                ),
                _FakeResponse(status=200, body={"records": [{"id": 5, "name": "e"}]}),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "offset",
                "offset": {"param": "offset", "initial": 0},
                "limit": {"param": "limit", "max": 2},
                "stop_when": {
                    "lt": [
                        {"ref": "response.record_count"},
                        {"ref": "runtime.batch_size"},
                    ]
                },
            },
        )
        batches = await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=10,
        )

        assert [r["id"] for b in batches for r in b.to_pylist()] == [1, 2, 3, 4, 5]
        # The clamp reached the wire too, not just the predicate.
        assert [c[2].get("limit") for c in session.calls] == [2, 2, 2]

    @pytest.mark.asyncio
    async def test_fractional_pagination_setting_is_rejected_not_truncated(self):
        """A non-whole offset would shift every later page boundary in silence."""
        session = _FakeSession(
            [_FakeResponse(status=200, body={"records": [{"id": 1, "name": "a"}]})]
        )
        runtime = _runtime_with_session(session, parameters={"start": 10.9})
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "offset",
                "offset": {
                    "param": "offset",
                    "initial": {"ref": "connection.parameters.start"},
                },
                "stop_when": {"empty": {"ref": "response.body.records"}},
            },
        )
        with pytest.raises(ReadError, match="whole number"):
            await _consume(
                connector,
                runtime,
                config={
                    "endpoint_document": endpoint,
                    "stream_source": _stream_source(),
                },
                state_manager=MagicMock(),
                stream_name="items",
                batch_size=10,
            )

    @pytest.mark.asyncio
    async def test_link_refuses_a_cross_origin_next_url(self):
        """A provider must not be able to redirect the read at another host.

        The session carries this connection's auth headers on every request it
        makes, so following a provider-named foreign origin would hand the
        credentials over.
        """
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={
                        "records": [{"id": 1, "name": "a"}],
                        "next": "https://evil.example.com/items?page=2",
                    },
                ),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "link",
                "link": {"next_url": {"ref": "response.body.next"}},
                "stop_when": {"missing": {"ref": "response.body.next"}},
            },
        )
        with pytest.raises(ReadError, match="outside the endpoint origin"):
            await _consume(
                connector,
                runtime,
                config={
                    "endpoint_document": endpoint,
                    "stream_source": _stream_source(),
                },
                state_manager=MagicMock(),
                stream_name="items",
                batch_size=10,
            )
        # Only the first page was ever requested.
        assert len(session.calls) == 1

    @pytest.mark.asyncio
    async def test_link_follows_the_same_origin_spelled_differently(self):
        """Case and an explicit default port name the same origin, so it is followed."""
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={
                        "records": [{"id": 1, "name": "a"}],
                        "next": "HTTPS://API.EXAMPLE.TEST:443/items?page=2",
                    },
                ),
                _FakeResponse(status=200, body={"records": [{"id": 2, "name": "b"}]}),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "link",
                "link": {"next_url": {"ref": "response.body.next"}},
                "stop_when": {"missing": {"ref": "response.body.next"}},
            },
        )
        batches = await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=10,
        )

        assert [r["id"] for b in batches for r in b.to_pylist()] == [1, 2]

    @pytest.mark.asyncio
    async def test_link_rejects_a_relative_next_url(self):
        """A relative link cannot be origin-checked, so it fails by name."""
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={
                        "records": [{"id": 1, "name": "a"}],
                        "next": "/items?page=2",
                    },
                ),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "link",
                "link": {"next_url": {"ref": "response.body.next"}},
                "stop_when": {"missing": {"ref": "response.body.next"}},
            },
        )
        with pytest.raises(ReadError, match="not an absolute URL"):
            await _consume(
                connector,
                runtime,
                config={
                    "endpoint_document": endpoint,
                    "stream_source": _stream_source(),
                },
                state_manager=MagicMock(),
                stream_name="items",
                batch_size=10,
            )

    @pytest.mark.asyncio
    async def test_request_body_sees_the_same_clamped_page_size(self):
        """The body and the stop predicate must agree on the page size.

        A body-paginated endpoint asks for its page size in the JSON body. If
        the body sends the unclamped engine batch size while `stop_when`
        judges against the clamped one, the read requests a size it never
        reasons about — the provider either rejects it or returns pages larger
        than the loop believes it asked for.
        """
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={"records": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]},
                ),
                _FakeResponse(status=200, body={"records": [{"id": 3, "name": "c"}]}),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "offset",
                "offset": {"param": "offset", "initial": 0},
                "limit": {"param": "limit", "max": 2},
                "stop_when": {
                    "lt": [
                        {"ref": "response.record_count"},
                        {"ref": "runtime.batch_size"},
                    ]
                },
            },
            request_body={"page_size": {"ref": "runtime.batch_size"}},
        )
        await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=10,
        )

        # Every body carries the clamped size, matching what stop_when judges.
        assert [b["page_size"] for b in session.bodies] == [2, 2]

    @pytest.mark.asyncio
    async def test_stop_when_with_an_unknown_scope_raises_read_error(self):
        """A typo'd scope is an authoring defect, so it must not escape as KeyError.

        The resolver reports an unknown scope name as a bare `KeyError`. Left
        unwrapped it reaches the extract boundary as an internal engine
        failure, sending whoever reads the log after the engine rather than
        after the typo in their own document.
        """
        session = _FakeSession(
            [_FakeResponse(status=200, body={"records": [{"id": 1, "name": "a"}]})]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "offset",
                "offset": {"param": "offset", "initial": 0},
                "stop_when": {"missing": {"ref": "respones.body.next"}},
            },
        )
        with pytest.raises(ReadError, match="not evaluable against this response"):
            await _consume(
                connector,
                runtime,
                config={
                    "endpoint_document": endpoint,
                    "stream_source": _stream_source(),
                },
                state_manager=MagicMock(),
                stream_name="items",
                batch_size=10,
            )

    @pytest.mark.asyncio
    async def test_redirect_is_refused_rather_than_followed(self):
        """A redirect must not carry the connection's credentials to a new host.

        aiohttp follows redirects by default and strips only `Authorization`,
        forwarding every other default header — and connection auth is
        routinely a custom header. Checking a `next_url` string is no defence:
        the redirect works on any request under any strategy, so the engine
        stops following redirects entirely.
        """
        session = _FakeSession(
            [
                _FakeResponse(
                    status=302,
                    body={},
                    headers={"Location": "https://evil.example.com/collect"},
                )
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records()
        with pytest.raises(ReadError, match="Redirects are not"):
            await _consume(
                connector,
                runtime,
                config={
                    "endpoint_document": endpoint,
                    "stream_source": _stream_source(),
                },
                state_manager=MagicMock(),
                stream_name="items",
                batch_size=10,
            )

    @pytest.mark.asyncio
    async def test_link_rejects_a_next_url_carrying_userinfo(self):
        """Userinfo is not part of the origin, so it must not ride through the check.

        `http://evil:pw@api.provider.com/x` is same-origin by hostname, but
        aiohttp turns the userinfo into an outgoing `Authorization: Basic`
        header — credentials the provider chose for a request the engine makes.
        """
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={
                        "records": [{"id": 1, "name": "a"}],
                        "next": "https://evil:pw@api.example.test/items?page=2",
                    },
                )
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "link",
                "link": {"next_url": {"ref": "response.body.next"}},
                "stop_when": {"missing": {"ref": "response.body.next"}},
            },
        )
        with pytest.raises(ReadError, match="not an absolute URL|outside the endpoint"):
            await _consume(
                connector,
                runtime,
                config={
                    "endpoint_document": endpoint,
                    "stream_source": _stream_source(),
                },
                state_manager=MagicMock(),
                stream_name="items",
                batch_size=10,
            )

    @pytest.mark.asyncio
    async def test_a_cursor_that_never_changes_stops_the_read(self):
        """A provider echoing one cursor would otherwise page forever.

        `stop_when` is the declared terminator, but it is written against
        provider data and cannot be trusted on a provider that misbehaves. Each
        page is yielded downstream, so an unbounded loop feeds the destination
        the same records without limit.
        """
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={"records": [{"id": 1, "name": "a"}], "next": "SAME"},
                )
                for _ in range(6)
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "cursor",
                "cursor": {
                    "param": "cursor",
                    "next_cursor": {"ref": "response.body.next"},
                },
                "stop_when": {"missing": {"ref": "response.body.next"}},
            },
        )
        with pytest.raises(ReadError, match="would send the identical request again"):
            await _consume(
                connector,
                runtime,
                config={
                    "endpoint_document": endpoint,
                    "stream_source": _stream_source(),
                },
                state_manager=MagicMock(),
                stream_name="items",
                batch_size=10,
            )
        # Stopped on the repeat, not after draining every queued response.
        assert len(session.calls) == 2

    @pytest.mark.asyncio
    async def test_next_cursor_with_an_unknown_scope_raises_read_error(self):
        """A typo in next_cursor classifies like the same typo in stop_when."""
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={"records": [{"id": 1, "name": "a"}], "next": "abc"},
                )
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "cursor",
                "cursor": {
                    "param": "cursor",
                    "next_cursor": {"ref": "respones.body.next"},
                },
                # Must not fire, or `advance` never runs and the typo is unseen.
                "stop_when": {"missing": {"ref": "response.body.records"}},
            },
        )
        with pytest.raises(
            ReadError, match=r"pagination\.cursor\.next_cursor is not resolvable"
        ):
            await _consume(
                connector,
                runtime,
                config={
                    "endpoint_document": endpoint,
                    "stream_source": _stream_source(),
                },
                state_manager=MagicMock(),
                stream_name="items",
                batch_size=10,
            )

    @pytest.mark.asyncio
    async def test_cursor_missing_while_stop_when_says_continue_raises(self):
        """The pager cannot continue but the document said it should: not a quiet end.

        Ending here would drop every remaining page while the run reported
        success — the #346 shape. A provider that omits its token on the last
        page is declared through `stop_when`, which fires before `advance`.
        """
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={"records": [{"id": 1, "name": "a"}], "next": None},
                )
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "cursor",
                "cursor": {
                    "param": "cursor",
                    "next_cursor": {"ref": "response.body.next"},
                },
                # Deliberately does not cover the token-absent case.
                "stop_when": {"empty": {"ref": "response.body.records"}},
            },
        )
        with pytest.raises(ReadError, match="stop_when did not fire"):
            await _consume(
                connector,
                runtime,
                config={
                    "endpoint_document": endpoint,
                    "stream_source": _stream_source(),
                },
                state_manager=MagicMock(),
                stream_name="items",
                batch_size=10,
            )


class TestPaginationInvariants:
    """One test per invariant, covering every site the invariant reaches.

    These replace the per-site tests they grew out of. Each defect in this PR
    was a correct fix applied at one site while a sibling site kept the bug —
    `_require_scalar` on the keyset advance key but not its seed,
    validate-before-emit in the page loop but not the cursor path. A test
    written per site passes while the gap stays open; a test written per
    invariant fails the moment a site is added or missed.
    """

    # Every place a value the engine did not compute becomes a request param.
    # `link` is absent deliberately: its resolved value is a URL, checked by
    # `_origin` rather than by `_require_scalar`.
    NON_SCALAR_SITES = [
        pytest.param(
            {
                "type": "cursor",
                "cursor": {
                    "param": "cursor",
                    "next_cursor": {"ref": "response.body.next"},
                },
                "stop_when": {"empty": {"ref": "response.body.records"}},
            },
            {"records": [{"id": 1, "name": "a"}], "next": {"token": "abc"}},
            None,
            id="cursor.next_cursor",
        ),
        pytest.param(
            {
                "type": "keyset",
                "keyset": {"param": "since", "order_by_field": "id"},
                "stop_when": {"empty": {"ref": "response.body.records"}},
            },
            {"records": [{"id": ["a", "b"], "name": "a"}]},
            None,
            id="keyset.advance_key",
        ),
        pytest.param(
            {
                "type": "keyset",
                "keyset": {
                    "param": "since",
                    "order_by_field": "id",
                    "initial": {"ref": "connection.parameters.since"},
                },
                "stop_when": {"empty": {"ref": "response.body.records"}},
            },
            {"records": [{"id": 1, "name": "a"}]},
            {"since": ["a", "b"]},
            id="keyset.initial_seed",
        ),
    ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("pagination,body,parameters", NON_SCALAR_SITES)
    async def test_no_site_sends_a_non_scalar_as_a_request_param(
        self, pagination, body, parameters
    ):
        """A dict/list/bool must never reach the query string.

        A dict surfaces as a bare `TypeError` from inside yarl; a list is
        worse, silently expanding into repeated query values so a provider can
        inject extra values for a param the endpoint declared.
        """
        session = _FakeSession([_FakeResponse(status=200, body=body)])
        runtime = _runtime_with_session(session, parameters=parameters)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(pagination=pagination)
        with pytest.raises(ReadError, match="cannot be sent as a single request"):
            await _consume(
                connector,
                runtime,
                config={
                    "endpoint_document": endpoint,
                    "stream_source": _stream_source(),
                },
                state_manager=MagicMock(),
                stream_name="items",
                batch_size=10,
            )

    # Every deterministic document defect detectable once a page has arrived,
    # across both places a batch is emitted (the page loop and the cursor path).
    DOCUMENT_DEFECTS = [
        pytest.param(
            {
                "type": "keyset",
                "keyset": {"param": "since", "order_by_field": "updated_at"},
                "stop_when": {"empty": {"ref": "response.body.records"}},
            },
            _stream_source(),
            "carries no 'updated_at'",
            id="keyset_order_by_field_absent",
        ),
        pytest.param(
            {
                "type": "offset",
                "offset": {"param": "offset", "initial": 0},
                "stop_when": {"missing": {"ref": "respones.body.next"}},
            },
            _stream_source(),
            "not evaluable against this response",
            id="stop_when_unknown_scope",
        ),
        pytest.param(
            {
                "type": "offset",
                "offset": {"param": "offset", "initial": 0},
                "stop_when": {"gte": [{"ref": "response.record_count"}]},
            },
            _stream_source(),
            "not evaluable against this response",
            id="stop_when_wrong_arity",
        ),
        pytest.param(
            None,
            _stream_source(replication_method="incremental", cursor_field="updated_at"),
            "records carry no 'updated_at'",
            id="incremental_cursor_field_absent",
        ),
    ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("pagination,stream_source,message", DOCUMENT_DEFECTS)
    async def test_no_records_are_emitted_on_a_document_defect(
        self, pagination, stream_source, message
    ):
        """A stream that cannot run must not write part of itself first.

        The extract stage enqueues a yielded batch immediately, so emitting
        before the page is fully decided lets a deterministically broken
        document transform and write page 1 and only then fail. Every defect
        here is knowable the moment the page arrives.
        """
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={"records": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]},
                )
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(pagination=pagination)
        emitted: list[Any] = []
        with pytest.raises(ReadError, match=message):
            async for batch in connector.read_batches(
                runtime,
                {"endpoint_document": endpoint, "stream_source": stream_source},
                checkpoint=MagicMock(),
                stream_name="items",
                batch_size=10,
            ):
                emitted.append(batch)

        assert (
            emitted == []
        ), "records reached the destination on a read that cannot run"

    # Every next-page control expression, in both strategies that have one.
    # A template whose field is absent must read as "no value", not as the
    # static text rendered around the hole.
    CONTROL_EXPRESSIONS = [
        pytest.param(
            {
                "type": "cursor",
                "cursor": {
                    "param": "cursor",
                    "next_cursor": {"template": "tok-${response.body.next}"},
                },
                "stop_when": {"empty": {"ref": "response.body.records"}},
            },
            id="cursor.next_cursor",
        ),
        pytest.param(
            {
                "type": "link",
                "link": {
                    "next_url": {
                        "template": (
                            "https://api.example.test/items" "?c=${response.body.next}"
                        )
                    }
                },
                "stop_when": {"empty": {"ref": "response.body.records"}},
            },
            id="link.next_url",
        ),
    ]

    # Settings that decide where the read starts or how far it steps. Same
    # invariant as CONTROL_EXPRESSIONS, resolved before the first request
    # rather than per page — the strictness has to reach here too, which is
    # what the round-5 fix missed by defining the invariant too narrowly.
    SETTING_EXPRESSIONS = [
        pytest.param(
            {
                "type": "offset",
                "offset": {
                    "param": "offset",
                    "initial": {"template": "${connection.parameters.start}0"},
                },
                "stop_when": {"empty": {"ref": "response.body.records"}},
            },
            id="offset.initial",
        ),
        pytest.param(
            {
                "type": "page",
                "page": {
                    "param": "page",
                    "initial": 1,
                    "increment_by": {"template": "${connection.parameters.step}1"},
                },
                "stop_when": {"empty": {"ref": "response.body.records"}},
            },
            id="page.increment_by",
        ),
        pytest.param(
            {
                "type": "keyset",
                "keyset": {
                    "param": "since",
                    "order_by_field": "id",
                    "initial": {"template": "tok-${connection.parameters.seed}"},
                },
                "stop_when": {"empty": {"ref": "response.body.records"}},
            },
            id="keyset.initial",
        ),
        pytest.param(
            {
                "type": "offset",
                "offset": {"param": "offset", "initial": 0},
                "limit": {
                    "param": "limit",
                    "default": {"template": "${connection.parameters.size}0"},
                },
                "stop_when": {"empty": {"ref": "response.body.records"}},
            },
            id="limit.default",
        ),
    ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("pagination", SETTING_EXPRESSIONS)
    async def test_no_setting_is_fabricated_from_absent_config(self, pagination):
        """A template over missing config must fail, not render a usable value.

        Lenient resolution turns `${connection.parameters.start}0` into `"0"`,
        which passes the integer check — so the read would start, step or size
        its pages from a number invented out of configuration that was never
        supplied. These settings decide which records are read.
        """
        session = _FakeSession(
            [_FakeResponse(status=200, body={"records": [{"id": 1, "name": "a"}]})]
        )
        # No `start` / `step` / `seed` / `size` anywhere in the connection.
        runtime = _runtime_with_session(session, parameters={})
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(pagination=pagination)
        with pytest.raises(ReadError, match="resolved to no value|not resolvable"):
            await _consume(
                connector,
                runtime,
                config={
                    "endpoint_document": endpoint,
                    "stream_source": _stream_source(),
                },
                state_manager=MagicMock(),
                stream_name="items",
                batch_size=10,
            )
        assert session.calls == [], "a fabricated setting reached the wire"

    # Page 1 is `(https://api.example.test/items, {"limit": 50})`. Each of
    # these is a link back to that same request wearing different clothes:
    # scheme and host are case-insensitive, `:443` is what `https` means
    # anyway, and a fragment never leaves the client. The transport folds all
    # of them into one set of bytes, so the repeat check has to as well.
    EQUIVALENT_SELF_LINKS = [
        pytest.param("https://api.example.test/items?limit=50", id="verbatim"),
        pytest.param("https://API.EXAMPLE.TEST/items?limit=50", id="host-case"),
        pytest.param("https://api.example.test:443/items?limit=50", id="default-port"),
        pytest.param("https://api.example.test/items?limit=50#top", id="fragment"),
    ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("self_link", EQUIVALENT_SELF_LINKS)
    async def test_a_self_referential_link_emits_no_duplicate(self, self_link):
        """`(base, {limit: 50})` and `(base?limit=50, {})` are the same request.

        Compared literally they look like progress, so a provider returning its
        own URL would get one duplicate page emitted before the repeat was
        noticed on the following iteration. Every spelling here reaches the
        socket as those same bytes, so every one of them is a repeat.
        """
        body = {
            "records": [{"id": 1, "name": "a"}],
            "next": self_link,
        }
        session = _FakeSession([_FakeResponse(status=200, body=body) for _ in range(4)])
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            params={"limit": {"in": "query", "default": {"literal": 50}}},
            pagination={
                "type": "link",
                "link": {"next_url": {"ref": "response.body.next"}},
                "stop_when": {"missing": {"ref": "response.body.next"}},
            },
        )
        emitted: list[Any] = []
        with pytest.raises(ReadError, match="would send the identical request again"):
            async for batch in connector.read_batches(
                runtime,
                {
                    "endpoint_document": endpoint,
                    "stream_source": _stream_source(),
                },
                checkpoint=MagicMock(),
                stream_name="items",
                batch_size=10,
            ):
                emitted.append(batch)

        assert len(session.calls) == 1
        assert len(emitted) == 1, "the same page was emitted twice"

    @pytest.mark.asyncio
    async def test_a_repeated_query_key_is_not_a_repeated_request(self):
        """`?tag=a&tag=b` and `?tag=b` are two pages, not one.

        The repeat check is only safe if it distinguishes every request the
        transport distinguishes. Collapsing a link's query into a plain dict
        keeps the last value for a repeated key, which makes these two URLs
        identical and aborts a perfectly valid read on its third page.
        """
        pages = [
            {
                "records": [{"id": 1, "name": "a"}],
                "next": "https://api.example.test/items?limit=50&tag=a&tag=b",
            },
            {
                "records": [{"id": 2, "name": "b"}],
                "next": "https://api.example.test/items?limit=50&tag=b",
            },
            {"records": [{"id": 3, "name": "c"}]},
        ]
        session = _FakeSession([_FakeResponse(status=200, body=p) for p in pages])
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            params={"limit": {"in": "query", "default": {"literal": 50}}},
            pagination={
                "type": "link",
                "link": {"next_url": {"ref": "response.body.next"}},
                "stop_when": {"missing": {"ref": "response.body.next"}},
            },
        )
        batches = await _consume(
            connector,
            runtime,
            config={
                "endpoint_document": endpoint,
                "stream_source": _stream_source(),
            },
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=10,
        )

        assert len(session.calls) == 3
        assert [row["id"] for batch in batches for row in batch.to_pylist()] == [
            1,
            2,
            3,
        ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("pagination", CONTROL_EXPRESSIONS)
    async def test_no_control_expression_fabricates_a_value(self, pagination):
        """A missing template field must not become a made-up cursor or link.

        Lenient resolution renders `tok-${response.body.next}` as `"tok-"` when
        the field is absent — not blank, so the engine would request a page
        using a token it invented from data the provider never sent.
        """
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={"records": [{"id": 1, "name": "a"}]},  # no `next`
                )
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(pagination=pagination)
        with pytest.raises(ReadError, match="stop_when did not fire"):
            await _consume(
                connector,
                runtime,
                config={
                    "endpoint_document": endpoint,
                    "stream_source": _stream_source(),
                },
                state_manager=MagicMock(),
                stream_name="items",
                batch_size=10,
            )
        # The fabricated value never reached the wire.
        assert len(session.calls) == 1

    @pytest.mark.asyncio
    async def test_metadata_keeps_a_null_a_provider_actually_sent(self):
        """`response.metadata.x` and a direct ref to the same field must agree.

        A field present with JSON null resolves to None through a direct ref,
        so `missing` is False. Routing it through `response.metadata` must not
        flip that verdict to True — same data, same answer, whichever path the
        author took.
        """
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={"records": [{"id": 1, "name": "a"}], "cursor": None},
                )
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            metadata={"cursor": {"ref": "response.body.cursor"}},
            pagination={
                "type": "offset",
                "offset": {"param": "offset", "initial": 0},
                # Present-but-null, so this must NOT fire; the read ends on the
                # empty second page instead.
                "stop_when": {"missing": {"ref": "response.metadata.cursor"}},
            },
        )
        session._responses.append(_FakeResponse(status=200, body={"records": []}))
        batches = await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=10,
        )

        assert [r["id"] for b in batches for r in b.to_pylist()] == [1]
        assert len(session.calls) == 2, "a null metadata value read as missing"

    @pytest.mark.asyncio
    async def test_progress_is_judged_on_the_wire_not_the_param_table(self):
        """A param that never reaches the request is not progress.

        `offset` is declared `in: body`, so it is kept out of the query string
        — but the body binds nothing, so it never reaches the request either.
        The param table advances 0, 2, 4 while the wire request stays
        byte-identical, and comparing param tables would call that progress and
        page forever, re-yielding the same records to the destination.
        """
        page = {"records": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]}
        session = _FakeSession([_FakeResponse(status=200, body=page) for _ in range(6)])
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            params={"offset": {"in": "body", "controlled_by": "pagination"}},
            request_body={"static": {"literal": 1}},
            pagination={
                "type": "offset",
                "offset": {"param": "offset", "initial": 0},
                "stop_when": {"empty": {"ref": "response.body.records"}},
            },
        )
        with pytest.raises(ReadError, match="would send the identical request again"):
            await _consume(
                connector,
                runtime,
                config={
                    "endpoint_document": endpoint,
                    "stream_source": _stream_source(),
                },
                state_manager=MagicMock(),
                stream_name="items",
                batch_size=10,
            )
        # Caught on the second attempt, not after draining every response.
        assert len(session.calls) == 1

    # Every place the endpoint document resolves an expression. A typo'd scope
    # raises a plain KeyError from the resolver, which the source worker does
    # not treat as deterministic -- so an unwrapped one is retried until the
    # attempts run out on a typo that cannot fix itself.
    TYPOED_SCOPE_SITES = [
        pytest.param(
            {"params": {"q": {"in": "query", "default": {"ref": "runtme.batch_size"}}}},
            id="params.default",
        ),
        pytest.param(
            {"request_body": {"size": {"ref": "runtme.batch_size"}}},
            id="request.body",
        ),
        pytest.param(
            {
                "pagination": {
                    "type": "offset",
                    "offset": {"param": "offset", "initial": 0},
                    "limit": {"param": "limit", "default": {"ref": "runtme.batch"}},
                    "stop_when": {"empty": {"ref": "response.body.records"}},
                }
            },
            id="limit.default",
        ),
        pytest.param(
            {
                "pagination": {
                    "type": "offset",
                    "offset": {
                        "param": "offset",
                        "initial": {"ref": "runtme.batch_size"},
                    },
                    "stop_when": {"empty": {"ref": "response.body.records"}},
                }
            },
            id="offset.initial",
        ),
    ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("doc", TYPOED_SCOPE_SITES)
    async def test_a_typoed_scope_is_a_config_error_everywhere(self, doc):
        """A typo in the document must never surface as an engine failure.

        `ReadError` is in the worker's deterministic set; a bare `KeyError`
        from the resolver is not, so it is reported as retryable and the run
        burns every attempt on a defect that cannot resolve itself.
        """
        session = _FakeSession(
            [_FakeResponse(status=200, body={"records": [{"id": 1, "name": "a"}]})]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        with pytest.raises(ReadError):
            await _consume(
                connector,
                runtime,
                config={
                    "endpoint_document": _endpoint_doc_with_records(**doc),
                    "stream_source": _stream_source(),
                },
                state_manager=MagicMock(),
                stream_name="items",
                batch_size=10,
            )

    @pytest.mark.asyncio
    async def test_a_two_step_link_cycle_is_caught(self):
        """A -> B -> A makes no progress, but never repeats consecutively.

        Comparing each request only against the one before it calls every page
        progress, so the read re-emits the same two pages for as long as
        `stop_when` stays false.
        """
        base = "https://api.example.test/items"
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={"records": [{"id": 1, "name": "a"}], "next": f"{base}?p=B"},
                ),
                _FakeResponse(
                    status=200,
                    body={"records": [{"id": 2, "name": "b"}], "next": f"{base}?p=A"},
                ),
                _FakeResponse(
                    status=200,
                    body={"records": [{"id": 1, "name": "a"}], "next": f"{base}?p=B"},
                ),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "link",
                "link": {"next_url": {"ref": "response.body.next"}},
                "stop_when": {"missing": {"ref": "response.body.next"}},
            },
        )
        with pytest.raises(ReadError, match="would send the identical request again"):
            await _consume(
                connector,
                runtime,
                config={
                    "endpoint_document": endpoint,
                    "stream_source": _stream_source(),
                },
                state_manager=MagicMock(),
                stream_name="items",
                batch_size=10,
            )
        # Caught when A came round again, not left to cycle.
        assert len(session.calls) == 3
