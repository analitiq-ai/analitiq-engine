"""Behavioural tests for :meth:`src.source.connectors.api.APIConnector.read_batches`.

The connector consumes the contract endpoint document directly. These
tests drive ``read_batches`` against a mocked aiohttp session and a
pre-materialized ``ConnectionRuntime`` to verify:

* request URL composition (base + endpoint path),
* offset pagination loop terminates on a short page,
* page pagination increments the page param and stops on a short page,
* cursor pagination follows ``next_cursor`` until absent,
* keyset pagination advances the key from the last record and stops on
  a short page,
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

from cdk.connection_runtime import ConnectionRuntime
from cdk.secrets import InMemorySecretsResolver
from src.source.connectors.api import APIConnector, _extract_next_cursor
from src.source.connectors.base import ReadError, TransientReadError

# ---------------------------------------------------------------------------
# Fakes for the aiohttp session + state manager
# ---------------------------------------------------------------------------


class _FakeResponse:
    """Drop-in for the ``aiohttp`` response object used by the connector."""

    def __init__(self, *, status: int, body: Any):
        self.status = status
        self._body = body

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
        self, method: str, url: str, *, params: dict[str, Any], json: Any = None
    ):
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
) -> dict[str, Any]:
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
                            "properties": {
                                "id": {"type": "integer", "arrow_type": "Int64"},
                                "name": {"type": "string", "arrow_type": "Utf8"},
                            },
                        },
                    },
                },
            },
            "records": {"ref": "response.body.records"},
        },
    }
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
    async def test_offset_loop_advances_and_stops_on_short_page(self):
        # batch_size=2; first page = 2 rows (full), second = 1 row (short -> stop)
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


# ---------------------------------------------------------------------------
# Page pagination
# ---------------------------------------------------------------------------


class TestReadBatchesPagePagination:
    @pytest.mark.asyncio
    async def test_page_loop_increments_page_and_stops_on_short_page(self):
        # batch_size=2; first page full (2 rows), second short (1 row -> stop).
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
                "type": "page",
                "page": {"param": "page", "initial": 1},
                "limit": {"param": "limit"},
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
        pages = [c[2].get("page") for c in session.calls]
        # Starts at the declared initial page and increments by one.
        assert pages == [1, 2]


# ---------------------------------------------------------------------------
# Cursor pagination
# ---------------------------------------------------------------------------


class TestReadBatchesCursorPagination:
    @pytest.mark.asyncio
    async def test_cursor_pagination_follows_next_cursor_until_absent(self):
        # First response carries next_cursor; second omits it -> loop stops.
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={
                        "records": [{"id": 1, "name": "a"}],
                        "next_cursor": "token-2",
                    },
                ),
                _FakeResponse(
                    status=200,
                    body={"records": [{"id": 2, "name": "b"}]},
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
                    "from": {"ref": "response.body.next_cursor"},
                },
                "limit": {"param": "limit"},
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
        # First request has no page_token; second carries the returned token.
        assert "page_token" not in session.calls[0][2]
        assert session.calls[1][2]["page_token"] == "token-2"


# ---------------------------------------------------------------------------
# Keyset pagination
# ---------------------------------------------------------------------------


class TestReadBatchesKeysetPagination:
    @pytest.mark.asyncio
    async def test_keyset_advances_key_from_last_record_and_stops_on_short_page(self):
        # batch_size=2; first page full -> advance key from last record's id,
        # second page short -> stop.
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
                "keyset": {"param": "after_id", "from_record": "id"},
                "limit": {"param": "limit"},
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
        assert "after_id" not in session.calls[0][2]
        assert session.calls[1][2]["after_id"] == 2

    @pytest.mark.asyncio
    async def test_keyset_without_from_record_raises_read_error(self):
        session = _FakeSession(
            [
                _FakeResponse(status=200, body={"records": [{"id": 1, "name": "a"}]}),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={"type": "keyset", "keyset": {"param": "after_id"}},
        )
        with pytest.raises(ReadError, match="keyset.from_record"):
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


class TestExtractNextCursor:
    """``_extract_next_cursor`` is exercised end-to-end by the cursor-pagination
    tests above; these pin its edge cases directly, restoring the direct
    coverage that moved out with the removed dedup-helper test module."""

    @pytest.mark.unit
    def test_extracts_token_from_body_path(self):
        assert _extract_next_cursor({"next": "abc"}, "response.body.next") == "abc"

    @pytest.mark.unit
    def test_non_dict_data_returns_none(self):
        assert _extract_next_cursor("not-a-dict", "response.body.next") is None

    @pytest.mark.unit
    def test_none_token_returns_none(self):
        assert _extract_next_cursor({"next": None}, "response.body.next") is None

    @pytest.mark.unit
    def test_empty_string_token_returns_none(self):
        assert _extract_next_cursor({"next": ""}, "response.body.next") is None

    @pytest.mark.unit
    def test_body_only_ref_returns_none(self):
        # ``response.body`` with no trailing field can never name a cursor.
        assert _extract_next_cursor({"next": "abc"}, "response.body") is None

    @pytest.mark.unit
    def test_integer_token_coerced_to_str(self):
        assert _extract_next_cursor({"next": 42}, "response.body.next") == "42"


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
