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

Tie-breaker dedup is intentionally not exercised here: ``read_batches``
initialises ``state["bookmarks"]`` empty, so the dedup comparators are
unreachable through this entry point. They are pinned directly as pure
functions in ``test_api_dedup_helpers.py``.

No live HTTP. The session is a ``MagicMock`` with ``request`` returning
an async context manager that yields a stub response.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import AsyncMock, MagicMock

import pyarrow as pa
import pytest

from cdk.secrets import InMemorySecretsResolver
from cdk.connection_runtime import ConnectionRuntime
from src.source.connectors.api import APIConnector
from src.source.connectors.base import ReadError

# ---------------------------------------------------------------------------
# Fakes for the aiohttp session + state manager
# ---------------------------------------------------------------------------


class _FakeResponse:
    """Drop-in for the ``aiohttp`` response object used by the connector."""

    def __init__(self, *, status: int, body: Any):
        self.status = status
        self._body = body

    async def __aenter__(self) -> "_FakeResponse":
        return self

    async def __aexit__(self, *_exc) -> None:
        return None

    async def json(self) -> Any:
        return self._body

    async def text(self) -> str:
        return json.dumps(self._body) if not isinstance(self._body, str) else self._body


class _FakeSession:
    """Records each ``request(...)`` call and returns the next queued response.

    The connector uses ``session.request(method, url, params=...)`` as an
    async context manager. We mirror that shape and capture each call so
    tests can assert on the URL + params actually sent.
    """

    def __init__(self, responses: List[_FakeResponse]):
        self._responses = list(responses)
        self.calls: List[Tuple[str, str, Dict[str, Any]]] = []

    def request(self, method: str, url: str, *, params: Dict[str, Any]):
        self.calls.append((method, url, dict(params)))
        if not self._responses:
            raise AssertionError(f"unexpected extra request: {method} {url} {params}")
        return self._responses.pop(0)


def _runtime_with_session(session: _FakeSession) -> ConnectionRuntime:
    """Build a ``ConnectionRuntime`` whose transport is already
    materialized with ``session`` so ``connect()`` adopts it.
    """
    runtime = ConnectionRuntime(
        raw_config={"host": "https://api.example.test", "parameters": {}},
        connection_id="test-conn",
        connector_type="api",
        driver=None,
        resolver=InMemorySecretsResolver({}),
    )
    runtime._session = session
    runtime._base_url = "https://api.example.test"
    runtime._materialized = True
    return runtime


def _endpoint_doc_with_records(
    pagination: Optional[Dict[str, Any]] = None,
    replication: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    read_block: Dict[str, Any] = {
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
    cursor_field: Optional[str] = None,
    safety_window: Optional[int] = None,
    tie_breaker_fields: Optional[List[str]] = None,
) -> Dict[str, Any]:
    block: Dict[str, Any] = {
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
    if tie_breaker_fields:
        block["replication"]["tie_breaker_fields"] = tie_breaker_fields
    return block


async def _consume(
    connector: APIConnector,
    runtime: ConnectionRuntime,
    *,
    config: Dict[str, Any],
    state_manager: Any,
    stream_name: str,
    partition: Optional[Dict[str, Any]] = None,
    batch_size: int = 1000,
) -> List[pa.RecordBatch]:
    """Drive ``read_batches`` with the runtime it now owns.

    ``read_batches`` connects and disconnects internally, so callers pass the
    runtime directly (no prior ``connect()``); ``state_manager`` is forwarded as
    the ``checkpoint`` argument.
    """
    batches: List[pa.RecordBatch] = []
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
            return_value={"primary": {"value": "2024-01-01T12:00:00Z"}}
        )

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
    async def test_non_200_response_raises_read_error(self):
        session = _FakeSession(
            [
                _FakeResponse(status=500, body="server exploded"),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        with pytest.raises(ReadError, match="status 500"):
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
