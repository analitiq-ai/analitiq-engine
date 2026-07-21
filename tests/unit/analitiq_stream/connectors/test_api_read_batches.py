"""Behavioural tests for :meth:`src.source.connectors.api.APIConnector.read_batches`.

The connector consumes the contract endpoint document directly. These
tests drive ``read_batches`` against a mocked aiohttp session and a
pre-materialized ``ConnectionRuntime`` to verify:

* request URL composition (base + endpoint path),
* offset pagination loop terminates on a short page,
* page pagination increments the page param and stops on a short page,
* cursor pagination follows the declared ``next_cursor`` expression until
  it resolves to nothing,
* keyset pagination advances the key from each page's last
  ``order_by_field`` value (seeded by ``initial`` when authored),
* link pagination follows the resolved ``next_url`` with no params
  traversing,
* the strategy's declared ``stop_when`` predicate ends every loop,
* incremental replication reads cursor from state manager and applies
  the safety window to the outgoing request params,
* non-200 responses raise :class:`ReadError`,
* missing or contract-invalid ``endpoint_document`` raises
  :class:`ReadError`.

Every fixture document is contract-valid: the connector validates the
endpoint document into ``ApiEndpointDoc`` before reading (issue #349).

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
from src.source.connectors.api import APIConnector
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


def _collect_from_params(node: Any) -> set[str]:
    """Every param name referenced by a ``{"from_param": ...}`` node."""
    names: set[str] = set()
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "from_param" and isinstance(value, str):
                names.add(value)
            else:
                names.update(_collect_from_params(value))
    elif isinstance(node, list):
        for item in node:
            names.update(_collect_from_params(item))
    return names


def _endpoint_doc_with_records(  # skipcq: PY-R1000
    pagination: dict[str, Any] | None = None,
    replication: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
    request: dict[str, Any] | None = None,
    extra_properties: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a contract-valid api-endpoint document around ``/items``.

    The connector validates the document into ``ApiEndpointDoc`` before
    reading, so every fixture must satisfy the contract's cross-field
    rules. This builder wires the boilerplate those rules demand:

    * every pagination/replication-referenced param the test did not
      declare explicitly is declared as a ``controlled_by`` query param;
    * every declared non-body param the request does not already bind is
      bound exactly once via ``request.query.{name}.from_param``;
    * ``pagination.stop_when`` and ``replication.supported_methods`` get
      contract-required defaults when the test omits them.
    """
    declared: dict[str, Any] = dict(params or {})

    if pagination is not None:
        pagination = dict(pagination)
        pagination.setdefault("stop_when", {"empty": {"ref": "response.body.records"}})
        for key in ("offset", "page", "cursor", "keyset", "limit"):
            block = pagination.get(key)
            if isinstance(block, dict) and block.get("param"):
                declared.setdefault(
                    block["param"],
                    {
                        "in": "query",
                        "type": "integer",
                        "required": False,
                        "controlled_by": "pagination",
                    },
                )
    if replication is not None:
        replication = {
            "supported_methods": ["full_refresh", "incremental"],
            **replication,
        }
        for mapping in replication.get("cursor_mappings") or []:
            if mapping.get("param"):
                declared.setdefault(
                    mapping["param"],
                    {
                        "in": "query",
                        "type": "string",
                        "required": False,
                        "controlled_by": "replication",
                    },
                )

    request = dict(request or {"method": "GET", "path": "/items"})
    already_bound = _collect_from_params(request)
    query_bindings = dict(request.get("query") or {})
    for name, decl in declared.items():
        if name in already_bound or decl.get("in") == "body":
            continue
        query_bindings[name] = {"from_param": name}
    if query_bindings:
        request["query"] = query_bindings

    read_block: dict[str, Any] = {
        "request": request,
        "response": {
            "schema": {
                "type": "object",
                "properties": {
                    "records": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": {
                                    "type": "integer",
                                    "native_type": "integer",
                                    "arrow_type": "Int64",
                                },
                                "name": {
                                    "type": "string",
                                    "native_type": "string",
                                    "arrow_type": "Utf8",
                                },
                                **(extra_properties or {}),
                            },
                        },
                    },
                },
            },
            "records": {"ref": "response.body.records"},
        },
    }
    if declared:
        read_block["params"] = declared
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


# Record property declaring the incremental cursor field; the contract
# requires the cursor_field of every replication mapping to exist in the
# record shape.
_UPDATED_AT_PROPERTY: dict[str, Any] = {
    "updated_at": {"type": "string", "native_type": "string", "arrow_type": "Utf8"},
}

_RECORD_ITEMS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "id": {"type": "integer", "native_type": "integer", "arrow_type": "Int64"},
        "name": {"type": "string", "native_type": "string", "arrow_type": "Utf8"},
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
        # the contract model's own cross-field validation rejects the
        # document at the connector's parse point, before any HTTP request.
        """A records.ref naming an undeclared field fails at the parse point."""
        session = _FakeSession([])
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records()
        endpoint["operations"]["read"]["response"]["records"][
            "ref"
        ] = "response.body.does_not_exist"
        with pytest.raises(
            ReadError,
            match=r"(?s)contract validation.*does_not_exist",
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


class TestReadBatchesOffsetIncrementBy:
    """An authored ``offset.increment_by`` fixes the advance step."""

    @pytest.mark.asyncio
    async def test_authored_increment_by_overrides_page_size_step(self):
        """The offset advances by the declared step, not the page size."""
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
                "offset": {"param": "offset", "initial": 0, "increment_by": 10},
                "limit": {"param": "limit"},
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

        assert [c[2].get("offset") for c in session.calls] == [0, 10]


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
        """The cursor loop follows the next_cursor response field until absent."""
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
                    "next_cursor": {"ref": "response.body.next_cursor"},
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
    """Keyset pagination per the contract: param / order_by_field / initial."""

    def _keyset_pagination(self, **keyset_extra):
        """Keyset block advancing ``after_id`` from each page's last ``id``."""
        return {
            "type": "keyset",
            "keyset": {"param": "after_id", "order_by_field": "id", **keyset_extra},
            "limit": {"param": "limit"},
        }

    @pytest.mark.asyncio
    async def test_keyset_advances_key_from_last_record(self):
        """The key advances from the last record's order_by_field value and
        the loop stops on a short page."""
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

        endpoint = _endpoint_doc_with_records(pagination=self._keyset_pagination())
        batches = await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=2,
        )

        assert [b.num_rows for b in batches] == [2, 1]
        # First request carries no key (no initial authored); the second
        # carries the last id of page 1, natively typed.
        assert "after_id" not in session.calls[0][2]
        assert session.calls[1][2]["after_id"] == 2
        assert [c[2].get("limit") for c in session.calls] == [2, 2]

    @pytest.mark.asyncio
    async def test_keyset_initial_seeds_the_first_request(self):
        """An authored ``initial`` rides the first request, including 0."""
        session = _FakeSession(
            [
                _FakeResponse(status=200, body={"records": [{"id": 1, "name": "a"}]}),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination=self._keyset_pagination(initial=0)
        )
        await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=2,
        )

        assert session.calls[0][2]["after_id"] == 0

    @pytest.mark.asyncio
    async def test_keyset_without_limit_continues_past_provider_sized_pages(self):
        """Without a declared limit param the provider's own page size bounds
        every page, so a short page must not stop the loop — only an empty
        page (or stop_when) may."""
        session = _FakeSession(
            [
                _FakeResponse(status=200, body={"records": [{"id": 1, "name": "a"}]}),
                _FakeResponse(status=200, body={"records": [{"id": 2, "name": "b"}]}),
                _FakeResponse(status=200, body={"records": []}),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "keyset",
                "keyset": {"param": "after_id", "order_by_field": "id"},
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

        # Two provider-default-sized pages, then the empty page stops the loop.
        assert [b.num_rows for b in batches] == [1, 1]
        assert session.calls[1][2]["after_id"] == 1
        assert session.calls[2][2]["after_id"] == 2

    @pytest.mark.asyncio
    async def test_keyset_full_page_without_key_field_raises_before_yield(self):
        """A full page whose last record lacks order_by_field cannot advance —
        malformed data, not a stop signal — and it fails BEFORE the page is
        yielded, so the engine can never commit a page the read cannot
        correctly follow."""
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={"records": [{"id": 1, "name": "a"}, {"name": "keyless"}]},
                ),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(pagination=self._keyset_pagination())
        reader = connector.read_batches(
            runtime,
            {"endpoint_document": endpoint, "stream_source": _stream_source()},
            checkpoint=MagicMock(),
            stream_name="items",
            batch_size=2,
        )
        # The very first advance raises: no batch was handed downstream.
        with pytest.raises(ReadError, match="order_by_field"):
            await reader.__anext__()


class TestReadBatchesLinkPagination:
    """Link pagination per the contract: the resolved next_url replaces the
    entire request, no params traverse."""

    def _link_pagination(self):
        """Link block following ``response.body.next`` until absent."""
        return {
            "type": "link",
            "link": {"next_url": {"ref": "response.body.next"}},
            "stop_when": {"empty": {"ref": "response.body.records"}},
        }

    @pytest.mark.asyncio
    async def test_link_follows_next_url_until_absent(self):
        """Follow-up pages hit the resolved URL with no params and no body."""
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={
                        "records": [{"id": 1, "name": "a"}],
                        "next": "https://api.example.test/items?page=2",
                    },
                ),
                _FakeResponse(status=200, body={"records": [{"id": 2, "name": "b"}]}),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(pagination=self._link_pagination())
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
        assert session.calls[1][1] == "https://api.example.test/items?page=2"
        # The next URL replaces the entire request: no params traverse.
        assert session.calls[1][2] == {}
        assert session.bodies[1] is None

    @pytest.mark.asyncio
    async def test_link_relative_next_url_joins_base(self):
        """A relative next URL is joined onto the connection's base URL."""
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={"records": [{"id": 1, "name": "a"}], "next": "/items/p2"},
                ),
                _FakeResponse(status=200, body={"records": [{"id": 2, "name": "b"}]}),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(pagination=self._link_pagination())
        await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=10,
        )

        assert session.calls[1][1] == "https://api.example.test/items/p2"

    @pytest.mark.asyncio
    async def test_link_equivalent_origin_spellings_accepted(self):
        """An explicit default port or different host casing is the same
        origin and must not be rejected."""
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={
                        "records": [{"id": 1, "name": "a"}],
                        "next": "https://API.example.test:443/items/p2",
                    },
                ),
                _FakeResponse(status=200, body={"records": [{"id": 2, "name": "b"}]}),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(pagination=self._link_pagination())
        batches = await _consume(
            connector,
            runtime,
            config={"endpoint_document": endpoint, "stream_source": _stream_source()},
            state_manager=MagicMock(),
            stream_name="items",
            batch_size=10,
        )

        assert [b.num_rows for b in batches] == [1, 1]
        assert session.calls[1][1] == "https://API.example.test:443/items/p2"

    @pytest.mark.asyncio
    async def test_link_cross_origin_next_url_refused(self):
        """An absolute next_url on another host is refused: the shared
        session would send the connection's auth headers to it."""
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={
                        "records": [{"id": 1, "name": "a"}],
                        "next": "https://evil.example.org/items?page=2",
                    },
                ),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(pagination=self._link_pagination())
        with pytest.raises(ReadError, match="origin"):
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
        # Exactly one request went out — the cross-origin URL was never hit.
        assert len(session.calls) == 1

    @pytest.mark.asyncio
    async def test_link_non_string_next_url_raises(self):
        """A next_url resolving to a non-string is malformed, not a stop."""
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={"records": [{"id": 1, "name": "a"}], "next": 7},
                ),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(pagination=self._link_pagination())
        with pytest.raises(ReadError, match="next_url"):
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


class TestReadBatchesCursorTemplate:
    """The next_cursor value expression supports the full grammar."""

    @pytest.mark.asyncio
    async def test_template_next_cursor_interpolates_response(self):
        """A template next_cursor builds the token from response values
        through the shared resolver's response scope."""
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={"records": [{"id": 1, "name": "a"}], "next_page": 2},
                ),
                _FakeResponse(status=200, body={"records": [{"id": 2, "name": "b"}]}),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "cursor",
                "cursor": {
                    "param": "page_token",
                    "next_cursor": {"template": "page-${response.body.next_page}"},
                },
                "limit": {"param": "limit"},
                # Templates render missing placeholders leniently ("page-"),
                # so the declared stop condition — not token absence — ends
                # this loop.
                "stop_when": {"missing": {"ref": "response.body.next_page"}},
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
        assert session.calls[1][2]["page_token"] == "page-2"
        assert len(session.calls) == 2


class TestReadBatchesStopWhen:
    """The strategy's declared stop_when predicate ends the loop."""

    @pytest.mark.asyncio
    async def test_stop_when_ends_offset_loop_despite_full_page(self):
        """A true predicate stops the loop even when the page is full (the
        short-page heuristic alone would have continued)."""
        session = _FakeSession(
            [
                _FakeResponse(
                    status=200,
                    body={
                        "records": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}],
                        "has_more": False,
                    },
                ),
            ]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            pagination={
                "type": "offset",
                "offset": {"param": "offset", "initial": 0},
                "limit": {"param": "limit"},
                "stop_when": {"eq": [{"ref": "response.body.has_more"}, False]},
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

        # One full page, then the predicate stops the loop — no second call.
        assert [b.num_rows for b in batches] == [2]
        assert len(session.calls) == 1

    @pytest.mark.asyncio
    async def test_stop_when_evaluation_failure_raises(self):
        """An unevaluable predicate is a deterministic read error, not a
        guessed truth value."""
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
                "type": "offset",
                "offset": {"param": "offset", "initial": 0},
                "limit": {"param": "limit"},
                # ``total`` is absent from the body: ordering None against a
                # number cannot be answered.
                "stop_when": {"lt": [{"ref": "response.body.total"}, 10]},
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
# Incremental replication
# ---------------------------------------------------------------------------


class TestReadBatchesIncrementalReplication:
    @pytest.mark.asyncio
    async def test_incremental_cursor_flows_into_params_with_safety_window(self):
        """The stored cursor minus the safety window rides the mapped param."""
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
                    {"cursor_field": "updated_at", "param": "since", "operator": "gte"},
                ],
            },
            extra_properties=_UPDATED_AT_PROPERTY,
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
        """With no prior cursor the first run sends no incremental filter."""
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
                "cursor_mappings": [
                    {"cursor_field": "updated_at", "param": "since", "operator": "gte"}
                ]
            },
            extra_properties=_UPDATED_AT_PROPERTY,
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
        from src.source.connectors.base import ConnectorConnectionError

        runtime = _runtime_with_session(_FakeSession([]))

        async def _boom(*_args, **_kwargs):
            raise RuntimeError("materialize boom")

        runtime.materialize = _boom  # type: ignore[method-assign]

        connector = APIConnector("test")
        with pytest.raises(ConnectorConnectionError):
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
        """A function-form param default resolves through the expression grammar."""
        import base64

        session = _FakeSession(
            [_FakeResponse(status=200, body={"records": [{"id": 1, "name": "a"}]})]
        )
        runtime = _runtime_with_session(session, parameters={"api_token": "tok-123"})
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            params={
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
        )
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
        """A default that cannot resolve omits the param from the request."""
        session = _FakeSession(
            [_FakeResponse(status=200, body={"records": [{"id": 1, "name": "a"}]})]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            params={
                "team": {
                    "in": "query",
                    "type": "string",
                    "required": False,
                    "default": {"ref": "connection.parameters.missing_team"},
                },
            }
        )
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
        """A runtime.batch_size ref resolves to the effective page size."""
        session = _FakeSession(
            [_FakeResponse(status=200, body={"records": [{"id": 1, "name": "a"}]})]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            params={
                "page_size": {
                    "in": "query",
                    "type": "integer",
                    "required": False,
                    "default": {"ref": "runtime.batch_size"},
                },
            }
        )
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
        """A template default with a missing placeholder resolves partially."""
        session = _FakeSession(
            [_FakeResponse(status=200, body={"records": [{"id": 1, "name": "a"}]})]
        )
        runtime = _runtime_with_session(session, parameters={"org": "acme"})
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            params={
                "scope": {
                    "in": "query",
                    "type": "string",
                    "required": False,
                    "default": {
                        "template": (
                            "${connection.parameters.org}/"
                            "${connection.parameters.gone}"
                        )
                    },
                },
            }
        )
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
        """A declared POST-read body resolves and rides as the JSON body."""
        session = _FakeSession(
            [_FakeResponse(status=200, body={"records": [{"id": 1, "name": "a"}]})]
        )
        runtime = _runtime_with_session(session, parameters={"region": "eu"})
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            request={
                "method": "POST",
                "path": "/items/search",
                "body": {
                    "region": {"ref": "connection.parameters.region"},
                    "missing": {"ref": "connection.parameters.not_there"},
                    "static": "all",
                },
            }
        )
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
        """Pagination re-sends the declared body on every page request."""
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
            request={
                "method": "POST",
                "path": "/items/search",
                "body": {"region": {"ref": "connection.parameters.region"}},
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

        assert session.bodies == [{"region": "eu"}, {"region": "eu"}]

    @pytest.mark.asyncio
    async def test_from_param_binds_resolved_param_into_body(self):
        # Bodies may mix literals with {"from_param": ...} per the contract;
        # a param declared ``in: body`` lands in the body via its binding
        # and stays out of the query string.
        """An in-body param binds via from_param and stays out of the query."""
        session = _FakeSession(
            [_FakeResponse(status=200, body={"records": [{"id": 1, "name": "a"}]})]
        )
        runtime = _runtime_with_session(session, parameters={"team_id": "t-9"})
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            params={
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
            },
            request={
                "method": "POST",
                "path": "/items/search",
                "body": {"filter": {"team": {"from_param": "team"}}},
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

        params = session.calls[0][2]
        assert params == {"verbose": True}  # in: body param not in query
        assert session.bodies[0] == {"filter": {"team": "t-9"}}

    @pytest.mark.asyncio
    async def test_from_param_for_missing_param_drops_field(self):
        # A from_param naming a param with no resolved value (declared but
        # no default) binds None, which the expression pass omits — never
        # the raw binding dict.
        """A from_param over an unresolved param drops the body field."""
        session = _FakeSession(
            [_FakeResponse(status=200, body={"records": [{"id": 1, "name": "a"}]})]
        )
        runtime = _runtime_with_session(session)
        connector = APIConnector("test")

        endpoint = _endpoint_doc_with_records(
            params={"q": {"in": "body", "type": "string", "required": False}},
            request={
                "method": "POST",
                "path": "/items/search",
                "body": {"q": {"from_param": "q"}, "static": "all"},
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

        assert session.bodies[0] == {"static": "all"}

    @pytest.mark.asyncio
    async def test_replication_param_binds_into_body(self):
        # Incremental POST-search endpoints carry the cursor filter in the
        # body: the replication-derived param value must reach from_param.
        """The replication-derived param value binds into the request body."""
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
                    {"cursor_field": "updated_at", "param": "since", "operator": "gte"},
                ],
            },
            params={
                "since": {
                    "in": "body",
                    "type": "string",
                    "required": False,
                    "controlled_by": "replication",
                },
            },
            request={
                "method": "POST",
                "path": "/items/search",
                "body": {"updated_after": {"from_param": "since"}},
            },
            extra_properties=_UPDATED_AT_PROPERTY,
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

        assert session.bodies[0] == {"updated_after": "2024-01-01T11:59:00Z"}

    @pytest.mark.asyncio
    async def test_body_paginated_endpoint_advances_offset_in_body(self):
        # Pagination params declared ``in: body`` ride inside the body via
        # from_param — rebuilt per page so the offset actually advances —
        # and never appear in the query string.
        """Body-placed pagination params advance per page and skip the query."""
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
            params={
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
            },
            request={
                "method": "POST",
                "path": "/items/search",
                "body": {
                    "paging": {
                        "offset": {"from_param": "offset"},
                        "limit": {"from_param": "limit"},
                    },
                },
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
            "amount": {
                "type": "number",
                "native_type": "number",
                "arrow_type": "Decimal128(20,8)",
            },
            "rate": {
                "type": "number",
                "native_type": "number",
                "arrow_type": "Float64",
            },
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
        """A fractional keyset key parses to Decimal; it must reach the next
        request's query as its full-precision string (yarl would otherwise
        truncate the Decimal and silently skip rows)."""
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
                                "score": {
                                    "type": "number",
                                    "native_type": "number",
                                    "arrow_type": "Float64",
                                },
                            },
                        },
                    },
                },
            },
        )
        endpoint["operations"]["read"]["params"] = {
            "after": {
                "in": "query",
                "type": "number",
                "required": False,
                "controlled_by": "pagination",
            },
            "limit": {
                "in": "query",
                "type": "integer",
                "required": False,
                "controlled_by": "pagination",
            },
        }
        endpoint["operations"]["read"]["request"]["query"] = {
            "after": {"from_param": "after"},
            "limit": {"from_param": "limit"},
        }
        endpoint["operations"]["read"]["pagination"] = {
            "type": "keyset",
            "keyset": {"param": "after", "order_by_field": "score"},
            "limit": {"param": "limit"},
            "stop_when": {"empty": {"ref": "response.body.records"}},
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
        """A keyset param declared ``in: body`` reaches the JSON body as its
        native type; stringifying it (to dodge yarl in the query) would send
        an integer key as a string a numeric body schema can reject."""
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
            },
            params={
                "after": {
                    "in": "body",
                    "type": "integer",
                    "required": False,
                    "controlled_by": "pagination",
                },
                "limit": {
                    "in": "body",
                    "type": "integer",
                    "required": False,
                    "controlled_by": "pagination",
                },
            },
            request={
                "method": "POST",
                "path": "/items/search",
                "body": {
                    "after": {"from_param": "after"},
                    "limit": {"from_param": "limit"},
                },
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

        # Body param stays out of the query and keeps its native int type.
        assert "after" not in session.calls[1][2]
        assert session.bodies[1]["after"] == 2
        assert isinstance(session.bodies[1]["after"], int)

    @pytest.mark.asyncio
    async def test_keyset_body_param_narrows_fractional_decimal_to_float(self):
        """A fractional keyset key parses to Decimal; in the body it becomes
        a float (stdlib json.dumps, which aiohttp uses, cannot serialize a
        Decimal and would raise on page 2)."""
        page1 = '{"records": [{"score": 1.5}, {"score": 2.5}]}'
        page2 = '{"records": [{"score": 3.5}]}'
        # No limit param is declared, so a short page does not stop the
        # loop; the empty third page does.
        session = _FakeSession(
            [
                _FakeResponse(status=200, body=page1),
                _FakeResponse(status=200, body=page2),
                _FakeResponse(status=200, body='{"records": []}'),
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
                                "score": {
                                    "type": "number",
                                    "native_type": "number",
                                    "arrow_type": "Float64",
                                },
                            },
                        },
                    },
                },
            },
        )
        endpoint["operations"]["read"]["request"] = {
            "method": "POST",
            "path": "/items/search",
            "body": {"after": {"from_param": "after"}},
        }
        endpoint["operations"]["read"]["params"] = {
            "after": {
                "in": "body",
                "type": "number",
                "required": False,
                "controlled_by": "pagination",
            },
        }
        endpoint["operations"]["read"]["pagination"] = {
            "type": "keyset",
            "keyset": {"param": "after", "order_by_field": "score"},
            "stop_when": {"empty": {"ref": "response.body.records"}},
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
        assert body["after"] == 2.5
        assert isinstance(body["after"], float)
