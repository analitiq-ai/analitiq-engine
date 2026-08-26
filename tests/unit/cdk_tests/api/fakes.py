"""A scripted HTTP session and a pre-materialized runtime, for both roles.

No live HTTP: the retry client accepts any session whose ``request`` is
awaitable and answers a response object, so the fakes here script the
statuses and bodies a test wants and record what was actually sent.
"""

from __future__ import annotations

import json
from typing import Any, cast

from cdk.connection_runtime import ConnectionRuntime
from cdk.secrets import InMemorySecretsResolver
from cdk.transport_factory import HttpTransport

BASE_URL = "https://api.example.test"


class FakeRequestInfo:
    """What the client's response errors read off a request when printed."""

    def __init__(self, url: str = BASE_URL):
        self.real_url = url
        self.url = url
        self.method = "GET"
        self.headers: dict[str, str] = {}


class FakeResponse:
    """One scripted response, shaped like the client's own."""

    def __init__(self, *, status: int = 200, body: Any = None, text: str | None = None):
        self.status = status
        self.method = "GET"
        self.closed = False
        self.request_info = FakeRequestInfo()
        self.history = ()
        self._body = body
        self._text = text

    def close(self) -> None:
        self.closed = True

    async def json(self, *, loads: Any = json.loads, content_type: Any = "") -> Any:
        raw = self._raw()
        # The real client answers None for an empty body rather than asking
        # the decoder to parse nothing -- a 204 must decode, not raise.
        if not raw.strip():
            return None
        return loads(raw)

    async def text(self) -> str:
        return self._raw()

    def _raw(self) -> str:
        if self._text is not None:
            return self._text
        return json.dumps(self._body)


class FakeSession:
    """Answers each request from a script and records what was sent."""

    def __init__(self, responses: list[FakeResponse] | None = None):
        self._responses = list(responses or [])
        self.headers: dict[str, str] = {}
        self.calls: list[dict[str, Any]] = []
        self.closed = False

    def queue(self, response: FakeResponse) -> None:
        self._responses.append(response)

    async def request(self, method: str, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append({"method": method, "url": url, **kwargs})
        if not self._responses:
            raise AssertionError(f"unexpected extra request: {method} {url} {kwargs}")
        response = self._responses.pop(0)
        response.method = method
        return response

    async def close(self) -> None:
        self.closed = True


#: The ref the default transport is registered under below. Named, because
#: a request declaring ``transport_ref`` on the default transport (which is
#: what a real connector's endpoints do) has to reach the same session.
DEFAULT_TRANSPORT_REF = "api"


def runtime_with(
    session: FakeSession,
    *,
    parameters: dict[str, Any] | None = None,
    error_map: dict[str, Any] | None = None,
    base_url: str = BASE_URL,
    rate_limiter: Any = None,
    transports: dict[str, tuple[FakeSession, str]] | None = None,
) -> ConnectionRuntime:
    """Build a runtime whose HTTP transports are already materialized.

    ``transports`` registers further named transports as
    ``ref -> (session, base_url)``, already open -- the shape a run has
    once the bootstrap resolved their specs and a request opened them.
    """
    runtime = ConnectionRuntime(
        raw_config={"host": base_url, "parameters": parameters or {}},
        connection_id="test-conn",
        connector_id="test-connector",
        connector_type="api",
        driver=None,
        resolver=InMemorySecretsResolver({}),
    )
    opened = {DEFAULT_TRANSPORT_REF: (session, base_url), **(transports or {})}
    runtime._transport_specs = {
        ref: {
            "transport_type": "http",
            "base_url": url,
            "headers": {},
            "timeout_seconds": 30.0,
            "rate_limit": None,
        }
        for ref, (_session, url) in opened.items()
    }
    runtime._http_transports = {
        ref: HttpTransport(
            session=cast(Any, transport_session),
            base_url=url,
            headers={},
            # The declared ceiling belongs to the transport block, so a
            # named transport paces independently of the default.
            rate_limiter=rate_limiter if ref == DEFAULT_TRANSPORT_REF else None,
        )
        for ref, (transport_session, url) in opened.items()
    }
    runtime._default_transport_ref = DEFAULT_TRANSPORT_REF
    runtime._session = session
    runtime._base_url = base_url
    runtime._rate_limiter = rate_limiter
    runtime._materialized = True
    runtime._declared_error_map = error_map
    return runtime


def sent_query(call: dict[str, Any]) -> dict[str, Any]:
    """The query one recorded call sent, keyed by name.

    The client takes name/value PAIRS, because a query may repeat a name
    (an exploded ``form`` array sends ``tags=a&tags=b``), so that is what
    the fake records. A test asking about one key wants a mapping, and a
    test about repetition reads the pairs directly.
    """
    return dict(call["params"])


class FakeCheckpoint:
    """The read path's cursor store, in memory."""

    def __init__(self, cursor: dict[str, Any] | None = None):
        self.saved: list[dict[str, Any]] = []
        self._cursor = cursor

    async def get_cursor(
        self, stream_name: str, partition: dict[str, Any] | None = None
    ) -> dict[str, Any] | None:
        return self._cursor

    async def save_cursor(
        self,
        stream_name: str,
        partition: dict[str, Any] | None,
        cursor: dict[str, Any],
    ) -> None:
        self.saved.append(cursor)


def endpoint_document(
    *,
    pagination: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
    replication: dict[str, Any] | None = None,
    request: dict[str, Any] | None = None,
    records_ref: str = "response.body.records",
) -> dict[str, Any]:
    """Build a read endpoint document around ``/items``.

    Written raw, as the engine hands it across the boundary: the CDK
    navigates an already-validated document and must not import a contract
    model to build one.
    """
    read: dict[str, Any] = {
        "request": request or {"method": "GET", "path": "/items"},
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
                    }
                },
            },
            "records": {"ref": records_ref},
        },
    }
    if params:
        read["params"] = params
    if pagination:
        read["pagination"] = pagination
    if replication:
        read["replication"] = replication
    return {
        "$schema": "https://schemas.analitiq.ai/api-endpoint/latest.json",
        "endpoint_id": "items",
        "operations": {"read": read},
    }


def stream_source(
    *,
    method: str = "full_refresh",
    cursor_field: str | None = None,
    safety_window: int | None = None,
    filters: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build the stream's source block the engine passes with the document."""
    replication: dict[str, Any] = {"method": method}
    if cursor_field:
        replication["cursor_field"] = cursor_field
    if safety_window is not None:
        replication["safety_window_seconds"] = safety_window
    block: dict[str, Any] = {
        "endpoint_ref": {
            "scope": "connector",
            "connection_id": "test-conn",
            "endpoint_id": "items",
        },
        "primary_keys": ["id"],
        "replication": replication,
    }
    if filters:
        block["filters"] = filters
    return block
