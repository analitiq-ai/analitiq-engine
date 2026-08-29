"""A scripted HTTP session and a pre-materialized runtime, for both roles.

No live HTTP: the retry client accepts any session whose ``request`` is
awaitable and answers a response object, so the fakes here script the
statuses and bodies a test wants and record what was actually sent.
"""

from __future__ import annotations

import json
from typing import Any, cast

from analitiq.contracts.endpoints import ApiEndpointDoc
from analitiq.contracts.stream import StreamSource
from multidict import CIMultiDict

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

    def __init__(
        self,
        *,
        status: int = 200,
        body: Any = None,
        text: str | None = None,
        headers: dict[str, str] | None = None,
    ):
        self.status = status
        # The client hands back a case-insensitive map; so does the fake.
        self.headers = CIMultiDict(headers or {})
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

    A session's own ``headers`` are the transport's: the spec they were
    resolved from carries the same names, which is what an operation's
    declared headers are judged against.
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
            "headers": dict(transport_session.headers),
            "timeout_seconds": 30.0,
            "rate_limit": None,
        }
        for ref, (transport_session, url) in opened.items()
    }
    runtime._http_transports = {
        ref: HttpTransport(
            session=cast(Any, transport_session),
            base_url=url,
            headers=dict(transport_session.headers),
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


def endpoint_json(
    *,
    pagination: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
    replication: dict[str, Any] | None = None,
    request: dict[str, Any] | None = None,
    records_ref: str = "response.body.records",
    response_fields: dict[str, Any] | None = None,
    record_fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """A read endpoint document around ``/items``, unparsed.

    Only for the tests that deliberately author a document the contract
    REFUSES and assert the refusal. Everything else goes through
    :func:`endpoint_document`, which is this document parsed.

    ``response_fields`` declares further members of the response body
    beside ``records`` (a next link, a ``has_more`` flag); ``record_fields``
    declares further members of one record. Both exist because the contract
    resolves every ``response.body.<path>`` a document references against
    this schema, so a paging value naming a field the schema does not
    declare is an authoring defect rather than a shortcut a fixture may
    take.
    """
    record_properties: dict[str, Any] = {
        "id": {"type": "integer", "native_type": "integer", "arrow_type": "Int64"},
        "name": {"type": "string", "native_type": "text", "arrow_type": "Utf8"},
        **(record_fields or {}),
    }
    read: dict[str, Any] = {
        "request": request or {"method": "GET", "path": "/items"},
        "response": {
            "schema": {
                "type": "object",
                "properties": {
                    "records": {
                        "type": "array",
                        "items": {"type": "object", "properties": record_properties},
                    },
                    **(response_fields or {}),
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


def endpoint_document(**shape: Any) -> dict[str, Any]:
    """The same document, parsed and handed back as the JSON it was.

    JSON, because that is what the caller under test is handed: the engine
    serializes the document across the worker boundary and the connector
    parses it on the far side, so a fixture that started as a model would
    skip the parse every real run performs.

    Parsed here all the same, and the result thrown away. A fixture written
    as a hand-shaped dict can drift from the published contract with no
    test noticing -- which is how contract fields ship unread -- so this
    parse is the fake refusing itself: a document ``ApiEndpointDoc`` would
    not accept fails in the test that built it, rather than surviving as a
    shape no real run can send.
    """
    document = endpoint_json(**shape)
    ApiEndpointDoc.model_validate(document)
    return document


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
    # Parsed here and the result thrown away, exactly as the endpoint
    # document above is: the read path parses this block against
    # ``StreamSource``, so a fixture the contract would refuse fails in the
    # test that built it rather than pinning a read no engine can send.
    StreamSource.model_validate(block)
    return block
