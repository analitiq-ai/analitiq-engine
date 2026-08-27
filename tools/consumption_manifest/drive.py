"""Run the engine's api path over one corpus document and watch it read.

Nothing here restates what the read or the write consumes. The corpus
case is handed to :class:`~cdk.api.generic.GenericAPIConnector` -- the
class a pipeline actually runs -- against a scripted session, and the
manifest is whatever that run looked up. A field the connector never asks
for is a field the engine does not consume, which is the entire claim the
artifact makes.

The session is scripted rather than live for the same reason the
conformance kit's is: a run that reaches a provider certifies the
provider. Every response a case declares is the page or the write
acknowledgement the document's own schema says it will be.

A drive that raises unexpectedly is a defect in the corpus case, not a
finding: the census depends on the run reaching the end of the path, and
a case that fails part way silently claims less than the engine reads.

Some fields ARE the refusal, though. ``request.headers_remove`` is
contract-valid and the engine reads it only to refuse the document --
the connection's default headers live on a shared session and no
per-request instruction can delete one. A case declares that with
``refused``, naming what the engine must say; the drive then insists on
the refusal rather than on a clean run. Reading a field in order to
refuse it is still reading it: the class this manifest exists to catch
is the field nothing looks at, whose author is never told.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa

from cdk.api import GenericAPIConnector
from cdk.connection_runtime import ConnectionRuntime
from cdk.secrets import InMemorySecretsResolver
from cdk.transport_factory import HttpTransport
from cdk.types import Cursor, SchemaSpec, WriteMode

__all__ = ["DriveError", "drive_case"]

#: The origin every scripted transport serves. Cases that exercise
#: ``transport_ref`` name the second one.
BASE_URL = "https://api.example.test"
SECOND_BASE_URL = "https://files.example.test"

#: The transport refs the corpus may name. The default carries a name of
#: its own so a request declaring the default explicitly reaches the same
#: session as one declaring nothing.
DEFAULT_TRANSPORT_REF = "api"
SECOND_TRANSPORT_REF = "files"

_WRITE_MODES = {
    "insert": WriteMode.WRITE_MODE_INSERT,
    "upsert": WriteMode.WRITE_MODE_UPSERT,
    "truncate_insert": WriteMode.WRITE_MODE_TRUNCATE_INSERT,
}


class DriveError(RuntimeError):
    """A corpus case did not run to the end of the engine's path."""


class _ScriptedResponse:
    """One response the scripted session answers, shaped like the client's."""

    def __init__(self, body: Any, status: int = 200) -> None:
        self.status = status
        self.method = "GET"
        self.history: tuple[Any, ...] = ()
        self.request_info = _RequestInfo()
        self.closed = False
        self._body = body

    def close(self) -> None:
        """Match the client's response surface."""
        self.closed = True

    async def json(self, *, loads: Any = json.loads, content_type: Any = "") -> Any:
        """Decode the scripted body through the caller's own decoder.

        Round-tripped through JSON rather than handed over as live Python
        objects, so the engine reads the types a provider's bytes would
        actually decode to.
        """
        return loads(await self.text())

    async def text(self) -> str:
        """Return the scripted body, as the bytes a provider would send."""
        return json.dumps(self._body)


class _RequestInfo:
    """What a client error prints about the request that produced it."""

    def __init__(self) -> None:
        self.real_url = BASE_URL
        self.url = BASE_URL
        self.method = "GET"
        self.headers: dict[str, str] = {}


class _ScriptedSession:
    """Answers each request from a script, and refuses to invent one.

    Running out of responses ends the drive loudly: a page the case
    forgot to script would otherwise look like an endpoint that stopped
    paginating, and the fields the next page's reads would have credited
    would go missing from the manifest without a word.
    """

    def __init__(self, responses: list[_ScriptedResponse]) -> None:
        self._responses = list(responses)
        self.headers: dict[str, str] = {}
        self.calls: list[dict[str, Any]] = []

    async def request(self, method: str, url: str, **kwargs: Any) -> _ScriptedResponse:
        """Answer the next scripted response, recording what was sent."""
        self.calls.append({"method": method, "url": url, **kwargs})
        if not self._responses:
            raise DriveError(f"the case scripted no response for {method} {url}")
        response = self._responses.pop(0)
        response.method = method
        return response

    async def close(self) -> None:
        """Match the client's session surface; nothing is held open."""


class _NoCheckpoint:
    """A cursor store with one stored cursor and no persistence."""

    def __init__(self, cursor: dict[str, Any] | None) -> None:
        self._cursor = cursor
        self.saved: list[dict[str, Any]] = []

    async def get_cursor(
        self, stream_name: str, partition: dict[str, Any] | None = None
    ) -> dict[str, Any] | None:
        """Answer the cursor the case declared, if any."""
        return self._cursor

    async def save_cursor(
        self,
        stream_name: str,
        partition: dict[str, Any] | None,
        cursor: dict[str, Any],
    ) -> None:
        """Keep what the read advanced to; nothing reads it back."""
        self.saved.append(cursor)


def _runtime(bodies: list[Any], opened: list["_ScriptedSession"]) -> ConnectionRuntime:
    """Build a runtime whose transports are already materialized, as a run's are.

    Both transports answer the same script. Which one a case dispatches
    through is the case's business -- an operation naming ``transport_ref``
    goes out on the second -- and scripting them separately would only mean
    every such case had to say the same thing twice.
    """
    session = _ScriptedSession([_ScriptedResponse(body) for body in bodies])
    second = _ScriptedSession([_ScriptedResponse(body) for body in bodies])
    opened.extend((session, second))
    runtime = ConnectionRuntime(
        raw_config={"host": BASE_URL, "parameters": {}},
        connection_id="census-connection",
        connector_id="census-connector",
        connector_type="api",
        resolver=InMemorySecretsResolver({}),
    )
    transports = {
        DEFAULT_TRANSPORT_REF: (session, BASE_URL),
        SECOND_TRANSPORT_REF: (second, SECOND_BASE_URL),
    }
    runtime._transport_specs = {
        ref: {
            "transport_type": "http",
            "base_url": url,
            "headers": {},
            "timeout_seconds": 30.0,
            "rate_limit": None,
        }
        for ref, (_, url) in transports.items()
    }
    runtime._http_transports = {
        ref: HttpTransport(
            session=scripted,  # type: ignore[arg-type]
            base_url=url,
            headers={},
            rate_limiter=None,
        )
        for ref, (scripted, url) in transports.items()
    }
    runtime._default_transport_ref = DEFAULT_TRANSPORT_REF
    runtime._session = session  # type: ignore[assignment]
    runtime._base_url = BASE_URL
    runtime._materialized = True
    return runtime


async def drive_case(
    case: dict[str, Any], document: dict[str, Any]
) -> list[dict[str, Any]]:
    """Run every operation *document* declares, the way a pipeline runs it.

    Returns what went out on the wire, in order, so a caller can ask
    whether two runs of the same case sent the same requests -- which is
    how the recording document is held to observing the run rather than
    changing it.

    A case naming a ``refused`` message must produce it; every other case
    must run to the end. Both directions fail loud, because both silently
    change what the manifest claims.
    """
    refused = case.get("refused")
    sessions: list[_ScriptedSession] = []
    try:
        if (document.get("operations") or {}).get("read") is not None:
            await _drive_read(case, document, sessions)
        for mode in (document.get("operations") or {}).get("write") or {}:
            await _drive_write(case, document, mode, sessions)
    except DriveError:
        raise
    except Exception as err:
        if refused is not None and refused in str(err):
            return _calls(sessions)
        raise DriveError(f"{case['case_id']}: the drive raised {err!r}") from err
    if refused is not None:
        raise DriveError(
            f"{case['case_id']}: expected the engine to refuse with "
            f"{refused!r}, but the drive ran clean"
        )
    return _calls(sessions)


def _calls(sessions: list["_ScriptedSession"]) -> list[dict[str, Any]]:
    """Every request the drive sent, in the order the sessions saw them."""
    return [call for session in sessions for call in session.calls]


async def _drive_read(
    case: dict[str, Any], document: dict[str, Any], sessions: list["_ScriptedSession"]
) -> None:
    """Drain the read to exhaustion, one scripted page at a time."""
    connector = GenericAPIConnector()
    config = {
        "endpoint_document": document,
        "stream_source": case.get("stream_source") or _stream_source(document),
    }
    checkpoint = _NoCheckpoint(case.get("stored_cursor"))
    async for _ in connector.read_batches(
        _runtime(case.get("pages") or [{}], sessions),
        config,
        checkpoint=checkpoint,
        stream_name="census",
        batch_size=case.get("batch_size", 10),
    ):
        pass


async def _drive_write(
    case: dict[str, Any],
    document: dict[str, Any],
    mode: str,
    sessions: list["_ScriptedSession"],
) -> None:
    """Configure the stream for one declared mode and write one batch."""
    write = case.get("write") or {}
    records = write.get("records") or [{"id": 1}]
    responses = write.get("responses") or [{} for _ in records]
    connector = GenericAPIConnector()
    connector.set_stream_endpoints({"census": document})
    try:
        await connector.connect(_runtime(responses, sessions))
        accepted = await connector.configure_schema(
            SchemaSpec(
                stream_id="census",
                version=1,
                write_mode=_WRITE_MODES[mode],
                ack_timeout_seconds=30,
            )
        )
        if not accepted:
            # The rejection is a string on the connector, not a raise, so a
            # case expecting one has to be given something to match.
            raise RuntimeError(
                f"the {mode} schema was refused -- "
                f"{connector.last_schema_rejection}"
            )
        await connector.write_batch(
            run_id="census-run",
            stream_id="census",
            batch_seq=1,
            record_batch=pa.RecordBatch.from_pylist(records),
            record_ids=[f"r{index}" for index in range(len(records))],
            cursor=Cursor(b"census"),
            emitted_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
    finally:
        await connector.disconnect()


def _stream_source(document: dict[str, Any]) -> dict[str, Any]:
    """Return the stream block the engine passes beside the document."""
    return {
        "endpoint_ref": {
            "scope": "connector",
            "connection_id": "census-connection",
            "endpoint_id": document["endpoint_id"],
        },
        "primary_keys": ["id"],
        "replication": {"method": "full_refresh"},
    }
