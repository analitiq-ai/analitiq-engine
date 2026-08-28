"""One HTTP round trip: what it sends, and what its answer means.

The read role and the write role each had their own request method, and
they disagreed about things no author chose: what counts as success, which
statuses the transport re-attempts, whether a decimal survives the body,
whether a non-JSON body is diagnosable. :class:`HttpSender` is the one
round trip both roles make, so those answers are given once.

This is the only module in the package that imports an HTTP client, which
is what keeps the loop, the strategies, the predicates, the verdicts and
the URL rules (:mod:`cdk.api.urls`) testable without one.
"""

from __future__ import annotations

import base64
import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import aiohttp
import orjson
from aiohttp_retry import ExponentialRetry, RetryClient

from ..declarations import ErrorMap
from ..rate_limiter import RateLimiter
from .body import (
    FORM_CONTENT_TYPE,
    JSON_CONTENT_TYPE,
    encode_form,
    media_type,
    unsupported_media_type,
)
from .exceptions import RequestSpecError
from .verdicts import classify_exception, classify_status

if TYPE_CHECKING:
    from .dialects import ApiDialect

__all__ = [
    "ApiResponseError",
    "HttpSender",
    "Received",
    "SignedRequest",
    "encode_body",
    "failure_facts",
    "loads_preserving_decimals",
    "query_pairs",
]

logger = logging.getLogger(__name__)

#: Retry attempts used when the connection declares no ``max_retries``.
DEFAULT_MAX_RETRIES = 3


@dataclass(frozen=True)
class Received:
    """One successful answer, with what a declared expression can read of it.

    The status and the headers travel with the payload because the
    contract's response scope offers all three (``response.status``,
    ``response.headers``, ``response.body``): a write's declared
    ``success_when`` may key off any of them, and the sender is the only
    place that still holds the response object.
    """

    status: int
    #: The client's own header map, case-insensitive on purpose: HTTP/2
    #: lowercases every name, so a declared ``response.headers.Location``
    #: must find a ``location`` too.
    headers: Mapping[str, str]
    payload: Any


@dataclass(frozen=True)
class SignedRequest:
    """One request, complete enough for a dialect to sign.

    Frozen because :meth:`cdk.api.dialects.ApiDialect.sign_request` returns
    a new request rather than mutating this one -- a signature covers a
    request that cannot change under it.
    """

    method: str
    url: str
    #: The query as the ORDERED PAIRS the wire carries, not as a mapping.
    #: An exploded array repeats its name (``tags=a&tags=b``), which a
    #: mapping cannot hold and a canonicalizing signature would therefore
    #: sign in a shape the request never takes -- the hook is promised the
    #: final request, so the flattening happens before it, not after.
    params: list[tuple[str, Any]] = field(default_factory=list)
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes | None = None
    #: The media type ``body`` was encoded as, sent as ``Content-Type``.
    #: ``None`` means the endpoint declared none, so the body is JSON.
    #: Carried beside the bytes rather than pre-placed in ``headers`` so a
    #: dialect signing this request sees the same header the wire does.
    content_type: str | None = None


class ApiResponseError(aiohttp.ClientResponseError):
    """A response both roles treat as a failure, with its category attached.

    Subclasses the client library's response error so a handler catching
    ``aiohttp.ClientError`` keeps working. ``declared_category`` is the
    classification made at the raise site -- by the dialect or the declared
    ``error_map`` -- so the verdict does not have to be re-derived from the
    status a second time, further from the response that produced it.
    """

    def __init__(self, *args: Any, declared_category: str | None = None, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.declared_category = declared_category


def loads_preserving_decimals(payload: str) -> Any:
    """Decode a JSON body without flattening decimals to float.

    The stdlib default parses every floating-point token as a double,
    discarding digits before Arrow ever sees the value, so a Decimal-typed
    column lands a rounded number. Parsing those tokens as ``Decimal`` keeps
    the exact source digits; the schema contract then renders each value per
    its declared Arrow type. Integer tokens are untouched -- the default
    already parses them as arbitrary-precision ``int``.
    """
    return json.loads(payload, parse_float=Decimal)


def _orjson_default(obj: Any) -> Any:
    """Serialise the types orjson does not handle natively.

    orjson handles ``datetime`` / ``date`` / ``time`` / ``UUID`` /
    dataclasses / enums / numpy scalars directly -- only ``Decimal`` and
    ``bytes`` reach this hook. A ``Decimal`` renders as its exact decimal
    string: precision is the one thing that cannot be recovered downstream,
    and most providers accept string-or-number for a numeric field.
    ``bytes`` is base64-encoded per JSON convention.
    """
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(obj)).decode("ascii")
    raise TypeError(
        f"orjson cannot serialise {type(obj).__name__}; add a handler "
        f"if this type should appear in API request bodies"
    )


def encode_body(data: Any, content_type: str | None = None) -> bytes:
    """Serialise a request body to the bytes that go on the wire.

    One serialiser for both roles, selected by what the endpoint declared.
    JSON when it declared nothing, which is what every endpoint took before
    ``request.content_type`` existed.

    ``aiohttp``'s own ``json=`` argument calls the stdlib encoder, which
    understands neither ``datetime`` nor ``Decimal``; orjson handles the
    first natively and the hook above renders the second losslessly.

    An unsupported media type raises rather than falling back to JSON under
    a header claiming otherwise -- though it should not reach here:
    ``request_block_problem`` refuses one before anything is sent, which is
    where a deterministic defect belongs. This is what makes that true
    rather than assumed.
    """
    problem = unsupported_media_type(content_type)
    if problem is not None:
        raise RequestSpecError(problem)
    if media_type(content_type) == FORM_CONTENT_TYPE:
        return encode_form(data)
    return orjson.dumps(data, default=_orjson_default)


def query_value(value: Any) -> Any:
    """Render one value for the query string.

    ``yarl`` truncates a ``Decimal`` in a query string, so it goes as its
    exact decimal string -- the same rendering the body gets, so one
    authored value does not land two ways depending on where it was
    declared.
    """
    return str(value) if isinstance(value, Decimal) else value


def query_pairs(query: Mapping[str, Any]) -> list[tuple[str, Any]]:
    """Flatten the built query into the name/value pairs the wire carries.

    A key holding a list is the one query serialization that repeats a
    name (an exploded ``form`` array: ``tags=a&tags=b``), and a mapping
    cannot hold the same key twice -- so the repetition lives as a list
    from the moment the style decides it until here.

    Called when the request is ASSEMBLED, before the dialect signs it:
    :attr:`SignedRequest.params` is promised to be the request that goes
    out, and a signature canonicalized over ``{"tags": ["a", "b"]}``
    covers a shape the wire never carries. What to send is decided in
    :mod:`cdk.api.query_style`; how each value is spelled is ``yarl``'s.
    """
    return [
        (name, query_value(item))
        for name, value in query.items()
        for item in (value if isinstance(value, list) else [value])
    ]


def failure_facts(
    exc: BaseException, *, error_map: ErrorMap | None
) -> tuple[int | None, str | None]:
    """Read the status and declared category off a caught transport failure.

    The one place a client-library exception becomes the two facts every
    verdict is built from, so the read role and the write role cannot
    classify the same failure differently. A response error resolves by its
    status; a status-less one by the declared exception family. Keeping the
    branches separate is what stops a broad declared exception class from
    claiming deterministic 4xx rejections.
    """
    declared = getattr(exc, "declared_category", None)
    if isinstance(exc, aiohttp.ClientResponseError):
        status = exc.status
        if declared is None and error_map is not None:
            match = error_map.match_http(status)
            declared = match.category if match is not None else None
        return status, declared
    if declared is None:
        declared = classify_exception(exc, error_map=error_map)
    return None, declared


class HttpSender:
    """The one HTTP round trip the API connector makes, in either role.

    Holds the retry client, the rate limiter and the dialect, so a caller
    hands it a finished request and gets back a decoded, dialect-unwrapped
    payload. The read role reads that payload; the write role ignores it.
    """

    def __init__(
        self,
        *,
        session: aiohttp.ClientSession,
        rate_limiter: RateLimiter | None,
        dialect: ApiDialect,
        retry_statuses: set[int],
        max_retries: int = DEFAULT_MAX_RETRIES,
    ) -> None:
        self._rate_limiter = rate_limiter
        self._dialect = dialect
        self._client = RetryClient(
            client_session=session,
            retry_options=ExponentialRetry(
                attempts=max_retries,
                statuses=retry_statuses,
                # The status set is authoritative -- it already carries the
                # built-in choices and whatever the connector declared.
                # aiohttp_retry otherwise re-attempts EVERY 5xx on top of
                # it, which would hammer a status the declaration calls
                # fatal and burn the ack deadline before the fatal ack
                # exists.
                retry_all_server_errors=False,
            ),
        )

    async def close(self) -> None:
        """Close the retry client (and with it the session it wraps)."""
        await self._client.close()

    async def send(self, request: SignedRequest, *, unwrap_page: bool) -> Received:
        """Issue one request and return the decoded answer.

        ``unwrap_page`` says whether the dialect's page unwrap applies.
        Only a read has pages: the hook exists to reach into a read
        envelope, and a write response is a different shape the same
        dialect was never written for. Running it on one would raise after
        the provider had already accepted the record, turning an accepted
        batch into a reported failure.

        Success is any status below 400, both roles. A read endpoint
        answering 201/202/204/206 is a legitimate provider, and failing a
        whole stream on one is the engine second-guessing HTTP; a 204 with
        an empty body decodes to ``None`` and extracts to zero records,
        which ends the traversal cleanly. Redirects never reach here -- the
        client follows them.

        The dialect classifies every response, success statuses included,
        so a provider that answers 200 with an error envelope raises here
        rather than being read as an empty page.
        """
        if request.body is not None:
            # The media type the endpoint declared, or JSON when it declared
            # none. Sent unconditionally rather than only when no header
            # says otherwise: since contract 1.0.0rc23 no header map may
            # name Content-Type (RULE-HTTP-003), so `content_type` is the
            # only place it can come from and there is nothing to defer to.
            request = replace(
                request,
                headers={
                    **request.headers,
                    "Content-Type": request.content_type or JSON_CONTENT_TYPE,
                },
            )
        request = self._dialect.sign_request(request)
        if self._rate_limiter:
            await self._rate_limiter.acquire()
        logger.debug("API %s %s params=%s", request.method, request.url, request.params)
        async with self._client.request(
            method=request.method,
            url=request.url,
            params=request.params,
            data=request.body,
            headers=request.headers or None,
        ) as response:
            payload, snippet = await self._read_body(response)
            category = classify_status(
                response.status,
                payload,
                dialect=self._dialect,
                error_map=self._dialect.error_map,
            )
            if response.status >= 400 or category is not None:
                raise ApiResponseError(
                    response.request_info,
                    response.history,
                    status=response.status,
                    message=(
                        f"API {request.method} {request.url} -> status "
                        f"{response.status}; body[:500]={snippet!r}"
                    ),
                    declared_category=category,
                )
            return Received(
                status=int(response.status),
                headers=response.headers,
                payload=self._dialect.unwrap_page(payload) if unwrap_page else payload,
            )

    async def probe(self, url: str) -> int:
        """Return the status of a bare GET, for a liveness check.

        Separate from :meth:`send` because a health check judges the status
        itself: a 404 on the base URL means the API answered, which is the
        question being asked, while ``send`` is right to fail on it.
        """
        async with self._client.get(url) as response:
            return int(response.status)

    @staticmethod
    async def _read_body(response: aiohttp.ClientResponse) -> tuple[Any, str]:
        """Return the decoded body and a snippet of it, for either outcome.

        The content type is not enforced: a provider answering
        ``application/octet-stream`` used to raise a bare
        ``ContentTypeError`` no classifier ever saw, and an empty 204 body
        must decode to ``None`` rather than raise.

        A body that is genuinely not JSON raises a payload error carrying a
        snippet, so the failure is diagnosable from the ack rather than
        only from this process's logs -- but only on a status that was
        otherwise a success. On a failing status the status IS the fact
        being reported, and raising a decode error instead would throw away
        the one thing the verdict is built from.
        """
        try:
            payload = await response.json(
                loads=loads_preserving_decimals, content_type=None
            )
        except (
            json.JSONDecodeError,
            aiohttp.ContentTypeError,
            UnicodeDecodeError,
        ) as err:
            snippet = (await response.text())[:500]
            if response.status < 400:
                raise aiohttp.ClientPayloadError(
                    f"API returned status {response.status} with a non-JSON "
                    f"body: {err}; body[:500]={snippet!r}"
                ) from err
            return None, snippet
        return payload, str(payload)[:500]
