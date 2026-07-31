"""One HTTP round trip, and every rule about a URL.

The read role and the write role each had their own request method, and
they disagreed about things no author chose: what counts as success, which
statuses the transport re-attempts, whether a decimal survives the body,
whether a non-JSON body is diagnosable. :class:`HttpSender` is the one
round trip both roles make, so those answers are given once.

This is the only module in the package that imports an HTTP client, which
is what keeps the loop, the strategies, the predicates and the verdicts
testable without one.
"""

from __future__ import annotations

import base64
import json
import logging
from dataclasses import dataclass, field, replace
from decimal import Decimal
from typing import TYPE_CHECKING, Any
from urllib.parse import SplitResult, urljoin, urlsplit

import aiohttp
import orjson
from aiohttp_retry import ExponentialRetry, RetryClient

from ..declarations import ErrorMap
from ..rate_limiter import RateLimiter
from .verdicts import classify_exception, classify_status

if TYPE_CHECKING:
    from .dialects import ApiDialect

__all__ = [
    "ApiResponseError",
    "HttpSender",
    "SignedRequest",
    "encode_body",
    "failure_facts",
    "follow_url",
    "join_url",
    "loads_preserving_decimals",
    "same_origin",
]

logger = logging.getLogger(__name__)

#: Retry attempts used when the connection declares no ``max_retries``.
DEFAULT_MAX_RETRIES = 3

#: Scheme defaults, so ``https://host:443`` and ``https://host`` compare as
#: the one origin they are.
_DEFAULT_PORTS = {"http": 80, "https": 443}


@dataclass(frozen=True)
class SignedRequest:
    """One request, complete enough for a dialect to sign.

    Frozen because :meth:`cdk.api.dialects.ApiDialect.sign_request` returns
    a new request rather than mutating this one -- a signature covers a
    request that cannot change under it.
    """

    method: str
    url: str
    params: dict[str, Any] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes | None = None


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


def join_url(base: str, path: str) -> str:
    """Append *path* to *base*, keeping both segments.

    Not ``urljoin``: it treats a leading ``/`` on the path as
    absolute-path-relative and drops the base's own path segment
    (``/api/v1`` + ``/Foo`` -> ``/Foo``, not ``/api/v1/Foo``).
    """
    return base.rstrip("/") + "/" + path.lstrip("/")


def same_origin(base: SplitResult, target: SplitResult) -> bool:
    """Whether two split URLs share scheme, host and effective port.

    Compares normalized parts -- case-insensitive scheme and host, default
    ports made explicit -- so ``https://api.example.test:443`` and
    ``https://API.example.test`` count as the origin they are.
    """
    base_scheme = base.scheme.lower()
    target_scheme = target.scheme.lower()
    return (
        base_scheme == target_scheme
        and (base.hostname or "").lower() == (target.hostname or "").lower()
        and (base.port or _DEFAULT_PORTS.get(base_scheme))
        == (target.port or _DEFAULT_PORTS.get(target_scheme))
    )


def follow_url(current: str, target: str, *, origin: str) -> str:
    """Resolve a provider-supplied next-page URL against the page it came from.

    An absolute target must stay on the connection's origin. The session
    sends the connection's default headers -- auth included -- on every
    request, so following a URL a response body named to another host would
    hand those credentials to it.

    A relative target resolves against the CURRENT page URL per RFC 3986,
    so a query-only link like ``?page=2`` continues from the endpoint path
    instead of the connection root.

    Classification is by parsing, never by string prefix: a target carrying
    any scheme or authority is absolute whatever its case, and the origin
    check then also rejects non-HTTP schemes and ambiguous
    protocol-relative URLs loudly.
    """
    if not isinstance(target, str):
        raise ValueError(
            f"next_url resolved to a {type(target).__name__}, expected a URL string"
        )
    parsed = urlsplit(target)
    if not parsed.scheme and not parsed.netloc:
        return urljoin(current, target)
    root = urlsplit(origin)
    if not same_origin(root, parsed):
        raise ValueError(
            f"next_url {target!r} leaves the connection's origin "
            f"{root.scheme}://{root.netloc}; refusing to send the "
            f"connection's headers to another host"
        )
    return target


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


def encode_body(data: Any) -> bytes:
    """Serialise a request body to the bytes that go on the wire.

    One serialiser for both roles. ``aiohttp``'s own ``json=`` argument
    calls the stdlib encoder, which understands neither ``datetime`` nor
    ``Decimal``; orjson handles the first natively and the hook above
    renders the second losslessly.
    """
    return orjson.dumps(data, default=_orjson_default)


def query_value(value: Any) -> Any:
    """Render one value for the query string.

    ``yarl`` truncates a ``Decimal`` in a query string, so it goes as its
    exact decimal string -- the same rendering the body gets, so one
    authored value does not land two ways depending on where it was
    declared.
    """
    return str(value) if isinstance(value, Decimal) else value


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

    async def send(self, request: SignedRequest) -> Any:
        """Issue one request and return the decoded, unwrapped payload.

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
        if request.body is not None and not any(
            name.lower() == "content-type" for name in request.headers
        ):
            request = replace(
                request,
                headers={**request.headers, "Content-Type": "application/json"},
            )
        request = self._dialect.sign_request(request)
        if self._rate_limiter:
            await self._rate_limiter.acquire()
        logger.debug("API %s %s params=%s", request.method, request.url, request.params)
        async with self._client.request(
            method=request.method,
            url=request.url,
            params={name: query_value(value) for name, value in request.params.items()},
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
        return self._dialect.unwrap_page(payload)

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
