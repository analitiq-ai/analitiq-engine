"""The one round trip, and every rule about a URL.

The URL rules are the part the landed strategy adapter dropped: without
them a provider-supplied ``next_url`` sends the connection's baked-in auth
headers to any host a response body names.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from urllib.parse import urlsplit

import aiohttp
import pytest

from cdk.api.dialects import ApiDialect
from cdk.api.http import (
    ApiResponseError,
    HttpSender,
    SignedRequest,
    encode_body,
    failure_facts,
    follow_url,
    join_url,
    loads_preserving_decimals,
    same_origin,
)
from cdk.declarations import parse_declared_error_map

from .fakes import BASE_URL, FakeResponse, FakeSession

pytestmark = pytest.mark.unit


def _sender(
    session: FakeSession,
    *,
    dialect: ApiDialect | None = None,
    retry_statuses: set[int] | None = None,
    max_retries: int = 1,
) -> HttpSender:
    return HttpSender(
        session=session,
        rate_limiter=None,
        dialect=dialect or ApiDialect(),
        retry_statuses=retry_statuses or set(),
        max_retries=max_retries,
    )


class TestJoinUrl:
    def test_both_segments_survive(self) -> None:
        # urljoin would drop the base's own path on a leading slash.
        assert join_url("https://x/api/v1", "/items") == "https://x/api/v1/items"
        assert join_url("https://x/api/v1/", "items") == "https://x/api/v1/items"


class TestFollowUrl:
    def test_a_relative_target_joins_the_current_page(self) -> None:
        # A query-only link continues from the endpoint path, not the
        # connection root.
        assert (
            follow_url(f"{BASE_URL}/v1/items", "?page=2", origin=BASE_URL)
            == f"{BASE_URL}/v1/items?page=2"
        )

    def test_an_absolute_same_origin_target_is_followed(self) -> None:
        target = f"{BASE_URL}/v1/items?page=2"
        assert follow_url(f"{BASE_URL}/v1/items", target, origin=BASE_URL) == target

    def test_equivalent_origin_spellings_are_the_same_origin(self) -> None:
        target = "https://API.example.test:443/v1/items?page=2"
        assert follow_url(f"{BASE_URL}/v1/items", target, origin=BASE_URL) == target

    def test_a_cross_origin_target_is_refused(self) -> None:
        # The session sends the connection's auth headers on every request.
        with pytest.raises(ValueError, match="leaves the connection's origin"):
            follow_url(
                f"{BASE_URL}/v1/items", "https://evil.test/steal", origin=BASE_URL
            )

    def test_an_uppercase_scheme_still_classifies_as_absolute(self) -> None:
        with pytest.raises(ValueError, match="leaves the connection's origin"):
            follow_url(f"{BASE_URL}/v1/items", "HTTPS://evil.test/x", origin=BASE_URL)

    def test_a_protocol_relative_target_is_refused(self) -> None:
        with pytest.raises(ValueError, match="leaves the connection's origin"):
            follow_url(f"{BASE_URL}/v1/items", "//evil.test/x", origin=BASE_URL)

    def test_a_non_string_target_is_refused(self) -> None:
        with pytest.raises(ValueError, match="expected a URL string"):
            follow_url(f"{BASE_URL}/v1", {"href": "/x"}, origin=BASE_URL)

    def test_same_origin_compares_normalized_parts(self) -> None:
        assert same_origin(urlsplit("https://a.test"), urlsplit("https://A.test:443"))
        assert not same_origin(urlsplit("https://a.test"), urlsplit("http://a.test"))


class TestSerialisation:
    def test_a_decimal_keeps_its_exact_digits_in_the_body(self) -> None:
        # The read path used to narrow body decimals to float for the
        # stdlib encoder's benefit -- an admitted lossy conversion, done
        # for a serialiser rather than for the provider.
        assert encode_body({"amount": Decimal("1.50")}) == b'{"amount":"1.50"}'

    def test_the_decode_keeps_fractional_tokens_exact(self) -> None:
        parsed = loads_preserving_decimals('{"amount": 1.50}')
        assert parsed["amount"] == Decimal("1.50")

    def test_an_unserialisable_type_fails_loud(self) -> None:
        # orjson re-raises the hook's refusal under its own message; the
        # hook's reason is the cause, which is what a traceback shows.
        with pytest.raises(TypeError) as caught:
            encode_body({"x": object()})
        assert "orjson cannot serialise" in str(caught.value.__cause__)


class TestFailureFacts:
    def test_a_response_error_resolves_by_status(self) -> None:
        error_map = parse_declared_error_map({"http": {"429": "rate_limited"}})
        exc = aiohttp.ClientResponseError(None, (), status=429)
        assert failure_facts(exc, error_map=error_map) == (429, "rate_limited")

    def test_a_category_decided_at_the_raise_site_is_kept(self) -> None:
        exc = ApiResponseError(None, (), status=200, declared_category="config")
        assert failure_facts(exc, error_map=None) == (200, "config")

    def test_a_status_less_error_resolves_by_the_exception_family(self) -> None:
        error_map = parse_declared_error_map(
            {"exception": {"ClientPayloadError": "transient"}}
        )
        exc = aiohttp.ClientPayloadError("truncated")
        assert failure_facts(exc, error_map=error_map) == (None, "transient")


@pytest.mark.asyncio
class TestSend:
    async def test_it_returns_the_decoded_payload(self) -> None:
        session = FakeSession([FakeResponse(body={"records": [{"id": 1}]})])
        sender = _sender(session)
        payload = await sender.send(
            SignedRequest(method="GET", url=f"{BASE_URL}/items")
        )
        assert payload == {"records": [{"id": 1}]}

    @pytest.mark.parametrize("status", [200, 201, 202, 206])
    async def test_any_status_below_400_is_a_success(self, status: int) -> None:
        # A read endpoint answering 202 is a legitimate provider; failing a
        # whole stream on it is the engine second-guessing HTTP.
        session = FakeSession([FakeResponse(status=status, body={"records": []})])
        assert await _sender(session).send(
            SignedRequest(method="GET", url=f"{BASE_URL}/items")
        ) == {"records": []}

    async def test_an_empty_body_decodes_to_nothing_rather_than_raising(self) -> None:
        session = FakeSession([FakeResponse(status=204, text="")])
        assert (
            await _sender(session).send(
                SignedRequest(method="GET", url=f"{BASE_URL}/items")
            )
            is None
        )

    async def test_a_failing_status_raises_carrying_the_status(self) -> None:
        session = FakeSession([FakeResponse(status=404, body={"error": "nope"})])
        with pytest.raises(ApiResponseError) as caught:
            await _sender(session).send(
                SignedRequest(method="GET", url=f"{BASE_URL}/items")
            )
        assert caught.value.status == 404

    async def test_a_failing_status_with_a_non_json_body_keeps_its_status(self) -> None:
        # The status IS the fact the verdict is built from; raising a decode
        # error instead would throw it away and read a 400 as retryable.
        session = FakeSession([FakeResponse(status=400, text="<html>no</html>")])
        with pytest.raises(ApiResponseError) as caught:
            await _sender(session).send(
                SignedRequest(method="GET", url=f"{BASE_URL}/items")
            )
        assert caught.value.status == 400
        assert "<html>no</html>" in str(caught.value)

    async def test_a_successful_non_json_body_is_diagnosable(self) -> None:
        session = FakeSession([FakeResponse(status=200, text="<html>hi</html>")])
        with pytest.raises(aiohttp.ClientPayloadError, match="non-JSON body"):
            await _sender(session).send(
                SignedRequest(method="GET", url=f"{BASE_URL}/items")
            )

    async def test_the_declared_map_classifies_at_the_raise_site(self) -> None:
        class Runtime:
            declared_error_map = {"http": {"404": "config"}}
            connector_id = "test-connector"

        session = FakeSession([FakeResponse(status=404, body={})])
        sender = _sender(session, dialect=ApiDialect.for_runtime(Runtime()))
        with pytest.raises(ApiResponseError) as caught:
            await sender.send(SignedRequest(method="GET", url=f"{BASE_URL}/items"))
        assert caught.value.declared_category == "config"

    async def test_a_dialect_can_fail_a_success_status(self) -> None:
        # The one thing a declaration cannot express: a 200 carrying an
        # error envelope.
        class Provider(ApiDialect):
            def classify(self, status: int, body: Any) -> str | None:
                if isinstance(body, dict) and body.get("error"):
                    return "rate_limited"
                return None

        session = FakeSession([FakeResponse(status=200, body={"error": "slow down"})])
        with pytest.raises(ApiResponseError) as caught:
            await _sender(session, dialect=Provider()).send(
                SignedRequest(method="GET", url=f"{BASE_URL}/items")
            )
        assert caught.value.declared_category == "rate_limited"

    async def test_the_dialect_unwraps_the_body_before_anything_reads_it(self) -> None:
        class Provider(ApiDialect):
            def unwrap_page(self, body: Any) -> Any:
                return body["result"]

        session = FakeSession([FakeResponse(body={"result": {"records": [{"id": 1}]}})])
        payload = await _sender(session, dialect=Provider()).send(
            SignedRequest(method="GET", url=f"{BASE_URL}/items")
        )
        assert payload == {"records": [{"id": 1}]}

    async def test_the_dialect_signs_the_final_request(self) -> None:
        class Provider(ApiDialect):
            def sign_request(self, request: SignedRequest) -> SignedRequest:
                signed = dict(request.headers)
                signed["X-Signature"] = f"{request.method}:{len(request.body or b'')}"
                return SignedRequest(
                    method=request.method,
                    url=request.url,
                    params=request.params,
                    headers=signed,
                    body=request.body,
                )

        session = FakeSession([FakeResponse(body={})])
        await _sender(session, dialect=Provider()).send(
            SignedRequest(
                method="POST", url=f"{BASE_URL}/items", body=encode_body({"a": 1})
            )
        )
        headers = session.calls[0]["headers"]
        assert headers["X-Signature"] == "POST:7"
        # The content type was settled before signing, so a signature can
        # cover it.
        assert headers["Content-Type"] == "application/json"

    async def test_a_decimal_param_goes_as_its_exact_string(self) -> None:
        # yarl truncates a Decimal in a query string.
        session = FakeSession([FakeResponse(body={})])
        await _sender(session).send(
            SignedRequest(
                method="GET", url=f"{BASE_URL}/items", params={"since": Decimal("1.50")}
            )
        )
        assert session.calls[0]["params"] == {"since": "1.50"}

    async def test_the_transport_re_attempts_a_retryable_status(self) -> None:
        session = FakeSession(
            [FakeResponse(status=503, body={}), FakeResponse(body={"ok": True})]
        )
        sender = _sender(session, retry_statuses={503}, max_retries=2)
        assert await sender.send(
            SignedRequest(method="GET", url=f"{BASE_URL}/items")
        ) == {"ok": True}
        assert len(session.calls) == 2

    async def test_a_status_outside_the_set_is_attempted_once(self) -> None:
        session = FakeSession([FakeResponse(status=400, body={})])
        sender = _sender(session, retry_statuses={503}, max_retries=3)
        with pytest.raises(ApiResponseError):
            await sender.send(SignedRequest(method="GET", url=f"{BASE_URL}/items"))
        assert len(session.calls) == 1

    async def test_probe_answers_the_status_itself(self) -> None:
        # A health check judges the status; 404 means the API answered.
        session = FakeSession([FakeResponse(status=404, body={})])
        assert await _sender(session).probe(BASE_URL) == 404
