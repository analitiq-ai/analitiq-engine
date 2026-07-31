"""The loop order and the stopping rules, one place, five schemes.

Each assertion here answers a disagreement the five hand-written loops had.
Pages are scripted directly: the loop takes ``fetch`` as an argument, so
nothing in this file touches HTTP.
"""

from __future__ import annotations

import inspect
from typing import Any

import pytest

from cdk.api import page_loop as page_loop_module
from cdk.api.page_loop import Page, PageLoop, PageRequest

pytestmark = pytest.mark.unit


class _ScriptedFetch:
    """Answers each request from a script, recording what was asked."""

    def __init__(self, pages: list[Page]) -> None:
        self._pages = list(pages)
        self.requests: list[PageRequest] = []

    async def __call__(self, request: PageRequest) -> Page:
        self.requests.append(request)
        if not self._pages:
            return Page(records=[])
        return self._pages.pop(0)


class _Counting:
    """A minimal strategy: walk a fixed number of pages, then stop."""

    def __init__(self, pages: int) -> None:
        self._left = pages
        self.advanced = 0

    def first(self) -> PageRequest:
        return PageRequest(url="/things", params={"page": 1})

    def advance(self, page: Page) -> PageRequest | None:
        self.advanced += 1
        self._left -= 1
        if self._left <= 0:
            return None
        return PageRequest(url="/things", params={"page": self.advanced + 1})


def _rows(n: int) -> list[dict[str, Any]]:
    return [{"id": i} for i in range(n)]


async def _drain(loop: PageLoop) -> list[list[dict[str, Any]]]:
    return [records async for records in loop]


class TestStopping:
    @pytest.mark.asyncio
    async def test_an_empty_page_ends_the_traversal(self) -> None:
        fetch = _ScriptedFetch([Page(_rows(2)), Page([])])
        loop = PageLoop(_Counting(9), fetch=fetch, stop_when=lambda page: False)
        assert await _drain(loop) == [_rows(2)]

    @pytest.mark.asyncio
    async def test_a_short_page_does_not_end_the_traversal(self) -> None:
        # The disagreement offset and page had with cursor, keyset and link.
        # Providers return short pages for filtering, rate limiting and
        # per-request caps, so stopping on one silently truncates the read.
        fetch = _ScriptedFetch([Page(_rows(10)), Page(_rows(3)), Page(_rows(7))])
        loop = PageLoop(_Counting(3), fetch=fetch, stop_when=lambda page: False)
        assert await _drain(loop) == [_rows(10), _rows(3), _rows(7)]

    @pytest.mark.asyncio
    async def test_advance_returning_none_ends_the_traversal(self) -> None:
        fetch = _ScriptedFetch([Page(_rows(2)), Page(_rows(2)), Page(_rows(2))])
        loop = PageLoop(_Counting(2), fetch=fetch, stop_when=lambda page: False)
        assert await _drain(loop) == [_rows(2), _rows(2)]

    @pytest.mark.asyncio
    async def test_the_declared_stop_condition_ends_the_traversal(self) -> None:
        fetch = _ScriptedFetch(
            [
                Page(_rows(2), payload={"done": False}),
                Page(_rows(2), payload={"done": True}),
                Page(_rows(2), payload={"done": False}),
            ]
        )
        loop = PageLoop(
            _Counting(9),
            fetch=fetch,
            stop_when=lambda page: bool(page.payload and page.payload.get("done")),
        )
        # The page that satisfies the condition is still yielded: it carried
        # records, and the condition says "this was the last one", not
        # "discard this one".
        assert await _drain(loop) == [_rows(2), _rows(2)]

    @pytest.mark.asyncio
    async def test_the_extra_request_a_short_page_costs_is_one(self) -> None:
        # Not stopping on a short page costs exactly one more round trip:
        # the empty page that proves the end. That is the price of the
        # silent-truncation risk going away.
        fetch = _ScriptedFetch([Page(_rows(3)), Page([])])
        loop = PageLoop(_Counting(9), fetch=fetch, stop_when=lambda page: False)
        await _drain(loop)
        assert len(fetch.requests) == 2


class TestLoopOrder:
    @pytest.mark.asyncio
    async def test_a_page_is_advanced_from_before_it_is_yielded(self) -> None:
        # The disagreement page pagination had with the other four. A caller
        # may commit the records it receives, so a page the scheme cannot
        # advance from must fail before the yield, not after.
        seen: list[str] = []

        class _Recording(_Counting):
            def advance(self, page: Page) -> PageRequest | None:
                seen.append("advance")
                return super().advance(page)

        fetch = _ScriptedFetch([Page(_rows(1)), Page(_rows(1))])
        loop = PageLoop(_Recording(2), fetch=fetch, stop_when=lambda page: False)
        async for _records in loop:
            seen.append("yield")
        assert seen == ["advance", "yield", "advance", "yield"]

    @pytest.mark.asyncio
    async def test_a_scheme_that_cannot_advance_fails_before_the_yield(self) -> None:
        # Keyset raises when the ordering field is missing from the last
        # record. That has to surface before the page is handed over.
        class _Stuck(_Counting):
            def advance(self, page: Page) -> PageRequest | None:
                raise ValueError(
                    "keyset.order_by_field is missing from the last record"
                )

        fetch = _ScriptedFetch([Page(_rows(2))])
        loop = PageLoop(_Stuck(9), fetch=fetch, stop_when=lambda page: False)
        yielded: list[Any] = []
        with pytest.raises(ValueError, match="order_by_field"):
            async for records in loop:
                yielded.append(records)
        assert yielded == []

    @pytest.mark.asyncio
    async def test_the_first_request_comes_from_the_strategy(self) -> None:
        fetch = _ScriptedFetch([Page(_rows(1))])
        loop = PageLoop(_Counting(1), fetch=fetch, stop_when=lambda page: False)
        await _drain(loop)
        assert fetch.requests[0] == PageRequest(url="/things", params={"page": 1})

    @pytest.mark.asyncio
    async def test_each_later_request_comes_from_advance(self) -> None:
        fetch = _ScriptedFetch([Page(_rows(1)), Page(_rows(1)), Page(_rows(1))])
        loop = PageLoop(_Counting(3), fetch=fetch, stop_when=lambda page: False)
        await _drain(loop)
        assert [r.params["page"] for r in fetch.requests] == [1, 2, 3]


class TestTheLoopKnowsNoTransport:
    def test_the_module_imports_no_http_client(self) -> None:
        # `fetch` is injected precisely so the loop stays testable without a
        # server and reusable by the conformance kit, which runs no I/O.
        source = inspect.getsource(page_loop_module)
        assert "aiohttp" not in source
        assert "import requests" not in source


@pytest.mark.asyncio
class TestNothingIsDecidedAfterTheCallerHasTheRecords:
    """The caller may commit what it is handed, so both decisions come first.

    Advancing was already ordered this way. Evaluating the author's stop
    condition was not, which left the one deterministic check that can fail
    a page running after that page had been forwarded.
    """

    class _Exploding:
        def first(self) -> PageRequest:
            return PageRequest(url="/things")

        def advance(self, page: Page) -> PageRequest | None:
            raise ValueError("no continuation on this page")

    async def test_a_raising_stop_condition_fails_before_the_yield(self) -> None:
        loop = PageLoop(
            _Counting(pages=3),
            fetch=_ScriptedFetch([Page(_rows(2))]),
            stop_when=_raises,
        )
        seen: list[list[dict[str, Any]]] = []
        with pytest.raises(ValueError, match="stop condition"):
            async for records in loop:
                seen.append(records)
        assert seen == [], "the page reached the caller before the read failed"

    async def test_a_stopping_page_is_never_advanced_from(self) -> None:
        # The last page is exactly where a continuation value is legitimately
        # absent -- no next link, no cursor. Asking for one there fails a read
        # that was just told it was complete.
        strategy = self._Exploding()
        loop = PageLoop(
            strategy,
            fetch=_ScriptedFetch([Page(_rows(2))]),
            stop_when=lambda page: True,
        )
        assert await _drain(loop) == [_rows(2)]

    async def test_a_continuing_page_is_still_advanced_from(self) -> None:
        strategy = _Counting(pages=2)
        loop = PageLoop(
            strategy,
            fetch=_ScriptedFetch([Page(_rows(1)), Page(_rows(1))]),
            stop_when=lambda page: False,
        )
        await _drain(loop)
        assert strategy.advanced == 2


def _raises(page: Page) -> bool:
    raise ValueError("stop condition could not be evaluated")
