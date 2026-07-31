"""Each paging scheme, driven through the loop it now shares.

The adapters answer only where a traversal starts and where it goes next.
Blocks are the contract's own shape as plain dictionaries -- the engine
validates the document before this layer reads it, so nothing here imports a
contract model.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from cdk.api.page_loop import Page, PageLoop, PageRequest
from cdk.api.strategies import UnknownPaginationStrategy, build_strategy

pytestmark = pytest.mark.unit


def _rows(n: int, start: int = 0) -> list[dict[str, Any]]:
    return [{"id": start + i} for i in range(n)]


def _scripted(pages: list[Page]):
    """Answer each request from a fixed script, bound now rather than later."""
    sent = iter(pages)

    async def fetch(request: PageRequest) -> Page:
        return next(sent)

    return fetch


def _follow(current: str, target: str) -> str:
    """Follow a link verbatim.

    The real follower refuses a target that leaves the connection's origin,
    which needs a connection; the strategy only has to hand it the current
    page and the declared target.
    """
    return target


def _build(block, *, resolve=lambda expr, page: expr, base=None, follow=_follow):
    return build_strategy(
        block,
        url="/things",
        base_params=base or {},
        resolve=resolve,
        follow_url=follow,
    )


class TestOffset:
    def test_the_first_request_carries_the_declared_start(self) -> None:
        s = _build(
            {
                "type": "offset",
                "offset": {"param": "skip", "initial": 0, "increment_by": 100},
            }
        )
        assert s.first() == PageRequest("/things", {"skip": 0})

    def test_it_advances_by_the_declared_step(self) -> None:
        s = _build(
            {
                "type": "offset",
                "offset": {"param": "skip", "initial": 0, "increment_by": 100},
            }
        )
        assert s.advance(Page(_rows(100))).params == {"skip": 100}
        assert s.advance(Page(_rows(100))).params == {"skip": 200}

    def test_the_step_is_resolved_per_page(self) -> None:
        # The contract lets offset declare increment_by as a value
        # expression, so a provider reporting its own page size can drive
        # the step. Resolving once would freeze the first page's answer.
        sizes = iter([10, 25])

        def resolve(expr: Any, page: Page | None) -> Any:
            return next(sizes) if expr == {"ref": "size"} else expr

        s = _build(
            {
                "type": "offset",
                "offset": {
                    "param": "skip",
                    "initial": 0,
                    "increment_by": {"ref": "size"},
                },
            },
            resolve=resolve,
        )
        assert s.advance(Page(_rows(10))).params == {"skip": 10}
        assert s.advance(Page(_rows(25))).params == {"skip": 35}

    @pytest.mark.parametrize("step", [0, -1, True, False, 2.5, "ten", None])
    def test_a_step_that_would_not_advance_is_refused(self, step: Any) -> None:
        # A zero or negative step re-requests the same page forever, a bool
        # would read as 1 because bool is an int, and a fractional step is a
        # different intent than any whole one.
        s = _build(
            {
                "type": "offset",
                "offset": {"param": "skip", "initial": 0, "increment_by": step},
            }
        )
        with pytest.raises(ValueError, match="offset.increment_by"):
            s.advance(Page(_rows(1)))

    @pytest.mark.parametrize("step", ["10", 10.0, Decimal("10")])
    def test_a_step_whose_integer_value_is_exact_is_accepted(self, step: Any) -> None:
        # The step can arrive from a response body whose typing the author
        # does not control: the lossless JSON parse turns 10.0 into
        # Decimal("10"), and a provider reporting its page size as a string
        # is not declaring something else.
        s = _build(
            {
                "type": "offset",
                "offset": {"param": "skip", "initial": 0, "increment_by": step},
            }
        )
        assert s.advance(Page(_rows(1))).params == {"skip": 10}


class TestPage:
    def test_it_advances_by_one_when_nothing_is_declared(self) -> None:
        s = _build({"type": "page", "page": {"param": "p", "initial": 1}})
        assert s.first().params == {"p": 1}
        assert s.advance(Page(_rows(5))).params == {"p": 2}

    def test_the_step_is_resolved_once(self) -> None:
        # A page number advances by a fixed stride; re-resolving per page
        # would let the stride change mid-read.
        calls: list[Any] = []

        def resolve(expr: Any, page: Page | None) -> Any:
            calls.append(expr)
            return 2

        s = _build(
            {
                "type": "page",
                "page": {"param": "p", "initial": 1, "increment_by": {"ref": "stride"}},
            },
            resolve=resolve,
        )
        s.advance(Page(_rows(5)))
        s.advance(Page(_rows(5)))
        assert len(calls) == 1


class TestCursor:
    def test_the_first_request_sends_no_token(self) -> None:
        # There is nothing to continue from yet, and an empty token asks the
        # provider to resume from a position it never issued.
        s = _build(
            {
                "type": "cursor",
                "cursor": {"param": "after", "next_cursor": {"ref": "next"}},
            }
        )
        assert s.first().params == {}

    def test_it_follows_the_token_the_page_carried(self) -> None:
        s = _build(
            {
                "type": "cursor",
                "cursor": {"param": "after", "next_cursor": {"ref": "next"}},
            },
            resolve=lambda expr, page: page.payload["next"],
        )
        nxt = s.advance(Page(_rows(2), payload={"next": "abc"}))
        assert nxt is not None and nxt.params == {"after": "abc"}

    @pytest.mark.parametrize("token", [None, ""])
    def test_an_absent_token_ends_the_traversal(self, token: Any) -> None:
        s = _build(
            {
                "type": "cursor",
                "cursor": {"param": "after", "next_cursor": {"ref": "next"}},
            },
            resolve=lambda expr, page: token,
        )
        assert s.advance(Page(_rows(2))) is None


class TestKeyset:
    def test_it_follows_the_last_records_ordering_value(self) -> None:
        s = _build(
            {"type": "keyset", "keyset": {"param": "since", "order_by_field": "id"}}
        )
        nxt = s.advance(Page(_rows(3)))
        assert nxt is not None and nxt.params == {"since": 2}

    def test_it_reads_a_nested_ordering_field(self) -> None:
        s = _build(
            {
                "type": "keyset",
                "keyset": {"param": "since", "order_by_field": "meta.seq"},
            }
        )
        nxt = s.advance(Page([{"meta": {"seq": 7}}]))
        assert nxt is not None and nxt.params == {"since": 7}

    def test_a_missing_ordering_value_fails_rather_than_truncating(self) -> None:
        # Without it there is no next page. Failing is the honest answer;
        # returning None would silently end the read mid-stream.
        s = _build(
            {"type": "keyset", "keyset": {"param": "since", "order_by_field": "id"}}
        )
        with pytest.raises(ValueError, match="order_by_field"):
            s.advance(Page([{"other": 1}]))

    def test_a_declared_initial_seeds_the_first_request(self) -> None:
        s = _build(
            {
                "type": "keyset",
                "keyset": {"param": "since", "order_by_field": "id", "initial": 100},
            }
        )
        assert s.first().params == {"since": 100}


class TestLink:
    def test_the_declared_params_bind_on_the_first_request_only(self) -> None:
        # A next URL is absolute and carries the provider's own query, so
        # re-appending ours would fight it. The contract says so for link.
        s = _build(
            {"type": "link", "link": {"next_url": {"ref": "next"}}},
            resolve=lambda expr, page: page.payload["next"],
            base={"limit": 50},
        )
        assert s.first().params == {"limit": 50}
        nxt = s.advance(Page(_rows(2), payload={"next": "https://api/x?page=2"}))
        # The declared body goes the same way as the declared params: the
        # resolved URL replaces the whole request, not half of it.
        assert nxt == PageRequest("https://api/x?page=2", {}, sends_declared_body=False)

    @pytest.mark.parametrize("target", [None, ""])
    def test_an_absent_link_ends_the_traversal(self, target: Any) -> None:
        s = _build(
            {"type": "link", "link": {"next_url": {"ref": "next"}}},
            resolve=lambda expr, page: target,
        )
        assert s.advance(Page(_rows(2))) is None

    def test_a_non_url_is_refused(self) -> None:
        s = _build(
            {"type": "link", "link": {"next_url": {"ref": "next"}}},
            resolve=lambda expr, page: {"href": "/x"},
        )
        with pytest.raises(ValueError, match="not a URL"):
            s.advance(Page(_rows(2)))


class TestTheUnionIsClosed:
    def test_an_unknown_scheme_fails_loud_naming_the_union(self) -> None:
        with pytest.raises(UnknownPaginationStrategy, match="cursor"):
            _build({"type": "time"})

    def test_every_contract_scheme_builds(self) -> None:
        blocks = [
            {
                "type": "offset",
                "offset": {"param": "s", "initial": 0, "increment_by": 1},
            },
            {"type": "page", "page": {"param": "p", "initial": 1}},
            {"type": "cursor", "cursor": {"param": "a", "next_cursor": {}}},
            {"type": "keyset", "keyset": {"param": "k", "order_by_field": "id"}},
            {"type": "link", "link": {"next_url": {}}},
        ]
        for block in blocks:
            assert _build(block).first() is not None


class TestThroughTheLoop:
    @pytest.mark.asyncio
    async def test_every_scheme_stops_on_an_empty_page(self) -> None:
        # The shared rule, exercised per scheme: none of the five decides
        # this for itself any more.
        blocks = [
            {
                "type": "offset",
                "offset": {"param": "s", "initial": 0, "increment_by": 2},
            },
            {"type": "page", "page": {"param": "p", "initial": 1}},
            {"type": "keyset", "keyset": {"param": "k", "order_by_field": "id"}},
        ]
        for block in blocks:
            loop = PageLoop(
                _build(block),
                fetch=_scripted([Page(_rows(2)), Page(_rows(2, start=2)), Page([])]),
                stop_when=lambda page: False,
            )
            collected = [records async for records in loop.__aiter__()]
            assert collected == [_rows(2), _rows(2, start=2)], block["type"]
