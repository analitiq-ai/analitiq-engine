"""One loop over an API's pages, whatever scheme the provider paginates by.

Five strategies used to be five ``while`` bodies, and they disagreed about
things no author ever chose. Two stopped on a short page and three did not.
Four advanced before yielding a page and one after. Each re-derived when to
evaluate the author's stop condition. None of that was a decision about the
provider; it was drift.

:class:`PageLoop` owns the loop order and the stopping, and a
:class:`PaginationStrategy` owns only what its scheme knows: what the first
request looks like, and what the next one looks like given the page that came
back. Collapsing "build the next request" and "are we done" into one
``advance`` makes the ordering hold by construction -- the loop cannot reach
its ``yield`` without having advanced first, so a page the loop cannot
advance from fails before the engine can commit it.

The loop takes ``fetch`` as an argument, so it imports no HTTP client and its
tests script pages directly.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

__all__ = [
    "Page",
    "PageLoop",
    "PageRequest",
    "PaginationStrategy",
    "Resolve",
    "StopCondition",
]


@dataclass(frozen=True)
class PageRequest:
    """One request the loop should issue.

    ``sends_declared_body`` is how a scheme says whether the endpoint's
    declared request body goes with this request. A continuation URL the
    provider handed us replaces the whole request -- its own query is
    already in the URL, and re-sending the declared body would fight the
    link rather than follow it. Empty ``params`` cannot express that on its
    own: a body is built from the param table, not from the query, so a
    request carrying no params can still carry a body.
    """

    url: str
    params: dict[str, Any] = field(default_factory=dict)
    sends_declared_body: bool = True


@dataclass(frozen=True)
class Page:
    """One response the loop got back.

    ``payload`` is the decoded body, kept beside the extracted ``records``
    because a declared stop condition and a declared next-page value are
    written against the body, not against the records the engine pulled out
    of it.
    """

    records: list[dict[str, Any]]
    payload: Any = None


class PaginationStrategy(Protocol):
    """How one paging scheme walks a provider's pages.

    Stateful and single-use: one instance per read, because every scheme
    carries a position (an offset, a page number, a cursor token, the last
    key seen) that only means anything within one traversal.
    """

    def first(self) -> PageRequest:
        """Return the request that starts the traversal."""
        ...

    def advance(self, page: Page) -> PageRequest | None:
        """Return the request after ``page``, or ``None`` when the pages ran out.

        Called before the page is yielded, so a scheme that cannot work out
        where to go next raises here -- before the engine commits records it
        would then be unable to follow.
        """
        ...


#: Answers "does the author's stop condition hold for this page?".
StopCondition = Callable[[Page], bool]

#: Resolves a declared value expression against the page it was written for.
Resolve = Callable[[Any, Page | None], Any]

#: Issues one request and returns what came back.
Fetch = Callable[[PageRequest], Awaitable[Page]]


class PageLoop:
    """Walks a provider's pages and yields each page's records.

    Stopping is three rules and no more:

    - an empty page ends the traversal. This is the only count-based rule:
      a *short* page is not an exhaustion signal, because providers return
      short pages for filtering, rate limiting and per-request caps, so
      treating one as the end silently truncates a read;
    - ``advance`` returning ``None`` ends it, which is how a scheme says it
      has nowhere left to go;
    - the author's ``stop_when`` ends it, which every strategy in the
      published contract is required to declare. That requirement is why
      the short-page heuristic is not needed: an authoritative stop
      condition is already the author's obligation, and guessing from row
      counts was the engine second-guessing a field the contract made
      mandatory.
    """

    def __init__(
        self,
        strategy: PaginationStrategy,
        *,
        fetch: Fetch,
        stop_when: StopCondition,
    ) -> None:
        self._strategy = strategy
        self._fetch = fetch
        # Required, never optional: every strategy in the contract declares
        # stop_when with no default, so a loop built without one would be
        # running against a document that could not have validated.
        self._stop_when = stop_when

    def __aiter__(self) -> AsyncIterator[Page]:
        """Return the traversal.

        A plain method, not an ``async def``: the protocol asks ``__aiter__``
        for the iterator itself, and only the body that walks the pages is
        asynchronous. Writing both as one ``async def`` happens to work --
        calling an async generator function returns its generator without
        awaiting -- but it makes ``async for`` depend on that coincidence.
        """
        return self._pages()

    async def _pages(self) -> AsyncIterator[Page]:
        """Yield each page until one of the three rules fires.

        The whole page, not its records alone: the caller turns the records
        into a batch, and the declared ``response.metadata`` that batch
        carries is written against the body the records came out of.

        The empty page that ends the traversal is yielded too, before the
        loop returns: a provider reports its total or its remaining budget
        on that page as on any other (``{"records": [], "total": 0}``), and
        the caller decides whether an empty page is worth a batch. It is
        neither evaluated for stopping nor advanced from -- it is the end.
        """
        request: PageRequest | None = self._strategy.first()
        while request is not None:
            page = await self._fetch(request)
            if not page.records:
                yield page
                return
            # Both decisions are made before the caller sees the records.
            # The caller may commit what it receives, so a page this loop
            # cannot evaluate or cannot advance from has to fail while the
            # failure still costs nothing.
            #
            # Stopping is decided first, and a stopping page is never
            # advanced from: the last page is exactly where a continuation
            # value is legitimately absent -- no next link, no cursor to
            # follow -- and asking for one there would fail a read that had
            # just been told it was complete.
            following = None if self._stop_when(page) else self._strategy.advance(page)
            yield page
            request = following
