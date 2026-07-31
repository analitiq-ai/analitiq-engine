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

__all__ = ["Page", "PageLoop", "PageRequest", "PaginationStrategy"]


@dataclass(frozen=True)
class PageRequest:
    """One request the loop should issue."""

    url: str
    params: dict[str, Any] = field(default_factory=dict)


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

    async def __aiter__(self) -> AsyncIterator[list[dict[str, Any]]]:
        """Yield each page's records until one of the three rules fires."""
        request: PageRequest | None = self._strategy.first()
        while request is not None:
            page = await self._fetch(request)
            if not page.records:
                return
            # Advance before yielding, structurally: the caller may commit
            # the records it receives, so a page this scheme cannot advance
            # from has to fail while the failure still costs nothing.
            following = self._strategy.advance(page)
            yield page.records
            if self._stop_when(page):
                return
            request = following
