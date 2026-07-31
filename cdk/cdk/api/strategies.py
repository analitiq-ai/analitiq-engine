"""The five paging schemes the contract declares, one adapter each.

Each adapter answers only what its scheme knows: where the traversal starts,
and where it goes after a page. Loop order, the empty-page rule and the
author's stop condition belong to :class:`~cdk.api.page_loop.PageLoop` and
are not re-decided here -- that re-deciding is what let the five hand-written
loops drift apart.

Blocks arrive as plain dictionaries. The engine validates the endpoint
document against the published contract before anything reads it, so this
layer navigates an already-validated document raw and the CDK's dependency
set stays SQLAlchemy + Pydantic. The vocabulary is still the contract's: an
adapter cannot rename a field, and an unknown ``type`` fails loud naming the
strategy union rather than silently reading nothing.

Value expressions (a next cursor, a next URL, a per-page increment) are
resolved through an injected ``resolve``, so this module knows neither the
resolver's construction nor the response scope's shape.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .page_loop import Page, PageRequest, PaginationStrategy

__all__ = ["UnknownPaginationStrategy", "build_strategy"]

#: Resolves a declared value expression against the page it was written for.
Resolve = Callable[[Any, Page | None], Any]

#: Builds one adapter from its declared block. A Protocol cannot be
#: instantiated, so the table is typed by what building one looks like.
_StrategyFactory = Callable[
    [dict[str, Any], dict[str, Any], str, Resolve], PaginationStrategy
]


class UnknownPaginationStrategy(ValueError):
    """A pagination block naming a scheme this build cannot walk.

    The contract's strategy union is closed, so this is a contract-version
    skew, not an author error: the mirror of a missing connector package
    naming the operation it cannot perform. A sixth scheme is a contract
    release, never a subclass.
    """


def _positive_step(value: Any, *, context: str) -> int:
    """Read a page-advance step, refusing anything that would not advance.

    A zero or negative step re-requests the same page forever, and a boolean
    is an author error that Python would otherwise read as 1 -- ``bool`` is
    an ``int``, so the ordinary integer check passes it.
    """
    if isinstance(value, bool):
        raise ValueError(f"pagination {context} must be an integer, got {value!r}")
    if not isinstance(value, int):
        raise ValueError(f"pagination {context} must be an integer, got {value!r}")
    if value <= 0:
        raise ValueError(f"pagination {context} must be positive, got {value}")
    return value


class _Offset:
    """Walk by a row offset the provider counts from."""

    def __init__(
        self, block: dict[str, Any], base: dict[str, Any], url: str, resolve: Resolve
    ) -> None:
        cursor = block["offset"]
        self._param = cursor["param"]
        self._offset = int(cursor["initial"])
        # Resolved per page, deliberately: the contract lets offset declare
        # increment_by as a value expression, so a provider that reports its
        # own page size can drive the step.
        self._increment_by = cursor["increment_by"]
        self._base = base
        self._url = url
        self._resolve = resolve

    def first(self) -> PageRequest:
        return PageRequest(self._url, {**self._base, self._param: self._offset})

    def advance(self, page: Page) -> PageRequest | None:
        step = _positive_step(
            self._resolve(self._increment_by, page), context="offset.increment_by"
        )
        self._offset += step
        return PageRequest(self._url, {**self._base, self._param: self._offset})


class _Page:
    """Walk by a page number the provider counts from."""

    def __init__(
        self, block: dict[str, Any], base: dict[str, Any], url: str, resolve: Resolve
    ) -> None:
        cursor = block["page"]
        self._param = cursor["param"]
        self._page = int(cursor["initial"])
        # Resolved once, before the first request: the contract types page's
        # increment_by loosely and a page number advances by a fixed stride,
        # so re-resolving per page would invite a step that changes mid-read.
        declared = cursor.get("increment_by")
        self._step = (
            1
            if declared is None
            else _positive_step(resolve(declared, None), context="page.increment_by")
        )
        self._base = base
        self._url = url

    def first(self) -> PageRequest:
        return PageRequest(self._url, {**self._base, self._param: self._page})

    def advance(self, page: Page) -> PageRequest | None:
        self._page += self._step
        return PageRequest(self._url, {**self._base, self._param: self._page})


class _Cursor:
    """Walk by an opaque token the provider hands back with each page."""

    def __init__(
        self, block: dict[str, Any], base: dict[str, Any], url: str, resolve: Resolve
    ) -> None:
        cursor = block["cursor"]
        self._param = cursor["param"]
        self._next_cursor = cursor["next_cursor"]
        self._base = base
        self._url = url
        self._resolve = resolve

    def first(self) -> PageRequest:
        # The first request carries no token: there is nothing to continue
        # from yet, and sending an empty one asks the provider to resume
        # from a position it never issued.
        return PageRequest(self._url, dict(self._base))

    def advance(self, page: Page) -> PageRequest | None:
        token = self._resolve(self._next_cursor, page)
        if token is None or token == "":  # nosec B105 - an absent token, not a secret
            return None
        return PageRequest(self._url, {**self._base, self._param: token})


class _Keyset:
    """Walk by the last value of an ordered field."""

    def __init__(
        self, block: dict[str, Any], base: dict[str, Any], url: str, resolve: Resolve
    ) -> None:
        keyset = block["keyset"]
        self._param = keyset["param"]
        self._field = keyset["order_by_field"]
        self._last = keyset.get("initial")
        self._base = base
        self._url = url

    def first(self) -> PageRequest:
        params = dict(self._base)
        if self._last is not None:
            params[self._param] = self._last
        return PageRequest(self._url, params)

    def advance(self, page: Page) -> PageRequest | None:
        last = page.records[-1]
        value: Any = last
        for token in self._field.split("."):
            value = value.get(token) if isinstance(value, dict) else None
            if value is None:
                break
        if value is None:
            # Raised here, before the loop yields: without the ordering value
            # there is no next page, and letting the records through first
            # would commit rows the read cannot continue past.
            raise ValueError(
                f"keyset.order_by_field {self._field!r} is missing from the "
                f"last record of a page; keyset pagination cannot continue"
            )
        self._last = value
        return PageRequest(self._url, {**self._base, self._param: value})


class _Link:
    """Follow a URL the provider puts in each page."""

    def __init__(
        self, block: dict[str, Any], base: dict[str, Any], url: str, resolve: Resolve
    ) -> None:
        self._next_url = block["link"]["next_url"]
        # The declared params bind on the first request only: a next URL is
        # absolute and carries the provider's own query, so re-appending
        # ours would fight it. That is the contract's own rule for link.
        self._base = base
        self._url = url
        self._resolve = resolve

    def first(self) -> PageRequest:
        return PageRequest(self._url, dict(self._base))

    def advance(self, page: Page) -> PageRequest | None:
        target = self._resolve(self._next_url, page)
        if target is None or target == "":
            return None
        if not isinstance(target, str):
            raise ValueError(
                f"link.next_url resolved to {type(target).__name__}, not a URL"
            )
        return PageRequest(target, {})


#: The contract's closed strategy union, by its own discriminator.
_STRATEGIES: dict[str, _StrategyFactory] = {
    "offset": _Offset,
    "page": _Page,
    "cursor": _Cursor,
    "keyset": _Keyset,
    "link": _Link,
}


def build_strategy(
    block: dict[str, Any],
    *,
    url: str,
    base_params: dict[str, Any],
    resolve: Resolve,
) -> PaginationStrategy:
    """Build the adapter for a declared pagination block.

    One instance per read: every scheme carries a position that only means
    anything within one traversal.
    """
    declared = str(block.get("type", ""))
    strategy = _STRATEGIES.get(declared)
    if strategy is None:
        raise UnknownPaginationStrategy(
            f"pagination.type {declared!r} is not one of "
            f"{sorted(_STRATEGIES)}; the contract's strategy union is closed, "
            f"so this build cannot walk it"
        )
    return strategy(block, base_params, url, resolve)
