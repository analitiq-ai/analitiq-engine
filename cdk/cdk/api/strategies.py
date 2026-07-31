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

import logging
from collections.abc import Callable
from typing import Any

from .page_loop import Page, PageRequest, PaginationStrategy
from .records import walk_path

__all__ = [
    "UnknownPaginationStrategy",
    "build_strategy",
    "resolve_page_size",
]

logger = logging.getLogger(__name__)

#: Resolves a declared value expression against the page it was written for.
Resolve = Callable[[Any, Page | None], Any]

#: Resolves a provider-supplied next-page URL against the page it came from.
#: Injected rather than imported: the rules about a URL -- the same-origin
#: refusal, the RFC-3986 join -- belong to the transport, and this module
#: must stay importable without one.
FollowUrl = Callable[[str, str], str]

#: Builds one adapter from its declared block. A Protocol cannot be
#: instantiated, so the table is typed by what building one looks like.
_StrategyFactory = Callable[
    [dict[str, Any], dict[str, Any], str, Resolve, FollowUrl], PaginationStrategy
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

    A step whose integer value is exact is accepted whatever its Python
    type. The value can arrive from a response body whose typing the author
    does not control: the lossless JSON parse turns ``50.0`` into
    ``Decimal("50")``, and a provider that reports its own page size as a
    string is not describing a different intent. A fractional step IS a
    different intent and is refused.
    """
    if isinstance(value, bool):
        raise ValueError(f"pagination {context} must be an integer, got {value!r}")
    try:
        step = int(value)
    except (TypeError, ValueError) as err:
        raise ValueError(
            f"pagination {context} must be an integer, got {value!r}"
        ) from err
    # int() truncates a fractional float/Decimal; a string either parsed
    # exactly above or raised, so it needs no comparison.
    if not isinstance(value, str) and step != value:
        raise ValueError(f"pagination {context} must be an integer, got {value!r}")
    if step <= 0:
        raise ValueError(f"pagination {context} must be positive, got {step}")
    return step


def resolve_page_size(
    block: dict[str, Any] | None,
    *,
    batch_size: int,
    resolve: Callable[[Any], Any],
) -> int:
    """Resolve the page size a paginated read asks for.

    An authored ``limit.default`` value expression declares it
    (``runtime.batch_size`` is in scope, so a connector wires the engine's
    batch size in by reference); without one the engine's ``batch_size``
    applies. ``limit.max`` is the provider's cap and clamps the result
    either way, so the engine never asks for more than the provider says it
    can serve.

    A default that resolves to nothing falls back to ``batch_size`` with a
    warning naming the fallback; one that resolves to something that is not
    a positive integer is an authoring defect and fails before any request.
    The contract bounds ``limit.default`` above zero in its bare-integer
    spelling only, so a document reaching here can still declare a
    non-positive size as an expression.
    """
    limit = (block or {}).get("limit") or {}
    size = batch_size
    if limit.get("default") is not None:
        resolved = resolve(limit["default"])
        if resolved is not None:
            size = _positive_step(resolved, context="limit.default")
        else:
            logger.warning(
                "pagination limit.default did not resolve; falling back to "
                "engine batch_size %d",
                batch_size,
            )
    if limit.get("max") is not None:
        size = min(size, int(limit["max"]))
    return size


class _Offset:
    """Walk by a row offset the provider counts from."""

    def __init__(
        self,
        block: dict[str, Any],
        base: dict[str, Any],
        url: str,
        resolve: Resolve,
        follow: FollowUrl,
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
        self,
        block: dict[str, Any],
        base: dict[str, Any],
        url: str,
        resolve: Resolve,
        follow: FollowUrl,
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
        self,
        block: dict[str, Any],
        base: dict[str, Any],
        url: str,
        resolve: Resolve,
        follow: FollowUrl,
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
        self,
        block: dict[str, Any],
        base: dict[str, Any],
        url: str,
        resolve: Resolve,
        follow: FollowUrl,
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
        # order_by_field is a dotted record path per the contract, walked by
        # the same helper every other record-field access in this package
        # uses.
        value = walk_path(page.records[-1], self._field.split("."))
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
        self,
        block: dict[str, Any],
        base: dict[str, Any],
        url: str,
        resolve: Resolve,
        follow: FollowUrl,
    ) -> None:
        self._next_url = block["link"]["next_url"]
        # The declared params bind on the first request only: a next URL is
        # absolute and carries the provider's own query, so re-appending
        # ours would fight it. That is the contract's own rule for link.
        self._base = base
        self._url = url
        self._current = url
        self._resolve = resolve
        self._follow = follow

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
        # The target is vetted against the page it came from before the
        # loop yields, so a link that leaves the connection's origin fails
        # rather than sending the connection's headers to another host.
        self._current = self._follow(self._current, target)
        # No params and no body: the contract's link rule is that the
        # resolved URL replaces the entire request, which is also what makes
        # the declared limit first-request-only.
        return PageRequest(self._current, {}, sends_declared_body=False)


class _Single:
    """Read the one page an endpoint that declares no pagination serves.

    Not a scheme the contract names -- the absence of one. It exists so the
    unpaginated read runs on the same loop as every other: the empty-page
    rule, the fetch, and the yield are then written once rather than once
    here and once in a special case beside the loop.
    """

    def __init__(
        self,
        block: dict[str, Any],
        base: dict[str, Any],
        url: str,
        resolve: Resolve,
        follow: FollowUrl,
    ) -> None:
        self._base = base
        self._url = url

    def first(self) -> PageRequest:
        return PageRequest(self._url, dict(self._base))

    def advance(self, page: Page) -> PageRequest | None:
        return None


#: The contract's closed strategy union, by its own discriminator.
_STRATEGIES: dict[str, _StrategyFactory] = {
    "offset": _Offset,
    "page": _Page,
    "cursor": _Cursor,
    "keyset": _Keyset,
    "link": _Link,
}


def build_strategy(
    block: dict[str, Any] | None,
    *,
    url: str,
    base_params: dict[str, Any],
    resolve: Resolve,
    follow_url: FollowUrl,
) -> PaginationStrategy:
    """Build the adapter for a declared pagination block.

    One instance per read: every scheme carries a position that only means
    anything within one traversal. ``None`` is the endpoint that declares
    no pagination, which walks one page and stops.

    ``follow_url`` is required, not defaulted: the rule it carries is a
    refusal to send the connection's credentials to a host a response body
    named, and a default would be a way to skip that refusal by forgetting
    an argument.
    """
    if block is None:
        return _Single({}, base_params, url, resolve, follow_url)
    declared = str(block.get("type", ""))
    strategy = _STRATEGIES.get(declared)
    if strategy is None:
        raise UnknownPaginationStrategy(
            f"pagination.type {declared!r} is not one of "
            f"{sorted(_STRATEGIES)}; the contract's strategy union is closed, "
            f"so this build cannot walk it"
        )
    return strategy(block, base_params, url, resolve, follow_url)
