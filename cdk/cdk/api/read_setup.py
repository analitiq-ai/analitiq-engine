"""What a read settles before its first request, with no client involved.

Three things stand between an endpoint document and a walkable traversal:
the resolver a strategy resolves its per-page expressions through, the
author's stop condition adapted to what the loop asks of it, and the
paging adapter itself with the page size bound and the origin guard armed.
None of them sends anything, and none of them needs an HTTP client -- they
are the read's deterministic half, which is exactly the half that can be
certified from a definition alone.

They live here rather than on the connector for that reason. The
conformance kit drives an endpoint document as far as a definition goes,
from an install that carries no transport; a kit that could not import
this would have to rebuild it, and a rebuilt setup drifts from the one the
engine runs -- so the suite would certify an ordering the read no longer
uses. Every failure here is deterministic and costs nothing to raise,
which is why the read settles all of it before a single page is fetched.
"""

from __future__ import annotations

from functools import partial
from typing import Any

from analitiq.contracts.endpoints import Pagination, Predicate

from ..exceptions import ReadError
from ..resolver import Resolver
from .exceptions import RequestSpecError, request_spec_errors
from .page_loop import Page, PaginationStrategy, StopCondition
from .predicates import evaluate_predicate
from .records import page_resolver
from .request import ParamTable
from .strategies import Resolve, build_strategy, resolve_page_size
from .urls import follow_url

__all__ = ["build_read_strategy", "page_expression_resolver", "stop_condition"]


def page_expression_resolver(resolver: Resolver) -> Resolve:
    """Adapt the read's resolver to what a strategy asks of it.

    The resolution boundary is :func:`~cdk.api.exceptions.request_spec_errors`,
    the same one the request build uses, so a defect inside a pagination
    expression is classified where it happens rather than by whichever
    catch site happened to list its exception type. The read error is what
    the traversal's callers already handle.
    """

    def resolve(expr: Any, page: Page | None) -> Any:
        try:
            with request_spec_errors("pagination expression"):
                return page_resolver(resolver, page).resolve_for_request(expr)
        except RequestSpecError as err:
            raise ReadError(str(err)) from err

    return resolve


def stop_condition(declared: Predicate | None, resolver: Resolver) -> StopCondition:
    """Adapt the declared stop condition to what the loop asks of it."""

    def stop_when(page: Page) -> bool:
        if declared is None:
            # No pagination block, so the strategy already ends the
            # traversal after its one page.
            return False
        try:
            with request_spec_errors("pagination stop_when"):
                return evaluate_predicate(
                    declared, page_resolver(resolver, page).resolve_for_request
                )
        except RequestSpecError as err:
            raise ReadError(str(err)) from err

    return stop_when


def build_read_strategy(
    pagination: Pagination | None,
    *,
    table: ParamTable,
    resolver: Resolver,
    url: str,
    origin: str,
    batch_size: int,
) -> PaginationStrategy:
    """Build the paging adapter, binding the page size it walks with.

    The page size binds here rather than in the loop: the loop has no
    page-size concept, so a read that skipped this would raise nothing and
    quietly take the provider's own default forever.

    It binds into the params this adapter walks with, NOT into *table*: the
    table is the caller's, the request builder holds the same object, and a
    build function that silently rewrites its argument makes the page size
    depend on the order two calls happen in. Every page carries the value
    anyway -- it rides the ``PageRequest`` the adapter answers, which is
    what the builder binds from.

    ``origin`` arms the containment guard with the origin of the
    transport THIS read dispatches through, which is where every page of
    it goes out: the session carries that transport's credentials, and
    the endpoint's own ``request.headers`` were declared for that host.
    A next link off it is refused rather than followed, because there is
    nothing correct to send to another origin -- an endpoint's
    declaration belongs to the host it was written for.
    """
    try:
        with request_spec_errors("pagination"):
            page_size = resolve_page_size(
                pagination, batch_size=batch_size, resolve=resolver.resolve_for_request
            )
            base_params = dict(table.values)
            limit = pagination.limit if pagination is not None else None
            if limit is not None and limit.param:
                base_params[limit.param] = page_size

            return build_strategy(
                pagination,
                url=url,
                base_params=base_params,
                resolve=page_expression_resolver(resolver),
                follow_url=partial(follow_url, origin=origin),
            )
    except RequestSpecError as err:
        # An unknown scheme, a page size that cannot advance, a step that
        # is not a whole number, a ``limit.default`` reading a scope that
        # is not there: authoring defects the loop cannot run at all. They
        # are deterministic, so they must reach the worker as a read error
        # rather than as a bare builtin it would classify as worth retrying.
        raise ReadError(f"pagination could not be set up: {err}") from err
