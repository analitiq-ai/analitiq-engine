"""The CDK's API connector family: one class, read and write.

Public surface:

* ``GenericAPIConnector`` -- the connector both worker roles construct.
* ``ApiDialect`` -- the one override seam a connector package subclasses.
* ``PageLoop`` / ``Page`` / ``PageRequest`` / ``build_strategy`` /
  ``resolve_page_size`` -- the one paging loop and the five scheme
  adapters.
* ``evaluate_predicate`` -- a declared stop condition, evaluated raw.
* the error types this family raises.

Everything except the connector itself imports neither ``aiohttp`` nor
``pyarrow``, so the loop, the strategies and the predicates are importable
-- and testable -- without the ``api`` extra. ``GenericAPIConnector``
resolves lazily (PEP 562) and names the extra to install when it cannot.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .._extras import reraise_for_missing_extra
from .dialects import ApiDialect, dialect_overrides
from .exceptions import ApiConnectorError, ConnectorConnectionError
from .page_loop import Page, PageLoop, PageRequest, PaginationStrategy
from .predicates import UnknownPredicate, evaluate_predicate
from .strategies import UnknownPaginationStrategy, build_strategy, resolve_page_size

# The connector is the only module here that needs the HTTP client and
# Arrow. Keeping it out of the eager import graph is what lets a consumer
# import the loop without either.
_LAZY_API = frozenset({"GenericAPIConnector"})

if TYPE_CHECKING:
    from .generic import GenericAPIConnector  # noqa: F401


def __getattr__(name: str) -> Any:
    if name in _LAZY_API:
        try:
            from . import generic
        except ImportError as exc:
            reraise_for_missing_extra(
                exc,
                feature=f"cdk.api.{name}",
                extra="api",
                modules=("aiohttp", "aiohttp_retry", "orjson", "pyarrow"),
            )
        return getattr(generic, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "ApiConnectorError",
    "ApiDialect",
    "ConnectorConnectionError",
    "GenericAPIConnector",
    "Page",
    "PageLoop",
    "PageRequest",
    "PaginationStrategy",
    "UnknownPaginationStrategy",
    "UnknownPredicate",
    "build_strategy",
    "dialect_overrides",
    "evaluate_predicate",
    "resolve_page_size",
]
