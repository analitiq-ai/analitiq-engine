"""Where a page's records live in a decoded body, and how to reach them.

``operations.read.response.records.ref`` is one path, read by two pieces of
code: the payload walk that pulls the records out of a live response, and
the schema walk that finds the per-record item schema. They parsed the
anchor separately and could disagree about what a ref meant, so the parse
is :func:`split_records_ref` and both call it.

Extraction fails loud. A ref that addresses nothing used to answer zero
records, and under the loop's empty-page rule zero records ends the
traversal -- so a mistyped path silently read a stream as empty instead of
naming the mistake.
"""

from __future__ import annotations

import logging
from typing import Any

from ..exceptions import ReadError
from ..resolver import Resolver
from .page_loop import Page

__all__ = [
    "PAGE_SCOPE_KEYS",
    "extract_records",
    "page_resolver",
    "page_scope",
    "split_records_ref",
    "walk_path",
]

logger = logging.getLogger(__name__)

#: The scope every records ref is anchored at, per the contract.
_ANCHOR = "response.body"

#: The two keys a page's response scope carries, named once here and used by
#: :func:`page_scope` and :data:`PAGE_SCOPE_KEYS` alike. Private: the pair a
#: caller needs is the exported tuple, not the individual names.
_PAGE_SCOPE_BODY = "body"
_PAGE_SCOPE_RECORD_COUNT = "record_count"


def walk_path(data: Any, path: list[str]) -> Any:
    """Walk *data* by *path*, returning the terminal value or ``None`` on any miss.

    A key whose stored value is ``None`` is indistinguishable from a missing
    key in the return value -- both produce ``None``.
    """
    current = data
    for key in path:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return None
    return current


def split_records_ref(ref: Any) -> list[str]:
    """Return the field path a records ref addresses under the response body.

    ``response.body`` is the body itself (an empty path);
    ``response.body.<field>[.<field>...]`` is the fields below it. Anything
    else is an authoring defect and raises naming the ref: the contract
    anchors records at the response body, and reading an unanchored ref as
    "nothing found" is what let a mistyped path pass for an empty stream.
    """
    if not isinstance(ref, str) or not ref:
        raise ReadError(
            f"records.ref must be a non-empty string anchored at "
            f"{_ANCHOR!r}; read {ref!r}"
        )
    if ref == _ANCHOR:
        return []
    if ref.startswith(_ANCHOR + "."):
        return ref[len(_ANCHOR) + 1 :].split(".")
    raise ReadError(
        f"unsupported records.ref {ref!r}; expected {_ANCHOR!r} or "
        f"'{_ANCHOR}.<field>[.<field>...]'"
    )


def extract_records(payload: Any, ref: str) -> list[dict[str, Any]]:
    """Pull a page's records out of a decoded body, per the declared ref.

    A list of objects is the records; a single object is one record. A
    value that is neither raises naming the ref and what was found --
    answering zero records there would end the traversal at page one and
    report a truncated read as a complete one.

    Non-object items inside a records list are dropped with a warning
    rather than raised on: a provider that puts a ``null`` in its array
    reads correctly today, and one bad item is not a reason to fail a
    stream. The count is logged so the loss is visible.
    """
    if payload is None:
        # A success with no body at all -- a 204, or an empty 200. The
        # provider said "nothing here", which is an empty page and ends the
        # traversal; only a body that IS there and holds something other
        # than records is the defect below.
        return []
    path = split_records_ref(ref)
    found = payload if not path else walk_path(payload, path)
    if isinstance(found, list):
        records = [item for item in found if isinstance(item, dict)]
        dropped = len(found) - len(records)
        if dropped:
            logger.warning(
                "records.ref %r: dropped %d non-object item(s) from the page",
                ref,
                dropped,
            )
        return records
    if isinstance(found, dict):
        return [found]
    raise ReadError(
        f"records.ref {ref!r} addresses a {type(found).__name__}, which "
        f"carries no records; expected an array of objects or one object"
    )


def page_scope(page: Page) -> dict[str, Any]:
    """Build the ``response`` scope a page's declared expressions resolve against.

    ``record_count`` is the contract's name for how many records the page
    carried; an offset that counts records advances by it. Built once per
    page so ``stop_when``, ``next_cursor``, ``next_url`` and ``increment_by``
    all see the same scope.
    """
    return {
        _PAGE_SCOPE_BODY: page.payload,
        _PAGE_SCOPE_RECORD_COUNT: len(page.records),
    }


#: What a page's response scope carries, and nothing else -- the keys
#: :func:`page_scope` builds. Read by the conformance kit, which refuses a
#: declared ``response.<x>`` a page could never carry; derived from the
#: builder rather than restated so a key the loop gains cannot fail a
#: connector that reads it.
PAGE_SCOPE_KEYS = (_PAGE_SCOPE_BODY, _PAGE_SCOPE_RECORD_COUNT)


def page_resolver(resolver: Resolver, page: Page | None) -> Resolver:
    """Give *resolver* the page's body as its ``response`` scope.

    Every declared expression a page carries -- ``stop_when``,
    ``next_cursor``, ``next_url``, ``increment_by`` -- resolves against this
    one scope, and ``None`` is the pre-first-request phase where no page
    exists yet. It lives beside :func:`page_scope` rather than in the
    connector so the conformance kit resolves a page expression exactly as
    the read does; the kit installs no HTTP client, and a rule it had to
    copy is a rule that drifts.
    """
    return resolver if page is None else resolver.with_response(page_scope(page))
