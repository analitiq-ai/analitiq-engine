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
from .page_loop import Page

__all__ = ["extract_records", "page_scope", "split_records_ref", "walk_path"]

logger = logging.getLogger(__name__)

#: The scope every records ref is anchored at, per the contract.
_ANCHOR = "response.body"


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
    return {"body": page.payload, "record_count": len(page.records)}
