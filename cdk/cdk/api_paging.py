"""What an API paging loop offers a page's value expressions, and what it takes back.

An endpoint document declares its paging behaviour as value expressions
resolved once per page — ``pagination.stop_when``, ``cursor.next_cursor``,
``link.next_url``, ``offset.increment_by``, ``limit.default``. Two facts
decide whether such a declaration can execute at all, and both live here
so the loop that runs them and the conformance kit that certifies them
read the same statement:

* :func:`page_response_scope` — the ``response`` scope a page expression
  resolves against. The contract reserves six response names; a loop
  populates two of them, so a ``stop_when`` on ``response.headers``
  addresses nothing and silently ends the read after one page.
* :func:`positive_page_value` — the parse every authored step and page
  size goes through. A non-positive step cannot advance its loop (the
  same request would repeat unbounded) and a non-positive page size is a
  meaningless request, so both are authoring defects that must fail
  before the first request rather than at runtime.
"""

from __future__ import annotations

from typing import Any

#: The reserved response names a paging loop actually populates. The
#: contract reserves four more (``headers``, ``status``, ``records``,
#: ``metadata``); a page expression naming one of those resolves to
#: nothing, which is why the set a loop offers is stated here rather than
#: left implicit in the loop that builds it.
PAGE_RESPONSE_NAMES: frozenset[str] = frozenset({"body", "record_count"})


class PageValueError(ValueError):
    """An authored paging step or page size is not a positive integer.

    A deterministic authoring defect in the endpoint document: no retry
    can heal it, and the fix is in the declaration.
    """


def page_response_scope(body: Any, records: list[dict[str, Any]]) -> dict[str, Any]:
    """Build the ``response`` scope one page's expressions resolve against.

    ``record_count`` is the contract's name for how many records the page
    carried; an offset that counts records advances by it. Every
    strategy's per-page expressions — ``stop_when``, ``next_cursor``,
    ``next_url``, ``increment_by`` — see exactly this scope.
    """
    return {"body": body, "record_count": len(records)}


def positive_page_value(value: Any, *, context: str) -> int:
    """Parse an authored paging step or page size into a positive int.

    ``context`` names the declaration in the failure message
    (``offset.increment_by``, ``limit.default``, ...).
    """
    try:
        step = int(value)
    except (TypeError, ValueError) as err:
        raise PageValueError(
            f"pagination {context} must be an integer, got {value!r}"
        ) from err
    if not isinstance(value, str) and step != value:
        # int() truncates fractional floats/Decimals; a fractional step or
        # page size is malformed data, not a roundable one. (Strings either
        # parse as exact ints above or raise.)
        raise PageValueError(f"pagination {context} must be an integer, got {value!r}")
    if step <= 0:
        raise PageValueError(f"pagination {context} must be positive, got {step}")
    return step
