"""Which connector kinds a run can assess, and the verdict when none.

A check module states the connector kinds its checks apply to in a
module-level ``APPLIES_TO_KINDS`` tuple. The suite's conftest turns that
one statement into both halves of applicability: the skip for every
other kind, and the record of what the run covered — so the declaration
that gates a check is the same one that reports what it certified, and
the two cannot drift.

A run whose collected checks name no kind matching the target's has
assessed nothing about that connector, and says so with a failure.
"Passed" and "not assessed" are different outcomes; a gate that reports
them identically certifies nothing, and the fix belongs here rather than
in a kind branch in every connector repo's CI.

The declaration is a plain module attribute, not a pytest marker: these
check modules are imported as ordinary modules by other harnesses (the
engine's own kit tests call the check functions directly), where no
pytest session exists to register a marker against.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from .target import ConformanceTarget
from .violations import Violation

CHECK = "kind-applicability"

#: The module attribute a check module sets to state its scope.
KINDS_ATTRIBUTE = "APPLIES_TO_KINDS"


def declared_kinds(item: Any) -> frozenset[str]:
    """Return the kinds *item*'s module states its checks apply to.

    Empty for a module that states none: those checks are kind-agnostic
    (the definition parses, the declaration is not self-contradictory)
    and certify no kind, so they never satisfy applicability.
    """
    declared = getattr(getattr(item, "module", None), KINDS_ATTRIBUTE, None)
    if declared is None:
        return frozenset()
    if isinstance(declared, str):
        return frozenset({declared})
    return frozenset(str(kind) for kind in declared)


def assessed_kinds(items: Iterable[Any]) -> frozenset[str]:
    """Every connector kind the collected checks apply to."""
    kinds: set[str] = set()
    for item in items:
        kinds |= declared_kinds(item)
    return frozenset(kinds)


def check_kind_applicability(
    target: ConformanceTarget, items: Iterable[Any]
) -> list[Violation]:
    """Certify that this run has at least one check for *target*'s kind.

    *items* is the run's collected checks (``session.items``), so the
    verdict is derived from the suite that actually ran: a suite that
    grows checks for a new kind clears this gate for that kind without
    anything here changing.
    """
    kinds = assessed_kinds(items)
    if target.kind in kinds:
        return []
    scope = (
        f"the checks collected here apply to kind "
        f"{', '.join(repr(kind) for kind in sorted(kinds))}"
        if kinds
        else "no check collected here states a kind it applies to"
    )
    return [
        Violation(
            CHECK,
            f"no check in this run applies to connector kind "
            f"{target.kind!r}, so this connector is ungated: {scope}. "
            f"Nothing here certifies how this connector behaves, and a "
            f"green run would report 'not assessed' as 'passed'. Until the "
            f"suite carries checks for kind {target.kind!r}, its result is "
            f"no evidence about this connector.",
        )
    ]
