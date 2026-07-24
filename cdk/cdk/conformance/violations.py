"""The kit's finding type: one violation per broken conformance rule.

Checks return ``list[Violation]`` instead of raising on the first
problem, so a run reports every defect at once and the pytest wrappers
can fail with the full, actionable list.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class Violation:
    """One conformance failure, named by check and explained for the author.

    ``check`` is a stable kebab-case identifier (``override-surface``,
    ``declaration-consistency``, ``type-map-convergence``, ...);
    ``message`` names the offending member or rule and what to change in
    the connector package.
    """

    check: str
    message: str

    def __str__(self) -> str:
        """Render as ``[check] message`` for reports and assertions."""
        return f"[{self.check}] {self.message}"


def violation_report(violations: Sequence[Violation]) -> str:
    """Render *violations* as a numbered, one-per-line failure report."""
    lines = [f"{i}. {v}" for i, v in enumerate(violations, start=1)]
    count = len(violations)
    noun = "violation" if count == 1 else "violations"
    return f"{count} conformance {noun}:\n" + "\n".join(lines)
