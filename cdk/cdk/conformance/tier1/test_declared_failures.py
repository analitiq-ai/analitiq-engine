"""What a connector declares about its own failures.

The engine classifies a failure from what the boundary that caught it
declared, and never from the exception's type or message. That makes a
connector's ``error_map`` load-bearing: a category it declares becomes the
code a customer is shown, so an off-vocabulary or malformed declaration is
a defect the connector owns.

These checks drive the real parser
(:func:`~cdk.declarations.parse_declared_error_map`) over the connector's own
definition, so what the kit certifies and what the worker resolves at runtime
cannot diverge.

The vocabulary itself is kind-independent -- an API connector declares HTTP
statuses where a database declares SQLSTATEs -- but the kit assesses only
``kind: database`` today, and a module that claimed more would report a kind
as covered while every other tier-1 module still cannot assess it.
"""

from __future__ import annotations

import pytest

from cdk.conformance.target import ConformanceTarget
from cdk.declarations import (
    ERROR_CATEGORY_VALUES,
    ConnectorDeclarationError,
    ErrorMap,
    parse_declared_error_map,
)

#: Scoped to the one kind the kit can assess end to end.
APPLIES_TO_KINDS = ("database",)


def _declared_error_map(target: ConformanceTarget) -> ErrorMap | None:
    block = target.definition.get("error_map")
    return parse_declared_error_map(block, source=f"connector {target.connector_id!r}")


def test_declared_error_map_parses(
    conformance_target: ConformanceTarget,
) -> None:
    """A declared ``error_map`` parses, or the connector is defective.

    Declaring nothing is allowed and means the raising stage names the code.
    Declaring something malformed is not: the worker parses this block at
    connect time, so a defect here fails every run rather than one batch.
    """
    try:
        _declared_error_map(conformance_target)
    except ConnectorDeclarationError as exc:
        pytest.fail(str(exc))


def test_declared_categories_are_in_the_engine_vocabulary(
    conformance_target: ConformanceTarget,
) -> None:
    """Every declared category is one the engine can act on.

    The parser already refuses an unknown value; this states the rule the
    parser enforces as a property of the shipped definition, so a connector
    cannot invent a category and have it silently mean nothing.
    """
    error_map = _declared_error_map(conformance_target)
    # Declaring nothing is legitimate -- the raising stage names the code --
    # so an absent block is an empty declared set, not a skipped check.
    declared: set[str] = set()
    if error_map is not None:
        declared = (
            set(error_map.sqlstate.values())
            | set(error_map.exception.values())
            | set(error_map.vendor_code.values())
            | set(error_map.http.values())
        )
    unknown = sorted(declared - set(ERROR_CATEGORY_VALUES))
    assert not unknown, (
        f"connector {conformance_target.connector_id!r} declares categories "
        f"{unknown}; the engine vocabulary is {list(ERROR_CATEGORY_VALUES)}"
    )
