"""The connector definition itself: files present, blocks parse.

Loading already failed loud (:func:`~cdk.conformance.target.load_target`
raises on a missing definition, malformed JSON, an invalid
``sql_capabilities`` vocabulary, or unparseable type-map rules); these
tests pin the facts loading alone does not enforce -- including the one
loading deliberately does NOT raise on, an endpoint document the
published contract refuses.
"""

from __future__ import annotations

import pytest

from cdk.conformance.target import ConformanceTarget, check_endpoint_documents
from cdk.conformance.violations import violation_report


def test_definition_identifies_the_connector(
    conformance_target: ConformanceTarget,
) -> None:
    """``connector.json`` names the connector and its kind."""
    assert conformance_target.connector_id
    assert conformance_target.kind


def test_connector_ships_a_read_type_map(
    conformance_target: ConformanceTarget,
) -> None:
    """No connector can canonicalize its source types without a read map.

    The read map is the connector's type vocabulary whatever its kind:
    a database canonicalizes the native types discovery returns, an API
    canonicalizes each field's declared JSON ``type``/``format``. Without
    one, every stream whose schema the connector does not hand-annotate
    fails on its first read.
    """
    assert conformance_target.type_mapper is not None, (
        "the connector ships no definition/type-map-read.json; the engine "
        "canonicalizes every source type it discovers through that map"
    )


def test_every_endpoint_document_satisfies_its_contract(
    conformance_target: ConformanceTarget,
) -> None:
    """A document the contract refuses is an endpoint no other check assesses."""
    violations = check_endpoint_documents(conformance_target)
    if violations:
        pytest.fail(violation_report(violations))
