"""The connector definition itself: files present, blocks parse.

Loading already failed loud (:func:`~cdk.conformance.target.load_target`
raises on a missing definition, malformed JSON, an invalid
``sql_capabilities`` vocabulary, or unparseable type-map rules); these
tests pin the facts loading alone does not enforce.
"""

from __future__ import annotations

from cdk.conformance.target import ConformanceTarget


def test_definition_identifies_the_connector(
    conformance_target: ConformanceTarget,
) -> None:
    """``connector.json`` names the connector and its kind."""
    assert conformance_target.connector_id
    assert conformance_target.kind


def test_database_connector_ships_a_read_type_map(
    conformance_target: ConformanceTarget,
) -> None:
    """A database connector cannot canonicalize types without a read map."""
    if not conformance_target.is_database:
        return
    assert conformance_target.type_mapper is not None, (
        "kind 'database' requires definition/type-map-read.json; the engine "
        "cannot canonicalize discovered native types without it"
    )
