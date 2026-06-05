"""Request-construction binding forms for API endpoint bodies.

The api-endpoint contract lets a declared ``request.body`` mix literals
and value expressions with two binding forms that the expression grammar
does not know:

* ``{"from_param": "<name>"}`` — the resolved value of a declared
  operation param (read and write operations).
* ``{"from_input": "record" | "records" | "record.<dotted.path>"}`` —
  the in-flight record data (write operations only).

Binding replaces each node with ``{"literal": <value>}`` so the
downstream expression pass (:meth:`cdk.resolver.Resolver.resolve_for_request`)
returns the data verbatim — bound *values* are payload, never
re-inspected for expression or binding markers. The opaqueness boundary
is exactly ``literal`` nodes: what sits inside one is data (whether
authored or produced by an earlier binding pass). Every other node —
including a ``function`` node's ``input`` — is authored structure, so
the binders recurse into it; ``{"function": "base64_encode", "input":
{"from_input": "record.id"}}`` binds the record field before the
function evaluates.

A binding whose data is missing (an undeclared param name, a dotted
path to an absent record field) binds ``None``, which the expression
pass drops per the contract's omit-unresolved rule. A binding that
contradicts the request shape (``records`` where one record is sent,
sibling keys next to the marker) is an authoring error and raises.
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional


def bind_param_refs(spec: Any, params: Mapping[str, Any]) -> Any:
    """Replace ``{"from_param": ...}`` nodes with the named param's value."""
    if isinstance(spec, Mapping):
        if "from_param" in spec:
            if len(spec) != 1:
                raise ValueError(
                    f"`from_param` must be the only key in the node; "
                    f"got siblings {sorted(set(spec) - {'from_param'})}"
                )
            name = spec["from_param"]
            if not isinstance(name, str) or not name:
                raise ValueError(
                    f"`from_param` must be a non-empty string, got {name!r}"
                )
            return {"literal": params.get(name)}
        if "literal" in spec:
            return spec
        return {key: bind_param_refs(value, params) for key, value in spec.items()}
    if isinstance(spec, list):
        return [bind_param_refs(item, params) for item in spec]
    return spec


def collect_from_input_selectors(spec: Any) -> set:
    """All ``from_input`` selector strings authored in a body spec.

    Walks the same structure as :func:`bind_record_inputs` (``literal``
    nodes are opaque) without binding anything — used to validate a body
    spec against its batching mode before any record is in flight.
    """
    selectors: set = set()
    if isinstance(spec, Mapping):
        if "from_input" in spec:
            selector = spec["from_input"]
            if isinstance(selector, str):
                selectors.add(selector)
            return selectors
        if "literal" in spec:
            return selectors
        for value in spec.values():
            selectors |= collect_from_input_selectors(value)
    elif isinstance(spec, list):
        for item in spec:
            selectors |= collect_from_input_selectors(item)
    return selectors


def bind_record_inputs(
    spec: Any,
    *,
    record: Optional[Dict[str, Any]] = None,
    records: Optional[List[Dict[str, Any]]] = None,
) -> Any:
    """Replace ``{"from_input": ...}`` nodes with the in-flight record data.

    ``"record"`` binds the single in-flight record, ``"records"`` binds the
    whole batch, ``"record.<dotted.path>"`` binds one record field. A
    ``from_input`` that does not match the active batching mode (e.g.
    ``"record"`` in bulk mode) is an authoring error and raises.
    """
    if isinstance(spec, Mapping):
        if "from_input" in spec:
            if len(spec) != 1:
                raise ValueError(
                    f"`from_input` must be the only key in the node; "
                    f"got siblings {sorted(set(spec) - {'from_input'})}"
                )
            return {"literal": _record_input_value(spec["from_input"], record, records)}
        if "literal" in spec:
            return spec
        return {
            key: bind_record_inputs(value, record=record, records=records)
            for key, value in spec.items()
        }
    if isinstance(spec, list):
        return [
            bind_record_inputs(item, record=record, records=records)
            for item in spec
        ]
    return spec


def _record_input_value(
    selector: Any,
    record: Optional[Dict[str, Any]],
    records: Optional[List[Dict[str, Any]]],
) -> Any:
    """Resolve one ``from_input`` selector against the in-flight data."""
    if not isinstance(selector, str) or not selector:
        raise ValueError(f"`from_input` must be a non-empty string, got {selector!r}")
    if selector == "records":
        if records is None:
            raise ValueError(
                "`from_input: records` requires batching mode bulk or batch; "
                "this stream sends one record per request"
            )
        return records
    if selector == "record" or selector.startswith("record."):
        if record is None:
            raise ValueError(
                f"`from_input: {selector}` requires batching mode single; "
                f"this stream sends multiple records per request"
            )
        if selector == "record":
            return record
        cursor: Any = record
        for segment in selector[len("record."):].split("."):
            if not isinstance(cursor, Mapping) or segment not in cursor:
                return None
            cursor = cursor[segment]
        return cursor
    raise ValueError(
        f"Unsupported `from_input` selector {selector!r}; expected "
        f"'record', 'records', or 'record.<dotted.path>'"
    )
