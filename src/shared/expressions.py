"""Resolution helpers for contract value expressions.

The contract artifacts (endpoint params, pagination defaults, …) use a
small expression vocabulary to bind values at runtime:

* ``{"literal": <value>}`` — pass-through scalar.
* ``{"ref": "<scope>.<dotted.path>"}`` — look up a value in a named
  scope (``connection``, ``runtime``, ``response``, …).
* ``{"template": "...${scope.path}..."}`` — string concatenation of
  scoped values, with missing values rendered as empty strings.

This module is the single seam where those expressions are resolved.
Connectors call :func:`resolve_value_expression` instead of inlining the
ref/template machinery.
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional

from cdk.connection_runtime import ConnectionRuntime


def resolve_value_expression(
    node: Any,
    runtime: ConnectionRuntime,
    extra_scopes: Optional[Mapping[str, Any]] = None,
) -> Any:
    """Resolve a value-expression node against the connection scopes.

    Plain scalars pass through unchanged. ``literal``/``ref``/``template``
    expressions are resolved against the connection's parameters,
    selections, and discovered values, plus any caller-supplied scopes
    (e.g. ``runtime``).
    """
    if not isinstance(node, dict):
        return node
    if "literal" in node:
        return node["literal"]
    scopes = _build_scopes(runtime, extra_scopes)
    if "ref" in node:
        return _walk_dotted(scopes, node["ref"])
    if "template" in node:
        return _expand_template(node["template"], scopes)
    return node


def _build_scopes(
    runtime: ConnectionRuntime,
    extra_scopes: Optional[Mapping[str, Any]],
) -> Dict[str, Any]:
    raw = runtime.raw_config
    scopes: Dict[str, Any] = {
        "connection": {
            "parameters": dict(raw.get("parameters") or {}),
            "selections": dict(raw.get("selections") or {}),
            "discovered": dict(raw.get("discovered") or {}),
        },
    }
    if extra_scopes:
        for k, v in extra_scopes.items():
            scopes[k] = v
    return scopes


def _walk_dotted(scopes: Mapping[str, Any], path: str) -> Any:
    cursor: Any = scopes
    for segment in path.split("."):
        if not isinstance(cursor, Mapping) or segment not in cursor:
            return None
        cursor = cursor[segment]
    return cursor


def _expand_template(template: str, scopes: Mapping[str, Any]) -> str:
    out: List[str] = []
    i = 0
    while i < len(template):
        j = template.find("${", i)
        if j < 0:
            out.append(template[i:])
            break
        out.append(template[i:j])
        k = template.find("}", j + 2)
        if k < 0:
            out.append(template[i:])
            break
        path = template[j + 2 : k]
        value = _walk_dotted(scopes, path)
        out.append("" if value is None else str(value))
        i = k + 1
    return "".join(out)
