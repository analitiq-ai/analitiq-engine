"""What each module in the API package is allowed to import.

Two rules, both structural. The transport lives in one module and the
connector that drives it, so the loop, the strategies, the predicates and
the verdicts stay testable -- and importable -- without an HTTP client.
And the predicate walker evaluates a stop condition without importing the
contract's predicate models: everything else here reads the document as
the models it parsed into, but a branch selected by ``isinstance`` would
drag all seventeen of them into the one module that must stay a walk.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

import cdk.api

pytestmark = pytest.mark.unit

_PACKAGE = Path(cdk.api.__file__).parent

#: The HTTP client and its companions belong to the round trip and the
#: connector that makes it.
_TRANSPORT_MODULES = {"aiohttp", "aiohttp_retry", "orjson"}
_MAY_IMPORT_TRANSPORT = {"http.py", "generic.py"}

#: Arrow is the read's output shape, which only the connector produces.
_MAY_IMPORT_ARROW = {"generic.py"}


def _imported_roots(path: Path) -> set[str]:
    """Every top-level package a module imports, absolute imports only."""
    roots: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(), filename=str(path))):
        if isinstance(node, ast.Import):
            roots |= {alias.name.split(".", 1)[0] for alias in node.names}
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            roots.add(node.module.split(".", 1)[0])
    return roots


def _modules() -> list[Path]:
    return sorted(_PACKAGE.rglob("*.py"))


def test_the_transport_lives_in_one_module() -> None:
    offenders = [
        f"{path.name}: {sorted(_imported_roots(path) & _TRANSPORT_MODULES)}"
        for path in _modules()
        if path.name not in _MAY_IMPORT_TRANSPORT
        and _imported_roots(path) & _TRANSPORT_MODULES
    ]
    assert not offenders, (
        "only the round trip and the connector may reach for an HTTP client; "
        "the loop and the strategies must stay testable without one:\n  "
        + "\n  ".join(offenders)
    )


def test_only_the_connector_produces_arrow() -> None:
    offenders = [
        path.name
        for path in _modules()
        if path.name not in _MAY_IMPORT_ARROW and "pyarrow" in _imported_roots(path)
    ]
    assert not offenders, (
        "Arrow is the read's output shape, produced once by the connector: "
        f"{offenders}"
    )


#: The predicate walk and the helper that feeds it the authored form. Both,
#: because the promise holds only end to end: a contract import in either
#: puts the seventeen predicate models back on the stop condition's path.
_PREDICATE_WALK = (
    _PACKAGE / "predicates.py",
    _PACKAGE.parent / "json_utils.py",
)


def test_the_predicate_walk_imports_no_contract_model() -> None:
    # A stop condition is a one-key mapping whose key names the operator,
    # exactly as the contract's own union serialises -- so evaluating one is
    # a walk over the authored form, not a branch by isinstance over the
    # seventeen predicate models. This is the promise predicates.py's
    # docstring makes; every other module here reads the parsed models.
    offenders = [
        path.name for path in _PREDICATE_WALK if "analitiq" in _imported_roots(path)
    ]
    assert not offenders, (
        "the stop condition is evaluated in its authored form, so no contract "
        f"model may reach it: {offenders}"
    )


def test_the_connector_is_not_imported_eagerly() -> None:
    # A thin install without the api extra must still be able to import the
    # loop; the connector resolves through the package's lazy accessor.
    surface = _imported_roots(_PACKAGE / "__init__.py")
    assert "aiohttp" not in surface and "pyarrow" not in surface
    assert "GenericAPIConnector" in cdk.api.__all__
