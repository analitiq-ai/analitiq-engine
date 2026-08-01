"""What each module in the API package is allowed to import.

Two rules, both structural. The transport lives in one module and the
connector that drives it, so the loop, the strategies, the predicates and
the verdicts stay testable -- and importable -- without an HTTP client.
And nothing here imports a contract model: the engine validates the
document before it crosses the boundary, and this package navigates the
already-validated document raw.
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


def test_no_module_imports_a_contract_model() -> None:
    # The engine validates the document against the published contract
    # before anything here reads it, so this package navigates it raw --
    # dispatching on the raw discriminator key, never by isinstance over
    # seventeen models.
    offenders = [
        path.name for path in _modules() if "analitiq" in _imported_roots(path)
    ]
    assert not offenders, f"cdk.api must not import contract models: {offenders}"


def test_the_connector_is_not_imported_eagerly() -> None:
    # A thin install without the api extra must still be able to import the
    # loop; the connector resolves through the package's lazy accessor.
    surface = _imported_roots(_PACKAGE / "__init__.py")
    assert "aiohttp" not in surface and "pyarrow" not in surface
    assert "GenericAPIConnector" in cdk.api.__all__
