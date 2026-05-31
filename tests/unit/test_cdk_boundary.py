"""Import-audit gate (ADR §4.1): the CDK must not depend on anything engine-side.

The one rule: the dependency points **engine -> CDK, never back**. This test
statically parses every module under the ``cdk`` package and fails if any of them
imports an engine package (``src.*``) or the gRPC / protobuf stack (the CDK is
transport-neutral — explicitly not ``grpcio`` / ``protobuf``). It is the "one
rule as code": equivalent to an import-linter forbidden contract, with no extra
dependency, and it catches the next stray wire at PR time rather than in review.

Scope: this is a static scan of ``*.py`` source. It checks both ``import x.y``
and ``from x import y`` forms (including ``from google import protobuf``), but a
dynamic ``importlib.import_module("src...")`` with a runtime-built name string is
out of reach — by design, the CDK builds module names only from closed,
data-driven dicts of third-party driver packages, never engine code.
"""

from __future__ import annotations

import ast
from pathlib import Path

import cdk

_CDK_DIR = Path(cdk.__file__).resolve().parent


def _is_forbidden(module: str) -> bool:
    """Is *module* an absolute import the CDK is forbidden to make?"""
    root = module.split(".", 1)[0]
    if root in {"src", "grpc", "grpcio"}:
        return True
    # gRPC's protobuf runtime — the CDK speaks CDK-native value types, never
    # the wire messages.
    if module == "google.protobuf" or module.startswith("google.protobuf."):
        return True
    return False


def _absolute_imports(tree: ast.AST):
    """Yield (module, lineno) for every absolute import in *tree*.

    Relative imports (``from . import x`` / ``from ..y import z``) stay inside
    the CDK package, so they are allowed and skipped.
    """
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                yield alias.name, node.lineno
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module is not None:
                yield node.module, node.lineno
                # ``from google import protobuf`` has module ``google`` (an
                # allowed root) but pulls in ``google.protobuf``. Yield the
                # fully-qualified ``module.name`` for each imported name so the
                # forbidden check sees the real target.
                for alias in node.names:
                    yield f"{node.module}.{alias.name}", node.lineno


def test_cdk_imports_nothing_engine_side_or_grpc():
    violations: list[str] = []
    for path in sorted(_CDK_DIR.rglob("*.py")):
        tree = ast.parse(path.read_text(), filename=str(path))
        for module, lineno in _absolute_imports(tree):
            if _is_forbidden(module):
                rel = path.relative_to(_CDK_DIR.parent)
                violations.append(f"{rel}:{lineno}: imports {module!r}")

    assert not violations, (
        "CDK modules must not import engine-side (src.*) or gRPC/protobuf "
        "packages (ADR §4.1 — the dependency points engine -> CDK, never "
        "back):\n  " + "\n  ".join(violations)
    )


def _forbidden_modules(source: str) -> set[str]:
    """Run the gate's static scan over a source string; return the hits."""
    tree = ast.parse(source)
    return {
        module for module, _ in _absolute_imports(tree) if _is_forbidden(module)
    }


def test_gate_catches_forbidden_import_forms():
    """The scan must catch every shape a forbidden import can take.

    Guards against regressions in ``_absolute_imports`` / ``_is_forbidden`` —
    in particular the ``from google import protobuf`` form, whose module is the
    allowed root ``google``. A ``from X import Y`` yields both ``X`` and
    ``X.Y``, so we assert the forbidden target is among the hits rather than
    exact-matching the set.
    """
    assert _forbidden_modules("import grpc") == {"grpc"}
    assert _forbidden_modules("import grpc as g") == {"grpc"}
    assert _forbidden_modules("import src.models.stream") == {"src.models.stream"}
    assert "grpc" in _forbidden_modules("from grpc import aio")
    assert "src.engine" in _forbidden_modules("from src.engine import engine")
    # The evasion form: module root ``google`` is allowed; the real target is
    # ``google.protobuf``, which the scan must surface.
    assert "google.protobuf" in _forbidden_modules("from google import protobuf")
    assert "google.protobuf.message" in _forbidden_modules(
        "import google.protobuf.message"
    )


def test_gate_allows_legitimate_imports():
    """Third-party and stdlib imports the CDK legitimately uses must pass."""
    assert _forbidden_modules("import pyarrow as pa") == set()
    assert _forbidden_modules("from sqlalchemy import Column") == set()
    assert _forbidden_modules("from google.cloud import bigquery") == set()
    assert _forbidden_modules("from . import types") == set()  # relative, skipped
