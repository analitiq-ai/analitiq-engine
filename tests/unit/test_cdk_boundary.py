"""Import-audit gate (ADR §4.1): the CDK must not depend on anything engine-side.

The one rule: the dependency points **engine -> CDK, never back**. This test
statically parses every module under the ``cdk`` package and fails if any of them
imports an engine package (``src.*``) or the gRPC / protobuf stack (the CDK is
transport-neutral — explicitly not ``grpcio`` / ``protobuf``). It is the "one
rule as code": equivalent to an import-linter forbidden contract, with no extra
dependency, and it catches the next stray wire at PR time rather than in review.
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
