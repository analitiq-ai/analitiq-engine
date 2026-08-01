"""Import-audit gate (issue #432): the engine binds no connector class.

A connector family is an installable CDK package, not engine code. The
engine resolves a connector through ``cdk.registry`` and never names one:
the moment a module under ``src/`` imports ``GenericSQLConnector`` or
``cdk.file.generic``, a kind's answer lives in two places again and the
two can disagree — which is exactly the drift ``KIND_DEFAULTS`` exists to
end.

The forbidden set is **derived from** ``KIND_DEFAULTS``, so a kind added
later is covered without this file changing.

Deliberately excluded: ``cdk.base_handler`` and the capability Protocols.
``src.destination`` and ``src.worker.proxy`` legitimately reference the
abstract base and the contracts — the invariant is about concrete kind
defaults, the classes that speak to a real system.

Scope: a static parse of ``*.py`` source, not an import. Importing every
engine module to find out would pull in the very transports the assertion
is about. A dynamic ``importlib.import_module`` with a runtime-built name
is out of reach, and intentionally so: resolving by name through the
registry is the sanctioned path this test exists to keep open.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

import src
from cdk.registry import KIND_DEFAULTS

_SRC_DIR = Path(src.__file__).resolve().parent

#: The modules that define a kind default, and the class names they export.
_FORBIDDEN_MODULES = {
    entry.class_path.split(":")[0] for entry in KIND_DEFAULTS.values()
}
_FORBIDDEN_NAMES = {entry.class_path.split(":")[1] for entry in KIND_DEFAULTS.values()}


def _imported_targets(tree: ast.AST):
    """Yield (module, name, lineno) for every import in *tree*.

    ``name`` is the imported symbol for the ``from x import y`` form and
    ``None`` for plain ``import x`` — a connector class can be reached
    either way, so both are checked.
    """
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                yield alias.name, None, node.lineno
        elif isinstance(node, ast.ImportFrom):
            # Relative imports inside src/ cannot reach the CDK.
            if node.level != 0 and node.module is None:
                continue
            module = node.module or ""
            for alias in node.names:
                yield module, alias.name, node.lineno
                # ``from cdk.file import generic`` names the module through
                # the imported symbol, not the module string.
                yield f"{module}.{alias.name}", None, node.lineno


def _violations(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(), filename=str(path))
    found = []
    for module, name, lineno in _imported_targets(tree):
        if module in _FORBIDDEN_MODULES:
            if name is None or name in _FORBIDDEN_NAMES:
                found.append(
                    f"{path}:{lineno}: imports {module}" + (f".{name}" if name else "")
                )
    return found


@pytest.mark.unit
def test_the_engine_imports_no_connector_class() -> None:
    """No module under ``src/`` imports a kind default."""
    offenders = [
        violation
        for path in sorted(_SRC_DIR.rglob("*.py"))
        for violation in _violations(path)
    ]
    assert not offenders, (
        "the engine must bind no connector class -- resolve it through "
        "cdk.registry instead:\n" + "\n".join(offenders)
    )


@pytest.mark.unit
def test_the_gate_catches_a_reintroduced_connector_import(tmp_path: Path) -> None:
    """The audit fails on a real import, so a green run means something.

    Without this, a bug in the AST walk would leave the gate passing on
    every possible input -- the failure mode an import-audit test is least
    able to notice about itself.
    """
    offender = tmp_path / "regression.py"
    offender.write_text("from cdk.file.generic import GenericFileConnector\n")
    assert _violations(offender)

    module_form = tmp_path / "module_form.py"
    module_form.write_text("import cdk.sql.generic\n")
    assert _violations(module_form)

    submodule_form = tmp_path / "submodule_form.py"
    submodule_form.write_text("from cdk.stdout import generic\n")
    assert _violations(submodule_form)

    allowed = tmp_path / "allowed.py"
    allowed.write_text(
        "from cdk.base_handler import BaseDestinationHandler\n"
        "from cdk.registry import build_registries\n"
    )
    assert not _violations(allowed)
