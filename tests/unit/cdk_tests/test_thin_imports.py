"""The SQL control-plane surface imports without the ``arrow``/``api`` extras.

``cdk.sql`` (discovery + standalone ``create_table``) and the string-only
``cdk.type_map`` surface must stay importable when neither ``pyarrow`` nor
``aiohttp`` is installed -- that is the whole point of making them optional
extras (a control-plane process introspects schemas and creates tables without
the columnar streaming weight).

We cannot uninstall pyarrow/aiohttp from the test venv, so each case runs in a
fresh subprocess that installs a meta-path finder blocking those modules
*before* importing ``cdk``. The lazy (PEP 562) accessors must therefore raise
``ModuleNotFoundError`` only when actually touched, never at package import.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

# The CDK package lives at ``<repo>/cdk/cdk``; pytest puts ``<repo>/cdk`` on the
# path via ``pythonpath = ["cdk"]`` (pyproject). A bare subprocess does not
# inherit that, so hand it the same source dir.
_CDK_SRC = str(Path(__file__).resolve().parents[3] / "cdk")

# Prologue: block ``pyarrow`` and ``aiohttp`` (and any submodule) at import
# time, exactly as a thin ``analitiq-cdk`` install without the extras would.
_BLOCK = textwrap.dedent(
    """
    import sys

    _BLOCKED = ("pyarrow", "aiohttp")

    class _Blocker:
        def find_spec(self, name, path=None, target=None):
            top = name.split(".", 1)[0]
            if top in _BLOCKED:
                raise ModuleNotFoundError(f"blocked for test: {name}")
            return None

    sys.meta_path.insert(0, _Blocker())
    # Drop anything already cached so the block takes effect.
    for _m in list(sys.modules):
        if _m.split(".", 1)[0] in _BLOCKED:
            del sys.modules[_m]
    """
)


def _run(body: str) -> subprocess.CompletedProcess:
    script = _BLOCK + textwrap.dedent(body)
    env = dict(os.environ)
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        f"{_CDK_SRC}{os.pathsep}{existing}" if existing else _CDK_SRC
    )
    return subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env=env,
    )


@pytest.mark.unit
class TestThinControlPlaneImports:
    def test_cdk_sql_imports_without_arrow(self):
        """``cdk.sql`` control-plane surface loads with pyarrow/aiohttp blocked."""
        result = _run(
            """
            import cdk.sql

            # The control-plane functions must be reachable.
            for name in (
                "list_schemas", "list_tables", "list_columns",
                "create_table", "build_create_table_sql",
                "get_dialect", "SqlDialect", "SUPPORTED_DIALECTS",
                "fetch_rows", "execute_ddl",
            ):
                assert hasattr(cdk.sql, name), name

            print("OK")
            """
        )
        assert result.returncode == 0, result.stderr
        assert "OK" in result.stdout

    def test_type_map_string_surface_imports_without_arrow(self):
        """``cdk.type_map`` string surface loads with pyarrow blocked."""
        result = _run(
            """
            import cdk.type_map as tm

            assert tm.TypeMapper is not None
            assert tm.parse_rules is not None
            assert tm.normalize_native_type is not None
            assert tm.UnmappedTypeError is not None

            print("OK")
            """
        )
        assert result.returncode == 0, result.stderr
        assert "OK" in result.stdout

    def test_connection_runtime_imports_without_aiohttp(self):
        """``ConnectionRuntime`` + ``materialize_runtime`` load with aiohttp blocked."""
        result = _run(
            """
            from cdk.connection_runtime import (
                ConnectionRuntime, materialize_runtime,
            )

            assert ConnectionRuntime is not None
            assert materialize_runtime is not None

            print("OK")
            """
        )
        assert result.returncode == 0, result.stderr
        assert "OK" in result.stdout

    def test_arrow_helpers_are_lazy_and_raise_when_blocked(self):
        """Touching the Arrow accessors raises only on access, not at import."""
        result = _run(
            """
            import cdk.sql
            import cdk.type_map as tm

            # Importing succeeded (asserted by reaching here). Now the lazy
            # Arrow accessors must raise because pyarrow is blocked.
            for owner, attr in (
                (cdk.sql, "AdbcReader"),
                (cdk.sql, "open_adbc_reader"),
                (tm, "parse_arrow_type"),
                (tm, "resolve_arrow_type"),
            ):
                try:
                    getattr(owner, attr)
                except ModuleNotFoundError:
                    pass
                else:
                    raise AssertionError(f"{attr} did not raise with pyarrow blocked")

            print("OK")
            """
        )
        assert result.returncode == 0, result.stderr
        assert "OK" in result.stdout

    def test_unknown_lazy_attr_raises_attribute_error(self):
        """The PEP 562 hooks still raise AttributeError for unknown names."""
        result = _run(
            """
            import cdk.sql
            import cdk.type_map as tm

            for owner in (cdk.sql, tm):
                try:
                    owner.does_not_exist
                except AttributeError:
                    pass
                else:
                    raise AssertionError("expected AttributeError")

            print("OK")
            """
        )
        assert result.returncode == 0, result.stderr
        assert "OK" in result.stdout
