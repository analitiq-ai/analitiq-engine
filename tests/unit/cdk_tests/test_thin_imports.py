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
                # Set ``name`` like the real import machinery does, so the
                # CDK's missing-extra discriminator can identify the package.
                raise ModuleNotFoundError(f"blocked for test: {name}", name=name)
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
    env["PYTHONPATH"] = f"{_CDK_SRC}{os.pathsep}{existing}" if existing else _CDK_SRC
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

            # The control-plane functions + published error types must be
            # reachable without the arrow extra.
            for name in (
                "list_schemas", "list_tables", "list_columns",
                "create_table", "build_create_table_sql",
                "SqlDialect",
                "fetch_rows", "execute_ddl",
                "SqlIntrospectionError", "UnsupportedDialectOperationError",
                "DiscoveryError", "CreateTableError", "ReadError",
            ):
                assert hasattr(cdk.sql, name), name

            # The direct ``from cdk.sql import create_table`` form (the one the
            # control-plane actually uses) must resolve, not just attr access.
            from cdk.sql import create_table, list_columns  # noqa: F401

            # _adbc_utils is on the eager import path via execution.py; any
            # pyarrow import added there would break this assertion.
            from cdk.sql._adbc_utils import _adbc_execute  # noqa: F401
            assert callable(_adbc_execute)

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

    def test_arrow_helpers_are_lazy_and_name_the_extra_when_blocked(self):
        """Touching an Arrow accessor raises only on access, and the error
        names the ``arrow`` extra to install."""
        result = _run(
            """
            import cdk.sql
            import cdk.type_map as tm
            from cdk._extras import MissingExtraError

            # Importing succeeded (asserted by reaching here). Now the lazy
            # Arrow accessors must raise because pyarrow is blocked, with an
            # actionable message pointing at analitiq-cdk[arrow].
            for owner, attr in (
                (cdk.sql, "AdbcReader"),
                (cdk.sql, "open_adbc_reader"),
                (tm, "parse_arrow_type"),
                (tm, "resolve_arrow_type"),
            ):
                try:
                    getattr(owner, attr)
                except MissingExtraError as exc:
                    assert "analitiq-cdk[arrow]" in str(exc), str(exc)
                    assert "pyarrow" in str(exc), str(exc)
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


@pytest.mark.unit
class TestExtrasPresentSurface:
    """With the extras installed (the test venv has pyarrow/aiohttp), the lazy
    re-exports must resolve via the ``from cdk.sql import X`` form the engine
    uses -- this exercises the PEP-562 success path, which the blocked tests
    cannot."""

    def test_arrow_reexports_resolve_when_pyarrow_present(self):
        from cdk.sql import AdbcReader, open_adbc_reader
        from cdk.type_map import parse_arrow_type, resolve_arrow_type

        assert AdbcReader is not None
        assert open_adbc_reader is not None
        assert parse_arrow_type is not None
        assert resolve_arrow_type is not None

    def test_arrow_reexport_is_the_real_object(self):
        from cdk import sql
        from cdk.sql import adbc_reader as adbc_reader_mod

        # The lazy accessor must return the genuine submodule attribute.
        assert sql.AdbcReader is adbc_reader_mod.AdbcReader


@pytest.mark.unit
class TestReraiseForMissingExtra:
    """The discriminator re-labels only the extra's own missing package; any
    unrelated ImportError is re-raised untouched so a real bug is not masked."""

    def test_missing_extra_package_is_relabelled(self):
        from cdk._extras import MissingExtraError, reraise_for_missing_extra

        original = ModuleNotFoundError("No module named 'pyarrow'", name="pyarrow")
        with pytest.raises(MissingExtraError) as ei:
            reraise_for_missing_extra(
                original,
                feature="cdk.sql.AdbcReader",
                extra="arrow",
                modules=("pyarrow",),
            )
        msg = str(ei.value)
        assert "analitiq-cdk[arrow]" in msg
        assert "cdk.sql.AdbcReader" in msg
        assert ei.value.__cause__ is original

    def test_unrelated_import_error_is_reraised_unchanged(self):
        from cdk._extras import MissingExtraError, reraise_for_missing_extra

        # pyarrow imported fine, but a transitive dep of it did not.
        original = ModuleNotFoundError(
            "No module named 'some_transitive_dep'",
            name="some_transitive_dep",
        )
        with pytest.raises(ModuleNotFoundError) as ei:
            reraise_for_missing_extra(
                original,
                feature="cdk.sql.AdbcReader",
                extra="arrow",
                modules=("pyarrow",),
            )
        # Same object, NOT re-wrapped as MissingExtraError.
        assert ei.value is original
        assert not isinstance(ei.value, MissingExtraError)

    def test_broken_install_submodule_failure_is_not_relabelled(self):
        """A *present but broken* extra (e.g. partial PyArrow build failing on
        ``pyarrow.lib``) must surface its real cause, not be mislabelled as a
        missing extra -- the package IS installed."""
        from cdk._extras import MissingExtraError, reraise_for_missing_extra

        original = ModuleNotFoundError(
            "No module named 'pyarrow.lib'",
            name="pyarrow.lib",
        )
        with pytest.raises(ModuleNotFoundError) as ei:
            reraise_for_missing_extra(
                original,
                feature="cdk.sql.AdbcReader",
                extra="arrow",
                modules=("pyarrow",),
            )
        assert ei.value is original
        assert not isinstance(ei.value, MissingExtraError)


@pytest.mark.unit
@pytest.mark.asyncio
class TestHttpTransportLazyAiohttp:
    """The lazy ``import aiohttp`` inside build_http_transport: it must run
    (success) with aiohttp present and name the ``api`` extra when absent."""

    async def test_build_http_transport_succeeds_with_aiohttp(self):
        import aiohttp

        from cdk.transport_factory import HttpTransport, build_http_from_spec

        transport = await build_http_from_spec(
            {
                "transport_type": "http",
                "base_url": "https://example.test",
                "headers": {},
                "timeout_seconds": 30.0,
                "rate_limit": None,
            }
        )
        try:
            assert isinstance(transport, HttpTransport)
            assert isinstance(transport.session, aiohttp.ClientSession)
            assert transport.base_url == "https://example.test"
        finally:
            await transport.session.close()

    async def test_build_http_transport_names_api_extra_when_aiohttp_absent(
        self,
        monkeypatch,
    ):
        from cdk._extras import MissingExtraError
        from cdk.transport_factory import build_http_from_spec

        # ``sys.modules[name] = None`` makes ``import name`` raise ImportError
        # with ``name`` set -- the same shape as a genuinely-absent package.
        monkeypatch.setitem(sys.modules, "aiohttp", None)

        with pytest.raises(MissingExtraError) as ei:
            await build_http_from_spec(
                {
                    "transport_type": "http",
                    "base_url": "https://example.test",
                    "headers": {},
                    "timeout_seconds": 30.0,
                    "rate_limit": None,
                }
            )
        assert "analitiq-cdk[api]" in str(ei.value)
