"""Tests for :mod:`cdk.transport_factory`.

These cover transport selection + spec resolution
(:func:`resolve_transport_spec`, which now dispatches to the per-kind
resolve phase and returns the JSON-safe worker payload) and the kind
registry (register / unregister / dispatch via :func:`build_transport`,
where each kind is a resolve/build pair). The live engine/session probes
that require a database / HTTP endpoint are not exercised here.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from cdk.resolver import ResolutionContext
from cdk.transport_factory import (
    build_transport,
    register_transport_kind,
    registered_transport_kinds,
    resolve_transport_spec,
    unregister_transport_kind,
)

# ---------------------------------------------------------------------------
# resolve_transport_spec — selection + per-kind resolution
# ---------------------------------------------------------------------------


class TestResolveTransportSpec:
    def test_default_transport_used_when_ref_not_given(self):
        connector = {
            "connector_id": "demo",
            "default_transport": "api",
            "transports": {
                "api": {
                    "transport_type": "http",
                    "base_url": "https://api.example.com",
                }
            },
        }
        ctx = ResolutionContext(connector=connector)
        spec = resolve_transport_spec(connector, context=ctx)
        assert spec["transport_type"] == "http"
        assert spec["base_url"] == "https://api.example.com"

    def test_transport_defaults_merged_into_named_transport(self):
        connector = {
            "connector_id": "demo",
            "default_transport": "api",
            "transport_defaults": {
                "transport_type": "http",
                "headers": {"Accept": "application/json"},
            },
            "transports": {
                "api": {
                    "base_url": "https://api.example.com",
                    "headers": {"Authorization": "Bearer x"},
                }
            },
        }
        ctx = ResolutionContext(connector=connector)
        spec = resolve_transport_spec(connector, context=ctx)
        assert spec["transport_type"] == "http"
        assert spec["headers"] == {
            "Accept": "application/json",
            "Authorization": "Bearer x",
        }

    def test_transport_specific_value_overrides_default(self):
        connector = {
            "connector_id": "demo",
            "default_transport": "api",
            "transport_defaults": {
                "headers": {"Authorization": "Bearer default"},
            },
            "transports": {
                "api": {
                    "transport_type": "http",
                    "base_url": "https://api.example.com",
                    "headers": {"Authorization": "Basic specific"},
                }
            },
        }
        ctx = ResolutionContext(connector=connector)
        spec = resolve_transport_spec(connector, context=ctx)
        assert spec["headers"]["Authorization"] == "Basic specific"

    def test_unknown_transport_ref_rejected(self):
        connector = {
            "connector_id": "demo",
            "default_transport": "api",
            "transports": {
                "api": {"transport_type": "http", "base_url": "https://x"}
            },
        }
        ctx = ResolutionContext(connector=connector)
        with pytest.raises(KeyError, match="not in declared transports"):
            resolve_transport_spec(connector, transport_ref="other", context=ctx)

    def test_no_transports_block_rejected(self):
        ctx = ResolutionContext()
        with pytest.raises(ValueError, match="has no `transports` block"):
            resolve_transport_spec({"connector_id": "demo"}, context=ctx)

    def test_no_default_transport_rejected(self):
        connector = {
            "connector_id": "demo",
            "transports": {
                "api": {"transport_type": "http", "base_url": "https://x"}
            },
        }
        ctx = ResolutionContext(connector=connector)
        with pytest.raises(ValueError, match="default_transport not declared"):
            resolve_transport_spec(connector, context=ctx)

    def test_missing_transport_type_rejected(self):
        connector = {
            "connector_id": "demo",
            "default_transport": "api",
            "transports": {"api": {"base_url": "https://x"}},
        }
        ctx = ResolutionContext(connector=connector)
        with pytest.raises(ValueError, match="transport_type"):
            resolve_transport_spec(connector, context=ctx)


# Transport kind registry (register / build / unregister lifecycle)
# ---------------------------------------------------------------------------


class TestTransportKindRegistry:
    @pytest.fixture(autouse=True)
    def _restore_registry(self):
        # The registry is module-level; a test that fails mid-flight could
        # leave state behind and corrupt sibling tests. Snapshot before,
        # restore after — irrespective of what the test body did.
        from cdk.transport_factory import _TRANSPORT_KINDS

        snapshot = dict(_TRANSPORT_KINDS)
        try:
            yield
        finally:
            _TRANSPORT_KINDS.clear()
            _TRANSPORT_KINDS.update(snapshot)

    def test_built_in_kinds_registered_at_import_time(self):
        kinds = registered_transport_kinds()
        assert "sqlalchemy" in kinds
        assert "adbc" in kinds
        assert "http" in kinds

    def test_register_rejects_empty_kind(self):
        with pytest.raises(ValueError, match="non-empty string"):
            register_transport_kind(
                "", resolve_spec=MagicMock(), build_from_spec=AsyncMock()
            )

    def test_register_rejects_non_string_kind(self):
        with pytest.raises(ValueError, match="non-empty string"):
            register_transport_kind(
                42, resolve_spec=MagicMock(), build_from_spec=AsyncMock()  # type: ignore[arg-type]
            )

    def test_register_rejects_non_callable_phases(self):
        # Non-callable phases fail loudly at registration so the bug is
        # near the registration site, not deep in build_transport.
        with pytest.raises(TypeError, match="callable"):
            register_transport_kind(
                "test_bad", resolve_spec=None, build_from_spec=AsyncMock()  # type: ignore[arg-type]
            )
        with pytest.raises(TypeError, match="callable"):
            register_transport_kind(
                "test_bad", resolve_spec=MagicMock(), build_from_spec="nope"  # type: ignore[arg-type]
            )

    def test_re_registering_existing_kind_rejected(self):
        # Use a throwaway kind rather than "http" — if the assertion regex
        # ever stops matching, registering against a built-in would silently
        # overwrite it for the rest of the suite.
        register_transport_kind(
            "test_dup_kind", resolve_spec=MagicMock(), build_from_spec=AsyncMock()
        )
        with pytest.raises(ValueError, match="already registered"):
            register_transport_kind(
                "test_dup_kind",
                resolve_spec=MagicMock(),
                build_from_spec=AsyncMock(),
            )

    def test_unregister_unknown_kind_raises(self):
        with pytest.raises(KeyError, match="not registered"):
            unregister_transport_kind("nope-not-a-kind")

    @pytest.mark.asyncio
    async def test_register_build_unregister_cycle(self):
        # Full lifecycle: a plugin-style registration teaches the engine a
        # new kind; build_transport runs its resolve phase then its build
        # phase; unregister cleans up; afterwards the kind is rejected.
        sentinel = object()
        resolve_phase = MagicMock(
            return_value={"transport_type": "test_kind", "marker": "value"}
        )
        build_phase = AsyncMock(return_value=sentinel)
        register_transport_kind(
            "test_kind", resolve_spec=resolve_phase, build_from_spec=build_phase
        )
        try:
            assert "test_kind" in registered_transport_kinds()

            connector = {
                "connector_id": "demo",
                "default_transport": "api",
                "transports": {
                    "api": {"transport_type": "test_kind", "marker": "value"}
                },
            }
            ctx = ResolutionContext(connector=connector)
            result = await build_transport(connector, context=ctx)
            assert result is sentinel
            resolve_phase.assert_called_once()
            build_phase.assert_awaited_once()
            (resolved,) = build_phase.await_args.args
            assert resolved["transport_type"] == "test_kind"
            assert resolved["marker"] == "value"
        finally:
            unregister_transport_kind("test_kind")

        assert "test_kind" not in registered_transport_kinds()
        connector = {
            "connector_id": "demo",
            "default_transport": "api",
            "transports": {"api": {"transport_type": "test_kind"}},
        }
        ctx = ResolutionContext(connector=connector)
        with pytest.raises(NotImplementedError, match="Unsupported transport_type"):
            await build_transport(connector, context=ctx)
