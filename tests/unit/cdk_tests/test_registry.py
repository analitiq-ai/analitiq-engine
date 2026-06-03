"""ConnectorRegistry: register / lookup + best-effort entry-point discovery."""

from __future__ import annotations

import pytest

import cdk.registry as reg
from cdk.registry import (
    ConnectorNotRegisteredError,
    ConnectorRegistry,
    build_registries,
)


class _A:
    pass


class _B:
    pass


class FakeEntryPoint:
    def __init__(self, name, loader):
        self.name = name
        self._loader = loader

    def load(self):
        return self._loader()


def _patch_entry_points(monkeypatch, by_group):
    monkeypatch.setattr(reg, "_entry_points", lambda group: tuple(by_group.get(group, ())))


class TestRegisterAndLookup:
    def test_register_get_create_kinds(self):
        r = ConnectorRegistry("source")
        r.register("database", _A)
        r.register("API", _B)  # case-insensitive key
        assert r.get("database") is _A
        assert r.get("api") is _B
        assert isinstance(r.create("database"), _A)
        assert r.kinds() == ["api", "database"]

    def test_missing_kind_raises_with_role_and_available(self):
        r = ConnectorRegistry("destination")
        r.register("file", _A)
        with pytest.raises(ConnectorNotRegisteredError) as exc:
            r.get("database")
        assert exc.value.kind == "database"
        assert exc.value.role == "destination"
        assert "file" in str(exc.value)

    def test_duplicate_kind_raises_unless_override(self):
        r = ConnectorRegistry("source")
        r.register("database", _A)
        # Same class re-registered is idempotent (not an error).
        r.register("database", _A)
        # Different class is a shadow -> loud failure.
        with pytest.raises(ValueError, match="already registered"):
            r.register("database", _B)
        # Explicit override replaces.
        r.register("database", _B, override=True)
        assert r.get("database") is _B


class TestEntryPointDiscovery:
    def test_discovers_and_registers(self, monkeypatch):
        _patch_entry_points(
            monkeypatch,
            {"grp": [FakeEntryPoint("database", lambda: _A),
                     FakeEntryPoint("api", lambda: _B)]},
        )
        r = ConnectorRegistry("source")
        r.discover_entry_points("grp")
        assert r.get("database") is _A and r.get("api") is _B

    def test_load_failure_is_skipped_not_fatal(self, monkeypatch):
        def boom():
            raise ImportError("missing driver")

        _patch_entry_points(
            monkeypatch,
            {"grp": [FakeEntryPoint("broken", boom),
                     FakeEntryPoint("api", lambda: _B)]},
        )
        r = ConnectorRegistry("source")
        r.discover_entry_points("grp")  # must not raise
        assert r.kinds() == ["api"]  # the good one still registered

    def test_duplicate_entry_point_is_skipped(self, monkeypatch):
        _patch_entry_points(
            monkeypatch, {"grp": [FakeEntryPoint("database", lambda: _B)]}
        )
        r = ConnectorRegistry("source")
        r.register("database", _A)  # builtin wins
        r.discover_entry_points("grp")  # duplicate -> skipped, no raise
        assert r.get("database") is _A


class TestBuildRegistries:
    def test_builtins_and_discovery(self, monkeypatch):
        _patch_entry_points(
            monkeypatch,
            {
                reg.SOURCE_GROUP: [FakeEntryPoint("custom_src", lambda: _B)],
                reg.DESTINATION_GROUP: [FakeEntryPoint("custom_dst", lambda: _B)],
            },
        )
        source, destination = build_registries(
            source_builtins={"database": _A},
            destination_builtins={"database": _A, "file": _A},
        )
        assert source.role == "source" and destination.role == "destination"
        assert source.get("database") is _A
        assert source.get("custom_src") is _B  # discovered
        assert destination.get("file") is _A
        assert destination.get("custom_dst") is _B

    def test_discover_disabled(self, monkeypatch):
        _patch_entry_points(
            monkeypatch, {reg.SOURCE_GROUP: [FakeEntryPoint("x", lambda: _B)]}
        )
        source, _ = build_registries(source_builtins={"database": _A}, discover=False)
        assert source.kinds() == ["database"]  # entry point NOT pulled in
