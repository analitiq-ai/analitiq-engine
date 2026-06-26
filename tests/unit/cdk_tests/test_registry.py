"""ConnectorRegistry: two-level (kind -> connector_id) resolution + discovery."""

from __future__ import annotations

import pytest

import cdk.registry as reg
from cdk.registry import (
    ConnectorNotRegisteredError,
    ConnectorRegistry,
    build_registries,
)


class _Generic:
    pass


class _Postgres:
    pass


class _Mysql:
    pass


class FakeEntryPoint:
    def __init__(self, name, loader):
        self.name = name
        self._loader = loader

    def load(self):
        return self._loader()


def _patch_entry_points(monkeypatch, by_group):
    monkeypatch.setattr(
        reg, "_entry_points", lambda group: tuple(by_group.get(group, ()))
    )


class TestResolution:
    def test_specific_class_wins_over_kind_default(self):
        r = ConnectorRegistry("source")
        r.register_default("database", _Generic)
        r.register("postgres", _Postgres)
        assert r.resolve("database", "postgres") is _Postgres
        assert isinstance(r.create("database", "postgres"), _Postgres)

    def test_unregistered_connector_id_falls_back_to_kind_default(self):
        r = ConnectorRegistry("source")
        r.register_default("database", _Generic)
        # sqlite ships no class -> the thin path serves it.
        assert r.resolve("database", "sqlite") is _Generic
        assert isinstance(r.create("database", "sqlite"), _Generic)

    def test_keys_are_case_insensitive(self):
        r = ConnectorRegistry("source")
        r.register_default("DATABASE", _Generic)
        r.register("Postgres", _Postgres)
        assert r.resolve("database", "postgres") is _Postgres
        assert r.resolve("Database", "MYSQL") is _Generic

    def test_both_lookups_missing_raises_with_context(self):
        r = ConnectorRegistry("destination")
        r.register_default("file", _Generic)
        r.register("xero", _Postgres)
        with pytest.raises(ConnectorNotRegisteredError) as exc:
            r.resolve("database", "postgres")
        assert exc.value.kind == "database"
        assert exc.value.connector_id == "postgres"
        assert exc.value.role == "destination"
        assert "xero" in str(exc.value)
        assert "file" in str(exc.value)

    def test_kinds_and_connector_ids_listings(self):
        r = ConnectorRegistry("source")
        r.register_default("database", _Generic)
        r.register_default("api", _Generic)
        r.register("postgres", _Postgres)
        r.register("mysql", _Mysql)
        assert r.kinds() == ["api", "database"]
        assert r.connector_ids() == ["mysql", "postgres"]


class TestRegistrationShadowing:
    def test_duplicate_kind_default_raises_unless_override(self):
        r = ConnectorRegistry("source")
        r.register_default("database", _Generic)
        # Same class re-registered is idempotent (not an error).
        r.register_default("database", _Generic)
        with pytest.raises(ValueError, match="already registered"):
            r.register_default("database", _Postgres)
        r.register_default("database", _Postgres, override=True)
        assert r.resolve("database", "anything") is _Postgres

    def test_duplicate_connector_id_raises_unless_override(self):
        r = ConnectorRegistry("source")
        r.register("postgres", _Postgres)
        r.register("postgres", _Postgres)  # idempotent
        with pytest.raises(ValueError, match="already registered"):
            r.register("postgres", _Mysql)
        r.register("postgres", _Mysql, override=True)
        assert r.resolve("database", "postgres") is _Mysql

    def test_connector_id_namespace_is_separate_from_kinds(self):
        # A connector_id that happens to equal a kind name must not collide
        # with the kind-default namespace.
        r = ConnectorRegistry("source")
        r.register_default("database", _Generic)
        r.register("database", _Postgres)  # weird but legal connector_id
        assert r.resolve("database", "database") is _Postgres
        assert r.resolve("database", "other") is _Generic


class TestEntryPointDiscovery:
    def test_discovers_and_registers_by_connector_id(self, monkeypatch):
        _patch_entry_points(
            monkeypatch,
            {
                "grp": [
                    FakeEntryPoint("postgres", lambda: _Postgres),
                    FakeEntryPoint("mysql", lambda: _Mysql),
                ]
            },
        )
        r = ConnectorRegistry("source")
        r.register_default("database", _Generic)
        r.discover_entry_points("grp")
        assert r.resolve("database", "postgres") is _Postgres
        assert r.resolve("database", "mysql") is _Mysql
        assert r.resolve("database", "sqlite") is _Generic  # fallback intact

    def test_load_failure_is_skipped_not_fatal(self, monkeypatch):
        def boom():
            raise ImportError("missing driver")

        _patch_entry_points(
            monkeypatch,
            {
                "grp": [
                    FakeEntryPoint("broken", boom),
                    FakeEntryPoint("mysql", lambda: _Mysql),
                ]
            },
        )
        r = ConnectorRegistry("source")
        r.discover_entry_points("grp")  # must not raise
        assert r.connector_ids() == ["mysql"]  # the good one still registered

    def test_duplicate_entry_point_is_skipped(self, monkeypatch):
        _patch_entry_points(
            monkeypatch, {"grp": [FakeEntryPoint("postgres", lambda: _Mysql)]}
        )
        r = ConnectorRegistry("source")
        r.register("postgres", _Postgres)  # first registration wins
        r.discover_entry_points("grp")  # duplicate -> skipped, no raise
        assert r.resolve("database", "postgres") is _Postgres


class TestBuildRegistries:
    def test_builtins_are_kind_defaults_and_discovery_adds_specifics(self, monkeypatch):
        _patch_entry_points(
            monkeypatch,
            {
                reg.SOURCE_GROUP: [FakeEntryPoint("postgres", lambda: _Postgres)],
                reg.DESTINATION_GROUP: [FakeEntryPoint("mysql", lambda: _Mysql)],
            },
        )
        source, destination = build_registries(
            source_builtins={"database": _Generic},
            destination_builtins={"database": _Generic, "file": _Generic},
        )
        assert source.role == "source" and destination.role == "destination"
        assert source.resolve("database", "postgres") is _Postgres  # discovered
        assert source.resolve("database", "sqlite") is _Generic  # fallback
        assert destination.resolve("database", "mysql") is _Mysql
        assert destination.resolve("file", "anything") is _Generic

    def test_discover_disabled(self, monkeypatch):
        _patch_entry_points(
            monkeypatch,
            {reg.SOURCE_GROUP: [FakeEntryPoint("postgres", lambda: _Postgres)]},
        )
        source, _ = build_registries(
            source_builtins={"database": _Generic}, discover=False
        )
        assert source.connector_ids() == []  # entry point NOT pulled in
        assert source.resolve("database", "postgres") is _Generic
