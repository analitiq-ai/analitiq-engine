"""ConnectorRegistry: two-level (kind -> connector_id) resolution + discovery."""

from __future__ import annotations

import pytest

import cdk.registry as reg
from cdk.contract import Readable, Writable
from cdk.registry import (
    KIND_DEFAULTS,
    ConnectorNotRegisteredError,
    ConnectorRegistry,
    KindDefault,
    build_registries,
    load_kind_default,
)
from cdk.sql.generic import GenericSQLConnector


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


#: A one-kind stand-in for KIND_DEFAULTS. The class path has to be genuinely
#: importable -- ``tests/`` is not a package, so a test double cannot be named
#: by dotted path -- and the SQL connector is the cheapest real one to import.
_SQL_ONLY = {
    "database": KindDefault(
        "cdk.sql.generic:GenericSQLConnector", "arrow", ("pyarrow",)
    )
}


class TestBuildRegistries:
    def test_kind_defaults_are_seeded_and_discovery_adds_specifics(self, monkeypatch):
        monkeypatch.setattr(reg, "KIND_DEFAULTS", _SQL_ONLY)
        _patch_entry_points(
            monkeypatch,
            {
                reg.SOURCE_GROUP: [FakeEntryPoint("postgres", lambda: _Postgres)],
                reg.DESTINATION_GROUP: [FakeEntryPoint("mysql", lambda: _Mysql)],
            },
        )
        source, destination = build_registries()
        assert source.role == "source" and destination.role == "destination"
        assert source.resolve("database", "postgres") is _Postgres  # discovered
        assert source.resolve("database", "sqlite") is GenericSQLConnector  # fallback
        assert destination.resolve("database", "mysql") is _Mysql
        assert destination.resolve("database", "anything") is GenericSQLConnector

    def test_discover_disabled(self, monkeypatch):
        monkeypatch.setattr(reg, "KIND_DEFAULTS", _SQL_ONLY)
        _patch_entry_points(
            monkeypatch,
            {reg.SOURCE_GROUP: [FakeEntryPoint("postgres", lambda: _Postgres)]},
        )
        source, _ = build_registries(discover=False)
        assert source.connector_ids() == []  # entry point NOT pulled in
        assert source.resolve("database", "postgres") is GenericSQLConnector

    def test_a_default_serving_neither_role_is_refused(self, monkeypatch):
        # A class that implements neither Protocol would land in no registry
        # at all, so every connection of that kind would fail far from the
        # cause. Refuse at build time, naming the class.
        monkeypatch.setattr(
            reg,
            "KIND_DEFAULTS",
            {
                "database": KindDefault(
                    "cdk.registry:KindDefault", "arrow", ("pyarrow",)
                )
            },
        )
        with pytest.raises(reg.ConnectorClassError, match="neither Readable nor"):
            build_registries(discover=False)


class TestKindDefaults:
    """The table is the single authority for what serves a kind."""

    def test_every_kind_default_serves_the_roles_its_class_implements(self):
        """Registry membership is derived, never declared twice.

        Looping the table rather than naming kinds means a kind added later
        is covered the day it lands, and a class that silently grows a
        ``read_batches`` stub moves registry on the next run instead of
        resolving a source that cannot read.
        """
        source, destination = build_registries(discover=False)

        assert set(KIND_DEFAULTS) == {"database", "api", "file", "s3", "stdout"}
        for kind in KIND_DEFAULTS:
            cls = load_kind_default(kind)
            if issubclass(cls, Readable):
                assert source.resolve(kind, "any") is cls
            else:
                with pytest.raises(ConnectorNotRegisteredError):
                    source.resolve(kind, "any")
            if issubclass(cls, Writable):
                assert destination.resolve(kind, "any") is cls
            else:
                with pytest.raises(ConnectorNotRegisteredError):
                    destination.resolve(kind, "any")

    def test_a_kind_serving_both_roles_seeds_one_class_not_two(self):
        """The api family shipped a class per direction and drifted on
        questions that belong to HTTP rather than to a direction; asserting
        object identity is what keeps a role-specific answer from
        reappearing (issue #431)."""
        source, destination = build_registries(discover=False)

        for kind in ("database", "api"):
            assert source.resolve(kind, "any") is destination.resolve(kind, "any")

    def test_a_write_only_kind_has_no_source_default(self):
        """``file`` / ``s3`` / ``stdout`` write only, so a ``kind: file``
        *source* must fail loud rather than resolve a class with no read
        path -- the one silent regression a derived table could introduce."""
        source, _ = build_registries(discover=False)

        for kind in ("file", "s3", "stdout"):
            with pytest.raises(ConnectorNotRegisteredError):
                source.resolve(kind, "anything")

    def test_an_unknown_kind_is_not_registered_in_either_registry(self):
        source, destination = build_registries(discover=False)

        for registry in (source, destination):
            with pytest.raises(ConnectorNotRegisteredError):
                registry.create("redis", "redis")

    def test_load_kind_default_rejects_an_unknown_kind(self):
        with pytest.raises(reg.UnknownConnectorKindError) as exc:
            load_kind_default("redis")
        message = str(exc.value)
        assert "redis" in message
        for kind in KIND_DEFAULTS:
            assert kind in message


class TestLoadClass:
    def test_a_reference_without_a_colon_is_refused(self):
        with pytest.raises(reg.ConnectorClassError, match="package.module:ClassName"):
            reg.load_class("cdk.sql.generic.GenericSQLConnector")

    def test_a_missing_attribute_names_the_module_and_the_attribute(self):
        with pytest.raises(reg.ConnectorClassError, match="has no attribute"):
            reg.load_class("cdk.sql.generic:NoSuchConnector")

    def test_a_reference_to_a_non_class_is_refused(self):
        with pytest.raises(reg.ConnectorClassError, match="not a class"):
            reg.load_class("cdk.registry:SOURCE_GROUP")

    def test_a_missing_module_propagates_the_import_error(self):
        # Only the caller knows whether this means "install an extra" or
        # "your reference is wrong", so load_class does not decide.
        with pytest.raises(ImportError):
            reg.load_class("cdk.no_such_module:Whatever")
