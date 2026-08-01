"""Unit tests for subpackage __init__ files."""

import pytest


class TestEnginePackageInit:
    """Test engine package __init__ file."""

    @pytest.mark.unit
    def test_engine_imports(self):
        """Test that engine package exports are available."""
        from src import engine

        # Check __all__ exports if defined
        if hasattr(engine, "__all__"):
            for export in engine.__all__:
                assert hasattr(engine, export)

    @pytest.mark.unit
    def test_engine_direct_imports(self):
        """Test direct imports from engine package."""
        from src.engine.engine import StreamingEngine

        assert StreamingEngine is not None


class TestStatePackageInit:
    """Test state package __init__ file."""

    @pytest.mark.unit
    def test_state_imports(self):
        """Test that state package can be imported."""
        import src.state as state

        # Check if __all__ is defined
        if hasattr(state, "__all__"):
            for export in state.__all__:
                assert hasattr(state, export)

    @pytest.mark.unit
    def test_state_modules(self):
        """Test that state modules are accessible."""
        from src.state import dead_letter_queue, state_manager

        assert dead_letter_queue is not None
        assert state_manager is not None


class TestDestinationConnectorsPackageInit:
    """Test destination/connectors package __init__ file."""

    @pytest.mark.unit
    def test_destination_connectors_imports(self):
        """Test that destination connectors package can be imported."""
        import src.destination.connectors as connectors

        # Check if __all__ is defined
        if hasattr(connectors, "__all__"):
            for export in connectors.__all__:
                assert hasattr(connectors, export)

    @pytest.mark.unit
    def test_worker_destination_registry_resolves_builtin_kinds(self):
        """The worker's destination registry serves every builtin kind."""
        from cdk.api import GenericAPIConnector
        from cdk.registry import ConnectorNotRegisteredError
        from src.destination.connectors import GenericSQLConnector
        from src.destination.connectors.file import FileDestinationHandler
        from src.destination.connectors.stream import StreamDestinationHandler
        from src.worker import build_worker_registries

        _, registry = build_worker_registries()

        # The unified SQL connector is the generic fallback for the
        # ``database`` kind (two-step resolution: connector_id first).
        assert registry.resolve("database", "anydb") is GenericSQLConnector
        assert isinstance(registry.create("database", "anydb"), GenericSQLConnector)
        assert registry.resolve("api", "anyapi") is GenericAPIConnector
        assert isinstance(registry.create("stdout", "stdout"), StreamDestinationHandler)
        # file and s3 share the file handler.
        assert isinstance(registry.create("file", "csvbox"), FileDestinationHandler)
        assert isinstance(registry.create("s3", "mybucket"), FileDestinationHandler)

        with pytest.raises(ConnectorNotRegisteredError):
            registry.create("redis", "redis")

    @pytest.mark.unit
    def test_worker_source_registry_resolves_builtin_kinds(self):
        """The worker's source registry serves the builtin source kinds."""
        from cdk.api import GenericAPIConnector
        from cdk.registry import ConnectorNotRegisteredError
        from src.destination.connectors import GenericSQLConnector
        from src.worker import build_worker_registries

        registry, _ = build_worker_registries()

        assert registry.resolve("database", "anydb") is GenericSQLConnector
        assert registry.resolve("api", "anyapi") is GenericAPIConnector

        with pytest.raises(ConnectorNotRegisteredError):
            registry.resolve("file", "csvbox")

    @pytest.mark.unit
    def test_both_registries_serve_one_class_for_a_unified_kind(self):
        """A kind whose connector serves both roles seeds one class, not two.

        The api family shipped as a class per direction and drifted on
        questions that belong to HTTP rather than to a direction; asserting
        object identity here is what keeps a role-specific answer from
        reappearing (issue #431).
        """
        from cdk.api import GenericAPIConnector
        from cdk.sql.generic import GenericSQLConnector
        from src.worker import build_worker_registries

        source, destination = build_worker_registries()

        for kind, expected in (
            ("database", GenericSQLConnector),
            ("api", GenericAPIConnector),
        ):
            assert source.resolve(kind, "any") is expected
            assert destination.resolve(kind, "any") is expected


class TestSharedPackageInit:
    """Test shared package __init__ file."""

    @pytest.mark.unit
    def test_shared_imports(self):
        """Test that shared package exports are available."""
        from src import shared

        # Check __all__ exports if defined
        if hasattr(shared, "__all__"):
            for export in shared.__all__:
                assert hasattr(shared, export)

    @pytest.mark.unit
    def test_shared_utilities(self):
        """Direct imports from the shared package match its current __all__.

        The connector plumbing (``ConnectionRuntime``, ``RateLimiter`` and the
        SQL/identifier helpers) moved into the ``cdk`` package when the CDK was
        carved out (ADR §4); what remains in ``src.shared`` is the engine-only
        run-id lifecycle.
        """
        from src.shared import get_or_generate_run_id, get_run_id, initialize_run_id

        for obj in (
            get_run_id,
            get_or_generate_run_id,
            initialize_run_id,
        ):
            assert obj is not None


class TestModelsPackageInit:
    """Test models package __init__ file."""

    @pytest.mark.unit
    def test_models_imports(self):
        """Test that models package can be imported."""
        import src.models as models

        # Package should be importable
        assert models is not None
