"""Unit tests for subpackage __init__ files."""

import pytest


class TestEnginePackageInit:
    """Test engine package __init__ file."""

    @pytest.mark.unit
    def test_engine_imports(self):
        """Test that engine package exports are available."""
        from src import engine

        # Check __all__ exports if defined
        if hasattr(engine, '__all__'):
            for export in engine.__all__:
                assert hasattr(engine, export)

    @pytest.mark.unit
    def test_engine_direct_imports(self):
        """Test direct imports from engine package."""
        from src.engine.engine import StreamingEngine
        from src.engine.pipeline import Pipeline

        assert StreamingEngine is not None
        assert Pipeline is not None


class TestSourceConnectorsPackageInit:
    """Test source/connectors package __init__ file."""

    @pytest.mark.unit
    def test_source_connectors_imports(self):
        """Test that source connectors package can be imported."""
        import src.source.connectors as connectors

        # Check if __all__ is defined
        if hasattr(connectors, '__all__'):
            for export in connectors.__all__:
                assert hasattr(connectors, export)

    @pytest.mark.unit
    def test_source_connectors_submodules(self):
        """Test that source connector submodules are accessible."""
        from src.source.connectors import api, base

        assert api is not None
        assert base is not None

        # Test that main classes can be imported
        from src.source.connectors.api import APIConnector
        from src.source.connectors.base import BaseConnector

        assert APIConnector is not None
        assert BaseConnector is not None


class TestStatePackageInit:
    """Test state package __init__ file."""

    @pytest.mark.unit
    def test_state_imports(self):
        """Test that state package can be imported."""
        import src.state as state

        # Check if __all__ is defined
        if hasattr(state, '__all__'):
            for export in state.__all__:
                assert hasattr(state, export)

    @pytest.mark.unit
    def test_state_modules(self):
        """Test that state modules are accessible."""
        from src.state import (
            circuit_breaker, dead_letter_queue, retry_handler,
            state_manager
        )

        assert circuit_breaker is not None
        assert dead_letter_queue is not None
        assert retry_handler is not None
        assert state_manager is not None


class TestDestinationConnectorsPackageInit:
    """Test destination/connectors package __init__ file."""

    @pytest.mark.unit
    def test_destination_connectors_imports(self):
        """Test that destination connectors package can be imported."""
        import src.destination.connectors as connectors

        # Check if __all__ is defined
        if hasattr(connectors, '__all__'):
            for export in connectors.__all__:
                assert hasattr(connectors, export)

    @pytest.mark.unit
    def test_destination_handler_registry(self):
        """Test that handler registry is available."""
        from src.destination.connectors import (
            HandlerRegistry, get_handler,
            DatabaseDestinationHandler
        )

        assert HandlerRegistry is not None
        assert get_handler is not None
        assert DatabaseDestinationHandler is not None


class TestSharedPackageInit:
    """Test shared package __init__ file."""

    @pytest.mark.unit
    def test_shared_imports(self):
        """Test that shared package exports are available."""
        from src import shared

        # Check __all__ exports if defined
        if hasattr(shared, '__all__'):
            for export in shared.__all__:
                assert hasattr(shared, export)

    @pytest.mark.unit
    def test_shared_utilities(self):
        """Direct imports from the shared package match its current __all__.

        The legacy ``DIALECT_MAP``, ``SSL_DIALECTS``,
        ``DatabaseConnectionParams``, ``extract_connection_params``, and
        ``canonical_ssl_to_connect_arg`` exports were removed when
        connector-driven transports replaced the hard-coded engine
        factory; what remains are pure SQL helpers and shared lifecycle
        primitives.
        """
        from src.shared import (
            ConnectionRuntime,
            RateLimiter,
            acquire_connection,
            convert_db_to_python,
            convert_record_from_db,
            get_default_clause,
            get_full_table_name,
            validate_sql_identifier,
        )

        for obj in (
            ConnectionRuntime,
            RateLimiter,
            acquire_connection,
            convert_db_to_python,
            convert_record_from_db,
            get_default_clause,
            get_full_table_name,
            validate_sql_identifier,
        ):
            assert obj is not None


class TestTransformationsPackageInit:
    """Test transformations package __init__ file."""

    @pytest.mark.unit
    def test_transformations_imports(self):
        """Test that transformations package can be imported."""
        import src.transformations as transformations

        # Package should be importable
        assert transformations is not None

    @pytest.mark.unit
    def test_transformations_registry_import(self):
        """Test that transformations can be imported from registry."""
        from src.transformations.registry import (
            TransformationRegistry, transformation_registry, TransformationError
        )

        assert TransformationRegistry is not None
        assert transformation_registry is not None
        assert TransformationError is not None
        assert isinstance(transformation_registry, TransformationRegistry)


class TestModelsPackageInit:
    """Test models package __init__ file."""

    @pytest.mark.unit
    def test_models_imports(self):
        """Test that models package can be imported."""
        import src.models as models

        # Package should be importable
        assert models is not None
