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
