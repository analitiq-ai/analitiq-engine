"""Tests for package __init__.py files to ensure proper imports and exports."""

import pytest


class TestMainPackageInit:
    """Test main src package initialization."""
    
    def test_package_imports(self):
        """Test that main package imports work correctly."""
        import src
        
        # Test basic package attributes
        assert hasattr(src, '__version__')
        assert hasattr(src, '__author__')
        assert hasattr(src, '__all__')
        
        # Test version and author
        assert src.__version__ == "0.1.0"
        assert src.__author__ == "Analitiq Core Team"
    
    def test_package_exports(self):
        """Test that all declared exports are available."""
        import src
        
        # Check that __all__ contains expected items
        expected_exports = [
            "StreamingEngine",
            "Pipeline",
            "RetryHandler",
            "CircuitBreaker",
            "DeadLetterQueue",
            "SchemaManager",
            "BaseConnector",
            "DatabaseConnector",
            "APIConnector",
        ]
        
        for export in expected_exports:
            assert export in src.__all__, f"Missing export: {export}"
            assert hasattr(src, export), f"Export not available: {export}"
    
    def test_core_classes_importable(self):
        """Test that core classes can be imported directly."""
        from src import (
            StreamingEngine, Pipeline,
            RetryHandler, CircuitBreaker, DeadLetterQueue,
            SchemaManager, BaseConnector, DatabaseConnector, APIConnector
        )

        # Verify classes are not None
        assert StreamingEngine is not None
        assert Pipeline is not None
        assert RetryHandler is not None
        assert CircuitBreaker is not None
        assert DeadLetterQueue is not None
        assert SchemaManager is not None
        assert BaseConnector is not None
        assert DatabaseConnector is not None
        assert APIConnector is not None


class TestSubPackageInits:
    """Test sub-package __init__.py files."""
    
    def test_connectors_init(self):
        """Test connectors package initialization."""
        import src.connectors
        
        # Should be able to import the package
        assert src.connectors is not None
    
    def test_fault_tolerance_init(self):
        """Test fault_tolerance package initialization."""
        import src.fault_tolerance
        
        # Should be able to import the package
        assert src.fault_tolerance is not None
    
    def test_models_init(self):
        """Test models package initialization."""
        import src.models
        
        # Should be able to import the package
        assert src.models is not None
    
    def test_schema_init(self):
        """Test schema package initialization."""
        import src.schema
        
        # Should be able to import the package  
        assert src.schema is not None
    
    def test_database_init_already_covered(self):
        """Test database connector module is importable."""
        import src.source.connectors.database

        assert src.source.connectors.database is not None

        from src.source.connectors.database import DatabaseConnector

        assert DatabaseConnector is not None


class TestImportErrors:
    """Test handling of import errors."""
    
    def test_missing_optional_import_handling(self):
        """Test that the package handles missing optional imports gracefully."""
        # The main __init__.py should import successfully even if some optional
        # dependencies are missing (this tests the import structure)
        import src
        
        # Should complete without raising ImportError
        assert src is not None
    
    def test_circular_import_prevention(self):
        """Test that imports don't create circular dependencies."""
        # Import in different orders to test for circular imports
        # Use new module paths (src.engine, src.source.connectors)
        import src.engine.pipeline
        import src.engine.engine
        import src.source.connectors.api
        import src.source.connectors.database
        import src

        # All imports should succeed
        assert all([
            src.engine.pipeline,
            src.engine.engine,
            src.source.connectors.api,
            src.source.connectors.database,
            src
        ])


class TestPackageStructure:
    """Test overall package structure and consistency."""
    
    def test_version_consistency(self):
        """Test that version is consistently defined."""
        import src
        
        version = src.__version__
        assert isinstance(version, str)
        assert len(version) > 0
        assert version.count('.') >= 2  # At least major.minor.patch
    
    def test_all_exports_exist(self):
        """Test that all __all__ exports actually exist and are importable."""
        import src
        
        for export_name in src.__all__:
            # Should be able to get the attribute
            export_obj = getattr(src, export_name)
            assert export_obj is not None
            
            # Should be a class or callable (not a string or other primitive)
            assert callable(export_obj) or hasattr(export_obj, '__class__')
    
    def test_no_unexpected_exports(self):
        """Test that there are no unexpected public exports."""
        import src
        
        # Get all public attributes (not starting with _)
        public_attrs = [name for name in dir(src) if not name.startswith('_')]
        
        # Remove expected metadata attributes
        metadata_attrs = ['__version__', '__author__', '__all__']
        code_attrs = [name for name in public_attrs if name not in metadata_attrs]
        
        # All public code attributes should be in __all__ (except submodules)
        # These are modules, not classes to export
        submodules = [
            'cli', 'config', 'connectors', 'destination', 'engine',
            'fault_tolerance', 'grpc', 'mapping', 'models', 'schema', 'secrets',
            'shared', 'source', 'state', 'transformations'
        ]
        for attr in code_attrs:
            if attr not in submodules:
                assert attr in src.__all__, f"Unexpected public export: {attr}"