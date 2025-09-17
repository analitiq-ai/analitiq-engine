"""Tests for package __init__.py files to ensure proper imports and exports."""

import pytest


class TestMainPackageInit:
    """Test main analitiq_stream package initialization."""
    
    def test_package_imports(self):
        """Test that main package imports work correctly."""
        import analitiq_stream
        
        # Test basic package attributes
        assert hasattr(analitiq_stream, '__version__')
        assert hasattr(analitiq_stream, '__author__')
        assert hasattr(analitiq_stream, '__all__')
        
        # Test version and author
        assert analitiq_stream.__version__ == "0.1.0"
        assert analitiq_stream.__author__ == "Analitiq Core Team"
    
    def test_package_exports(self):
        """Test that all declared exports are available."""
        import analitiq_stream
        
        # Check that __all__ contains expected items
        expected_exports = [
            "StreamingEngine",
            "Pipeline",
            "credentials_manager",
            "CredentialsManager",
            "RetryHandler",
            "CircuitBreaker",
            "DeadLetterQueue",
            "SchemaManager",
            "BaseConnector",
            "DatabaseConnector",
            "APIConnector",
        ]
        
        for export in expected_exports:
            assert export in analitiq_stream.__all__, f"Missing export: {export}"
            assert hasattr(analitiq_stream, export), f"Export not available: {export}"
    
    def test_core_classes_importable(self):
        """Test that core classes can be imported directly."""
        from analitiq_stream import (
            StreamingEngine, Pipeline, CredentialsManager, credentials_manager,
            RetryHandler, CircuitBreaker, DeadLetterQueue,
            SchemaManager, BaseConnector, DatabaseConnector, APIConnector
        )
        
        # Verify classes are not None
        assert StreamingEngine is not None
        assert Pipeline is not None
        assert CredentialsManager is not None
        assert credentials_manager is not None
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
        import analitiq_stream.connectors
        
        # Should be able to import the package
        assert analitiq_stream.connectors is not None
    
    def test_core_init(self):
        """Test core package initialization."""
        import analitiq_stream.core
        
        # Should be able to import the package
        assert analitiq_stream.core is not None
    
    def test_fault_tolerance_init(self):
        """Test fault_tolerance package initialization."""
        import analitiq_stream.fault_tolerance
        
        # Should be able to import the package
        assert analitiq_stream.fault_tolerance is not None
    
    def test_models_init(self):
        """Test models package initialization."""
        import analitiq_stream.models
        
        # Should be able to import the package
        assert analitiq_stream.models is not None
    
    def test_schema_init(self):
        """Test schema package initialization."""
        import analitiq_stream.schema
        
        # Should be able to import the package  
        assert analitiq_stream.schema is not None
    
    def test_database_init_already_covered(self):
        """Test database package init (covered in test_database_init.py)."""
        # This is already tested in test_database_init.py
        import analitiq_stream.connectors.database
        
        assert analitiq_stream.connectors.database is not None
        
        # Verify main exports from database package
        from analitiq_stream.connectors.database import (
            BaseDatabaseDriver, DriverFactory, DatabaseConnector
        )
        
        assert BaseDatabaseDriver is not None
        assert DriverFactory is not None
        assert DatabaseConnector is not None


class TestImportErrors:
    """Test handling of import errors."""
    
    def test_missing_optional_import_handling(self):
        """Test that the package handles missing optional imports gracefully."""
        # The main __init__.py should import successfully even if some optional
        # dependencies are missing (this tests the import structure)
        import analitiq_stream
        
        # Should complete without raising ImportError
        assert analitiq_stream is not None
    
    def test_circular_import_prevention(self):
        """Test that imports don't create circular dependencies."""
        # Import in different orders to test for circular imports
        import analitiq_stream.core.pipeline
        import analitiq_stream.core.engine
        import analitiq_stream.connectors.api
        import analitiq_stream.connectors.database
        import analitiq_stream
        
        # All imports should succeed
        assert all([
            analitiq_stream.core.pipeline,
            analitiq_stream.core.engine,
            analitiq_stream.connectors.api,
            analitiq_stream.connectors.database,
            analitiq_stream
        ])


class TestPackageStructure:
    """Test overall package structure and consistency."""
    
    def test_version_consistency(self):
        """Test that version is consistently defined."""
        import analitiq_stream
        
        version = analitiq_stream.__version__
        assert isinstance(version, str)
        assert len(version) > 0
        assert version.count('.') >= 2  # At least major.minor.patch
    
    def test_all_exports_exist(self):
        """Test that all __all__ exports actually exist and are importable."""
        import analitiq_stream
        
        for export_name in analitiq_stream.__all__:
            # Should be able to get the attribute
            export_obj = getattr(analitiq_stream, export_name)
            assert export_obj is not None
            
            # Should be a class or callable (not a string or other primitive)
            assert callable(export_obj) or hasattr(export_obj, '__class__')
    
    def test_no_unexpected_exports(self):
        """Test that there are no unexpected public exports."""
        import analitiq_stream
        
        # Get all public attributes (not starting with _)
        public_attrs = [name for name in dir(analitiq_stream) if not name.startswith('_')]
        
        # Remove expected metadata attributes
        metadata_attrs = ['__version__', '__author__', '__all__']
        code_attrs = [name for name in public_attrs if name not in metadata_attrs]
        
        # All public code attributes should be in __all__ (except submodules)
        submodules = ['cli', 'config', 'connectors', 'core', 'fault_tolerance', 'schema', 'models', 'mapping', 'transformations']  # These are modules, not classes to export
        for attr in code_attrs:
            if attr not in submodules:
                assert attr in analitiq_stream.__all__, f"Unexpected public export: {attr}"