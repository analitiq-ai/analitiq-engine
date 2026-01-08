"""Unit tests for subpackage __init__ files."""

import pytest


class TestCorePackageInit:
    """Test core package __init__ file."""

    @pytest.mark.unit
    def test_core_imports(self):
        """Test that core package exports are available."""
        from src import core

        # Check __all__ exports
        assert hasattr(core, '__all__')
        expected_exports = ["StreamingEngine", "Pipeline", "credentials_manager", "CredentialsManager"]

        for export in expected_exports:
            assert export in core.__all__
            assert hasattr(core, export)

    @pytest.mark.unit
    def test_core_direct_imports(self):
        """Test direct imports from core package."""
        from src.core import (
            StreamingEngine, Pipeline, credentials_manager,
            CredentialsManager
        )

        assert StreamingEngine is not None
        assert Pipeline is not None
        assert credentials_manager is not None
        assert CredentialsManager is not None


class TestConnectorsPackageInit:
    """Test connectors package __init__ file."""
    
    @pytest.mark.unit
    def test_connectors_imports(self):
        """Test that connectors package can be imported."""
        import src.connectors as connectors
        
        # Check if __all__ is defined
        if hasattr(connectors, '__all__'):
            for export in connectors.__all__:
                assert hasattr(connectors, export)
    
    @pytest.mark.unit
    def test_connectors_submodules(self):
        """Test that connector submodules are accessible."""
        from src.connectors import api, base
        
        assert api is not None
        assert base is not None
        
        # Test that main classes can be imported
        from src.connectors.api import APIConnector
        from src.connectors.base import BaseConnector
        
        assert APIConnector is not None
        assert BaseConnector is not None


class TestDatabasePackageInit:
    """Test database package __init__ file."""
    
    @pytest.mark.unit
    def test_database_imports(self):
        """Test that database package exports are available."""
        from src.connectors import database
        
        # Check __all__ exports if defined
        if hasattr(database, '__all__'):
            for export in database.__all__:
                assert hasattr(database, export)
    
    @pytest.mark.unit
    def test_database_direct_imports(self):
        """Test direct imports from database package."""
        from src.connectors.database import (
            DatabaseConnector, BaseDatabaseDriver, DriverFactory
        )
        
        assert DatabaseConnector is not None
        assert BaseDatabaseDriver is not None
        assert DriverFactory is not None


class TestFaultTolerancePackageInit:
    """Test fault_tolerance package __init__ file."""
    
    @pytest.mark.unit
    def test_fault_tolerance_imports(self):
        """Test that fault_tolerance package can be imported."""
        import src.fault_tolerance as ft
        
        # Check if __all__ is defined
        if hasattr(ft, '__all__'):
            for export in ft.__all__:
                assert hasattr(ft, export)
    
    @pytest.mark.unit
    def test_fault_tolerance_modules(self):
        """Test that fault tolerance modules are accessible."""
        from src.fault_tolerance import (
            circuit_breaker, dead_letter_queue, retry_handler,
            state_manager
        )

        assert circuit_breaker is not None
        assert dead_letter_queue is not None
        assert retry_handler is not None
        assert state_manager is not None


class TestSchemaPackageInit:
    """Test schema package __init__ file."""
    
    @pytest.mark.unit
    def test_schema_imports(self):
        """Test that schema package can be imported."""
        import src.schema as schema
        
        # Check if __all__ is defined
        if hasattr(schema, '__all__'):
            for export in schema.__all__:
                assert hasattr(schema, export)
    
    @pytest.mark.unit
    def test_schema_manager_import(self):
        """Test that SchemaManager can be imported from schema package."""
        from src.schema.schema_manager import SchemaManager
        
        assert SchemaManager is not None


class TestMappingPackageInit:
    """Test mapping package __init__ file."""
    
    @pytest.mark.unit
    def test_mapping_imports(self):
        """Test that mapping package can be imported."""
        import src.mapping as mapping
        
        # Package should be importable
        assert mapping is not None
    
    @pytest.mark.unit
    def test_mapping_processor_import(self):
        """Test that processor can be imported from mapping package."""
        from src.mapping.processor import FieldMappingProcessor

        assert FieldMappingProcessor is not None


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
    
    @pytest.mark.unit
    def test_models_api_import(self):
        """Test that API models can be imported."""
        from src.models.api import (
            EndpointConfig, HostConfig, APIConfig
        )
        
        assert EndpointConfig is not None
        assert HostConfig is not None
        assert APIConfig is not None


class TestPackageNamespaceConsistency:
    """Test consistency across package namespaces."""
    
    @pytest.mark.unit
    def test_no_namespace_collisions(self):
        """Test that there are no naming collisions between packages."""
        import src
        import src.core
        import src.connectors
        import src.fault_tolerance
        import src.schema
        
        # Get exports from each
        main_exports = set(src.__all__) if hasattr(src, '__all__') else set()
        core_exports = set(src.core.__all__) if hasattr(src.core, '__all__') else set()
        
        # Main package should re-export core items, so overlap is expected
        overlap = main_exports & core_exports
        expected_overlap = {"StreamingEngine", "Pipeline", "credentials_manager", "CredentialsManager"}
        
        assert overlap.issubset(expected_overlap) or overlap == expected_overlap
    
    @pytest.mark.unit
    def test_import_paths_consistency(self):
        """Test that classes can be imported from multiple paths consistently."""
        # Import from main package
        from src import Pipeline as MainPipeline
        
        # Import from subpackage
        from src.core import Pipeline as CorePipeline
        
        # Import from module directly
        from src.core.pipeline import Pipeline as ModulePipeline
        
        # All should be the same class
        assert MainPipeline is CorePipeline
        assert CorePipeline is ModulePipeline
    
    @pytest.mark.unit
    def test_re_export_consistency(self):
        """Test that re-exported items maintain their identity."""
        # Test with SchemaManager
        from src import SchemaManager as MainSchemaManager
        from src.schema.schema_manager import SchemaManager as DirectSchemaManager

        assert MainSchemaManager is DirectSchemaManager