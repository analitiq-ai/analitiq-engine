"""Unit tests for CredentialsManager."""

import json
import os
import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch, mock_open

from src.core.credentials import CredentialsManager, credentials_manager


@pytest.fixture
def manager():
    """Create a fresh CredentialsManager instance."""
    return CredentialsManager()


@pytest.fixture
def temp_credentials_file():
    """Create a temporary credentials file."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        credentials = {
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
            "user": "test_user",
            "password": "${DB_PASSWORD}"
        }
        json.dump(credentials, f)
        temp_path = f.name
    
    yield temp_path
    
    # Cleanup
    if Path(temp_path).exists():
        Path(temp_path).unlink()


class TestCredentialsManagerInit:
    """Test CredentialsManager initialization."""
    
    def test_init(self):
        """Test CredentialsManager initialization."""
        manager = CredentialsManager()
        assert manager.credentials_cache == {}
    
    def test_global_instance(self):
        """Test that global credentials_manager instance exists."""
        from src.core.credentials import credentials_manager
        
        assert credentials_manager is not None
        assert isinstance(credentials_manager, CredentialsManager)
        assert hasattr(credentials_manager, 'credentials_cache')


class TestLoadCredentials:
    """Test credentials loading functionality."""
    
    def test_load_credentials_success(self, manager, temp_credentials_file):
        """Test successful credentials loading."""
        with patch.dict(os.environ, {'DB_PASSWORD': 'secret123'}):
            credentials = manager.load_credentials(temp_credentials_file)
            
            assert credentials['host'] == 'localhost'
            assert credentials['port'] == 5432
            assert credentials['database'] == 'test_db'
            assert credentials['user'] == 'test_user'
            assert credentials['password'] == 'secret123'  # Environment variable expanded
    
    def test_load_credentials_file_not_found(self, manager):
        """Test loading credentials from non-existent file."""
        with pytest.raises(FileNotFoundError) as exc_info:
            manager.load_credentials('/non/existent/path.json')
        
        assert "Credentials file not found" in str(exc_info.value)
    
    def test_load_credentials_invalid_json(self, manager):
        """Test loading credentials from invalid JSON file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write('{"invalid": json}')  # Invalid JSON
            temp_path = f.name
        
        try:
            with pytest.raises(ValueError) as exc_info:
                manager.load_credentials(temp_path)
            
            assert "Invalid JSON in credentials file" in str(exc_info.value)
        finally:
            Path(temp_path).unlink()
    
    def test_load_credentials_caching(self, manager, temp_credentials_file):
        """Test that credentials are cached after first load."""
        with patch.dict(os.environ, {'DB_PASSWORD': 'secret123'}):
            # First load
            credentials1 = manager.load_credentials(temp_credentials_file)
            
            # Second load should use cache
            credentials2 = manager.load_credentials(temp_credentials_file)
            
            # Should be the same object (cached)
            assert credentials1 is credentials2
            assert temp_credentials_file in manager.credentials_cache
    
    def test_load_credentials_with_file_read_error(self, manager):
        """Test handling of file read errors."""
        with patch('pathlib.Path.exists', return_value=True):
            with patch('builtins.open', side_effect=PermissionError("Permission denied")):
                with pytest.raises(PermissionError):
                    manager.load_credentials('some_file.json')


class TestEnvironmentVariableExpansion:
    """Test environment variable expansion functionality."""
    
    def test_expand_environment_variables_string(self, manager):
        """Test expanding environment variables in strings."""
        credentials = {"password": "${DB_PASSWORD}", "host": "localhost"}
        
        with patch.dict(os.environ, {'DB_PASSWORD': 'secret123'}):
            expanded = manager._expand_environment_variables(credentials)
            
            assert expanded['password'] == 'secret123'
            assert expanded['host'] == 'localhost'
    
    def test_expand_environment_variables_nested(self, manager):
        """Test expanding environment variables in nested objects."""
        credentials = {
            "auth": {
                "token": "${API_TOKEN}",
                "type": "bearer"
            },
            "config": {
                "nested": {
                    "secret": "${SECRET_KEY}"
                }
            }
        }
        
        with patch.dict(os.environ, {'API_TOKEN': 'token123', 'SECRET_KEY': 'key456'}):
            expanded = manager._expand_environment_variables(credentials)
            
            assert expanded['auth']['token'] == 'token123'
            assert expanded['auth']['type'] == 'bearer'
            assert expanded['config']['nested']['secret'] == 'key456'
    
    def test_expand_environment_variables_list(self, manager):
        """Test expanding environment variables in lists."""
        credentials = {
            "hosts": ["${HOST1}", "${HOST2}", "static-host"],
            "ports": [8080, "${PORT}"]
        }
        
        with patch.dict(os.environ, {'HOST1': 'host1.com', 'HOST2': 'host2.com', 'PORT': '9090'}):
            expanded = manager._expand_environment_variables(credentials)
            
            assert expanded['hosts'] == ['host1.com', 'host2.com', 'static-host']
            assert expanded['ports'] == [8080, '9090']
    
    def test_expand_environment_variables_missing_var(self, manager):
        """Test handling of missing environment variables."""
        credentials = {"password": "${MISSING_VAR}"}
        
        # Missing variable should remain as-is with safe_substitute
        expanded = manager._expand_environment_variables(credentials)
        assert expanded['password'] == '${MISSING_VAR}'
    
    def test_expand_environment_variables_no_vars(self, manager):
        """Test expanding when no environment variables are present."""
        credentials = {"host": "localhost", "port": 5432}
        
        expanded = manager._expand_environment_variables(credentials)
        assert expanded == credentials


class TestValidateDatabaseCredentials:
    """Test database credentials validation."""
    
    def test_validate_database_credentials_success(self, manager):
        """Test successful database credentials validation."""
        credentials = {
            "host": "localhost",
            "database": "test_db", 
            "user": "test_user",
            "password": "test_pass",
            "port": 5432
        }
        
        assert manager.validate_database_credentials(credentials) is True
    
    def test_validate_database_credentials_missing_required(self, manager):
        """Test validation with missing required fields."""
        credentials = {
            "host": "localhost",
            "database": "test_db"
            # Missing user and password
        }
        
        assert manager.validate_database_credentials(credentials) is False
    
    def test_validate_database_credentials_invalid_port(self, manager):
        """Test validation with invalid port."""
        credentials = {
            "host": "localhost",
            "database": "test_db",
            "user": "test_user", 
            "password": "test_pass",
            "port": "invalid"
        }
        
        assert manager.validate_database_credentials(credentials) is False
    
    def test_validate_database_credentials_port_out_of_range(self, manager):
        """Test validation with port out of valid range."""
        credentials = {
            "host": "localhost",
            "database": "test_db",
            "user": "test_user",
            "password": "test_pass",
            "port": 70000  # Too high
        }
        
        assert manager.validate_database_credentials(credentials) is False
    
    def test_validate_database_credentials_no_port(self, manager):
        """Test validation without port (should pass)."""
        credentials = {
            "host": "localhost", 
            "database": "test_db",
            "user": "test_user",
            "password": "test_pass"
        }
        
        assert manager.validate_database_credentials(credentials) is True


class TestValidateAPICredentials:
    """Test API credentials validation."""
    
    def test_validate_api_credentials_basic(self, manager):
        """Test basic API credentials validation."""
        credentials = {
            "base_url": "https://api.example.com"
        }
        
        assert manager.validate_api_credentials(credentials) is True
    
    def test_validate_api_credentials_missing_base_url(self, manager):
        """Test validation with missing base_url."""
        credentials = {
            "timeout": 30
        }
        
        assert manager.validate_api_credentials(credentials) is False
    
    def test_validate_api_credentials_bearer_token(self, manager):
        """Test validation with bearer token authentication."""
        credentials = {
            "base_url": "https://api.example.com",
            "auth": {
                "type": "bearer_token",
                "token": "abc123"
            }
        }
        
        assert manager.validate_api_credentials(credentials) is True
    
    def test_validate_api_credentials_bearer_token_missing_token(self, manager):
        """Test validation with bearer token auth but missing token."""
        credentials = {
            "base_url": "https://api.example.com", 
            "auth": {
                "type": "bearer_token"
                # Missing token
            }
        }
        
        assert manager.validate_api_credentials(credentials) is False
    
    def test_validate_api_credentials_api_key(self, manager):
        """Test validation with API key authentication."""
        credentials = {
            "base_url": "https://api.example.com",
            "auth": {
                "type": "api_key",
                "api_key": "key123"
            }
        }
        
        assert manager.validate_api_credentials(credentials) is True
    
    def test_validate_api_credentials_api_key_missing(self, manager):
        """Test validation with API key auth but missing key."""
        credentials = {
            "base_url": "https://api.example.com",
            "auth": {
                "type": "api_key"
                # Missing api_key
            }
        }
        
        assert manager.validate_api_credentials(credentials) is False
    
    def test_validate_api_credentials_basic_auth(self, manager):
        """Test validation with basic authentication."""
        credentials = {
            "base_url": "https://api.example.com",
            "auth": {
                "type": "basic",
                "username": "user",
                "password": "pass"
            }
        }
        
        assert manager.validate_api_credentials(credentials) is True
    
    def test_validate_api_credentials_basic_auth_missing_creds(self, manager):
        """Test validation with basic auth but missing username/password."""
        credentials = {
            "base_url": "https://api.example.com",
            "auth": {
                "type": "basic",
                "username": "user"
                # Missing password
            }
        }
        
        assert manager.validate_api_credentials(credentials) is False


class TestMergeCredentialsWithConfig:
    """Test merging credentials with configuration."""
    
    def test_merge_credentials_basic(self, manager):
        """Test basic credentials merging."""
        config = {"batch_size": 100}
        credentials = {"host": "localhost", "user": "test"}
        
        merged = manager.merge_credentials_with_config(config, credentials)
        
        assert merged["batch_size"] == 100
        assert merged["connection"]["host"] == "localhost"
        assert merged["connection"]["user"] == "test"
    
    def test_merge_credentials_existing_connection(self, manager):
        """Test merging when connection section already exists."""
        config = {
            "batch_size": 100,
            "connection": {"timeout": 30}
        }
        credentials = {"host": "localhost", "user": "test"}
        
        merged = manager.merge_credentials_with_config(config, credentials)
        
        assert merged["batch_size"] == 100
        assert merged["connection"]["timeout"] == 30
        assert merged["connection"]["host"] == "localhost"
        assert merged["connection"]["user"] == "test"
    
    def test_merge_credentials_rate_limit_special_handling(self, manager):
        """Test special handling of rate_limit field."""
        config = {"batch_size": 100}
        credentials = {
            "host": "localhost",
            "rate_limit": {"max_requests": 100, "time_window": 60}
        }
        
        merged = manager.merge_credentials_with_config(config, credentials)
        
        # rate_limit should be at root level and in connection
        assert merged["rate_limit"]["max_requests"] == 100
        assert merged["connection"]["rate_limit"]["max_requests"] == 100
    
    def test_merge_credentials_no_mutation(self, manager):
        """Test that original config is not mutated."""
        original_config = {"batch_size": 100}
        credentials = {"host": "localhost"}
        
        merged = manager.merge_credentials_with_config(original_config, credentials)
        
        # Original should not have connection section
        assert "connection" not in original_config
        assert "connection" in merged


class TestCreateTemplateCredentials:
    """Test creating template credentials files."""
    
    def test_create_template_database(self, manager):
        """Test creating database credentials template."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_path = f.name
        
        try:
            manager.create_template_credentials("database", temp_path)
            
            with open(temp_path, 'r') as f:
                template = json.load(f)
            
            assert template["host"] == "localhost"
            assert template["port"] == 5432
            assert template["database"] == "your_database"
            assert template["user"] == "your_username"
            assert template["password"] == "${DB_PASSWORD}"
            assert template["driver"] == "postgresql"
        finally:
            Path(temp_path).unlink()
    
    def test_create_template_api(self, manager):
        """Test creating API credentials template."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_path = f.name
        
        try:
            manager.create_template_credentials("api", temp_path)
            
            with open(temp_path, 'r') as f:
                template = json.load(f)
            
            assert template["base_url"] == "https://api.example.com"
            assert template["auth"]["type"] == "bearer_token"
            assert template["auth"]["token"] == "${API_TOKEN}"
            assert template["rate_limit"]["max_requests"] == 100
        finally:
            Path(temp_path).unlink()
    
    def test_create_template_invalid_type(self, manager):
        """Test creating template with invalid credential type."""
        with pytest.raises(ValueError) as exc_info:
            manager.create_template_credentials("invalid", "/tmp/test.json")
        
        assert "Unknown credential type" in str(exc_info.value)
    
    def test_create_template_file_error(self, manager):
        """Test handling file creation errors."""
        with patch('builtins.open', side_effect=PermissionError("Permission denied")):
            with pytest.raises(PermissionError):
                manager.create_template_credentials("database", "/invalid/path.json")


class TestClearCache:
    """Test cache clearing functionality."""
    
    def test_clear_cache(self, manager, temp_credentials_file):
        """Test clearing credentials cache."""
        # Load credentials to populate cache
        with patch.dict(os.environ, {'DB_PASSWORD': 'secret123'}):
            manager.load_credentials(temp_credentials_file)
            assert len(manager.credentials_cache) == 1
        
        # Clear cache
        manager.clear_cache()
        assert len(manager.credentials_cache) == 0
    
    def test_clear_empty_cache(self, manager):
        """Test clearing empty cache."""
        assert len(manager.credentials_cache) == 0
        
        manager.clear_cache()  # Should not error
        assert len(manager.credentials_cache) == 0


class TestCredentialsManagerEdgeCases:
    """Test edge cases and error handling."""
    
    def test_load_credentials_empty_file(self, manager):
        """Test loading from empty file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write('')  # Empty file
            temp_path = f.name
        
        try:
            with pytest.raises(ValueError):
                manager.load_credentials(temp_path)
        finally:
            Path(temp_path).unlink()
    
    def test_expand_environment_variables_complex_nesting(self, manager):
        """Test environment variable expansion with complex nesting."""
        credentials = {
            "level1": {
                "level2": {
                    "level3": ["${VAR1}", {"nested": "${VAR2}"}]
                }
            }
        }
        
        with patch.dict(os.environ, {'VAR1': 'value1', 'VAR2': 'value2'}):
            expanded = manager._expand_environment_variables(credentials)
            
            assert expanded['level1']['level2']['level3'][0] == 'value1'
            assert expanded['level1']['level2']['level3'][1]['nested'] == 'value2'
    
    def test_validate_database_credentials_port_boundary_values(self, manager):
        """Test port validation with boundary values."""
        base_credentials = {
            "host": "localhost",
            "database": "test_db",
            "user": "test_user",
            "password": "test_pass"
        }
        
        # Valid boundary values
        assert manager.validate_database_credentials({**base_credentials, "port": 1}) is True
        assert manager.validate_database_credentials({**base_credentials, "port": 65535}) is True
        
        # Invalid boundary values
        assert manager.validate_database_credentials({**base_credentials, "port": 0}) is False
        assert manager.validate_database_credentials({**base_credentials, "port": 65536}) is False