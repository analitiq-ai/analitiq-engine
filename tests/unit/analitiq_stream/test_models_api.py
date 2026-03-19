"""Unit tests for API models."""

import pytest
from pydantic import ValidationError

from src.models.api import (
    RateLimitConfig,
    PaginationParams,
    TimeWindowParams,
    PaginationConfig,
    FilterConfig,
    APIConnectionConfig,
    APIReadConfig,
    APIWriteConfig
)


class TestRateLimitConfig:
    """Test RateLimitConfig model."""
    
    def test_rate_limit_config_defaults(self):
        """Test default values for rate limit config."""
        config = RateLimitConfig()
        
        assert config.max_requests == 100
        assert config.time_window == 60
    
    def test_rate_limit_config_custom_values(self):
        """Test custom values for rate limit config."""
        config = RateLimitConfig(max_requests=200, time_window=120)
        
        assert config.max_requests == 200
        assert config.time_window == 120
    
    def test_rate_limit_config_validation_positive_values(self):
        """Test validation requires positive values."""
        with pytest.raises(ValidationError) as exc_info:
            RateLimitConfig(max_requests=0, time_window=60)
        assert "greater than 0" in str(exc_info.value)
        
        with pytest.raises(ValidationError) as exc_info:
            RateLimitConfig(max_requests=100, time_window=-1)
        assert "greater than 0" in str(exc_info.value)
    
    def test_rate_limit_config_json_serialization(self):
        """Test JSON serialization of rate limit config."""
        config = RateLimitConfig(max_requests=150, time_window=90)
        
        json_data = config.model_dump()
        assert json_data == {"max_requests": 150, "time_window": 90}
        
        # Test round-trip
        config2 = RateLimitConfig(**json_data)
        assert config2.max_requests == 150
        assert config2.time_window == 90


class TestPaginationParams:
    """Test PaginationParams model."""
    
    def test_pagination_params_defaults(self):
        """Test default values for pagination params."""
        params = PaginationParams()
        
        assert params.limit_param is None
        assert params.max_limit is None
        assert params.page_param is None
        assert params.offset_param is None
        assert params.cursor_param is None
        assert params.next_cursor_field is None
        assert params.uses_link_header is False
    
    def test_pagination_params_custom_values(self):
        """Test custom values for pagination params."""
        params = PaginationParams(
            limit_param="limit",
            max_limit=1000,
            page_param="page",
            offset_param="offset",
            cursor_param="cursor",
            next_cursor_field="next_cursor",
            uses_link_header=True
        )
        
        assert params.limit_param == "limit"
        assert params.max_limit == 1000
        assert params.page_param == "page"
        assert params.offset_param == "offset"
        assert params.cursor_param == "cursor"
        assert params.next_cursor_field == "next_cursor"
        assert params.uses_link_header is True
    
    def test_pagination_params_max_limit_validation(self):
        """Test max_limit validation requires positive values."""
        with pytest.raises(ValidationError) as exc_info:
            PaginationParams(max_limit=0)
        assert "greater than 0" in str(exc_info.value)
        
        with pytest.raises(ValidationError) as exc_info:
            PaginationParams(max_limit=-100)
        assert "greater than 0" in str(exc_info.value)


class TestTimeWindowParams:
    """Test TimeWindowParams model."""
    
    def test_time_window_params_defaults(self):
        """Test default values for time window params."""
        params = TimeWindowParams()
        
        assert params.start_param is None
        assert params.end_param is None
    
    def test_time_window_params_custom_values(self):
        """Test custom values for time window params."""
        params = TimeWindowParams(start_param="start_time", end_param="end_time")
        
        assert params.start_param == "start_time"
        assert params.end_param == "end_time"


class TestPaginationConfig:
    """Test PaginationConfig model."""
    
    def test_pagination_config_required_type(self):
        """Test that pagination type is required."""
        with pytest.raises(ValidationError) as exc_info:
            PaginationConfig()
        assert "Field required" in str(exc_info.value)
    
    def test_pagination_config_valid_types(self):
        """Test valid pagination types."""
        valid_types = ["offset", "cursor", "page", "time", "link"]
        
        for pag_type in valid_types:
            config = PaginationConfig(type=pag_type)
            assert config.type == pag_type
            assert isinstance(config.params, PaginationParams)
    
    def test_pagination_config_invalid_type(self):
        """Test invalid pagination type."""
        with pytest.raises(ValidationError) as exc_info:
            PaginationConfig(type="invalid")
        assert "Input should be" in str(exc_info.value)
    
    def test_pagination_config_with_params(self):
        """Test pagination config with custom params."""
        params = PaginationParams(limit_param="size", page_param="pagenum")
        config = PaginationConfig(type="page", params=params)
        
        assert config.type == "page"
        assert config.params.limit_param == "size"
        assert config.params.page_param == "pagenum"


class TestFilterConfig:
    """Test FilterConfig model."""
    
    def test_filter_config_required_type(self):
        """Test that filter type is required."""
        with pytest.raises(ValidationError) as exc_info:
            FilterConfig()
        assert "Field required" in str(exc_info.value)
    
    def test_filter_config_basic(self):
        """Test basic filter config."""
        config = FilterConfig(type="string", value="test")
        
        assert config.type == "string"
        assert config.value == "test"
        assert config.default is None
        assert config.required is False
        assert config.description is None
    
    def test_filter_config_all_fields(self):
        """Test filter config with all fields."""
        config = FilterConfig(
            type="integer",
            value=42,
            default=0,
            required=True,
            description="Test filter"
        )
        
        assert config.type == "integer"
        assert config.value == 42
        assert config.default == 0
        assert config.required is True
        assert config.description == "Test filter"
    
    def test_filter_config_validation_required_with_value(self):
        """Test validation passes for required filter with value."""
        config = FilterConfig(type="string", value="test", required=True)
        
        assert config.required is True
        assert config.value == "test"
    
    def test_filter_config_validation_required_with_default(self):
        """Test validation passes for required filter with default."""
        config = FilterConfig(type="string", default="default", required=True)
        
        assert config.required is True
        assert config.default == "default"
    
    def test_filter_config_validation_required_no_value_or_default(self):
        """Test validation fails for required filter without value or default."""
        with pytest.raises(ValidationError) as exc_info:
            FilterConfig(type="string", required=True)
        
        assert "Required filter must have either 'value' or 'default'" in str(exc_info.value)


class TestAPIConnectionConfig:
    """Test APIConnectionConfig model."""

    def test_api_connection_config_minimal(self):
        """Test minimal API connection config."""
        config = APIConnectionConfig(host="https://api.example.com")

        assert config.host == "https://api.example.com"
        assert config.parameters.headers == {}
        assert config.parameters.timeout == 30
        assert config.parameters.max_connections == 10
        assert config.parameters.max_connections_per_host == 2
        assert config.parameters.rate_limit is None

    def test_api_connection_config_full(self):
        """Test full API connection config with parameters."""
        from src.models.api import APIConnectionParameters
        rate_limit = RateLimitConfig(max_requests=200, time_window=120)
        config = APIConnectionConfig(
            host="https://api.example.com",
            parameters=APIConnectionParameters(
                headers={"Authorization": "Bearer token"},
                timeout=60,
                max_connections=20,
                max_connections_per_host=5,
                rate_limit=rate_limit,
            ),
        )

        assert config.host == "https://api.example.com"
        assert config.parameters.headers["Authorization"] == "Bearer token"
        assert config.parameters.timeout == 60
        assert config.parameters.max_connections == 20
        assert config.parameters.max_connections_per_host == 5
        assert config.parameters.rate_limit.max_requests == 200

    def test_host_validation_empty(self):
        """Test host validation with empty string."""
        with pytest.raises(ValidationError) as exc_info:
            APIConnectionConfig(host="")
        assert "host cannot be empty" in str(exc_info.value)

    def test_host_validation_invalid_format(self):
        """Test host validation with invalid format."""
        with pytest.raises(ValidationError) as exc_info:
            APIConnectionConfig(host="not-a-url")
        assert "must be a valid URL with scheme and host" in str(exc_info.value)

    def test_host_validation_invalid_scheme(self):
        """Test host validation with invalid scheme."""
        with pytest.raises(ValidationError) as exc_info:
            APIConnectionConfig(host="ftp://example.com")
        assert "must use http or https scheme" in str(exc_info.value)

    def test_host_validation_valid_formats(self):
        """Test host validation with valid formats."""
        valid_urls = [
            "https://api.example.com",
            "http://localhost:8080",
            "https://api.example.com:443/v1",
            "http://192.168.1.1:3000"
        ]

        for url in valid_urls:
            config = APIConnectionConfig(host=url)
            assert config.host == url

    def test_headers_validation_empty_name(self):
        """Test headers validation with empty header name."""
        from src.models.api import APIConnectionParameters
        with pytest.raises(ValidationError) as exc_info:
            APIConnectionConfig(
                host="https://api.example.com",
                parameters=APIConnectionParameters(headers={"": "value"}),
            )
        assert "Header names must be non-empty strings" in str(exc_info.value)

    def test_headers_validation_non_string_value(self):
        """Test headers validation with non-string header value."""
        from src.models.api import APIConnectionParameters
        with pytest.raises(ValidationError) as exc_info:
            APIConnectionConfig(
                host="https://api.example.com",
                parameters=APIConnectionParameters(headers={"Content-Type": 123}),
            )
        assert "Input should be a valid string" in str(exc_info.value)

    def test_headers_validation_whitespace_handling(self):
        """Test headers validation handles whitespace correctly."""
        from src.models.api import APIConnectionParameters
        config = APIConnectionConfig(
            host="https://api.example.com",
            parameters=APIConnectionParameters(
                headers={" Authorization ": " Bearer token "}
            ),
        )

        assert config.parameters.headers["Authorization"] == "Bearer token"
        assert " Authorization " not in config.parameters.headers

    def test_timeout_validation_range(self):
        """Test timeout validation within valid range."""
        from src.models.api import APIConnectionParameters
        # Valid values
        APIConnectionConfig(
            host="https://api.example.com",
            parameters=APIConnectionParameters(timeout=1),
        )
        APIConnectionConfig(
            host="https://api.example.com",
            parameters=APIConnectionParameters(timeout=300),
        )

        # Invalid values
        with pytest.raises(ValidationError):
            APIConnectionConfig(
                host="https://api.example.com",
                parameters=APIConnectionParameters(timeout=0),
            )

        with pytest.raises(ValidationError):
            APIConnectionConfig(
                host="https://api.example.com",
                parameters=APIConnectionParameters(timeout=301),
            )

    def test_connection_limits_validation(self):
        """Test connection limits validation."""
        from src.models.api import APIConnectionParameters
        # Valid values
        APIConnectionConfig(
            host="https://api.example.com",
            parameters=APIConnectionParameters(max_connections=100),
        )
        APIConnectionConfig(
            host="https://api.example.com",
            parameters=APIConnectionParameters(max_connections_per_host=50),
        )

        # Invalid values
        with pytest.raises(ValidationError):
            APIConnectionConfig(
                host="https://api.example.com",
                parameters=APIConnectionParameters(max_connections=101),
            )

        with pytest.raises(ValidationError):
            APIConnectionConfig(
                host="https://api.example.com",
                parameters=APIConnectionParameters(max_connections_per_host=51),
            )


class TestAPIReadConfig:
    """Test APIReadConfig model."""
    
    def test_api_read_config_minimal(self):
        """Test minimal API read config."""
        config = APIReadConfig(endpoint="/users")
        
        assert config.endpoint == "/users"
        assert config.method == "GET"
        assert config.pagination is None
        assert config.filters == {}
        assert config.data_field == "data"
        assert config.pipeline_config is None
    
    def test_api_read_config_full(self):
        """Test full API read config."""
        pagination = PaginationConfig(type="page", params=PaginationParams(page_param="page"))
        filters = {"status": FilterConfig(type="string", value="active")}
        
        config = APIReadConfig(
            endpoint="/users",
            method="POST",
            pagination=pagination,
            filters=filters,
            data_field="results",
            pipeline_config={"batch_size": 100}
        )
        
        assert config.endpoint == "/users"
        assert config.method == "POST"
        assert config.pagination.type == "page"
        assert config.filters["status"].value == "active"
        assert config.data_field == "results"
        assert config.pipeline_config["batch_size"] == 100
    
    def test_endpoint_validation_empty(self):
        """Test endpoint validation with empty string."""
        with pytest.raises(ValidationError) as exc_info:
            APIReadConfig(endpoint="")
        assert "endpoint cannot be empty" in str(exc_info.value)
    
    def test_endpoint_validation_adds_leading_slash(self):
        """Test endpoint validation adds leading slash."""
        config = APIReadConfig(endpoint="users")
        assert config.endpoint == "/users"
        
        config = APIReadConfig(endpoint="/users")
        assert config.endpoint == "/users"
    
    def test_method_validation_valid_methods(self):
        """Test valid HTTP methods for read config."""
        valid_methods = ["GET", "POST", "PUT", "PATCH"]
        
        for method in valid_methods:
            config = APIReadConfig(endpoint="/test", method=method)
            assert config.method == method
    
    def test_method_validation_invalid_method(self):
        """Test invalid HTTP method for read config."""
        with pytest.raises(ValidationError) as exc_info:
            APIReadConfig(endpoint="/test", method="DELETE")
        assert "Input should be" in str(exc_info.value)
    
    def test_pagination_validation_offset_requires_param(self):
        """Test pagination validation for offset type."""
        pagination = PaginationConfig(type="offset")  # Missing offset_param
        
        with pytest.raises(ValidationError) as exc_info:
            APIReadConfig(endpoint="/test", pagination=pagination)
        assert "Offset pagination requires 'offset_param'" in str(exc_info.value)
    
    def test_pagination_validation_cursor_requires_param(self):
        """Test pagination validation for cursor type."""
        pagination = PaginationConfig(type="cursor")  # Missing cursor_param
        
        with pytest.raises(ValidationError) as exc_info:
            APIReadConfig(endpoint="/test", pagination=pagination)
        assert "Cursor pagination requires 'cursor_param'" in str(exc_info.value)
    
    def test_pagination_validation_page_requires_param(self):
        """Test pagination validation for page type."""
        pagination = PaginationConfig(type="page")  # Missing page_param
        
        with pytest.raises(ValidationError) as exc_info:
            APIReadConfig(endpoint="/test", pagination=pagination)
        assert "Page pagination requires 'page_param'" in str(exc_info.value)
    
    def test_pagination_validation_valid_configs(self):
        """Test valid pagination configurations."""
        # Offset pagination
        pagination = PaginationConfig(
            type="offset",
            params=PaginationParams(offset_param="offset")
        )
        config = APIReadConfig(endpoint="/test", pagination=pagination)
        assert config.pagination.type == "offset"
        
        # Cursor pagination
        pagination = PaginationConfig(
            type="cursor",
            params=PaginationParams(cursor_param="cursor")
        )
        config = APIReadConfig(endpoint="/test", pagination=pagination)
        assert config.pagination.type == "cursor"
        
        # Page pagination
        pagination = PaginationConfig(
            type="page", 
            params=PaginationParams(page_param="page")
        )
        config = APIReadConfig(endpoint="/test", pagination=pagination)
        assert config.pagination.type == "page"


class TestAPIWriteConfig:
    """Test APIWriteConfig model."""
    
    def test_api_write_config_basic(self):
        """Test basic API write config."""
        config = APIWriteConfig(endpoint="/users")
        
        assert config.endpoint == "/users"
        assert config.method == "POST"
    
    def test_api_write_config_custom_method(self):
        """Test API write config with custom method."""
        config = APIWriteConfig(endpoint="/users", method="PUT")
        
        assert config.endpoint == "/users"
        assert config.method == "PUT"
    
    def test_write_method_validation_valid_methods(self):
        """Test valid HTTP methods for write config."""
        valid_methods = ["POST", "PUT", "PATCH"]
        
        for method in valid_methods:
            config = APIWriteConfig(endpoint="/test", method=method)
            assert config.method == method
    
    def test_write_method_validation_invalid_method(self):
        """Test invalid HTTP method for write config."""
        with pytest.raises(ValidationError) as exc_info:
            APIWriteConfig(endpoint="/test", method="GET")
        assert "Input should be" in str(exc_info.value)


class TestAPIModelsIntegration:
    """Test integration between API models."""
    
    def test_models_can_be_nested(self):
        """Test that models can be properly nested."""
        from src.models.api import APIConnectionParameters
        rate_limit = RateLimitConfig(max_requests=500, time_window=300)
        connection_config = APIConnectionConfig(
            host="https://api.example.com",
            parameters=APIConnectionParameters(rate_limit=rate_limit),
        )
        
        pagination = PaginationConfig(
            type="offset",
            params=PaginationParams(offset_param="skip", limit_param="take")
        )
        
        read_config = APIReadConfig(
            endpoint="/data",
            method="GET",
            pagination=pagination,
            filters={
                "active": FilterConfig(type="boolean", value=True),
                "category": FilterConfig(type="string", default="general")
            }
        )
        
        # All models should work together
        assert connection_config.parameters.rate_limit.max_requests == 500
        assert read_config.pagination.params.offset_param == "skip"
        assert read_config.filters["active"].value is True
    
    def test_models_json_serialization_integration(self):
        """Test JSON serialization of nested models."""
        config = APIReadConfig(
            endpoint="/test",
            pagination=PaginationConfig(
                type="page",
                params=PaginationParams(page_param="p", limit_param="l")
            ),
            filters={
                "status": FilterConfig(type="string", value="active", required=True)
            }
        )
        
        json_data = config.model_dump()
        
        # Test structure
        assert json_data["endpoint"] == "/test"
        assert json_data["pagination"]["type"] == "page"
        assert json_data["pagination"]["params"]["page_param"] == "p"
        assert json_data["filters"]["status"]["type"] == "string"
        assert json_data["filters"]["status"]["value"] == "active"
        
        # Test round-trip
        config2 = APIReadConfig(**json_data)
        assert config2.endpoint == config.endpoint
        assert config2.pagination.type == config.pagination.type
        assert config2.filters["status"].value == config.filters["status"].value