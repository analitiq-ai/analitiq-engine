"""E2E test fixtures matching the wise_to_sevdesk configuration structure."""

import pytest
import uuid
from typing import Dict, Any


@pytest.fixture
def e2e_api_source_host_config():
    """API source host configuration for e2e tests."""
    return {
        "base_url": "http://test-api-source.local",
        "headers": {
            "Authorization": "Bearer test-source-token",
            "Content-Type": "application/json"
        },
        "rate_limit": {
            "max_requests": 100,
            "time_window": 60
        },
        "timeout": {
            "connect": 30,
            "read": 120
        }
    }


@pytest.fixture
def e2e_api_destination_host_config():
    """API destination host configuration for e2e tests."""
    return {
        "base_url": "http://test-api-destination.local",
        "headers": {
            "Authorization": "Bearer test-destination-token",
            "Content-Type": "application/json"
        },
        "rate_limit": {
            "max_requests": 10,
            "time_window": 60
        },
        "timeout": {
            "connect": 15,
            "read": 60
        }
    }


@pytest.fixture
def e2e_database_source_host_config():
    """Database source host configuration for e2e tests."""
    return {
        "driver": "postgresql",
        "host": "localhost",
        "port": 5432,
        "database": "test_source_db",
        "user": "test_user",
        "password": "test_password",
        "ssl_mode": "prefer",
        "connection_pool": {
            "min_connections": 2,
            "max_connections": 10,
            "max_overflow": 20,
            "pool_timeout": 30
        }
    }


@pytest.fixture
def e2e_database_destination_host_config():
    """Database destination host configuration for e2e tests."""
    return {
        "driver": "postgresql",
        "host": "localhost",
        "port": 5432,
        "database": "test_destination_db",
        "user": "test_user",
        "password": "test_password",
        "ssl_mode": "prefer",
        "connection_pool": {
            "min_connections": 2,
            "max_connections": 10,
            "max_overflow": 20,
            "pool_timeout": 30
        }
    }


@pytest.fixture
def e2e_api_source_endpoint_config():
    """API source endpoint configuration for e2e tests."""
    return {
        "endpoint": "/api/v1/users",
        "method": "GET",
        "query_params": {
            "limit": 1000,
            "offset": 0
        },
        "response_schema": {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Users API Response",
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "email": {"type": "string", "format": "email"},
                    "name": {"type": "string"},
                    "created_at": {"type": "string", "format": "date-time"},
                    "status": {"type": "string", "enum": ["active", "inactive"]}
                },
                "required": ["id", "email", "name"]
            }
        }
    }


@pytest.fixture
def e2e_api_destination_endpoint_config():
    """API destination endpoint configuration for e2e tests."""
    return {
        "endpoint": "/api/v1/users",
        "method": "POST",
        "request_schema": {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "User Creation Request",
            "type": "object",
            "properties": {
                "user_id": {"type": "integer"},
                "email_address": {"type": "string", "format": "email"},
                "full_name": {"type": "string"},
                "registration_date": {"type": "string", "format": "date-time"},
                "account_status": {"type": "string"}
            },
            "required": ["user_id", "email_address", "full_name"]
        },
        "response_schema": {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "success": {"type": "boolean"},
                "user_id": {"type": "integer"}
            }
        }
    }


@pytest.fixture
def e2e_database_source_endpoint_config():
    """Database source endpoint configuration for e2e tests."""
    return {
        "schema": "public",
        "table": "users",
        "primary_key": ["id"],
        "incremental_column": "updated_at",
        "table_schema": {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "id": {"type": "integer", "database_type": "BIGINT"},
                "email": {"type": "string", "database_type": "VARCHAR(255)"},
                "name": {"type": "string", "database_type": "VARCHAR(255)"},
                "created_at": {"type": "string", "format": "date-time", "database_type": "TIMESTAMPTZ"},
                "updated_at": {"type": "string", "format": "date-time", "database_type": "TIMESTAMPTZ"},
                "status": {"type": "string", "database_type": "VARCHAR(50)"}
            },
            "required": ["id", "email", "name", "created_at", "updated_at"]
        }
    }


@pytest.fixture
def e2e_database_destination_endpoint_config():
    """Database destination endpoint configuration for e2e tests."""
    return {
        "schema": "analytics",
        "table": "processed_users",
        "primary_key": ["user_id"],
        "write_mode": "upsert",
        "conflict_resolution": {
            "on_conflict": "user_id",
            "action": "update",
            "update_columns": ["email_address", "full_name", "account_status", "last_processed_at"]
        },
        "configure": {
            "auto_create_schema": True,
            "auto_create_table": True,
            "auto_create_indexes": [
                {
                    "name": "idx_processed_users_email",
                    "columns": ["email_address"],
                    "type": "btree"
                },
                {
                    "name": "idx_processed_users_status",
                    "columns": ["account_status"],
                    "type": "btree"
                }
            ]
        },
        "table_schema": {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "user_id": {"type": "integer", "database_type": "BIGINT"},
                "email_address": {"type": "string", "database_type": "VARCHAR(255)"},
                "full_name": {"type": "string", "database_type": "VARCHAR(255)"},
                "registration_date": {"type": "string", "format": "date-time", "database_type": "TIMESTAMPTZ"},
                "account_status": {"type": "string", "database_type": "VARCHAR(50)"},
                "last_processed_at": {"type": "string", "format": "date-time", "database_type": "TIMESTAMPTZ"}
            },
            "required": ["user_id", "email_address", "full_name"]
        }
    }


@pytest.fixture
def e2e_pipeline_config_base(mock_pipeline_id):
    """Base pipeline configuration matching wise_to_sevdesk structure."""
    return {
        "pipeline_id": mock_pipeline_id,
        "name": "E2E Test Pipeline",
        "version": "1.0",
        "src": {
            "host_id": "test-source-host-id",
            "name": "Test Source Platform"
        },
        "dst": {
            "host_id": "test-destination-host-id",
            "name": "Test Destination Platform"
        },
        "engine_config": {
            "batch_size": 10,
            "max_concurrent_batches": 2,
            "buffer_size": 100,
            "schedule": {
                "type": "interval",
                "interval_minutes": 5,
                "timezone": "UTC"
            }
        },
        "error_handling": {
            "strategy": "dlq",
            "retry_failed_records": True,
            "max_retries": 3,
            "retry_delay": 1,
            "error_categories": {
                "validation_error": "dlq",
                "transformation_error": "dlq",
                "api_error": "retry",
                "rate_limit_error": "retry_with_backoff"
            }
        },
        "monitoring": {
            "metrics_enabled": True,
            "log_level": "DEBUG",
            "checkpoint_interval": 5,
            "health_check_interval": 30,
            "progress_monitoring": "enabled"
        }
    }


@pytest.fixture
def e2e_api_to_api_pipeline_config(e2e_pipeline_config_base):
    """API to API pipeline configuration for e2e tests."""
    stream_id = str(uuid.uuid4())
    source_endpoint_id = str(uuid.uuid4())
    dest_endpoint_id = str(uuid.uuid4())

    config = e2e_pipeline_config_base.copy()
    config["name"] = "API to API E2E Test Pipeline"
    config["streams"] = {
        stream_id: {
            "name": "api-to-api-stream",
            "description": "API to API data sync with transformations",
            "src": {
                "endpoint_id": source_endpoint_id,
                "replication_method": "incremental",
                "cursor_field": "created_at",
                "cursor_mode": "inclusive",
                "safety_window_seconds": 60,
                "primary_key": ["id"]
            },
            "dst": {
                "endpoint_id": dest_endpoint_id,
                "refresh_mode": "upsert",
                "batch_support": False,
                "batch_size": 1
            },
            "mapping": {
                "field_mappings": {
                    "id": {
                        "target": "user_id"
                    },
                    "email": {
                        "target": "email_address",
                        "validation": {
                            "rules": [{"type": "not_null"}],
                            "error_action": "dlq"
                        }
                    },
                    "name": {
                        "target": "full_name",
                        "validation": {
                            "rules": [{"type": "not_null"}],
                            "error_action": "dlq"
                        }
                    },
                    "created_at": {
                        "target": "registration_date",
                        "transformations": ["iso_to_datetime"]
                    },
                    "status": {
                        "target": "account_status"
                    }
                },
                "computed_fields": {
                    "last_processed_at": {
                        "expression": "now()"
                    }
                }
            }
        }
    }
    return config, stream_id, source_endpoint_id, dest_endpoint_id


@pytest.fixture
def e2e_db_to_api_pipeline_config(e2e_pipeline_config_base):
    """Database to API pipeline configuration for e2e tests."""
    stream_id = str(uuid.uuid4())
    source_endpoint_id = str(uuid.uuid4())
    dest_endpoint_id = str(uuid.uuid4())

    config = e2e_pipeline_config_base.copy()
    config["name"] = "DB to API E2E Test Pipeline"
    config["streams"] = {
        stream_id: {
            "name": "db-to-api-stream",
            "description": "Database to API data sync",
            "src": {
                "endpoint_id": source_endpoint_id,
                "replication_method": "incremental",
                "cursor_field": "updated_at",
                "cursor_mode": "inclusive",
                "safety_window_seconds": 120,
                "primary_key": ["id"]
            },
            "dst": {
                "endpoint_id": dest_endpoint_id,
                "refresh_mode": "upsert",
                "batch_support": False,
                "batch_size": 1
            },
            "mapping": {
                "field_mappings": {
                    "id": {"target": "user_id"},
                    "email": {"target": "email_address"},
                    "name": {"target": "full_name"},
                    "created_at": {"target": "registration_date"},
                    "status": {"target": "account_status"}
                }
            }
        }
    }
    return config, stream_id, source_endpoint_id, dest_endpoint_id


@pytest.fixture
def e2e_api_to_db_pipeline_config(e2e_pipeline_config_base):
    """API to Database pipeline configuration for e2e tests."""
    stream_id = str(uuid.uuid4())
    source_endpoint_id = str(uuid.uuid4())
    dest_endpoint_id = str(uuid.uuid4())

    config = e2e_pipeline_config_base.copy()
    config["name"] = "API to DB E2E Test Pipeline"
    config["streams"] = {
        stream_id: {
            "name": "api-to-db-stream",
            "description": "API to Database data sync with auto-table creation",
            "src": {
                "endpoint_id": source_endpoint_id,
                "replication_method": "incremental",
                "cursor_field": "created_at",
                "cursor_mode": "inclusive",
                "safety_window_seconds": 60,
                "primary_key": ["id"]
            },
            "dst": {
                "endpoint_id": dest_endpoint_id,
                "refresh_mode": "upsert",
                "batch_support": True,
                "batch_size": 10
            },
            "mapping": {
                "field_mappings": {
                    "id": {"target": "user_id"},
                    "email": {"target": "email_address"},
                    "name": {"target": "full_name"},
                    "created_at": {"target": "registration_date"},
                    "status": {"target": "account_status"}
                },
                "computed_fields": {
                    "last_processed_at": {
                        "expression": "now()"
                    }
                }
            }
        }
    }
    return config, stream_id, source_endpoint_id, dest_endpoint_id


@pytest.fixture
def e2e_db_to_db_pipeline_config(e2e_pipeline_config_base):
    """Database to Database pipeline configuration for e2e tests."""
    users_stream_id = str(uuid.uuid4())
    orders_stream_id = str(uuid.uuid4())
    users_source_endpoint_id = str(uuid.uuid4())
    orders_source_endpoint_id = str(uuid.uuid4())
    users_dest_endpoint_id = str(uuid.uuid4())
    orders_dest_endpoint_id = str(uuid.uuid4())

    config = e2e_pipeline_config_base.copy()
    config["name"] = "DB to DB Multi-Table E2E Test Pipeline"
    config["streams"] = {
        users_stream_id: {
            "name": "users-stream",
            "description": "Users table sync",
            "src": {
                "endpoint_id": users_source_endpoint_id,
                "replication_method": "incremental",
                "cursor_field": "updated_at",
                "cursor_mode": "inclusive",
                "safety_window_seconds": 120,
                "primary_key": ["id"]
            },
            "dst": {
                "endpoint_id": users_dest_endpoint_id,
                "refresh_mode": "upsert",
                "batch_support": True,
                "batch_size": 50
            },
            "mapping": {
                "field_mappings": {
                    "id": {"target": "user_id"},
                    "email": {"target": "email_address"},
                    "name": {"target": "full_name"},
                    "created_at": {"target": "registration_date"},
                    "status": {"target": "account_status"}
                }
            }
        },
        orders_stream_id: {
            "name": "orders-stream",
            "description": "Orders table sync",
            "src": {
                "endpoint_id": orders_source_endpoint_id,
                "replication_method": "incremental",
                "cursor_field": "updated_at",
                "cursor_mode": "inclusive",
                "safety_window_seconds": 120,
                "primary_key": ["id"]
            },
            "dst": {
                "endpoint_id": orders_dest_endpoint_id,
                "refresh_mode": "upsert",
                "batch_support": True,
                "batch_size": 100
            },
            "mapping": {
                "field_mappings": {
                    "id": {"target": "order_id"},
                    "user_id": {"target": "customer_id"},
                    "amount": {"target": "order_amount"},
                    "created_at": {"target": "order_date"},
                    "status": {"target": "order_status"}
                }
            }
        }
    }
    return config, {
        "users_stream_id": users_stream_id,
        "orders_stream_id": orders_stream_id,
        "users_source_endpoint_id": users_source_endpoint_id,
        "orders_source_endpoint_id": orders_source_endpoint_id,
        "users_dest_endpoint_id": users_dest_endpoint_id,
        "orders_dest_endpoint_id": orders_dest_endpoint_id
    }


@pytest.fixture
def e2e_fault_tolerance_pipeline_config(e2e_pipeline_config_base):
    """Fault tolerance pipeline configuration for e2e tests."""
    stream_id = str(uuid.uuid4())
    source_endpoint_id = str(uuid.uuid4())
    dest_endpoint_id = str(uuid.uuid4())

    config = e2e_pipeline_config_base.copy()
    config["name"] = "Fault Tolerance E2E Test Pipeline"
    config["engine_config"]["batch_size"] = 1  # Small batches for fault tolerance testing
    config["error_handling"]["max_retries"] = 2
    config["streams"] = {
        stream_id: {
            "name": "fault-tolerance-stream",
            "description": "Stream for testing fault tolerance mechanisms",
            "src": {
                "endpoint_id": source_endpoint_id,
                "replication_method": "full_refresh",
                "primary_key": ["id"]
            },
            "dst": {
                "endpoint_id": dest_endpoint_id,
                "refresh_mode": "insert",
                "batch_support": False,
                "batch_size": 1
            },
            "mapping": {
                "field_mappings": {
                    "id": {"target": "record_id"},
                    "data": {"target": "record_data"},
                    "status": {"target": "record_status"}
                }
            }
        }
    }
    return config, stream_id, source_endpoint_id, dest_endpoint_id


@pytest.fixture
def e2e_data_quality_pipeline_config(e2e_pipeline_config_base):
    """Data quality pipeline configuration for e2e tests."""
    stream_id = str(uuid.uuid4())
    source_endpoint_id = str(uuid.uuid4())
    dest_endpoint_id = str(uuid.uuid4())

    config = e2e_pipeline_config_base.copy()
    config["name"] = "Data Quality E2E Test Pipeline"
    config["streams"] = {
        stream_id: {
            "name": "data-quality-stream",
            "description": "Stream for testing data quality and validation",
            "src": {
                "endpoint_id": source_endpoint_id,
                "replication_method": "full_refresh",
                "primary_key": ["id"]
            },
            "dst": {
                "endpoint_id": dest_endpoint_id,
                "refresh_mode": "insert",
                "batch_support": False,
                "batch_size": 1
            },
            "mapping": {
                "field_mappings": {
                    "id": {
                        "target": "user_id",
                        "validation": {
                            "rules": [{"type": "not_null"}],
                            "error_action": "dlq"
                        }
                    },
                    "email": {
                        "target": "email_address",
                        "validation": {
                            "rules": [{"type": "not_null"}],
                            "error_action": "dlq"
                        }
                    },
                    "name": {
                        "target": "full_name",
                        "transformations": ["trim", "title_case"]
                    }
                }
            }
        }
    }
    return config, stream_id, source_endpoint_id, dest_endpoint_id


@pytest.fixture
def e2e_performance_pipeline_config(e2e_pipeline_config_base):
    """Performance testing pipeline configuration for e2e tests."""
    stream_id = str(uuid.uuid4())
    source_endpoint_id = str(uuid.uuid4())
    dest_endpoint_id = str(uuid.uuid4())

    config = e2e_pipeline_config_base.copy()
    config["name"] = "Performance E2E Test Pipeline"
    config["engine_config"]["batch_size"] = 100  # Larger batches for performance testing
    config["engine_config"]["max_concurrent_batches"] = 5
    config["streams"] = {
        stream_id: {
            "name": "performance-stream",
            "description": "Stream for performance and throughput testing",
            "src": {
                "endpoint_id": source_endpoint_id,
                "replication_method": "full_refresh",
                "primary_key": ["id"]
            },
            "dst": {
                "endpoint_id": dest_endpoint_id,
                "refresh_mode": "insert",
                "batch_support": True,
                "batch_size": 100
            },
            "mapping": {
                "field_mappings": {
                    "id": {"target": "record_id"},
                    "name": {"target": "record_name"},
                    "data": {"target": "record_data"},
                    "created_at": {"target": "record_timestamp"}
                }
            }
        }
    }
    return config, stream_id, source_endpoint_id, dest_endpoint_id