"""Fixtures for PipelineConfigPrep testing."""

import pytest
from typing import Dict, Any


@pytest.fixture
def sample_wise_host_config():
    """Sample Wise API host configuration."""
    return {
        "host": "https://api.sandbox.transferwise.tech",
        "headers": {
            "Authorization": "Bearer ${WISE_API_TOKEN}",
            "Content-Type": "application/json",
            "User-Agent": "Analitiq-Stream/1.0"
        },
        "rate_limit": {
            "max_requests": 100,
            "time_window": 60,
            "backoff_strategy": "exponential"
        },
        "timeout": {
            "connect": 30,
            "read": 120
        },
        "retry": {
            "max_attempts": 3,
            "backoff_factor": 2
        }
    }


@pytest.fixture
def sample_sevdesk_host_config():
    """Sample SevDesk API host configuration."""
    return {
        "host": "https://my.sevdesk.de",
        "headers": {
            "Authorization": "${SEVDESK_API_TOKEN}",
            "Content-Type": "application/json"
        },
        "rate_limit": {
            "max_requests": 10,
            "time_window": 60,
            "backoff_strategy": "linear"
        },
        "timeout": {
            "connect": 15,
            "read": 60
        }
    }


@pytest.fixture
def sample_database_host_config():
    """Sample PostgreSQL database host configuration."""
    return {
        "driver": "postgresql",
        "host": "localhost",
        "port": 5432,
        "database": "analytics",
        "user": "postgres",
        "password": "${DB_PASSWORD}",
        "ssl_mode": "prefer",
        "connection_pool": {
            "min_connections": 2,
            "max_connections": 10,
            "max_overflow": 20,
            "pool_timeout": 30,
            "pool_recycle": 3600,
            "pool_pre_ping": True
        },
        "query_timeout": 300,
        "statement_timeout": 600
    }


@pytest.fixture
def sample_wise_endpoint_config():
    """Sample Wise API source endpoint configuration."""
    return {
        "endpoint": "/v1/profiles/{profile_id}/transfers",
        "method": "GET",
        "query_params": {
            "status": "outgoing_payment_sent,funds_converted",
            "limit": 1000,
            "offset": 0
        },
        "path_params": {
            "profile_id": "${WISE_PROFILE_ID}"
        },
        "response_schema": {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Wise Transfers Response",
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "id": {
                        "type": "integer",
                        "description": "Transfer ID"
                    },
                    "created": {
                        "type": "string",
                        "format": "date-time",
                        "description": "Creation timestamp"
                    },
                    "targetValue": {
                        "type": "number",
                        "description": "Transfer amount"
                    },
                    "targetCurrency": {
                        "type": "string",
                        "pattern": "^[A-Z]{3}$"
                    },
                    "status": {
                        "type": "string",
                        "enum": ["outgoing_payment_sent", "funds_converted", "cancelled"]
                    },
                    "details": {
                        "type": "object",
                        "properties": {
                            "reference": {"type": "string"},
                            "merchant": {
                                "type": "object",
                                "properties": {
                                    "name": {"type": "string"}
                                }
                            }
                        }
                    }
                },
                "required": ["id", "created", "targetValue", "status"]
            }
        },
        "pagination": {
            "type": "offset",
            "limit_param": "limit",
            "offset_param": "offset",
            "max_limit": 1000
        }
    }


@pytest.fixture
def sample_sevdesk_endpoint_config():
    """Sample SevDesk API destination endpoint configuration."""
    return {
        "endpoint": "/api/v1/CheckAccountTransaction",
        "method": "POST",
        "headers": {
            "Content-Type": "application/json"
        },
        "request_schema": {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "SevDesk CheckAccountTransaction Request",
            "type": "object",
            "properties": {
                "valueDate": {
                    "type": "string",
                    "format": "date",
                    "description": "Transaction date"
                },
                "amount": {
                    "type": "number",
                    "description": "Transaction amount"
                },
                "paymtPurpose": {
                    "type": "string",
                    "maxLength": 255,
                    "description": "Payment purpose"
                },
                "objectName": {
                    "type": "string",
                    "const": "CheckAccountTransaction"
                },
                "checkAccount": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "objectName": {"type": "string", "const": "CheckAccount"}
                    },
                    "required": ["id", "objectName"]
                },
                "status": {
                    "type": "string",
                    "enum": ["100", "200", "1000"]
                }
            },
            "required": ["valueDate", "amount", "objectName", "checkAccount", "status"]
        },
        "response_schema": {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "SevDesk API Response",
            "type": "object",
            "properties": {
                "objects": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "string"},
                            "objectName": {"type": "string"}
                        }
                    }
                },
                "success": {"type": "boolean"}
            }
        },
        "error_mapping": {
            "400": "validation_error",
            "401": "authentication_error",
            "403": "authorization_error",
            "429": "rate_limit_error",
            "500": "server_error"
        }
    }


@pytest.fixture
def sample_database_endpoint_config():
    """Sample PostgreSQL database destination endpoint configuration."""
    return {
        "schema": "wise_data",
        "table": "transactions",
        "primary_key": ["wise_id"],
        "unique_constraints": [
            {"name": "uk_wise_transactions_wise_id", "columns": ["wise_id"]},
            {"name": "uk_wise_transactions_external_ref", "columns": ["external_reference"]}
        ],
        "write_mode": "upsert",
        "conflict_resolution": {
            "on_conflict": "wise_id",
            "action": "update",
            "update_columns": ["amount", "status", "reference", "updated_at"],
            "where_condition": "EXCLUDED.updated_at > transactions.updated_at"
        },
        "configure": {
            "auto_create_schema": True,
            "auto_create_table": True,
            "auto_create_indexes": [
                {
                    "name": "idx_transactions_created_at",
                    "columns": ["created_at"],
                    "type": "btree",
                    "unique": False
                },
                {
                    "name": "idx_transactions_status",
                    "columns": ["status"],
                    "type": "btree",
                    "unique": False
                },
                {
                    "name": "idx_transactions_amount",
                    "columns": ["amount"],
                    "type": "btree",
                    "unique": False,
                    "where": "amount > 0"
                }
            ]
        },
        "table_schema": {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Wise Transactions Table Schema",
            "type": "object",
            "properties": {
                "wise_id": {
                    "type": "integer",
                    "database_type": "BIGINT",
                    "nullable": False,
                    "primary_key": True,
                    "description": "Wise transfer ID"
                },
                "created_at": {
                    "type": "string",
                    "format": "date-time",
                    "database_type": "TIMESTAMPTZ",
                    "nullable": False,
                    "description": "Transaction creation timestamp"
                },
                "updated_at": {
                    "type": "string",
                    "format": "date-time",
                    "database_type": "TIMESTAMPTZ",
                    "nullable": False,
                    "default": "CURRENT_TIMESTAMP",
                    "description": "Last update timestamp"
                },
                "amount": {
                    "type": "number",
                    "database_type": "DECIMAL(15,2)",
                    "nullable": False,
                    "description": "Transaction amount"
                },
                "currency": {
                    "type": "string",
                    "database_type": "VARCHAR(3)",
                    "nullable": False,
                    "pattern": "^[A-Z]{3}$",
                    "description": "ISO currency code"
                },
                "status": {
                    "type": "string",
                    "database_type": "VARCHAR(50)",
                    "nullable": False,
                    "description": "Transaction status"
                },
                "reference": {
                    "type": "string",
                    "database_type": "TEXT",
                    "nullable": True,
                    "description": "Payment reference"
                },
                "external_reference": {
                    "type": "string",
                    "database_type": "VARCHAR(255)",
                    "nullable": True,
                    "description": "External system reference"
                },
                "merchant_name": {
                    "type": "string",
                    "database_type": "VARCHAR(255)",
                    "nullable": True,
                    "description": "Merchant name"
                }
            },
            "required": ["wise_id", "created_at", "updated_at", "amount", "currency", "status"]
        },
        "indexes": [
            {
                "name": "idx_transactions_created_at",
                "columns": ["created_at"],
                "type": "btree"
            },
            {
                "name": "idx_transactions_status_amount",
                "columns": ["status", "amount"],
                "type": "btree"
            }
        ]
    }


@pytest.fixture
def sample_invalid_pipeline_config():
    """Sample invalid pipeline configuration for error testing."""
    return {
        "pipeline_id": "invalid-pipeline",
        "name": "Invalid Pipeline",
        # Missing required fields like source, destination, streams
        "engine_config": {
            "batch_size": "not_a_number"  # Invalid type
        }
    }


@pytest.fixture
def sample_s3_error_responses():
    """Sample S3 error responses for testing."""
    return {
        "no_such_key": {
            "Error": {
                "Code": "NoSuchKey",
                "Message": "The specified key does not exist."
            }
        },
        "no_such_bucket": {
            "Error": {
                "Code": "NoSuchBucket",
                "Message": "The specified bucket does not exist."
            }
        },
        "access_denied": {
            "Error": {
                "Code": "AccessDenied",
                "Message": "Access Denied"
            }
        }
    }


@pytest.fixture
def environment_variables():
    """Sample environment variables for credential expansion testing."""
    return {
        "WISE_API_TOKEN": "wise_test_token_123",
        "SEVDESK_API_TOKEN": "Bearer sevdesk_test_token_456",
        "DB_PASSWORD": "super_secret_db_password",
        "WISE_PROFILE_ID": "12345",
        "SEVDESK_BANK_ACCOUNT_ID": "5936402"
    }


@pytest.fixture
def multi_stream_pipeline_config():
    """Sample pipeline configuration with multiple streams."""
    return {
        "pipeline_id": "multi-stream-pipeline",
        "name": "Multi-Stream Pipeline Test",
        "version": "1.0",
        "source": {
            "connection_id": "wise-host-id",
            "name": "Wise Platform"
        },
        "destination": {
            "connection_id": "database-host-id",
            "name": "Analytics Database"
        },
        "engine_config": {
            "batch_size": 500,
            "max_concurrent_batches": 5,
            "buffer_size": 10000,
            "schedule": {
                "type": "cron",
                "cron_expression": "0 */6 * * *",
                "timezone": "UTC"
            }
        },
        "streams": {
            "transfers-stream": {
                "name": "wise-transfers",
                "description": "Wise transfer transactions",
                "source": {
                    "endpoint_id": "wise-transfers-endpoint",
                    "replication_method": "incremental",
                    "cursor_field": "created",
                    "cursor_mode": "inclusive",
                    "safety_window_seconds": 300,
                    "primary_key": ["id"],
                    "tie_breaker_fields": ["id"]
                },
                "destination": {
                    "endpoint_id": "db-transfers-endpoint",
                    "refresh_mode": "upsert",
                    "batch_support": True,
                    "batch_size": 100
                }
            },
            "accounts-stream": {
                "name": "wise-accounts",
                "description": "Wise account information",
                "source": {
                    "endpoint_id": "wise-accounts-endpoint",
                    "replication_method": "full_refresh",
                    "primary_key": ["id"]
                },
                "destination": {
                    "endpoint_id": "db-accounts-endpoint",
                    "refresh_mode": "truncate_insert",
                    "batch_support": True,
                    "batch_size": 50
                }
            }
        },
        "error_handling": {
            "strategy": "dlq",
            "retry_failed_records": True,
            "max_retries": 5,
            "retry_delay": 10,
            "exponential_backoff": True,
            "error_categories": {
                "validation_error": "dlq",
                "transformation_error": "dlq",
                "api_error": "retry",
                "rate_limit_error": "retry_with_backoff",
                "authentication_error": "fail_fast"
            }
        },
        "monitoring": {
            "metrics_enabled": True,
            "log_level": "INFO",
            "checkpoint_interval": 100,
            "health_check_interval": 180,
            "progress_monitoring": "enabled",
            "alerts": {
                "error_rate_threshold": 0.05,
                "latency_threshold_ms": 5000
            }
        }
    }