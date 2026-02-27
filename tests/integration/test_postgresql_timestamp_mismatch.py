"""Test PostgreSQL timestamp type mismatch errors."""

import pytest
import os
from datetime import datetime

from sqlalchemy import text
from src.shared.database_utils import create_database_engine, acquire_connection


@pytest.mark.integration
@pytest.mark.slow
class TestPostgreSQLTimestampMismatch:
    """Integration tests for PostgreSQL timestamp type handling."""

    @pytest.fixture
    async def engine(self):
        """Create shared database engine with database connection."""
        if not os.getenv("TEST_POSTGRES_URL"):
            pytest.skip("TEST_POSTGRES_URL not set")

        postgres_url = os.getenv("TEST_POSTGRES_URL")
        if postgres_url:
            import urllib.parse
            parsed = urllib.parse.urlparse(postgres_url)
            config = {
                "driver": "postgresql",
                "host": parsed.hostname or "localhost",
                "port": parsed.port or 5432,
                "user": parsed.username or "postgres",
                "password": parsed.password or "",
                "database": parsed.path.lstrip('/') or "test",
            }
        else:
            config = {
                "driver": "postgresql",
                "host": os.getenv("POSTGRES_HOST", "localhost"),
                "port": int(os.getenv("POSTGRES_PORT", "5432")),
                "user": os.getenv("POSTGRES_USER", "postgres"),
                "password": os.getenv("POSTGRES_PASSWORD", ""),
                "database": os.getenv("POSTGRES_DB", "test"),
            }

        eng, _ = await create_database_engine(config, require_port=True)
        try:
            yield eng
        finally:
            await eng.dispose()

    @pytest.fixture
    async def test_table(self, engine):
        """Create test table with TIMESTAMPTZ columns."""
        schema_name = "wise_data"
        table_name = "transactions"

        async with engine.begin() as conn:
            await conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
            await conn.execute(text(f"DROP TABLE IF EXISTS {schema_name}.{table_name}"))
            await conn.execute(text(f"""
                CREATE TABLE {schema_name}.{table_name} (
                    id BIGINT PRIMARY KEY,
                    user_id BIGINT,
                    target_account BIGINT,
                    source_account BIGINT,
                    quote BIGINT,
                    quote_uuid VARCHAR(255),
                    status VARCHAR(255) NOT NULL,
                    reference VARCHAR(255),
                    details_reference VARCHAR(255),
                    rate DECIMAL(15,6),
                    created TIMESTAMPTZ NOT NULL,
                    business BIGINT,
                    has_active_issues BOOLEAN,
                    source_currency VARCHAR(3),
                    source_value DECIMAL(15,2),
                    target_currency VARCHAR(3),
                    target_value DECIMAL(15,2),
                    customer_transaction_id VARCHAR(255),
                    originator_data JSONB,
                    raw_data TEXT,
                    synced_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ
                )
            """))

        yield (schema_name, table_name)

        async with engine.begin() as conn:
            await conn.execute(text(f"DROP TABLE IF EXISTS {schema_name}.{table_name}"))
            await conn.execute(text(f"DROP SCHEMA IF EXISTS {schema_name}"))

    @pytest.fixture
    def batch_with_string_timestamps(self):
        """Batch with string timestamps that will fail."""
        return [
            {
                'id': 12821221,
                'user_id': 3631891,
                'target_account': 6179781,
                'source_account': 6179811,
                'quote': None,
                'quote_uuid': None,
                'status': 'outgoing_payment_sent',
                'reference': '',
                'details_reference': '',
                'rate': 1.06245,
                'created': datetime(2016, 12, 13, 22, 57, 3),
                'business': None,
                'has_active_issues': False,
                'source_currency': 'EUR',
                'source_value': 235.31,
                'target_currency': 'USD',
                'target_value': 250.0,
                'customer_transaction_id': None,
                'originator_data': None,
                'raw_data': 'record_to_json()',
                'synced_at': '2025-08-19T14:16:14.290559+00:00',
                'updated_at': '2025-08-19T14:16:14.290571+00:00'
            }
        ]

    @pytest.mark.asyncio
    async def test_insert_fails_with_string_timestamps(
        self, engine, test_table, batch_with_string_timestamps
    ):
        """Test that insert fails when TIMESTAMPTZ columns receive string values."""
        schema_name, table_name = test_table
        record = batch_with_string_timestamps[0]
        columns = list(record.keys())
        placeholders = ", ".join([f"${i+1}" for i in range(len(columns))])
        columns_str = ", ".join(columns)
        query = f"INSERT INTO {schema_name}.{table_name} ({columns_str}) VALUES ({placeholders})"

        async with acquire_connection(engine) as conn:
            with pytest.raises(Exception) as exc_info:
                await conn.exec_driver_sql(query, tuple(record.values()))

        error_msg = str(exc_info.value)
        assert "expected a datetime.date or datetime.datetime instance, got 'str'" in error_msg

    @pytest.mark.asyncio
    async def test_insert_succeeds_with_datetime_objects(
        self, engine, test_table
    ):
        """Test that insert succeeds when all timestamp fields are datetime objects."""
        schema_name, table_name = test_table
        record = {
            'id': 12821221,
            'user_id': 3631891,
            'target_account': 6179781,
            'source_account': 6179811,
            'quote': None,
            'quote_uuid': None,
            'status': 'outgoing_payment_sent',
            'reference': '',
            'details_reference': '',
            'rate': 1.06245,
            'created': datetime(2016, 12, 13, 22, 57, 3),
            'business': None,
            'has_active_issues': False,
            'source_currency': 'EUR',
            'source_value': 235.31,
            'target_currency': 'USD',
            'target_value': 250.0,
            'customer_transaction_id': None,
            'originator_data': None,
            'raw_data': 'record_to_json()',
            'synced_at': datetime(2025, 8, 19, 14, 16, 14, 290559),
            'updated_at': datetime(2025, 8, 19, 14, 16, 14, 290571),
        }

        columns = list(record.keys())
        placeholders = ", ".join([f"${i+1}" for i in range(len(columns))])
        columns_str = ", ".join(columns)
        query = f"INSERT INTO {schema_name}.{table_name} ({columns_str}) VALUES ({placeholders})"

        async with engine.begin() as conn:
            await conn.exec_driver_sql(query, tuple(record.values()))

        async with acquire_connection(engine) as conn:
            result = await conn.execute(
                text(f"SELECT id FROM {schema_name}.{table_name} WHERE id = 12821221")
            )
            rows = result.fetchall()
            assert len(rows) == 1

    def test_parameter_position_verification(self, batch_with_string_timestamps):
        """Test that synced_at is at the expected parameter position."""
        columns = list(batch_with_string_timestamps[0].keys())
        synced_at_index = columns.index('synced_at')

        # Parameter positions are 1-indexed
        assert synced_at_index == 20  # Parameter $21
        assert columns[20] == 'synced_at'
