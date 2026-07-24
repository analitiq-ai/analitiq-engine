"""Capability properties follow what each handler can actually do.

supports_auto_create / supports_truncate are derived from the handler, never
hardcoded in the servicer. A non-relational destination (stdout/file/api)
cannot create a relation or truncate it; a SQL destination can.
"""

import pytest

from cdk.sql.generic import GenericSQLConnector
from src.destination.connectors.api import ApiDestinationHandler
from src.destination.connectors.file import FileDestinationHandler
from src.destination.connectors.stream import StreamDestinationHandler


@pytest.mark.unit
class TestCapabilityProperties:
    @pytest.mark.parametrize(
        "handler",
        [
            StreamDestinationHandler(),
            FileDestinationHandler(),
            ApiDestinationHandler(),
        ],
    )
    def test_non_relational_handlers_cannot_auto_create_or_truncate(self, handler):
        assert handler.supports_auto_create is False
        assert handler.supports_truncate is False

    def test_sql_handler_supports_auto_create(self):
        handler = GenericSQLConnector()
        assert handler.supports_auto_create is True

    @pytest.mark.parametrize("adbc_only", [False, True], ids=["sqlalchemy", "adbc"])
    def test_sql_handler_gates_insert_and_truncate_on_the_stage_predicate(
        self, adbc_only
    ):
        # Advertised modes must match what the schema handshake accepts:
        # every write on every transport runs the stage cycle (issues
        # #388/#389), so a connector without declared capabilities and a
        # stage-rendering dialect advertises neither INSERT nor
        # TRUNCATE_INSERT — identically on both transports.
        from cdk.sql.capabilities import SqlCapabilities
        from cdk.sql.dialects import SqlDialect

        handler = GenericSQLConnector()
        handler._adbc_only = adbc_only
        assert handler.supports_insert is False
        assert handler.supports_truncate is False

        class _StagingDialect(SqlDialect):
            def stage_table_sql(self, stage, target, *, temp):
                return "CREATE TABLE ..."

        handler.dialect = _StagingDialect()
        handler._capabilities = SqlCapabilities.from_declaration(
            {
                "catalog": "none",
                "session_targeting": "per_statement",
                "merge_form": "none",
                "bulk_load": "none",
                "stage": {
                    "scope": "temp",
                    "schema": "target",
                    "transactional_ddl": True,
                },
            }
        )
        assert handler.supports_insert is True
        assert handler.supports_truncate is True
