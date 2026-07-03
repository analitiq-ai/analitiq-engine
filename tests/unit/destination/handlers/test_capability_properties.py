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

    def test_sql_handler_supports_auto_create_and_truncate(self):
        # The properties are unconditional capability literals (no instance
        # state), so a bare instance is enough to assert them without a live
        # database runtime.
        handler = GenericSQLConnector.__new__(GenericSQLConnector)
        assert handler.supports_auto_create is True
        assert handler.supports_truncate is True
