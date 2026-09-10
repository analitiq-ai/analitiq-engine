"""The cursor epoch-format vocabulary shared by replication and response_schema.

One source of truth for the two things both modules need to agree on: how
large a unit each ``cursor_bounds`` format represents (:data:`EPOCH_UNIT`),
and which declared ``encoding: {"name": "epoch", "unit": ...}`` unit maps to
which format (:data:`CURSOR_EPOCH_FORMAT`). A separate module, not an import
between the two: ``replication`` imports ``FieldDeclaration`` from
``response_schema``, so the reverse import would cycle.

MICROSECOND/NANOSECOND/DAY have no entry: ``cursor_bounds`` only ever reads
an epoch cursor back in seconds or milliseconds.
"""

from datetime import timedelta
from typing import Final

EPOCH_UNIT: Final[dict[str, timedelta]] = {
    "epoch_seconds": timedelta(seconds=1),
    "epoch_milliseconds": timedelta(milliseconds=1),
}

CURSOR_EPOCH_FORMAT: Final[dict[str, str]] = {
    "SECOND": "epoch_seconds",
    "MILLISECOND": "epoch_milliseconds",
}
