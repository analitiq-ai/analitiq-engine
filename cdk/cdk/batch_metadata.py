"""The batch-level slot for what a source page reported about itself.

An API read declares ``response.metadata`` -- named values such as a
provider's total count or its remaining rate-limit budget, resolved once per
page. The values belong to the batch that page became, so they ride the
batch: Arrow schema metadata is carried inside the IPC bytes on every hop
(source worker -> engine -> destination) and needs no wire field of its own.

One key, one JSON document. Written by the read path, read by the engine's
extract stage; both call this module so the encoding is defined once.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

import pyarrow as pa

from .json_utils import decimals_to_float

__all__ = [
    "RESPONSE_METADATA_KEY",
    "response_metadata_of",
    "with_response_metadata",
]

#: The Arrow schema-metadata key the resolved ``response.metadata`` lives under.
RESPONSE_METADATA_KEY = b"analitiq.response_metadata"


def with_response_metadata(
    batch: pa.RecordBatch, metadata: Mapping[str, Any]
) -> pa.RecordBatch:
    """Return *batch* carrying *metadata* in its schema-metadata slot."""
    existing = dict(batch.schema.metadata or {})
    document = json.dumps(decimals_to_float(dict(metadata)), allow_nan=False)
    existing[RESPONSE_METADATA_KEY] = document.encode("utf-8")
    return batch.replace_schema_metadata(existing)


def response_metadata_of(batch: pa.RecordBatch) -> dict[str, Any] | None:
    """Return the response metadata *batch* carries, or ``None`` when it has none.

    ``None`` is a batch whose read declared no metadata -- a database read,
    or an API read without the block. A declared block always lands as a
    dict, even when every value resolved to ``None``.
    """
    stored = (batch.schema.metadata or {}).get(RESPONSE_METADATA_KEY)
    if stored is None:
        return None
    decoded = json.loads(stored)
    if not isinstance(decoded, dict):
        raise ValueError(
            f"batch schema metadata {RESPONSE_METADATA_KEY!r} holds a "
            f"{type(decoded).__name__}, not an object"
        )
    return decoded
