"""MongoDB (nosql) source connector.

Reads documents from a MongoDB collection and yields them as Arrow
``RecordBatch`` objects, following the same ``Readable`` contract as the
SQL and API source connectors.

Pagination uses ``_id``-keyset paging (``{_id: {$gt: last_id}}`` with
``sort(_id, 1)``), not ``skip()``/``limit()``, which degrades on large
collections. Incremental replication uses a caller-declared cursor field
(``updatedAt``, ``_id``, …) with a ``$gt``/``$gte`` filter applied before
the keyset page.

BSON special values are coerced to Python primitives before Arrow
ingestion:

* ``bson.ObjectId`` → ``str`` (hex representation)
* ``bson.Decimal128`` → ``str`` (preserves precision without lossy float cast)
* ``bson.Binary`` → ``bytes``
* ``datetime.datetime`` → Arrow normalises it as ``Timestamp``
* Embedded documents and arrays are passed through as-is; Arrow infers
  the nested type from the first non-null value in each batch (SchemaContract
  is applied at the destination to cast to the declared schema).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Dict, List, Optional

import pyarrow as pa

from .base import BaseConnector, ConnectionError, ReadError
from cdk.connection_runtime import ConnectionRuntime
from cdk.schema_contract import SchemaContract
from cdk.types import CheckpointStore

logger = logging.getLogger(__name__)

# _id sentinel used to detect the start of a fresh (non-resumed) keyset page.
_ZERO_OID_STR = "000000000000000000000000"


def _coerce_bson(value: Any) -> Any:
    """Convert BSON-specific types to Python primitives Arrow can handle."""
    try:
        from bson import ObjectId, Decimal128, Binary
    except ImportError:
        return value

    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, Decimal128):
        return str(value)
    if isinstance(value, Binary):
        return bytes(value)
    if isinstance(value, dict):
        return {k: _coerce_bson(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_coerce_bson(v) for v in value]
    return value


def _coerce_document(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Coerce all values in a document from BSON to Arrow-safe Python."""
    return {k: _coerce_bson(v) for k, v in doc.items()}


def _resolve_records_schema(endpoint_doc: Dict[str, Any]) -> Dict[str, Any]:
    """Extract the ``properties`` schema from a collection endpoint document.

    Supports both the MongoDB-native layout (top-level ``properties`` key)
    and the API-style layout (``operations.read.response.records.items``),
    so a connector definition can use either shape.
    """
    if "properties" in endpoint_doc:
        return endpoint_doc["properties"]
    read_op = ((endpoint_doc.get("operations") or {}).get("read") or {})
    items = (
        ((read_op.get("response") or {}).get("records") or {}).get("items") or {}
    )
    return items.get("properties") or {}


class MongoDbSourceConnector(BaseConnector):
    """Source connector for MongoDB collections (nosql connector kind)."""

    def __init__(self, name: str = "MongoDbSourceConnector") -> None:
        super().__init__(name)
        self._runtime: Optional[ConnectionRuntime] = None

    async def connect(self, runtime: ConnectionRuntime) -> None:
        try:
            self._runtime = runtime
            runtime.acquire()
            await runtime.materialize()
            self.is_connected = True
            logger.debug(
                "MongoDB source connected (database=%s)",
                runtime.mongo_default_database,
            )
        except Exception as exc:
            logger.error("Failed to connect to MongoDB: %s", exc)
            raise ConnectionError(f"MongoDB connection failed: {exc}") from exc

    async def disconnect(self) -> None:
        if self._runtime:
            await self._runtime.close()
        self.is_connected = False

    async def write_batch(self, batch: Any, config: Any) -> None:  # pragma: no cover
        raise NotImplementedError("MongoDbSourceConnector is read-only")

    # ------------------------------------------------------------------
    # Public read entry point (Readable contract)
    # ------------------------------------------------------------------

    async def read_batches(
        self,
        runtime: ConnectionRuntime,
        config: Dict[str, Any],
        *,
        checkpoint: CheckpointStore,
        stream_name: str,
        partition: Optional[Dict[str, Any]] = None,
        batch_size: int = 1000,
    ) -> AsyncIterator[pa.RecordBatch]:
        try:
            await self.connect(runtime)
            async for batch in self._read_batches_impl(
                config,
                checkpoint=checkpoint,
                stream_name=stream_name,
                batch_size=batch_size,
            ):
                yield batch
        finally:
            await self.disconnect()

    async def _read_batches_impl(
        self,
        config: Dict[str, Any],
        *,
        checkpoint: CheckpointStore,
        stream_name: str,
        batch_size: int,
    ) -> AsyncIterator[pa.RecordBatch]:
        endpoint_doc = config.get("endpoint_document")
        if not endpoint_doc:
            raise ReadError("MongoDbSourceConnector: config missing 'endpoint_document'")

        collection_name = endpoint_doc.get("collection")
        database_name = endpoint_doc.get("database") or self._runtime.mongo_default_database
        if not collection_name:
            raise ReadError(
                f"MongoDbSourceConnector: endpoint_document missing 'collection' field"
            )
        if not database_name:
            raise ReadError(
                "MongoDbSourceConnector: no database specified in endpoint or connector definition"
            )

        stream_source = config.get("stream_source") or {}
        replication = stream_source.get("replication") or {}
        replication_method = replication.get("method", "full_refresh")
        cursor_field = replication.get("cursor_field")
        if isinstance(cursor_field, list):
            cursor_field = cursor_field[0] if cursor_field else None
        safety_window_seconds = int(replication.get("safety_window_seconds") or 0)

        records_schema = _resolve_records_schema(endpoint_doc)
        schema_contract = SchemaContract(records_schema) if records_schema else None

        client = self._runtime.mongo_client
        db = client[database_name]
        collection = db[collection_name]

        # Load incremental cursor value from checkpoint
        cursor_value: Any = None
        if replication_method == "incremental" and cursor_field:
            raw_cursor = checkpoint.get_cursor(stream_name)
            if raw_cursor:
                cursor_value = raw_cursor

        # Determine safety-window cutoff for incremental runs
        cutoff: Optional[datetime] = None
        if safety_window_seconds > 0 and cursor_value is not None:
            try:
                if isinstance(cursor_value, datetime):
                    cutoff = datetime.now(tz=timezone.utc)
                    cursor_value = _subtract_safety_window(cursor_value, safety_window_seconds)
            except Exception:
                pass

        # Build the incremental filter
        incremental_filter: Dict[str, Any] = {}
        if replication_method == "incremental" and cursor_field and cursor_value is not None:
            incremental_filter = {cursor_field: {"$gte": cursor_value}}

        # Keyset paging: start after zero ObjectId for fresh runs
        try:
            from bson import ObjectId
            last_id: Any = ObjectId(_ZERO_OID_STR)
        except ImportError:
            last_id = None

        max_cursor_seen: Any = cursor_value

        while True:
            page_filter: Dict[str, Any] = {**incremental_filter}
            if last_id is not None:
                page_filter["_id"] = {"$gt": last_id}

            cursor_obj = (
                collection.find(page_filter)
                .sort("_id", 1)
                .limit(batch_size)
            )

            docs: List[Dict[str, Any]] = []
            async for doc in cursor_obj:
                docs.append(doc)

            if not docs:
                break

            last_id = docs[-1].get("_id", last_id)

            # Track the high-water mark for the declared cursor field
            if cursor_field:
                for doc in docs:
                    val = doc.get(cursor_field)
                    if val is not None and (
                        max_cursor_seen is None or val > max_cursor_seen
                    ):
                        max_cursor_seen = val

            coerced = [_coerce_document(d) for d in docs]

            if schema_contract is not None:
                batch = schema_contract.from_pylist(coerced)
            else:
                batch = pa.RecordBatch.from_pylist(coerced)

            self.metrics["records_read"] += len(coerced)
            self.metrics["batches_read"] += 1
            yield batch

            if len(docs) < batch_size:
                break

        # Persist the high-water mark so the next run resumes from here
        if replication_method == "incremental" and cursor_field and max_cursor_seen is not None:
            checkpoint.save_cursor(stream_name, max_cursor_seen)


def _subtract_safety_window(value: datetime, seconds: int) -> datetime:
    """Subtract *seconds* from a datetime, returning a tz-aware result."""
    from datetime import timedelta
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value - timedelta(seconds=seconds)
