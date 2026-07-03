"""MongoDB (nosql) source connector.

Reads documents from a MongoDB collection and yields them as Arrow
``RecordBatch`` objects, following the same ``Readable`` contract as the
SQL and API source connectors.

Pagination uses ``_id``-keyset paging — ``{_id: {$gt: last_id}}`` with
``sort(_id, 1)`` — not ``skip()``/``limit()``, which degrades on large
collections. The keyset filter is omitted on the first page so any ``_id``
type (ObjectId, string, integer, compound) works correctly. Incremental
replication adds a ``$gte`` filter on the declared cursor field combined with
the keyset clause.

BSON special values are coerced to Python primitives before Arrow ingestion:

* ``bson.ObjectId`` → ``str`` (24-character hex representation)
* ``bson.Decimal128`` → ``str`` (preserves precision without lossy float cast)
* ``bson.Binary`` → ``bytes``
* Embedded documents and arrays are recursively coerced.

``datetime.datetime`` values pass through unchanged; Arrow infers them as
``Timestamp`` during ``from_pylist``.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator, Dict, List, Optional

import pyarrow as pa

from .base import BaseConnector, ConnectionError, ReadError, TransientReadError
from cdk.connection_runtime import ConnectionRuntime
from cdk.schema_contract import SchemaContract
from cdk.types import CheckpointStore

logger = logging.getLogger(__name__)

try:
    from pymongo.errors import (
        AutoReconnect,
        CursorNotFound,
        ServerSelectionTimeoutError,
        WaitQueueTimeoutError,
    )
    _TRANSIENT_MOTOR_ERRORS: tuple = (
        AutoReconnect,
        CursorNotFound,
        ServerSelectionTimeoutError,
        WaitQueueTimeoutError,
    )
except ImportError:
    logger.warning(
        "pymongo.errors unavailable; Motor transient errors during cursor iteration "
        "will be classified as fatal ReadErrors until pymongo is installed."
    )
    _TRANSIENT_MOTOR_ERRORS = ()


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
    return {k: _coerce_bson(v) for k, v in doc.items()}


# Sentinel keys used to round-trip non-JSON BSON cursor types through the
# JSON-only checkpoint layer (mirroring MongoDB Extended JSON's ``$oid`` /
# ``$numberDecimal``). A bare string bound does not match ObjectId- or
# Decimal128-valued fields because Mongo range predicates are type-bracketed,
# so the type is preserved on save and rehydrated on load.
_OBJECTID_CURSOR_TAG = "$oid"
_DECIMAL128_CURSOR_TAG = "$numberDecimal"


def _encode_cursor_value(value: Any) -> Any:
    """Make a cursor value JSON-safe while preserving its BSON type."""
    try:
        from bson import ObjectId, Decimal128
    except ImportError:
        return value
    if isinstance(value, ObjectId):
        return {_OBJECTID_CURSOR_TAG: str(value)}
    if isinstance(value, Decimal128):
        return {_DECIMAL128_CURSOR_TAG: str(value)}
    return value


def _decode_cursor_value(value: Any) -> Any:
    """Rehydrate a cursor value produced by :func:`_encode_cursor_value`."""
    if not isinstance(value, dict):
        return value
    keys = set(value)
    if keys == {_OBJECTID_CURSOR_TAG}:
        cls_name, raw = "ObjectId", value[_OBJECTID_CURSOR_TAG]
    elif keys == {_DECIMAL128_CURSOR_TAG}:
        cls_name, raw = "Decimal128", value[_DECIMAL128_CURSOR_TAG]
    else:
        return value
    try:
        import bson
    except ImportError:
        # pymongo/bson absent: the connector cannot query Mongo at all, so fail
        # loudly rather than silently filtering on a wrongly-typed cursor.
        raise ReadError(
            f"Checkpoint holds a {cls_name} cursor but bson is not installed; "
            "cannot rehydrate the cursor for the incremental filter"
        )
    return getattr(bson, cls_name)(raw)


def _resolve_records_schema(endpoint_doc: Dict[str, Any]) -> Dict[str, Any]:
    """Return the schema wrapper dict to pass to ``SchemaContract``.

    Supports both the MongoDB-native layout (top-level ``properties`` key)
    and the API-style layout (``operations.read.response.records.items``).
    Returns an empty dict (falsy) when no typed schema is declared.

    ``SchemaContract`` expects the *wrapping* dict that carries the
    ``"columns"`` or ``"properties"`` key — not the bare field map itself.
    """
    if "properties" in endpoint_doc or "columns" in endpoint_doc:
        return endpoint_doc
    read_op = ((endpoint_doc.get("operations") or {}).get("read") or {})
    items = (
        ((read_op.get("response") or {}).get("records") or {}).get("items") or {}
    )
    return items


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
        runtime, self._runtime = self._runtime, None
        if runtime:
            await runtime.close()
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
        """Read upstream documents as Arrow batches.

        Manages its own connect/disconnect lifecycle — callers must not call
        ``connect()`` separately.
        """
        try:
            await self.connect(runtime)
            async for batch in self._read_batches_impl(
                config,
                checkpoint=checkpoint,
                stream_name=stream_name,
                partition=partition,
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
        partition: Optional[Dict[str, Any]],
        batch_size: int,
    ) -> AsyncIterator[pa.RecordBatch]:
        endpoint_doc = config.get("endpoint_document")
        if not endpoint_doc:
            raise ReadError("MongoDbSourceConnector: config missing 'endpoint_document'")

        collection_name = endpoint_doc.get("collection")
        database_name = endpoint_doc.get("database") or self._runtime.mongo_default_database
        if not collection_name:
            raise ReadError(
                "MongoDbSourceConnector: endpoint_document missing 'collection' field"
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

        # Load the stored cursor value from checkpoint (async Protocol).
        # The checkpoint stores {"cursor": value}; extract the raw value.
        cursor_value: Any = None
        if replication_method == "incremental" and cursor_field:
            cursor_state = await checkpoint.get_cursor(stream_name, partition)
            cursor_value = _decode_cursor_value((cursor_state or {}).get("cursor"))

        # Keep a pre-rollback copy so max_cursor_seen never drifts backwards:
        # if safety_window_seconds rolls cursor_value back for querying, we still
        # want the saved cursor to be at least as recent as the previous run.
        initial_cursor_value = cursor_value

        # Roll the incremental cursor back by the safety window so late-arriving
        # records are re-read. Capture the run-start timestamp as an upper bound
        # for max_cursor_seen so the saved checkpoint never advances past "now",
        # keeping the next run inside the same lookback window.
        cutoff: Optional[datetime] = None
        if safety_window_seconds > 0 and cursor_value is not None:
            if isinstance(cursor_value, datetime):
                cutoff = datetime.now(tz=timezone.utc)
                cursor_value = _subtract_safety_window(cursor_value, safety_window_seconds)
            else:
                logger.warning(
                    "safety_window_seconds configured for stream %r but cursor "
                    "value is %s (not datetime); safety window not applied",
                    stream_name,
                    type(cursor_value).__name__,
                )

        incremental_filter: Dict[str, Any] = {}
        if replication_method == "incremental" and cursor_field and cursor_value is not None:
            incremental_filter = {cursor_field: {"$gte": cursor_value}}

        # Keyset paging on _id. Start with None so the first page has no _id
        # filter — this works for any _id type (ObjectId, string, int, compound).
        last_id: Any = None
        # Use the pre-rollback cursor as the floor so a run with no new rows
        # never saves a cursor earlier than the previous run's high-water mark.
        max_cursor_seen: Any = initial_cursor_value

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
            try:
                async for doc in cursor_obj:
                    docs.append(doc)
            except _TRANSIENT_MOTOR_ERRORS as exc:
                logger.warning(
                    "Transient Motor error reading '%s' "
                    "(stream=%s, database=%s, last_id=%r): %s",
                    collection_name, stream_name, database_name, last_id, exc,
                    exc_info=True,
                )
                raise TransientReadError(
                    f"Transient Motor error reading '{collection_name}' "
                    f"(stream={stream_name!r}, database={database_name!r}): {exc}"
                ) from exc
            except Exception as exc:  # CancelledError is BaseException in 3.8+; propagates freely
                logger.error(
                    "Unexpected error reading '%s' "
                    "(stream=%s, database=%s, last_id=%r): %s",
                    collection_name, stream_name, database_name, last_id, exc,
                    exc_info=True,
                )
                raise ReadError(
                    f"MongoDB read error on '{collection_name}' "
                    f"(stream={stream_name!r}, database={database_name!r}): {exc}"
                ) from exc

            if not docs:
                break

            last_id = docs[-1].get("_id", last_id)

            if cursor_field:
                for doc in docs:
                    val = doc.get(cursor_field)
                    # Normalise tz-naive datetimes from Motor (tz_aware=False default)
                    # so comparisons with a tz-aware max_cursor_seen don't crash.
                    if isinstance(val, datetime) and val.tzinfo is None:
                        val = val.replace(tzinfo=timezone.utc)
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

        # Advance the checkpoint, capped by the safety-window cutoff so the
        # next run always re-reads the lookback window.
        if replication_method == "incremental" and cursor_field and max_cursor_seen is not None:
            value_to_save = (
                min(max_cursor_seen, cutoff)
                if cutoff is not None and isinstance(max_cursor_seen, datetime)
                else max_cursor_seen
            )
            # ObjectId is not JSON-serialisable; encode it with type information
            # so the next run rehydrates the correct BSON type before building
            # the {$gte: ...} filter (a bare string bound would not match
            # ObjectId-valued fields — Mongo range predicates are type-bracketed).
            await checkpoint.save_cursor(
                stream_name, partition, {"cursor": _encode_cursor_value(value_to_save)}
            )


def _subtract_safety_window(value: datetime, seconds: int) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value - timedelta(seconds=seconds)
