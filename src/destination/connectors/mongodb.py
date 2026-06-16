"""MongoDB (nosql) destination handler.

Writes Arrow ``RecordBatch`` records to a MongoDB collection, implementing
the full ``BaseDestinationHandler`` contract:

* **Insert** — ``collection.insert_many(docs, ordered=False)``; a duplicate
  key error on one document does not abort the batch.
* **Upsert** — ``collection.bulk_write([UpdateOne({key: val}, {$set: doc},
  upsert=True) for doc in batch], ordered=False)``; conflict key set comes
  from the endpoint's ``write.conflict_keys``.
* **Truncate-insert** — ``collection.delete_many({})`` then ``insert_many``.
  Not atomic: a concurrent reader sees an empty collection mid-window.

**Idempotency** is implemented via a ``_batch_commits`` collection in the
same database that records ``(run_id, stream_id, batch_seq)`` together with
the committed cursor token. A pre-flight ``find_one`` on that triple gates
replays and returns ``ALREADY_COMMITTED`` so the engine does not double-write.

**DDL equivalent** (schemaless Mongo):

* Ensures the target collection exists via ``create_collection`` (swallowing
  ``CollectionInvalid`` when it already exists).
* Ensures a unique compound index on conflict keys when in upsert mode.
* Ensures the ``_batch_commits`` collection and its unique index exist on
  first write.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Mapping, Optional

import pyarrow as pa

from cdk.base_handler import BaseDestinationHandler, BatchWriteResult
from cdk.connection_runtime import ConnectionRuntime
from cdk.types import AckStatus, Cursor, SchemaSpec, WriteMode

logger = logging.getLogger(__name__)

_BATCH_COMMITS_COLLECTION = "_batch_commits"


def _arrow_to_doc_list(record_batch: pa.RecordBatch) -> List[Dict[str, Any]]:
    """Convert an Arrow RecordBatch to a list of row dicts."""
    columns = record_batch.schema.names
    col_arrays = [record_batch.column(i).to_pylist() for i in range(record_batch.num_columns)]
    return [
        {col: col_arrays[i][row] for i, col in enumerate(columns)}
        for row in range(record_batch.num_rows)
    ]


class MongoDbDestinationHandler(BaseDestinationHandler):
    """Destination handler for MongoDB collections (nosql connector kind)."""

    def __init__(self) -> None:
        self._runtime: Optional[ConnectionRuntime] = None
        self._connected: bool = False
        # stream_id → endpoint document
        self._stream_endpoints: Dict[str, Mapping[str, Any]] = {}
        # stream_id → configured write mode (set by configure_schema)
        self._stream_modes: Dict[str, WriteMode] = {}
        # stream_id → conflict key list (for upsert)
        self._stream_conflict_keys: Dict[str, List[str]] = {}
        # stream_id → (database_name, collection_name)
        self._stream_collections: Dict[str, tuple[str, str]] = {}
        # Whether _batch_commits is set up in the current run
        self._batch_commits_ready: bool = False

    @property
    def connector_type(self) -> str:
        return "mongodb"

    @property
    def supports_transactions(self) -> bool:
        return False

    @property
    def supports_upsert(self) -> bool:
        return True

    @property
    def supports_bulk_load(self) -> bool:
        return False

    def set_stream_endpoints(
        self, stream_endpoints: Mapping[str, Mapping[str, Any]]
    ) -> None:
        self._stream_endpoints = dict(stream_endpoints)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self, runtime: ConnectionRuntime) -> None:
        try:
            self._runtime = runtime
            runtime.acquire()
            await runtime.materialize()
            self._connected = True
            logger.debug(
                "MongoDB destination connected (default_database=%s)",
                runtime.mongo_default_database,
            )
        except Exception as exc:
            logger.error("Failed to connect to MongoDB destination: %s", exc)
            raise

    async def disconnect(self) -> None:
        if self._runtime:
            await self._runtime.close()
        self._connected = False

    async def health_check(self) -> bool:
        if not self._connected or self._runtime is None:
            return False
        try:
            client = self._runtime.mongo_client
            await client.admin.command("ping")
            return True
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Schema configuration
    # ------------------------------------------------------------------

    async def configure_schema(self, schema_spec: SchemaSpec) -> bool:
        stream_id = schema_spec.stream_id
        write_mode = schema_spec.write_mode

        endpoint_doc = self._stream_endpoints.get(stream_id)
        if not endpoint_doc:
            logger.error(
                "MongoDbDestinationHandler.configure_schema: no endpoint doc for %r",
                stream_id,
            )
            return False

        collection_name = endpoint_doc.get("collection")
        database_name = (
            endpoint_doc.get("database")
            or self._runtime.mongo_default_database
        )
        if not collection_name or not database_name:
            logger.error(
                "MongoDbDestinationHandler: endpoint for %r missing database/collection",
                stream_id,
            )
            return False

        self._stream_modes[stream_id] = write_mode
        self._stream_collections[stream_id] = (database_name, collection_name)

        write_spec = (
            (endpoint_doc.get("operations") or {}).get("write") or {}
        )
        conflict_keys: List[str] = list(write_spec.get("conflict_keys") or [])
        self._stream_conflict_keys[stream_id] = conflict_keys

        client = self._runtime.mongo_client
        db = client[database_name]

        # Ensure the target collection exists
        try:
            from pymongo.errors import CollectionInvalid
            await db.create_collection(collection_name)
        except Exception as exc:
            # CollectionInvalid means it already exists — that is fine
            exc_type = type(exc).__name__
            if "CollectionInvalid" not in exc_type:
                logger.debug("create_collection(%r): %s", collection_name, exc)

        # Ensure conflict-key index for upsert mode
        if write_mode == WriteMode.WRITE_MODE_UPSERT and conflict_keys:
            coll = db[collection_name]
            index_spec = [(k, 1) for k in conflict_keys]
            try:
                await coll.create_index(index_spec, unique=True, background=True)
            except Exception as exc:
                logger.warning(
                    "Failed to create conflict-key index on %r: %s",
                    collection_name,
                    exc,
                )

        await self._ensure_batch_commits(db)
        return True

    async def _ensure_batch_commits(self, db: Any) -> None:
        if self._batch_commits_ready:
            return
        try:
            from pymongo.errors import CollectionInvalid
            await db.create_collection(_BATCH_COMMITS_COLLECTION)
        except Exception as exc:
            exc_type = type(exc).__name__
            if "CollectionInvalid" not in exc_type:
                logger.debug("create_collection(_batch_commits): %s", exc)

        coll = db[_BATCH_COMMITS_COLLECTION]
        try:
            await coll.create_index(
                [("run_id", 1), ("stream_id", 1), ("batch_seq", 1)],
                unique=True,
                background=True,
            )
        except Exception as exc:
            logger.warning("Failed to create _batch_commits index: %s", exc)

        self._batch_commits_ready = True

    # ------------------------------------------------------------------
    # Batch writing
    # ------------------------------------------------------------------

    async def write_batch(
        self,
        run_id: str,
        stream_id: str,
        batch_seq: int,
        record_batch: pa.RecordBatch,
        record_ids: List[str],
        cursor: Cursor,
    ) -> BatchWriteResult:
        if stream_id not in self._stream_collections:
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_FATAL_FAILURE,
                records_written=0,
                failure_summary=f"Stream {stream_id!r} not configured; configure_schema must run first",
            )

        database_name, collection_name = self._stream_collections[stream_id]
        write_mode = self._stream_modes.get(stream_id, WriteMode.WRITE_MODE_INSERT)
        conflict_keys = self._stream_conflict_keys.get(stream_id, [])

        client = self._runtime.mongo_client
        db = client[database_name]
        commits_coll = db[_BATCH_COMMITS_COLLECTION]
        target_coll = db[collection_name]

        commit_key = {"run_id": run_id, "stream_id": stream_id, "batch_seq": batch_seq}

        # Idempotency check
        existing = await commits_coll.find_one(commit_key)
        if existing is not None:
            stored_token: bytes = existing.get("cursor_token") or b""
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_ALREADY_COMMITTED,
                records_written=0,
                committed_cursor=Cursor(token=stored_token),
            )

        docs = _arrow_to_doc_list(record_batch)
        if not docs:
            await commits_coll.insert_one(
                {**commit_key, "cursor_token": cursor.token}
            )
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_SUCCESS,
                records_written=0,
                committed_cursor=cursor,
            )

        try:
            records_written = await self._write_docs(
                target_coll, docs, write_mode, conflict_keys
            )
        except Exception as exc:
            logger.error(
                "MongoDB write_batch failed (run=%s stream=%s seq=%d): %s",
                run_id,
                stream_id,
                batch_seq,
                exc,
            )
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                failure_summary=str(exc),
            )

        # Record commit (best-effort: if this fails the batch will be retried
        # and insert_many will hit duplicate-key errors, which is safe with
        # ordered=False)
        try:
            await commits_coll.insert_one(
                {**commit_key, "cursor_token": cursor.token}
            )
        except Exception as exc:
            logger.warning(
                "Failed to record batch commit (run=%s seq=%d): %s — "
                "batch will be retried but is already written",
                run_id,
                batch_seq,
                exc,
            )

        return BatchWriteResult(
            status=AckStatus.ACK_STATUS_SUCCESS,
            records_written=records_written,
            committed_cursor=cursor,
        )

    async def _write_docs(
        self,
        collection: Any,
        docs: List[Dict[str, Any]],
        write_mode: WriteMode,
        conflict_keys: List[str],
    ) -> int:
        if write_mode == WriteMode.WRITE_MODE_TRUNCATE_INSERT:
            await collection.delete_many({})
            result = await collection.insert_many(docs, ordered=False)
            return len(result.inserted_ids)

        if write_mode == WriteMode.WRITE_MODE_UPSERT and conflict_keys:
            from pymongo import UpdateOne
            ops = [
                UpdateOne(
                    {k: doc.get(k) for k in conflict_keys},
                    {"$set": doc},
                    upsert=True,
                )
                for doc in docs
            ]
            result = await collection.bulk_write(ops, ordered=False)
            return result.upserted_count + result.modified_count

        # Default: insert (ordered=False so a dup-key on one doc doesn't abort)
        try:
            result = await collection.insert_many(docs, ordered=False)
            return len(result.inserted_ids)
        except Exception as exc:
            # BulkWriteError may carry partial results; log and propagate
            details = getattr(exc, "details", {})
            n_inserted = (details.get("nInserted") or 0) if details else 0
            if n_inserted > 0:
                logger.warning(
                    "insert_many partial write (%d inserted, error: %s)", n_inserted, exc
                )
                return n_inserted
            raise
