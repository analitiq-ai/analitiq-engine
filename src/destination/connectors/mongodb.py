"""MongoDB (nosql) destination handler.

Writes Arrow ``RecordBatch`` records to a MongoDB collection, implementing
the full ``BaseDestinationHandler`` contract:

* **Insert** — ``collection.insert_many(docs, ordered=False)``; a duplicate
  key error on *some* documents does not abort the batch. A batch where
  *all* documents are duplicates is treated as an idempotent success (returns
  0 records written).
* **Upsert** — ``collection.bulk_write([UpdateOne({key: val}, {$set: doc},
  upsert=True) for doc in batch], ordered=False)``; conflict key set is read
  from ``_write_conflict_keys`` (set by the engine after primary-key fallback
  resolution) or falls back to the endpoint's ``write.conflict_keys``.
* **Truncate-insert** — ``collection.delete_many({})`` then ``insert_many``.
  Not atomic: a concurrent reader sees an empty collection mid-window.

**Idempotency** is implemented via a ``_batch_commits`` collection in the
same database that records ``(run_id, stream_id, batch_seq)`` together with
the committed cursor token. A pre-flight ``find_one`` on that triple gates
replays and returns ``ALREADY_COMMITTED`` so the engine does not double-write.
The ``_batch_commits`` collection is tracked per-database: if two streams
target different databases, each database gets its own collection and index.

**DDL equivalent** (schemaless Mongo):

* Ensures the target collection exists via ``create_collection`` (swallowing
  ``CollectionInvalid`` when it already exists; re-raising anything else).
* Ensures a unique compound index on conflict keys when in upsert mode.
* Ensures the ``_batch_commits`` collection and its unique index exist on
  first write to a given database.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Set

import pyarrow as pa

from cdk.base_handler import BaseDestinationHandler, BatchWriteResult
from cdk.connection_runtime import ConnectionRuntime
from cdk.json_utils import decode_json_fields
from cdk.types import AckStatus, Cursor, SchemaSpec, WriteMode

logger = logging.getLogger(__name__)

_BATCH_COMMITS_COLLECTION = "_batch_commits"


@dataclass
class _MongoStreamState:
    """All per-stream configuration, set atomically by configure_schema."""
    database_name: str
    collection_name: str
    write_mode: WriteMode
    conflict_keys: List[str] = field(default_factory=list)
    json_fields: Set[str] = field(default_factory=set)


def _collect_json_fields(endpoint_doc: Dict[str, Any]) -> Set[str]:
    """Return field names declared with ``arrow_type: "Json"`` in the endpoint."""
    props = endpoint_doc.get("properties") or {}
    return {
        name for name, defn in props.items()
        if isinstance(defn, dict) and defn.get("arrow_type") == "Json"
    }


def _arrow_to_doc_list(record_batch: pa.RecordBatch) -> List[Dict[str, Any]]:
    """Motor's insert_many/bulk_write require plain Python dicts."""
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
        self._stream_endpoints: Dict[str, Mapping[str, Any]] = {}
        self._streams: Dict[str, _MongoStreamState] = {}
        # Databases whose _batch_commits collection and index are ready.
        self._batch_commits_ready: Set[str] = set()

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
            try:
                await runtime.close()
            finally:
                self._runtime = None
            logger.error("Failed to connect to MongoDB destination: %s", exc, exc_info=True)
            raise

    async def disconnect(self) -> None:
        runtime, self._runtime = self._runtime, None
        if runtime:
            await runtime.close()
        self._connected = False

    async def health_check(self) -> bool:
        if not self._connected or self._runtime is None:
            return False
        try:
            client = self._runtime.mongo_client
            await client.admin.command("ping")
            return True
        except Exception as exc:
            logger.warning(
                "MongoDB health_check failed: %s", exc
            )
            return False

    # ------------------------------------------------------------------
    # Schema configuration
    # ------------------------------------------------------------------

    async def configure_schema(self, schema_spec: SchemaSpec) -> bool:
        if self._runtime is None:
            logger.error(
                "configure_schema called before connect() for stream %r",
                schema_spec.stream_id,
            )
            return False

        stream_id = schema_spec.stream_id
        write_mode = schema_spec.write_mode
        if write_mode == WriteMode.WRITE_MODE_UNSPECIFIED:
            logger.warning(
                "Stream %r has WRITE_MODE_UNSPECIFIED; treating as INSERT. "
                "This may indicate a configuration error.",
                stream_id,
            )

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

        write_spec = (
            (endpoint_doc.get("operations") or {}).get("write") or {}
        )
        # Prefer the engine-enriched key list (set by run_destination_mode after
        # resolving primary_keys fallback) over the static endpoint declaration.
        conflict_keys: List[str] = list(
            endpoint_doc.get("_write_conflict_keys")
            or write_spec.get("conflict_keys")
            or []
        )

        client = self._runtime.mongo_client
        db = client[database_name]

        try:
            await self._ensure_collection(db, collection_name)
        except Exception as exc:
            logger.error(
                "configure_schema: failed to ensure collection %r for stream %r: %s",
                collection_name, stream_id, exc,
                exc_info=True,
            )
            return False

        if write_mode == WriteMode.WRITE_MODE_UPSERT:
            if not conflict_keys:
                logger.error(
                    "configure_schema: UPSERT mode requires conflict_keys for stream %r "
                    "but none were provided; UPSERT cannot be satisfied without a key set",
                    stream_id,
                )
                return False
            coll = db[collection_name]
            index_spec = [(k, 1) for k in conflict_keys]
            try:
                await coll.create_index(index_spec, unique=True, background=True)
            except Exception as exc:
                logger.error(
                    "configure_schema: failed to create conflict-key index on %r for "
                    "stream %r: %s — UPSERT idempotency cannot be guaranteed",
                    collection_name, stream_id, exc,
                    exc_info=True,
                )
                return False

        try:
            await self._ensure_batch_commits(db, database_name)
        except Exception as exc:
            logger.error(
                "configure_schema: failed to set up _batch_commits for database %r: %s",
                database_name, exc,
                exc_info=True,
            )
            return False

        self._streams[stream_id] = _MongoStreamState(
            database_name=database_name,
            collection_name=collection_name,
            write_mode=write_mode,
            conflict_keys=conflict_keys,
            json_fields=_collect_json_fields(endpoint_doc),
        )
        return True

    async def _ensure_collection(self, db: Any, collection_name: str) -> None:
        """Create the collection if it does not already exist."""
        try:
            await db.create_collection(collection_name)
        except Exception as exc:
            try:
                from pymongo.errors import CollectionInvalid as _CI
                if isinstance(exc, _CI):
                    return  # already exists — fine
            except ImportError:
                pass  # pymongo not importable; fall through to name-based check
            if "CollectionInvalid" in type(exc).__name__:
                return  # already exists — fine (pymongo not imported above)
            raise

    async def _ensure_batch_commits(self, db: Any, database_name: str) -> None:
        """Ensure _batch_commits collection and its unique index exist."""
        if database_name in self._batch_commits_ready:
            return

        await self._ensure_collection(db, _BATCH_COMMITS_COLLECTION)

        coll = db[_BATCH_COMMITS_COLLECTION]
        try:
            await coll.create_index(
                [("run_id", 1), ("stream_id", 1), ("batch_seq", 1)],
                unique=True,
                background=True,
            )
        except Exception as exc:
            # Re-raise IndexOptionsConflict (code 85): the index exists but
            # lacks unique=True, which would silently break the idempotency
            # guard.  Any other exception is re-raised unless the message
            # confirms the identical unique index was already present.
            try:
                from pymongo.errors import OperationFailure
                if isinstance(exc, OperationFailure) and exc.code == 85:
                    raise
            except ImportError:
                pass  # pymongo not importable; fall through to string check
            if "already exists" not in str(exc).lower():
                raise

        # Only mark ready after all DDL succeeds.
        self._batch_commits_ready.add(database_name)

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
        state = self._streams.get(stream_id)
        if state is None:
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                failure_summary=f"Stream {stream_id!r} not configured; configure_schema must run first",
            )

        if self._runtime is None:
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                failure_summary="write_batch called before connect() or after disconnect()",
            )

        client = self._runtime.mongo_client
        db = client[state.database_name]
        commits_coll = db[_BATCH_COMMITS_COLLECTION]
        target_coll = db[state.collection_name]

        commit_key = {"run_id": run_id, "stream_id": stream_id, "batch_seq": batch_seq}

        try:
            existing = await commits_coll.find_one(commit_key)
        except Exception as exc:
            logger.error(
                "Idempotency check failed (run=%s stream=%s seq=%d): %s",
                run_id, stream_id, batch_seq, exc,
                exc_info=True,
            )
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                failure_summary=f"Idempotency check failed: {exc}",
            )
        if existing is not None:
            stored_token: bytes = existing.get("cursor_token") or b""
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_ALREADY_COMMITTED,
                records_written=0,
                committed_cursor=Cursor(token=stored_token),
            )

        # Truncate-insert empties the collection ONCE per run -- on the read's
        # first batch (batch_seq == 1), not per batch, which would keep only the
        # final batch of a multi-batch refresh (mirrors the SQL path, #311). A
        # committed batch_seq==1 replay is caught by the idempotency guard above,
        # so the delete never re-runs within a run; an empty first batch still
        # truncates so an emptied source produces an empty collection (#312).
        truncate_now = (
            state.write_mode == WriteMode.WRITE_MODE_TRUNCATE_INSERT and batch_seq == 1
        )

        try:
            docs = _arrow_to_doc_list(record_batch)
            if state.json_fields:
                docs = decode_json_fields(docs, state.json_fields)
        except ValueError as exc:
            logger.error(
                "Malformed Json field in batch (run=%s stream=%s seq=%d): %s",
                run_id, stream_id, batch_seq, exc,
                exc_info=True,
            )
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_FATAL_FAILURE,
                records_written=0,
                failure_summary=str(exc),
            )
        if not docs:
            try:
                if truncate_now:
                    await target_coll.delete_many({})
                await commits_coll.insert_one(
                    {**commit_key, "cursor_token": cursor.token}
                )
            except Exception as exc:
                logger.error(
                    "Failed to truncate/record empty-batch commit "
                    "(run=%s stream=%s seq=%d): %s",
                    run_id, stream_id, batch_seq, exc,
                    exc_info=True,
                )
                return BatchWriteResult(
                    status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                    records_written=0,
                    failure_summary=str(exc),
                )
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_SUCCESS,
                records_written=0,
                committed_cursor=cursor,
            )

        try:
            records_written = await self._write_docs(
                target_coll, docs, state.write_mode, state.conflict_keys, truncate_now
            )
        except Exception as exc:
            logger.error(
                "MongoDB write_batch failed (run=%s stream=%s seq=%d): %s",
                run_id, stream_id, batch_seq, exc,
                exc_info=True,
            )
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=0,
                failure_summary=str(exc),
            )

        # Record the commit token. Without it the idempotency guard cannot
        # detect a replay, so a retry would re-execute the write regardless of
        # mode. Return RETRYABLE so the engine can decide whether to replay;
        # the write succeeded but cannot be safely ACK'd without the record.
        try:
            await commits_coll.insert_one(
                {**commit_key, "cursor_token": cursor.token}
            )
        except Exception as exc:
            logger.error(
                "Failed to record batch commit (run=%s stream=%s seq=%d mode=%s): %s — "
                "idempotency token not saved; retry will re-execute the write",
                run_id, stream_id, batch_seq, state.write_mode.name, exc,
                exc_info=True,
            )
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_RETRYABLE_FAILURE,
                records_written=records_written,
                failure_summary=f"Commit record not saved: {exc}",
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
        truncate_now: bool,
    ) -> int:
        if write_mode == WriteMode.WRITE_MODE_TRUNCATE_INSERT:
            if truncate_now:
                await collection.delete_many({})
            result = await collection.insert_many(docs, ordered=False)
            return len(result.inserted_ids)

        if write_mode == WriteMode.WRITE_MODE_UPSERT:
            if not conflict_keys:
                # configure_schema rejects UPSERT without conflict_keys; if we
                # reach here the stream state is inconsistent — fail loudly.
                raise ValueError(
                    "UPSERT mode requires conflict_keys but none are set on the "
                    "stream state; this indicates a configure_schema logic error"
                )
            from pymongo import UpdateOne
            ops = []
            for doc in docs:
                missing = [k for k in conflict_keys if k not in doc]
                if missing:
                    raise ValueError(
                        f"Upsert document is missing conflict key(s) {missing}; "
                        "cannot build a deterministic filter"
                    )
                null_keys = [k for k in conflict_keys if doc[k] is None]
                if null_keys:
                    raise ValueError(
                        f"Upsert document has null conflict key(s) {null_keys}; "
                        "a null filter value also matches missing-field documents "
                        "in MongoDB and would collapse unrelated records"
                    )
                filter_doc = {k: doc[k] for k in conflict_keys}
                # _id is immutable: it must stay out of $set (Mongo rejects an
                # in-place _id change), but a source-provided _id still has to
                # survive an upsert *insert*. Put non-_id fields in $set and pin a
                # present _id via $setOnInsert when the filter does not already
                # supply it; otherwise the insert would mint a fresh _id and drop
                # the source's.
                set_doc = {k: v for k, v in doc.items() if k != "_id"}
                update: Dict[str, Any] = {}
                if set_doc:
                    update["$set"] = set_doc
                if "_id" in doc and "_id" not in conflict_keys:
                    update["$setOnInsert"] = {"_id": doc["_id"]}
                if not update:
                    # Doc carries only its _id conflict key; nothing to $set.
                    # $set rejects an empty document, so insert via $setOnInsert.
                    update["$setOnInsert"] = filter_doc
                ops.append(UpdateOne(filter_doc, update, upsert=True))
            # Let any bulk_write error (including duplicate-key) propagate. Unlike
            # plain inserts, a duplicate key on an unordered upsert means a racing
            # operation's $set was dropped, so the batch must fail and be retried
            # (idempotent: the losing op becomes an update on retry) rather than be
            # committed as a success that silently loses the update.
            result = await collection.bulk_write(ops, ordered=False)
            return result.upserted_count + result.modified_count

        try:
            result = await collection.insert_many(docs, ordered=False)
            return len(result.inserted_ids)
        except Exception as exc:
            # Narrow to BulkWriteError for partial-write recovery; re-raise
            # everything else (network errors, timeouts).
            try:
                from pymongo.errors import BulkWriteError
            except ImportError:
                raise exc  # pymongo unavailable; re-raise the original error
            if not isinstance(exc, BulkWriteError):
                raise

            details = exc.details or {}
            n_inserted = details.get("nInserted") or 0
            n_total = len(docs)
            write_errors = details.get("writeErrors") or []
            non_dup = [e for e in write_errors if e.get("code") != 11000]
            if non_dup:
                logger.error(
                    "insert_many: %d of %d documents failed with non-duplicate-key "
                    "errors (first: %s); raising",
                    len(non_dup), n_total, non_dup[0],
                    exc_info=True,
                )
                raise
            if n_inserted > 0:
                n_dropped = n_total - n_inserted
                logger.warning(
                    "insert_many partial write: %d inserted, %d skipped as duplicates "
                    "(%d total in batch)",
                    n_inserted, n_dropped, n_total,
                )
                return n_inserted
            # All documents were already present (all dup-key, zero inserted).
            # The batch was applied on a prior attempt; treat as idempotent success.
            logger.info(
                "insert_many: all %d documents were duplicates; batch already applied",
                n_total,
            )
            return 0
