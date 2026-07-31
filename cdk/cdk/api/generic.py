"""Generic API connector -- one class serving both roles.

``GenericAPIConnector`` is to REST APIs what ``GenericSQLConnector`` is to
databases: one class, one connect, one HTTP round trip, one answer to what
a status means, driven in one role at a time. The source worker constructs
it and calls ``read_batches``; the destination worker constructs it and
calls ``connect`` / ``configure_schema`` / ``write_batch``.

Two classes served those roles before, and they had drifted on nine
separate questions -- what counts as a successful status, which statuses a
retry can heal, whether a declared exception mapping is honoured, whether
a decimal survives the body, what an empty records path means. Every one
of those is a property of HTTP or of the endpoint document, not of the
direction data flows, so each is now answered once.

Per-provider quirks live in :class:`~cdk.api.dialects.ApiDialect`, carried
by ``dialect_class`` -- which a connector package overrides next to its own
connector class, exactly as the SQL family does.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from functools import partial
from typing import Any

import aiohttp
import pyarrow as pa

from ..base_handler import (
    BaseDestinationHandler,
    BatchRejected,
    BatchWriteResult,
    LandingBatch,
)
from ..connection_runtime import ConnectionRuntime
from ..exceptions import ReadError, TransportSpecError
from ..json_utils import decode_json_fields
from ..resolver import Resolver
from ..schema_contract import SchemaContract
from ..types import (
    AckStatus,
    CheckpointStore,
    FailureCategory,
    RetryVerdict,
    SchemaSpec,
)
from .dialects import ApiDialect
from .exceptions import ConnectorConnectionError
from .http import (
    DEFAULT_MAX_RETRIES,
    HttpSender,
    SignedRequest,
    encode_body,
    failure_facts,
    follow_url,
    join_url,
)
from .page_loop import (
    Fetch,
    Page,
    PageLoop,
    PageRequest,
    PaginationStrategy,
    StopCondition,
)
from .predicates import evaluate_predicate
from .records import extract_records, page_scope
from .replication import cursor_param_for, effective_start
from .request import ParamTable, RequestBuilder, build_write_body
from .response_schema import apply_read_type_map, records_items_schema
from .strategies import Resolve, build_strategy, resolve_page_size
from .verdicts import declared_retry_statuses, read_verdict, write_verdict
from .write_plan import (
    WRITE_MODE_KEYS,
    StreamWritePlan,
    body_with_idempotency_key,
    build_write_plan,
    content_idempotency_key,
    write_mode_block,
)

logger = logging.getLogger(__name__)

__all__ = ["GenericAPIConnector"]

#: Failures resolving a declared expression against a page. They are
#: authoring or data defects, and each becomes a read error naming what
#: was being resolved.
_RESOLUTION_FAILURES = (ValueError, KeyError, TransportSpecError)


@dataclass(frozen=True)
class _ReadPlan:
    """What one read needs, settled before its first request goes out.

    The three things the drain reads: the loop to walk, the contract that
    turns each page into Arrow, and the field whose last value advances the
    checkpoint (``None`` for a full refresh).
    """

    loop: PageLoop
    schema: SchemaContract
    cursor_field: str | None


def _read_operation(
    config: dict[str, Any],
) -> tuple[str, dict[str, Any], dict[str, Any], Mapping[str, Any]]:
    """Read the four things a read is addressed by, refusing a document without them.

    All four are contract-required, so an absent one is a wiring defect
    between the engine and this connector rather than an author's mistake --
    which is why each names what is missing instead of defaulting.
    """
    doc = config.get("endpoint_document")
    if not doc:
        raise ReadError("source config is missing 'endpoint_document'")
    endpoint_id = doc.get("endpoint_id", "<unnamed>")
    read = (doc.get("operations") or {}).get("read")
    if not read:
        raise ReadError(
            f"endpoint {endpoint_id!r}: operations.read is required to read "
            f"this endpoint as a source"
        )
    stream_source = config.get("stream_source") or {}
    endpoint_ref = stream_source.get("endpoint_ref")
    if not endpoint_ref:
        raise ReadError(
            "stream_source is missing 'endpoint_ref'; the source contract "
            "requires it to declare per-field types"
        )
    return endpoint_id, read, stream_source, endpoint_ref


class GenericAPIConnector(BaseDestinationHandler):
    """One API connector serving both roles, as the SQL one does for databases.

    A given instance is driven in one role at a time; both roles share one
    connect, one HTTP sender and one classification of what a status means.
    """

    #: The single overridable attribute (mirrors ``dialect_class`` on the
    #: SQL facade). A connector package's whole surface is this plus the
    #: dialect's three hooks.
    dialect_class: type[ApiDialect] = ApiDialect

    def __init__(self) -> None:
        """Construct an unconnected connector; both worker entry points do ``cls()``."""
        self._runtime: ConnectionRuntime | None = None
        self._http: HttpSender | None = None
        self.dialect: ApiDialect | None = None
        # None rather than "": join_url("", "/v1/x") answers "/v1/x", a
        # relative URL the client rejects with an unhelpful error instead of
        # the actionable "read attempted before connect()".
        self.base_url: str | None = None
        self._connected = False

        # Write role only.
        self._streams: dict[str, StreamWritePlan] = {}
        # Raw, unvalidated: the engine validated every document against the
        # published contract before it crossed the process boundary, so a
        # second parse here would only convert the keys the connector must
        # read into attribute names that do not exist in the document.
        self._stream_endpoints: dict[str, Mapping[str, Any]] = {}
        self._session_header_names: set[str] = set()
        self._write_resolver: Resolver | None = None
        self.last_schema_rejection: str | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self, runtime: ConnectionRuntime) -> None:
        """Materialize the connection and build the one sender both roles use."""
        try:
            self._runtime = runtime
            runtime.acquire()
            # The one binding site: a declaration becomes a dialect here and
            # nowhere else.
            self.dialect = self.dialect_class.for_runtime(runtime)
            await runtime.materialize()
            self.base_url = runtime.base_url
            self._http = HttpSender(
                session=runtime.session,
                rate_limiter=runtime.rate_limiter,
                dialect=self.dialect,
                retry_statuses=declared_retry_statuses(self.dialect.error_map),
                max_retries=runtime.raw_config.get("max_retries", DEFAULT_MAX_RETRIES),
            )
            self._session_header_names = {k.lower() for k in runtime.session.headers}
            # A write body has no batch-size concept, so this resolver is
            # built once; a read builds its own per call with the engine's
            # batch_size in the runtime scope.
            self._write_resolver = runtime.request_resolver()
            self._connected = True
            logger.debug("connected to API: %s", self.base_url)
        except Exception as err:
            logger.error("failed to connect to API: %s", err)
            raise ConnectorConnectionError(f"API connection failed: {err}") from err

    async def disconnect(self) -> None:
        """Close the sender and release the runtime reference. Idempotent."""
        if self._http is not None:
            await self._http.close()
        if self._runtime is not None:
            # Idempotent, so closing after the retry client already closed
            # the session is safe.
            await self._runtime.close()
            # Brief courtesy drain so the client's transports finish
            # closing. This runs in read_batches' ``finally``, so a
            # cancellation landing here must not displace a read error
            # already propagating; absorb it rather than let the drain
            # become the surfaced exception.
            try:
                await asyncio.sleep(0.25)
            except asyncio.CancelledError:
                # Absorbed on purpose: this drain is the last thing a
                # failing read does, and letting the cancellation out here
                # would replace the error the caller needs to see with the
                # teardown's own.
                pass
        self._http = None
        self._connected = False

    async def health_check(self) -> bool:
        """Report whether the API answers at all.

        A 404 on the base URL still means the API answered, which is the
        question; only a server error or no answer at all is unhealthy.
        """
        if self._http is None or not self._connected or self.base_url is None:
            return False
        try:
            return await self._http.probe(self.base_url) < 500
        except (aiohttp.ClientError, asyncio.TimeoutError) as err:
            logger.warning(
                "API health check failed: %s: %s",
                type(err).__name__,
                err,
                exc_info=True,
            )
            return False

    # ------------------------------------------------------------------
    # Read role
    # ------------------------------------------------------------------

    def read_batches(
        self,
        runtime: ConnectionRuntime,
        config: dict[str, Any],
        *,
        checkpoint: CheckpointStore,
        stream_name: str,
        partition: dict[str, Any] | None = None,
        batch_size: int = 1000,
    ) -> AsyncIterator[pa.RecordBatch]:
        """Read upstream records as Arrow batches, one batch per page.

        ``runtime`` is the only connection input: this connector opens the
        session on entry and closes it on exit, so no prior ``connect()`` is
        required. ``connect()`` runs inside the ``try`` so a failure there
        still reaches ``disconnect()`` and releases the reference it took --
        the lifecycle is balanced on every exit.
        """
        return self._read(
            runtime,
            config,
            checkpoint=checkpoint,
            stream_name=stream_name,
            partition=partition,
            batch_size=batch_size,
        )

    async def _read(
        self,
        runtime: ConnectionRuntime,
        config: dict[str, Any],
        *,
        checkpoint: CheckpointStore,
        stream_name: str,
        partition: dict[str, Any] | None,
        batch_size: int,
    ) -> AsyncIterator[pa.RecordBatch]:
        """Own the connection lifecycle around one read pass."""
        try:
            await self.connect(runtime)
            async for batch in self._read_pages(
                config,
                checkpoint=checkpoint,
                stream_name=stream_name,
                partition=partition or {},
                batch_size=batch_size,
            ):
                yield batch
        finally:
            await self.disconnect()

    async def _plan_read(
        self,
        config: dict[str, Any],
        *,
        checkpoint: CheckpointStore,
        stream_name: str,
        partition: dict[str, Any],
        batch_size: int,
    ) -> _ReadPlan:
        """Settle everything one read needs before its first request.

        Separate from the draining because the two fail differently: every
        authoring defect in here is deterministic and costs nothing to
        raise, while a failure once records are flowing has already handed
        the engine batches it may have committed.
        """
        runtime = self._runtime
        if runtime is None or self.base_url is None or self._http is None:
            raise ReadError("read attempted before connect() materialized the runtime")

        endpoint_id, read, stream_source, endpoint_ref = _read_operation(config)

        items_schema = records_items_schema(endpoint_id, read["response"])
        apply_read_type_map(items_schema, endpoint_ref, runtime)
        schema_contract = SchemaContract(items_schema)

        request_block = read["request"]
        method = request_block["method"]
        full_url = join_url(self.base_url, request_block["path"])
        records_ref = read["response"]["records"]["ref"]
        pagination = read.get("pagination")

        # One resolver per read, carrying the engine's page size in the
        # runtime scope. Every declared expression this read resolves --
        # param defaults, the body, the page size, the stop condition, the
        # next cursor -- comes from this object (response-scoped per page),
        # because a second one built anywhere else would leave
        # ``runtime.batch_size`` unresolvable and silently read at the wrong
        # page size.
        resolver = runtime.request_resolver(runtime_values={"batch_size": batch_size})

        table = ParamTable.for_read(
            read.get("params") or {},
            resolver,
            filters=stream_source.get("filters") or [],
        )
        replication_block = stream_source.get("replication") or {}
        cursor_field = replication_block.get("cursor_field")
        if replication_block.get("method") == "incremental":
            await self._bind_incremental_filter(
                table.values,
                read.get("replication"),
                replication_block,
                checkpoint=checkpoint,
                stream_name=stream_name,
                partition=partition,
            )

        builder = RequestBuilder(
            table,
            raw_body=request_block.get("body"),
            resolver=resolver,
            endpoint=request_block["path"],
        )

        strategy = self._build_strategy(
            pagination,
            table=table,
            resolver=resolver,
            url=full_url,
            base_url=self.base_url,
            batch_size=batch_size,
        )

        return _ReadPlan(
            loop=PageLoop(
                strategy,
                fetch=self._fetcher(
                    self._http, builder, method=method, records_ref=records_ref
                ),
                stop_when=self._stop_condition(
                    (pagination or {}).get("stop_when"), resolver
                ),
            ),
            schema=schema_contract,
            cursor_field=cursor_field,
        )

    def _build_strategy(
        self,
        pagination: dict[str, Any] | None,
        *,
        table: ParamTable,
        resolver: Resolver,
        url: str,
        base_url: str,
        batch_size: int,
    ) -> PaginationStrategy:
        """Build the paging adapter, binding the page size it walks with.

        The page size binds here rather than in the loop: the loop has no
        page-size concept, so a read that skipped this would raise nothing
        and quietly take the provider's own default forever.
        """
        try:
            page_size = resolve_page_size(
                pagination, batch_size=batch_size, resolve=resolver.resolve_for_request
            )
            limit = (pagination or {}).get("limit") or {}
            if limit.get("param"):
                table.values[limit["param"]] = page_size

            return build_strategy(
                pagination,
                url=url,
                base_params=table.values,
                resolve=self._page_expression_resolver(resolver),
                follow_url=partial(follow_url, origin=base_url),
            )
        except ValueError as err:
            # An unknown scheme, a page size that cannot advance, a step
            # that is not a whole number: authoring defects the loop cannot
            # run at all. They are deterministic, so they must reach the
            # worker as a read error rather than as a bare ValueError it
            # would classify as worth retrying.
            raise ReadError(f"pagination could not be set up: {err}") from err

    async def _read_pages(
        self,
        config: dict[str, Any],
        *,
        checkpoint: CheckpointStore,
        stream_name: str,
        partition: dict[str, Any],
        batch_size: int,
    ) -> AsyncIterator[pa.RecordBatch]:
        """Drain the planned page loop, yielding each page as an Arrow batch."""
        plan = await self._plan_read(
            config,
            checkpoint=checkpoint,
            stream_name=stream_name,
            partition=partition,
            batch_size=batch_size,
        )
        loop = plan.loop
        schema_contract = plan.schema
        cursor_field = plan.cursor_field

        batch_count = 0
        try:
            async for records in loop:
                yield schema_contract.from_pylist(records)
                batch_count += 1
                if cursor_field:
                    cursor_value = records[-1].get(cursor_field)
                    if cursor_value is not None:
                        await checkpoint.save_cursor(
                            stream_name, partition, {"cursor": cursor_value}
                        )
                    else:
                        # Safe under at-least-once plus upsert (a resume
                        # re-reads), but visible: an author debugging "the
                        # incremental stream keeps re-reading its tail"
                        # needs this signal.
                        logger.debug(
                            "stream %r: last record has no %r value; cursor "
                            "not advanced for batch %d",
                            stream_name,
                            cursor_field,
                            batch_count,
                        )
        except ValueError as err:
            # Same reason, mid-traversal: a link that leaves the origin, a
            # keyset page with no ordering value, a record a declared Arrow
            # type cannot hold. None of them heals on a retry.
            raise ReadError(f"read failed after {batch_count} batches: {err}") from err

    @staticmethod
    def _page_resolver(resolver: Resolver, page: Page | None) -> Resolver:
        """Give the resolver the page's body as its ``response`` scope."""
        return resolver if page is None else resolver.with_response(page_scope(page))

    def _page_expression_resolver(self, resolver: Resolver) -> Resolve:
        """Adapt the read's resolver to what a strategy asks of it."""

        def resolve(expr: Any, page: Page | None) -> Any:
            try:
                return self._page_resolver(resolver, page).resolve_for_request(expr)
            except _RESOLUTION_FAILURES as err:
                raise ReadError(
                    f"pagination expression failed to resolve: {err}"
                ) from err

        return resolve

    def _stop_condition(self, declared: Any, resolver: Resolver) -> StopCondition:
        """Adapt the declared stop condition to what the loop asks of it."""

        def stop_when(page: Page) -> bool:
            if declared is None:
                # No pagination block, so the strategy already ends the
                # traversal after its one page.
                return False
            try:
                return evaluate_predicate(
                    declared, self._page_resolver(resolver, page).resolve_for_request
                )
            except _RESOLUTION_FAILURES as err:
                raise ReadError(
                    f"pagination stop_when failed to evaluate: {err}"
                ) from err

        return stop_when

    def _fetcher(
        self,
        sender: HttpSender,
        builder: RequestBuilder,
        *,
        method: str,
        records_ref: str,
    ) -> Fetch:
        """Build the loop's fetch: one request, one page."""

        async def fetch(request: PageRequest) -> Page:
            if request.sends_declared_body:
                try:
                    query, body = builder.for_page(request.params)
                except ValueError as err:
                    raise ReadError(f"request body could not be built: {err}") from err
            else:
                # A provider-supplied continuation carries its own query in
                # the URL and takes no declared body, so nothing is rebuilt.
                query, body = dict(request.params), None
            signed = SignedRequest(
                method=method,
                url=request.url,
                params=query,
                headers={},
                body=None if body is None else encode_body(body),
            )
            try:
                payload = await sender.send(signed, unwrap_page=True)
            except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                status, category = failure_facts(err, error_map=self._error_map)
                raise read_verdict(
                    f"API request failed: {method} {request.url} -> {err}",
                    status=status,
                    category=category,
                ) from err
            return Page(records=extract_records(payload, records_ref), payload=payload)

        return fetch

    @staticmethod
    async def _bind_incremental_filter(
        params: dict[str, Any],
        declared_replication: Mapping[str, Any] | None,
        stream_replication: Mapping[str, Any],
        *,
        checkpoint: CheckpointStore,
        stream_name: str,
        partition: dict[str, Any],
    ) -> None:
        """Write the stored cursor, minus the safety window, into its param."""
        cursor_field = stream_replication.get("cursor_field")
        if not cursor_field:
            return
        param_name = cursor_param_for(declared_replication, cursor_field)
        if not param_name:
            logger.warning(
                "no replication.cursor_mappings entry for cursor field %r; "
                "running full replication",
                cursor_field,
            )
            return
        cursor_state = await checkpoint.get_cursor(stream_name, partition)
        cursor_value = (cursor_state or {}).get("cursor")
        if not cursor_value:
            logger.info(
                "no prior cursor for stream %r; first run performs full " "replication",
                stream_name,
            )
            return
        safety_window = stream_replication.get("safety_window_seconds")
        if safety_window is None:
            # Operational policy the engine owns and fills before the config
            # crosses the boundary. A connector never declares it, so an
            # absent value here is a wiring defect, not a default to invent
            # -- inventing one is how three copies of the number appeared.
            raise ReadError(
                f"stream {stream_name!r}: incremental replication has no "
                f"'safety_window_seconds'; the engine fills it before the "
                f"config reaches a connector"
            )
        start = effective_start(cursor_value, safety_window)
        params[param_name] = start
        logger.info(
            "incremental replication: %s -> %s = %s", cursor_field, param_name, start
        )

    # ------------------------------------------------------------------
    # Write role
    # ------------------------------------------------------------------

    def set_stream_endpoints(
        self, stream_endpoints: Mapping[str, Mapping[str, Any]]
    ) -> None:
        """Register each stream's contract endpoint document, raw."""
        self._stream_endpoints = dict(stream_endpoints)

    @property
    def connector_type(self) -> str:
        """Return the connector type identifier."""
        return "api"

    @property
    def supports_transactions(self) -> bool:
        """Report that HTTP endpoints offer no transaction."""
        return False

    @property
    def supports_upsert(self) -> bool:
        """Report whether any registered endpoint declares an upsert write.

        Contract-driven, never hardcoded: the endpoint document owns whether
        an endpoint can upsert. ``GetCapabilities`` advertises one
        connector-wide boolean, so this answers yes when at least one
        registered endpoint declares it; a stream whose own endpoint lacks
        the block is still refused at its schema handshake.
        """
        return any(
            write_mode_block(doc, "upsert") is not None
            for doc in self._stream_endpoints.values()
        )

    @property
    def supports_bulk_load(self) -> bool:
        """Report whether any registered endpoint declares write batching.

        Multi-record requests exist only where an endpoint declares the
        provider's per-request cap; without one every write is one request
        per record, so advertising bulk load would promise a capability no
        configured stream can use.
        """
        return any(
            block.get("batching") is not None
            for doc in self._stream_endpoints.values()
            for mode_key in WRITE_MODE_KEYS.values()
            if (block := write_mode_block(doc, mode_key)) is not None
        )

    def retry_semantics(self, stream_id: str) -> RetryVerdict:
        """Return the per-stream verdict computed at configure time (issue #286)."""
        plan = self._streams.get(stream_id)
        if plan is None or plan.retry_verdict is None:
            return super().retry_semantics(stream_id)
        return plan.retry_verdict

    def not_ready_reason(self, stream_id: str) -> str | None:
        """Report what an API write is still missing: a sender, a configured stream."""
        if self._http is None or not self._connected:
            return "Handler not connected"
        if self._streams.get(stream_id) is None:
            return "Schema not configured"
        return None

    async def configure_schema(self, schema_spec: SchemaSpec) -> bool:
        """Build the stream's write plan from its endpoint document."""
        stream_id = schema_spec.stream_id
        # The servicer reads last_schema_rejection right after this call.
        # The connector is shared across concurrent streams, so the reset ->
        # read window is race-free only while this method stays await-free.
        self.last_schema_rejection = None
        self.last_schema_failure_category = FailureCategory.FAILURE_CATEGORY_UNSPECIFIED

        doc = self._stream_endpoints.get(stream_id)
        if doc is None:
            return self._reject_schema(
                stream_id,
                f"no preloaded endpoint document for stream_id={stream_id!r}; "
                f"call set_stream_endpoints() before the gRPC server starts",
            )
        outcome = build_write_plan(
            doc, schema_spec, session_header_names=self._session_header_names
        )
        if isinstance(outcome, str):
            return self._reject_schema(stream_id, outcome)

        self._streams[stream_id] = outcome
        logger.info(
            "API schema configured for stream %r: %s %s, %s",
            stream_id,
            outcome.method,
            outcome.endpoint,
            "single-record"
            if outcome.max_records is None
            else f"batched (max_records={outcome.max_records})",
        )
        return True

    def _reject_schema(self, stream_id: str, reason: str) -> bool:
        """Log one configure-time rejection and record it for the ack.

        Every rejection this connector makes is a defect in the endpoint
        document or the stream's write config, so it declares CONFIG_DEFECT
        rather than leaving the engine to infer it: the connector was
        reachable and understood the request; what it refused was the
        configuration.
        """
        logger.error("schema rejected for stream %r: %s", stream_id, reason)
        self.last_schema_rejection = reason
        self.last_schema_failure_category = (
            FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT
        )
        return False

    async def land(self, batch: LandingBatch) -> int:
        """Send the batch's records to the endpoint.

        Records stay row-oriented: Arrow-native Python types survive into
        the dicts and the body serialiser handles them, so pre-casting in
        Arrow space would be a second pass for no gain.
        """
        plan = self._streams[batch.stream_id]
        records = batch.records
        decode_json_fields(records, plan.json_fields)
        if plan.max_records is None:
            written, failed_ids, detail, category = await self._write_one_by_one(
                plan, records, batch.record_ids
            )
        else:
            written, failed_ids, detail, category = await self._write_in_chunks(
                plan, records, batch.record_ids
            )
        logger.info(
            "API wrote batch %s: %s/%s records", batch.batch_seq, written, len(records)
        )
        total = len(records)
        if written == total and not failed_ids:
            return written
        # A shortfall is fatal, never retryable: the records that did land
        # are already written, so retrying the whole batch would duplicate
        # them. The count and the failed ids ride the refusal so the engine
        # dead-letters exactly what did not land.
        failed_count = len(failed_ids) or (total - written)
        summary = f"{failed_count}/{total} records failed to write to API"
        if detail:
            summary = f"{summary}; first failure: {detail}"
        raise BatchRejected(
            summary,
            category=category,
            records_written=written,
            failed_record_ids=tuple(failed_ids),
        )

    def os_error_failure(
        self,
        error: OSError,
        *,
        run_id: str,
        stream_id: str,
        batch_seq: int,
    ) -> BatchWriteResult:
        """Judge an HTTP transport failure, not a filesystem one.

        The client's connection errors derive from ``OSError`` and
        ``asyncio.TimeoutError`` IS the builtin ``TimeoutError``, so the
        base's errno table would otherwise swallow exactly the failures the
        connector's declared exception map exists to classify -- retrying a
        declared config defect to exhaustion, and dead-lettering a batch on
        a mid-request disconnect that carries EPIPE.
        """
        return self.unexpected_write_failure(
            error, run_id=run_id, stream_id=stream_id, batch_seq=batch_seq
        )

    def unexpected_write_failure(
        self,
        error: Exception,
        *,
        run_id: str,
        stream_id: str,
        batch_seq: int,
    ) -> BatchWriteResult:
        """Let the declared error map judge a transport failure first."""
        if isinstance(error, (aiohttp.ClientError, asyncio.TimeoutError)):
            status, category = failure_facts(error, error_map=self._error_map)
            ack_status, failure_category = write_verdict(
                status=status, category=category
            )
            logger.error("transport error writing to API: %s", error, exc_info=True)
            return BatchWriteResult(
                status=ack_status,
                records_written=0,
                failure_summary=f"{type(error).__name__}: {error}",
                failure_category=failure_category,
            )
        logger.error("fatal error writing to API: %s", error, exc_info=True)
        return BatchWriteResult(
            status=AckStatus.ACK_STATUS_FATAL_FAILURE,
            records_written=0,
            failure_summary=f"{type(error).__name__}: {error}",
        )

    @property
    def _error_map(self) -> Any:
        """The connector's declared error map, owned by the dialect."""
        return self.dialect.error_map if self.dialect is not None else None

    async def _write_one_by_one(
        self,
        plan: StreamWritePlan,
        records: list[dict[str, Any]],
        record_ids: list[str],
    ) -> tuple[int, list[str], str, FailureCategory]:
        """Write records one request each.

        Body construction is data-dependent (a record field can feed a
        derived function) and is caught per record, so a bad record fails
        just itself. Authoring and programming errors propagate and become
        fatal for the whole batch.

        A retryable transport failure re-raises immediately: the base's
        outer catch has no access to the local ``written`` counter, so it
        reports zero written and the engine retries the whole batch --
        records that already landed are re-sent. Streams with a declared
        idempotency key are protected; insert-mode streams without one are
        classified at-least-once for exactly this reason. A deterministic
        rejection instead fails just that record and the loop continues.
        """
        written = 0
        failed_ids: list[str] = []
        first_failure = ""
        first_category = FailureCategory.FAILURE_CATEGORY_UNSPECIFIED

        for index, record in enumerate(records):
            # Insert keys on the identity-derived record id (the first
            # occurrence of an identity wins, matching the SQL insert
            # anti-join); upsert keys on the full record content so a
            # changed row gets a new key and the provider applies the update
            # instead of replaying its cached response.
            key = (
                None
                if plan.idempotency_in is None
                else (
                    record_ids[index]
                    if plan.write_mode_key == "insert"
                    else content_idempotency_key(record)
                )
            )
            try:
                body = self._build_body(plan, record=record)
                if plan.idempotency_in == "body" and key is not None:
                    body = body_with_idempotency_key(plan, body, key)
            except (TypeError, ValueError) as err:
                logger.warning(
                    "failed to build body for record %s: %s: %s",
                    record_ids[index],
                    type(err).__name__,
                    err,
                )
                failed_ids.append(record_ids[index])
                first_failure = first_failure or f"{type(err).__name__}: {err}"
                continue
            headers = (
                {plan.idempotency_name: key}
                if plan.idempotency_in == "header" and key is not None
                else None
            )
            try:
                await self._send(plan, body, extra_headers=headers)
                written += 1
            except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                status, category = failure_facts(err, error_map=self._error_map)
                ack_status, failure_category = write_verdict(
                    status=status, category=category
                )
                if ack_status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE:
                    logger.warning(
                        "retryable error on record %s (index %d, %d already "
                        "written) -- aborting batch: %s: %s",
                        record_ids[index],
                        index,
                        written,
                        type(err).__name__,
                        err,
                    )
                    raise
                logger.warning(
                    "failed to write record %s: %s: %s",
                    record_ids[index],
                    type(err).__name__,
                    err,
                )
                failed_ids.append(record_ids[index])
                if not first_failure:
                    # The first failure names the batch verdict -- its
                    # declared category rides the ack alongside its reason.
                    first_category = failure_category
                first_failure = first_failure or f"{type(err).__name__}: {err}"

        if failed_ids:
            logger.warning(
                "failed to write %d records: %s...", len(failed_ids), failed_ids[:5]
            )
        return written, failed_ids, first_failure, first_category

    async def _write_in_chunks(
        self,
        plan: StreamWritePlan,
        records: list[dict[str, Any]],
        record_ids: list[str],
    ) -> tuple[int, list[str], str, FailureCategory]:
        """Write records in chunks of at most ``max_records``.

        Per-item partial failure inside a 2xx response body is NOT
        inspected: no endpoint contract declares where a per-item error
        array lives, so the response shape is opaque and a 2xx means the
        provider accepted the whole chunk.

        Any chunk failure stops the loop: the verdict is already fatal, the
        engine dead-letters the batch and a restart replays it, so every
        record sent past the first failed chunk would land only to be
        re-sent. Chunked streams can never carry an idempotency key (the
        contract excludes it with batching), so that duplication is
        unmitigated. A retryable failure before any chunk landed re-raises
        instead: nothing was written, so a retry cannot duplicate.
        """
        chunk_size = plan.max_records
        if chunk_size is None:
            raise RuntimeError(
                "chunked write dispatched for a stream with no batching "
                "declaration; land() routes those one request per record"
            )
        written = 0

        for start in range(0, len(records), chunk_size):
            chunk = records[start : start + chunk_size]
            try:
                body = self._build_body(plan, records=chunk)
            except (TypeError, ValueError) as err:
                logger.warning(
                    "failed to build body for chunk at offset %d (%d records "
                    "%s...): %s: %s",
                    start,
                    len(chunk),
                    record_ids[start : start + 3],
                    type(err).__name__,
                    err,
                )
                return (
                    written,
                    list(record_ids[start:]),
                    f"{type(err).__name__}: {err}",
                    FailureCategory.FAILURE_CATEGORY_UNSPECIFIED,
                )
            try:
                await self._send(plan, body)
            except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                status, category = failure_facts(err, error_map=self._error_map)
                ack_status, failure_category = write_verdict(
                    status=status, category=category
                )
                if (
                    written == 0
                    and ack_status == AckStatus.ACK_STATUS_RETRYABLE_FAILURE
                ):
                    raise
                logger.warning(
                    "failed to write batch chunk at offset %d (%d records): " "%s: %s",
                    start,
                    len(chunk),
                    type(err).__name__,
                    err,
                    exc_info=True,
                )
                return (
                    written,
                    list(record_ids[start:]),
                    f"{type(err).__name__}: {err}",
                    failure_category,
                )
            written += len(chunk)

        return written, [], "", FailureCategory.FAILURE_CATEGORY_UNSPECIFIED

    def _build_body(
        self,
        plan: StreamWritePlan,
        *,
        record: dict[str, Any] | None = None,
        records: list[dict[str, Any]] | None = None,
    ) -> Any:
        """Build one write body from the plan and the in-flight record(s)."""
        if self._write_resolver is None:
            raise RuntimeError("connector not connected: no request resolver")
        return build_write_body(
            body_spec=plan.body_spec,
            endpoint=plan.endpoint,
            params=ParamTable.for_write(plan.params_spec, self._write_resolver).values,
            resolver=self._write_resolver,
            record=record,
            records=records,
        )

    async def _send(
        self,
        plan: StreamWritePlan,
        data: Any,
        extra_headers: Mapping[str, str] | None = None,
    ) -> Any:
        """Send one write request through the shared sender."""
        if self._http is None or self.base_url is None:
            raise RuntimeError("connector not connected: no HTTP sender")
        return await self._http.send(
            SignedRequest(
                method=plan.method,
                url=join_url(self.base_url, plan.endpoint),
                params={},
                headers=dict(extra_headers or {}),
                body=encode_body(data),
            ),
            unwrap_page=False,
        )
