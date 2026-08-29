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
from typing import Any

import aiohttp
import pyarrow as pa
from analitiq.contracts.endpoints import ApiEndpointDoc, ReadOperation, Replication
from analitiq.contracts.stream import EndpointRef, IncrementalReplication, StreamSource
from pydantic import ValidationError

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
from ..write_keys import require_conflict_key_values
from .dialects import ApiDialect
from .exceptions import ConnectorConnectionError, RequestSpecError
from .http import (
    DEFAULT_MAX_RETRIES,
    HttpSender,
    Received,
    SignedRequest,
    encode_body,
    failure_facts,
    query_pairs,
)
from .page_loop import Fetch, Page, PageLoop, PageRequest
from .query_style import declared_query_styles
from .read_setup import build_read_strategy, stop_condition
from .records import extract_records
from .replication import cursor_param_for, effective_start
from .request import (
    ParamTable,
    RequestBuilder,
    bind_request_values,
    build_write_body,
    request_block_problem,
    substitute_path,
)
from .response_schema import apply_read_type_map, records_items_schema
from .urls import join_url, origin_of, require_declared_origin
from .verdicts import declared_retry_statuses, read_verdict, write_verdict
from .write_plan import (
    WRITE_MODE_KEYS,
    StreamWritePlan,
    body_with_idempotency_key,
    build_write_plan,
    content_idempotency_key,
    reserved_header_names,
    write_mode_block,
)
from .write_response import DeclaredWriteFailure, judge_write_response

logger = logging.getLogger(__name__)

__all__ = ["GenericAPIConnector"]


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


@dataclass(frozen=True)
class _Dispatch:
    """Everything that follows from the transport an operation goes out on.

    The api path used to have one of each of these, because a connection
    had one transport: one session, one base URL, one set of header names
    the connection owns, one origin. Per-operation selection makes each of
    them a fact ABOUT A TRANSPORT, and a path that carried some of them
    per-operation and read the rest off the default would validate a
    request against one transport's headers and send it on another's
    session.

    So they travel together, resolved once per ref. A caller that has a
    dispatch cannot accidentally use the default's anything.
    """

    sender: HttpSender
    base_url: str
    origin: str


def _read_operation(
    config: dict[str, Any],
) -> tuple[str, ReadOperation, StreamSource, EndpointRef]:
    """Read the four things a read is addressed by, refusing a document without them.

    All four are contract-required, so an absent one is a wiring defect
    between the engine and this connector rather than an author's mistake --
    which is why each names what is missing instead of defaulting.

    Both documents the engine hands over are parsed here, each against the
    contract model that owns it: the endpoint document is ``ApiEndpointDoc``
    and the stream's source block is ``StreamSource``. ``endpoint_ref`` is
    then the parsed ref off that block, not a key looked up in it -- the
    contract requires it, so the parse is what refuses a source without one.
    """
    doc = config.get("endpoint_document")
    if not doc:
        raise ReadError("source config is missing 'endpoint_document'")
    # The engine hands authored JSON across the worker boundary, so the
    # document is parsed here: the connector process is untrusted, and a
    # document the published contract refuses must fail before the first
    # request goes out rather than as a missing key mid-read. The parse is
    # also what makes every field below a named attribute.
    try:
        document = ApiEndpointDoc.model_validate(doc)
    except ValidationError as err:
        # ReadError, not the bare ValidationError: the worker classifies
        # this type as a deterministic read failure instead of retrying it.
        # Named by the slot the document arrived in rather than by
        # endpoint_id, because the id is one of the fields this parse may
        # have just rejected -- quoting it would name the endpoint with the
        # very value that failed.
        raise ReadError(
            f"source config 'endpoint_document' does not satisfy "
            f"ApiEndpointDoc: {err}"
        ) from err
    endpoint_id = document.endpoint_id
    # Still checked after a successful parse: the contract permits a
    # write-only document, which is a valid ApiEndpointDoc and an
    # unreadable source.
    read = document.operations.read
    if read is None:
        raise ReadError(
            f"endpoint {endpoint_id!r}: operations.read is required to read "
            f"this endpoint as a source"
        )
    source_block = config.get("stream_source")
    if not source_block:
        raise ReadError("source config is missing 'stream_source'")
    try:
        stream_source = StreamSource.model_validate(source_block)
    except ValidationError as err:
        # Same classification as the endpoint document above: a stream
        # source the published contract refuses is a deterministic read
        # failure, and a bare ValidationError would be retried.
        raise ReadError(
            f"source config 'stream_source' does not satisfy StreamSource: {err}"
        ) from err
    return endpoint_id, read, stream_source, stream_source.endpoint_ref


class _RecordFailures:
    """The per-record failures of one one-by-one write, in order.

    The first failure names the batch verdict: its reason and declared
    category ride the ack, the rest are counted.
    """

    def __init__(self) -> None:
        self.ids: list[str] = []
        self.first_reason = ""
        self.first_category = FailureCategory.FAILURE_CATEGORY_UNSPECIFIED

    def add(
        self,
        record_id: str,
        err: Exception,
        what: str,
        *,
        category: FailureCategory = FailureCategory.FAILURE_CATEGORY_UNSPECIFIED,
    ) -> None:
        logger.warning("%s %s: %s: %s", what, record_id, type(err).__name__, err)
        self.ids.append(record_id)
        if not self.first_reason:
            self.first_reason = f"{type(err).__name__}: {err}"
            self.first_category = category


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
        #: One dispatch per transport ref, built on first use. The default
        #: under its own name, so a request omitting ``transport_ref`` and
        #: one naming the default share it rather than opening the
        #: connection's session twice.
        self._dispatches: dict[str, _Dispatch] = {}
        #: Two streams reaching a named transport for the first time do so
        #: concurrently; without this each builds a sender and a retry
        #: client, and the loser is never closed.
        self._dispatch_lock = asyncio.Lock()

        #: The connection's retry budget, read once so every sender this
        #: connector opens gets the one the connection declared.
        self._max_retries: int = DEFAULT_MAX_RETRIES
        self.dialect: ApiDialect | None = None
        # None rather than "": join_url("", "/v1/x") answers "/v1/x", a
        # relative URL the client rejects with an unhelpful error instead of
        # the actionable "read attempted before connect()".
        self.base_url: str | None = None
        self._connected = False

        # Write role only.
        self._streams: dict[str, StreamWritePlan] = {}
        # Parsed, not raw. The engine does validate every document before it
        # crosses the process boundary, but this process runs untrusted,
        # AI-authored connector code and re-validating what arrives over
        # that boundary is defense in depth: the far side is exactly where a
        # document may no longer be the one the engine checked. The parse is
        # also what turns a contract field the engine happens to ignore into
        # an unused attribute a tool can find, rather than a key nobody can
        # prove is unread.
        self._stream_endpoints: dict[str, ApiEndpointDoc] = {}
        # Stream-id -> why that stream's document did not parse. Held here
        # rather than raised at registration: set_stream_endpoints() runs
        # before the gRPC server exists, so a raise there kills the worker
        # and every other stream on it. configure_schema() is the first
        # point with a per-stream ack to put the reason on.
        self._stream_endpoint_problems: dict[str, str] = {}
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
            self._max_retries = runtime.raw_config.get(
                "max_retries", DEFAULT_MAX_RETRIES
            )
            # Through the same call every later request goes through, so
            # the default transport is opened, wrapped and cached in one
            # place: a second construction site here is a second sender
            # over the one session, and a second connection pool with it.
            default = await self._dispatch_through(None)
            self._http, self.base_url = default.sender, default.base_url
            # A write body has no batch-size concept, so this resolver is
            # built once; a read builds its own per call with the engine's
            # batch_size in the runtime scope.
            self._write_resolver = runtime.request_resolver()
            self._connected = True
            logger.debug("connected to API: %s", self.base_url)
        except Exception as err:
            logger.error("failed to connect to API: %s", err)
            raise ConnectorConnectionError(f"API connection failed: {err}") from err

    async def _dispatch_through(self, transport_ref: str | None) -> _Dispatch:
        """Return everything an operation naming *transport_ref* goes out on.

        The one place ``request.transport_ref`` becomes a session --
        ``connect()`` opens the default through it too, so there is one
        construction site rather than one per role plus one for the
        default. A ref the run did not resolve is refused by the runtime,
        by name; the alternative, falling back to the default, is the
        silent failure this path exists to end: the request goes out on
        the wrong origin with the wrong headers and the provider answers
        it.

        The dialect, the retry statuses and the retry budget are the
        connection's, not the transport's: they describe how this provider
        answers, which does not change because a second origin serves the
        documents.
        """
        runtime, dialect = self._runtime, self.dialect
        if runtime is None or dialect is None:
            raise RuntimeError("connector not connected: no HTTP sender")
        ref = transport_ref or runtime.default_transport_ref
        dispatch = self._dispatches.get(ref)
        if dispatch is not None:
            return dispatch
        async with self._dispatch_lock:
            # Read again under the lock: two streams reaching this ref
            # together would otherwise each build a sender, and the loser's
            # retry client would never be closed.
            dispatch = self._dispatches.get(ref)
            if dispatch is not None:
                return dispatch
            transport = await runtime.http_transport(transport_ref)
            dispatch = _Dispatch(
                sender=HttpSender(
                    session=transport.session,
                    rate_limiter=transport.rate_limiter,
                    dialect=dialect,
                    retry_statuses=declared_retry_statuses(dialect.error_map),
                    max_retries=self._max_retries,
                ),
                base_url=transport.base_url,
                origin=origin_of(transport.base_url),
            )
            self._dispatches[ref] = dispatch
            return dispatch

    async def disconnect(self) -> None:
        """Close the sender and release the runtime reference. Idempotent."""
        # Every sender, not just the default: a named transport opened for
        # one endpoint owns a retry client and a connector pool of its own.
        for dispatch in self._dispatches.values():
            await dispatch.sender.close()
        self._dispatches.clear()
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
        """Report whether every host this connector will use answers.

        A 404 on a base URL still means the API answered, which is the
        question; only a server error or no answer at all is unhealthy.

        Every host, not just the connection's default: once streams are
        configured, the records go to the transports their write plans
        name, and a readiness probe of the default alone reports SERVING
        while the host that will actually receive them is down. So the
        probe follows the same selection the writes do -- the distinct
        transports the configured plans dispatch through, and the default
        when nothing is configured yet, which is what a source has and
        what a destination has before its first handshake.
        """
        if self._http is None or not self._connected or self.base_url is None:
            return False
        refs = {plan.transport_ref for plan in self._streams.values()} or {None}
        try:
            for ref in refs:
                dispatch = await self._dispatch_through(ref)
                if await dispatch.sender.probe(dispatch.base_url) >= 500:
                    return False
            return True
        except TransportSpecError as err:
            logger.warning("API health check: %s", err)
            return False
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

        items_schema = records_items_schema(endpoint_id, read.response)
        apply_read_type_map(items_schema, endpoint_ref, runtime)
        schema_contract = SchemaContract(items_schema)

        request_block = read.request
        method = request_block.method
        records_ref = read.response.records.ref
        pagination = read.pagination

        # One resolver per read, carrying the engine's page size in the
        # runtime scope. Every declared expression this read resolves --
        # param defaults, the body, the page size, the stop condition, the
        # next cursor -- comes from this object (response-scoped per page),
        # because a second one built anywhere else would leave
        # ``runtime.batch_size`` unresolvable and silently read at the wrong
        # page size.
        resolver = runtime.request_resolver(runtime_values={"batch_size": batch_size})

        # One try over every declaration this plan resolves -- the param
        # defaults and the path bindings. All of them leave the request
        # build as one classified error, and all of them mean the same
        # thing here: a read that cannot address its own URL, or fill its
        # own params, never heals on a retry.
        try:
            table = ParamTable.for_read(
                read.params,
                resolver,
                filters=stream_source.filters or [],
            )
            problem = request_block_problem(
                request_block,
                # The names the CONNECTION owns on the transport this read
                # dispatches through -- read from the resolved spec, so no
                # session is opened to judge a declaration, and never from
                # the default's when the read goes out elsewhere.
                reserved_headers=reserved_header_names(
                    runtime.transport_header_names(request_block.transport_ref)
                ),
                resolver=resolver,
                controlled_by=table.controlled_by,
                declared_params=read.params,
                pagination=pagination,
                endpoint=endpoint_id,
            )
            if problem is not None:
                raise ReadError(f"endpoint {endpoint_id!r}: {problem}")

            # isinstance, not a key test: the stream's replication is a
            # method-discriminated union, and ``cursor_field`` is a field of
            # the incremental member alone. Narrowing on the type is what
            # makes the attribute readable at all.
            replication_block = stream_source.replication
            incremental = (
                replication_block
                if isinstance(replication_block, IncrementalReplication)
                else None
            )
            cursor_field = incremental.cursor_field if incremental else None
            if incremental is not None:
                await self._bind_incremental_filter(
                    table.values,
                    read.replication,
                    incremental,
                    checkpoint=checkpoint,
                    stream_name=stream_name,
                    partition=partition,
                )

            # Substituted here, after the incremental filter has written the
            # cursor into the table: a path bound to an ordinary param the
            # filter overrides must see the value this run uses. A
            # placeholder bound to a param either loop OWNS is refused
            # above -- neither loop has produced a value at this point, and
            # the path is substituted once.
            path = substitute_path(
                request_block.path,
                bind_request_values(
                    request_block.path_params,
                    params=table.values,
                    resolver=resolver,
                    block="path_params",
                    endpoint=endpoint_id,
                ),
                endpoint=endpoint_id,
            )
        except RequestSpecError as err:
            raise ReadError(f"endpoint {endpoint_id!r}: {err}") from err

        # The transport this read dispatches through -- the one the request
        # block names, or the connection's default. Opened here rather than
        # at connect(), so a connector whose reads name nothing opens the
        # one session it always opened.
        try:
            dispatch = await self._dispatch_through(request_block.transport_ref)
        except TransportSpecError as err:
            raise ReadError(f"endpoint {endpoint_id!r}: {err}") from err
        full_url = join_url(dispatch.base_url, path)

        builder = RequestBuilder(
            table,
            # getattr, not attribute access: a read request is the
            # method-discriminated union, and only its POST branch declares
            # a body and the media type describing it. A GET read has no
            # such attribute at all, so asking for it by name would be an
            # AttributeError on the commonest read there is.
            raw_body=getattr(request_block, "body", None),
            resolver=resolver,
            endpoint=request_block.path,
            declared_query=request_block.query,
            declared_headers=request_block.headers,
            content_type=getattr(request_block, "content_type", None),
            query_styles=declared_query_styles(request_block.query, read.params),
        )

        strategy = build_read_strategy(
            pagination,
            table=table,
            resolver=resolver,
            url=full_url,
            origin=dispatch.origin,
            batch_size=batch_size,
        )

        return _ReadPlan(
            loop=PageLoop(
                strategy,
                fetch=self._fetcher(
                    dispatch, builder, method=method, records_ref=records_ref
                ),
                stop_when=stop_condition(
                    pagination.stop_when if pagination else None, resolver
                ),
            ),
            schema=schema_contract,
            cursor_field=cursor_field,
        )

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

    def _fetcher(
        self,
        dispatch: _Dispatch,
        builder: RequestBuilder,
        *,
        method: str,
        records_ref: str,
    ) -> Fetch:
        """Build the loop's fetch: one request, one page.

        Every page of a read goes out on the transport the read
        dispatches through -- the traversal never changes transport, which
        is what lets one header map, one credential and one rate limiter
        describe the whole read. A link off that transport's origin is
        refused before it is fetched, by the guard the strategy carries.
        """

        async def fetch(request: PageRequest) -> Page:
            try:
                prepared = builder.for_page(
                    request.params, sends_declared_body=request.sends_declared_body
                )
            except RequestSpecError as err:
                raise ReadError(f"request could not be built: {err}") from err
            signed = SignedRequest(
                method=method,
                url=request.url,
                params=query_pairs(prepared.query),
                headers=prepared.headers,
                body=(
                    None
                    if prepared.body is None
                    else encode_body(prepared.body, prepared.content_type)
                ),
                content_type=prepared.content_type,
            )
            try:
                received = await dispatch.sender.send(signed, unwrap_page=True)
            except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                status, category = failure_facts(err, error_map=self._error_map)
                raise read_verdict(
                    f"API request failed: {method} {request.url} -> {err}",
                    status=status,
                    category=category,
                ) from err
            payload = received.payload
            return Page(records=extract_records(payload, records_ref), payload=payload)

        return fetch

    @staticmethod
    async def _bind_incremental_filter(
        params: dict[str, Any],
        declared_replication: Replication | None,
        stream_replication: IncrementalReplication,
        *,
        checkpoint: CheckpointStore,
        stream_name: str,
        partition: dict[str, Any],
    ) -> None:
        """Write the stored cursor, minus the safety window, into its param.

        ``declared_replication`` is the endpoint document's block;
        ``stream_replication`` is the STREAM document's. Both are contract
        models, and the stream's is the incremental member specifically --
        the caller narrowed it, which is what makes ``cursor_field`` a
        field that exists here rather than one to test for.
        """
        cursor_field = stream_replication.cursor_field
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
        safety_window = stream_replication.safety_window_seconds
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
        """Register each stream's contract endpoint document.

        The engine hands authored JSON across the worker boundary, so the
        signature stays dict-in and each document is parsed here: storage
        stays typed, and a document that cannot satisfy the contract is
        caught now rather than surfacing later as a missing attribute deep
        in the write path.

        A parse failure is recorded against its own stream, never raised.
        This runs from the worker entry point before the gRPC server is
        constructed, so raising would exit the process and the engine would
        see a dead worker instead of a rejected stream -- taking down every
        other stream this worker serves for one malformed document. The
        stream keeps its failure until ``configure_schema`` can answer with
        it on that stream's SchemaAck.
        """
        parsed: dict[str, ApiEndpointDoc] = {}
        problems: dict[str, str] = {}
        for stream_id, document in stream_endpoints.items():
            try:
                parsed[stream_id] = ApiEndpointDoc.model_validate(document)
            except ValidationError as err:
                problems[stream_id] = (
                    f"stream {stream_id!r}: endpoint document does not "
                    f"satisfy ApiEndpointDoc: {err}"
                )
        self._stream_endpoints = parsed
        self._stream_endpoint_problems = problems

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
            block.batching is not None
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

        # Before the "never registered" branch: a stream whose document was
        # registered but did not parse is absent from _stream_endpoints too,
        # and telling its author to call set_stream_endpoints() would name
        # the wrong defect.
        problem = self._stream_endpoint_problems.get(stream_id)
        if problem is not None:
            return self._reject_schema(stream_id, problem)

        doc = self._stream_endpoints.get(stream_id)
        if doc is None:
            return self._reject_schema(
                stream_id,
                f"no preloaded endpoint document for stream_id={stream_id!r}; "
                f"call set_stream_endpoints() before the gRPC server starts",
            )
        runtime = self._runtime
        if self._write_resolver is None or runtime is None:
            return self._reject_schema(
                stream_id,
                "schema configured before connect() built the request resolver",
            )
        outcome = build_write_plan(
            doc,
            schema_spec,
            # The names the connection owns on whichever transport this
            # stream's write dispatches through -- asked by ref, because
            # only build_write_plan knows which mode block, and so which
            # transport_ref, this schema selects.
            header_names_for=runtime.transport_header_names,
            transport_problem=runtime.transport_problem,
            resolver=self._write_resolver,
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
            ack_status, failure_category = self._transport_verdict(error)
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
        failures = _RecordFailures()

        for index, record in enumerate(records):
            try:
                require_conflict_key_values(
                    plan.conflict_keys, (record,), target=plan.endpoint
                )
                encoded, headers = self._prepare_record_request(
                    plan, record, record_ids[index]
                )
            # Three defects, one verdict: the body build answers every way
            # its declaration can fail with RequestSpecError; a record with
            # no value for an upsert conflict key, and the engine-owned
            # idempotency key refusing a body it cannot be added to, raise
            # ValueError. All are deterministic and concern this one record.
            except (RequestSpecError, ValueError) as err:
                failures.add(record_ids[index], err, "failed to build body for record")
                continue
            try:
                received = await self._send(plan, encoded, extra_headers=headers)
            except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                ack_status, failure_category = self._transport_verdict(err)
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
                failures.add(
                    record_ids[index],
                    err,
                    "failed to write record",
                    category=failure_category,
                )
                continue
            rejection = self._judge_sent(
                plan,
                received,
                sent=1,
                written=written,
                record_ids=record_ids,
                position=index,
                failed_before=failures.ids,
            )
            if rejection is not None:
                # The provider accepted the request and rejected the record
                # in the body: this record's failure, and deterministic.
                failures.add(
                    record_ids[index],
                    rejection,
                    "provider rejected record",
                    category=FailureCategory.FAILURE_CATEGORY_WRITE_REJECTED,
                )
                continue
            written += 1

        if failures.ids:
            logger.warning(
                "failed to write %d records: %s...", len(failures.ids), failures.ids[:5]
            )
        return written, failures.ids, failures.first_reason, failures.first_category

    def _prepare_record_request(
        self, plan: StreamWritePlan, record: dict[str, Any], record_id: str
    ) -> tuple[bytes, dict[str, str] | None]:
        """Build one record's encoded body and idempotency header, if any.

        Insert keys on the identity-derived record id (the first occurrence
        of an identity wins, matching the SQL insert anti-join); upsert keys
        on the full record content so a changed row gets a new key and the
        provider applies the update instead of replaying its cached response.
        """
        key = (
            None
            if plan.idempotency_in is None
            else (
                record_id
                if plan.write_mode_key == "insert"
                else content_idempotency_key(record)
            )
        )
        body = self._build_body(plan, record=record)
        if plan.idempotency_in == "body" and key is not None:
            body = body_with_idempotency_key(plan, body, key)
        encoded = encode_body(body, plan.content_type)
        headers = (
            {plan.idempotency_name: key}
            if plan.idempotency_in == "header" and key is not None
            else None
        )
        return encoded, headers

    async def _write_in_chunks(
        self,
        plan: StreamWritePlan,
        records: list[dict[str, Any]],
        record_ids: list[str],
    ) -> tuple[int, list[str], str, FailureCategory]:
        """Write records in chunks of at most ``max_records``.

        Per-item partial failure inside a 2xx response body is visible
        only through the endpoint's declared ``response`` block: the
        contract names no per-item id extraction, so a chunk whose body
        fails ``success_when`` or whose ``affected_records`` disagrees
        with the chunk size is reported failed as a whole. Without a
        declaration a 2xx means the provider accepted the whole chunk.

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
                require_conflict_key_values(
                    plan.conflict_keys, chunk, target=plan.endpoint
                )
                body = self._build_body(plan, records=chunk)
                encoded = encode_body(body, plan.content_type)
            # A chunk with a record the provider cannot match on its
            # conflict keys fails the same way a body that cannot be built
            # does: deterministic, and the whole chunk with it.
            except (RequestSpecError, ValueError) as err:
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
                received = await self._send(plan, encoded)
            except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                ack_status, failure_category = self._transport_verdict(err)
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
            rejection = self._judge_sent(
                plan,
                received,
                sent=len(chunk),
                written=written,
                record_ids=record_ids,
                position=start,
            )
            if rejection is not None:
                logger.warning(
                    "provider rejected batch chunk at offset %d (%d records): %s",
                    start,
                    len(chunk),
                    rejection,
                )
                return (
                    written,
                    list(record_ids[start:]),
                    f"{type(rejection).__name__}: {rejection}",
                    FailureCategory.FAILURE_CATEGORY_WRITE_REJECTED,
                )
            written += len(chunk)

        return written, [], "", FailureCategory.FAILURE_CATEGORY_UNSPECIFIED

    def _transport_verdict(
        self, err: aiohttp.ClientError | asyncio.TimeoutError
    ) -> tuple[AckStatus, FailureCategory]:
        """Classify one transport failure through the declared error map."""
        status, category = failure_facts(err, error_map=self._error_map)
        return write_verdict(status=status, category=category)

    def _judge_sent(
        self,
        plan: StreamWritePlan,
        received: Received,
        *,
        sent: int,
        written: int,
        record_ids: list[str],
        position: int,
        failed_before: list[str] | None = None,
    ) -> DeclaredWriteFailure | None:
        """Let the declared ``response`` block judge one accepted request.

        Nothing declared means the success status was the whole verdict.
        A declared rejection is returned for the caller to pin on the ids
        it covers; a block that cannot read the answer raises the
        config-defect refusal, since whether the request landed is then
        unknowable. That refusal reports failed everything from
        ``position`` on plus ``failed_before``, the ids already failed
        earlier in the loop.

        What the provider handed back (``generated_keys``, ``metadata``)
        has no slot on the ack, so it surfaces on the debug log.
        """
        if plan.response is None:
            return None
        if self._write_resolver is None:
            raise RuntimeError("connector not connected: no request resolver")
        try:
            outcome = judge_write_response(
                plan.response, received, resolver=self._write_resolver, sent=sent
            )
        except DeclaredWriteFailure as err:
            return err
        except RequestSpecError as err:
            raise self._unreadable_response(
                err,
                written=written,
                failed_ids=list(failed_before or []) + record_ids[position:],
            ) from err
        if outcome.has_extractions:
            logger.debug(
                "API write response for %s: generated_keys=%r metadata=%r",
                plan.endpoint,
                outcome.generated_keys,
                outcome.metadata,
            )
        return None

    @staticmethod
    def _unreadable_response(
        err: RequestSpecError, *, written: int, failed_ids: list[str]
    ) -> BatchRejected:
        """Build the refusal for a declared response block that cannot be read.

        An authoring defect, so the configuration owns the fix; the
        records already counted stay written and everything from the
        unjudged request on is reported failed, because whether it landed
        cannot be known from a body the declaration cannot read.
        """
        return BatchRejected(
            f"write response could not be read: {err}",
            category=FailureCategory.FAILURE_CATEGORY_CONFIG_DEFECT,
            records_written=written,
            failed_record_ids=tuple(failed_ids),
        )

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
            # Resolved once at the schema handshake: write params read only
            # what ``request_resolver`` supplies (the connection subtrees and
            # the runtime values -- never secrets), so re-resolving them per
            # record and per chunk could only produce the same values again.
            params=plan.params,
            resolver=self._write_resolver,
            record=record,
            records=records,
        )

    async def _send(
        self,
        plan: StreamWritePlan,
        body: bytes,
        extra_headers: Mapping[str, str] | None = None,
    ) -> Received:
        """Send one write request through the shared sender.

        Takes finished bytes. Encoding lives with the body build instead,
        because it fails for the same reason and about the same records: a
        form body carrying a container is one record's defect, and raising
        it here -- outside the per-record catch, inside a block that expects
        only transport errors -- discarded the count of everything already
        written and let a replay send those records twice.
        """
        dispatch = await self._dispatch_through(plan.transport_ref)
        url = join_url(dispatch.base_url, plan.endpoint)
        # The same containment rule the read's next-page links answer to.
        # A write builds its URL from a declared path rather than from a
        # provider's string, so this holds by construction today -- and
        # asserting it here is what keeps that true when the path stops
        # being the only thing that decides where a write lands.
        require_declared_origin(url, origin=dispatch.origin)
        return await dispatch.sender.send(
            SignedRequest(
                method=plan.method,
                url=url,
                params=query_pairs(plan.query),
                headers={**plan.headers, **dict(extra_headers or {})},
                body=body,
                content_type=plan.content_type,
            ),
            unwrap_page=False,
        )
