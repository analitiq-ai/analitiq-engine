"""File destination handler for writing to the local filesystem.

This handler writes records to files using configurable formatters and a
local storage backend.
"""

import hashlib
import logging
from datetime import datetime
from string import Formatter
from typing import Any

import pyarrow as pa

from cdk.base_handler import (
    BaseDestinationHandler,
    BatchWriteResult,
    os_error_verdict,
    reject_batch,
)
from cdk.connection_runtime import ConnectionRuntime
from cdk.types import AckStatus, Cursor, RetrySemantics, RetryVerdict, SchemaSpec

from ..formatters import get_formatter
from ..formatters.base import BaseFormatter
from ..storage import get_storage_backend
from ..storage.base import BaseStorageBackend

logger = logging.getLogger(__name__)


# Fields build_path fills from the batch instant. A path_template that
# references any of them needs a real emitted_at; one without them (a static
# prefix) renders a deterministic path that never touches the instant.
_TIME_PARTITION_FIELDS = frozenset({"year", "month", "day", "hour"})


def _template_needs_timestamp(template: str | None) -> bool:
    """Report whether ``template`` references any time field build_path fills.

    Resolves field names with the same parser ``str.format`` uses
    (``string.Formatter``), so a format spec or conversion -- ``{year:04d}``,
    ``{hour!s}`` -- is recognized exactly as a bare ``{year}`` would be. The
    guard and ``build_path`` must share one definition of "references the
    instant"; a substring match on ``{year}`` would miss ``{year:04d}`` yet
    ``build_path`` would still substitute it, silently bucketing an unstamped
    batch under 1970. A malformed template raises here just as it would at
    ``build_path``, failing the batch loud.
    """
    if not template:
        return False
    referenced = {
        name.split(".")[0].split("[")[0]
        for _, name, _, _ in Formatter().parse(template)
        if name
    }
    return bool(referenced & _TIME_PARTITION_FIELDS)


def _is_stamped_utc(value: object) -> bool:
    """Report whether ``value`` is a real, engine-stamped tz-aware instant.

    The wire default for an unstamped emitted_at is epoch 0, which decodes to
    a tz-aware ``1970-01-01 00:00:00 UTC`` whose POSIX timestamp is 0; a naive
    datetime is a programming error. Both mean "no real stamp", so both are
    rejected -- only a positive-epoch aware datetime is accepted.
    """
    return (
        isinstance(value, datetime)
        and value.tzinfo is not None
        and value.timestamp() > 0
    )


class FileDestinationHandler(BaseDestinationHandler):
    """
    Destination handler that writes records to files.

    Supports:
    - Multiple output formats (jsonl, csv, parquet)
    - Content-addressed filenames (a replayed batch overwrites the same
      file with the same bytes)
    - Configurable file paths with partitioning

    The storage backend follows the runtime's connector kind. The registry
    routes both "file" and the planned "s3" kind here; only "file" has a
    backend, so an "s3" connection is refused at connect() with
    ``StorageBackendNotBuiltError``. Configuration:
    - file_format: Output format (jsonl, csv, parquet). Default: jsonl
    - path: Base path for files (required for local storage)
    - prefix: Key prefix (optional)
    - path_template: Template for file paths with placeholders
    """

    def __init__(self) -> None:
        """Initialize the file handler."""
        self._runtime: ConnectionRuntime | None = None
        self._storage: BaseStorageBackend | None = None
        self._formatter: BaseFormatter | None = None
        self._config: dict[str, Any] = {}
        self._connector_type: str = "file"
        self._connected: bool = False
        self._path_template: str | None = None

    @property
    def connector_type(self) -> str:
        """Return the connector type identifier."""
        return self._connector_type

    @property
    def supports_transactions(self) -> bool:
        """File destinations do not support transactions."""
        return False

    @property
    def supports_upsert(self) -> bool:
        """File destinations do not support upsert."""
        return False

    @property
    def supports_bulk_load(self) -> bool:
        """File destinations support bulk writes."""
        return True

    # skipcq PYL-R0201: this is the CDK's per-stream verdict hook, not a
    # utility. Its siblings answer from instance state (the API handler
    # returns the verdict it computed at configure time); a file sink's
    # verdict happens to follow from how it writes, so this override reads
    # no attribute. Making one implementation of an overridable hook a
    # @staticmethod would hide that it is an override.
    def retry_semantics(self, stream_id: str) -> RetryVerdict:  # skipcq: PYL-R0201
        """Report at-least-once: a restart may duplicate boundary rows (#306).

        Batch files are content-addressed: the filename carries a hash of
        the serialized bytes (issue #319), so a true replay of the same
        batch overwrites the same file with the same bytes. A same-run
        restart, however, re-reads the inclusive watermark boundary and
        re-batches those rows into different content, which lands in a new
        file alongside the old one. Duplicates are possible, drops are not
        — an append-only sink with no per-row dedup key cannot claim
        exactly-once across a restart.
        """
        _ = stream_id
        return RetryVerdict(
            semantics=RetrySemantics.RETRY_SEMANTICS_AT_LEAST_ONCE,
            reason=(
                "batch files are content-addressed, so a replay overwrites "
                "the same bytes, but a same-run restart re-reads the "
                "inclusive cursor boundary and writes those rows into a "
                "new file"
            ),
        )

    async def connect(self, runtime: ConnectionRuntime) -> None:
        """
        Initialize the file handler with configuration.

        Args:
            runtime: ConnectionRuntime with enriched config

        Raises:
            StorageBackendNotBuiltError: If the runtime's connector kind has
                no storage backend yet (the planned "s3" kind).
        """
        # The kind lives on the runtime (resolved from the connector
        # definition), not in the connection config — an s3 connection's
        # JSON carries no "connector_type" key.
        self._connector_type = runtime.connector_type

        # The kind alone decides the backend, so resolve it before the runtime
        # is acquired: an unbuilt kind then fails naming itself, with no
        # shared ownership taken, no config materialized and no storage
        # connection opened. It does not spare the secret store — the engine
        # shell resolves the connection (ConnectionRuntime.resolve_spec) into
        # the worker's launch bootstrap before this process starts, so the
        # credentials were already fetched by the time connect() runs.
        storage = get_storage_backend(self._connector_type)
        self._storage = storage

        runtime.acquire()
        await runtime.materialize()
        connection_config = runtime.resolved_config
        self._runtime = runtime

        try:
            await storage.connect(connection_config)

            # Create formatter
            file_format = connection_config.get("file_format", "jsonl")
            self._formatter = get_formatter(file_format)

            # Configure formatter with any format-specific options
            formatter_config = connection_config.get("formatter_config", {})
            self._formatter.configure(formatter_config)

            # Store path template if provided
            self._path_template = connection_config.get("path_template")

            # Retain only the non-secret fields needed after connect().
            # write_batch() uses path or prefix (as fallback) for build_path().
            # _path_template is stored separately above.
            self._config = {
                "path": connection_config.get("path", ""),
                "prefix": connection_config.get("prefix", ""),
            }
        finally:
            runtime.scrub_resolved_config()

        self._connected = True
        logger.info(
            "FileDestinationHandler connected: storage=%s, format=%s",
            storage.storage_type,
            file_format,
        )

    async def disconnect(self) -> None:
        """Disconnect the file handler."""
        if self._storage and self._connected:
            await self._storage.disconnect()
        if self._runtime:
            await self._runtime.close()
        self._connected = False
        logger.info("FileDestinationHandler disconnected")

    # skipcq PYL-R0201: configure_schema is abstract on
    # BaseDestinationHandler and a member of the Writable protocol, so the
    # contract owns its shape. A file destination pre-creates nothing, which
    # is why this implementation reads no attribute -- a reason to keep the
    # method empty of work, not a reason to restate the contract's own
    # signature as a @staticmethod.
    async def configure_schema(  # skipcq: PYL-R0201
        self, schema_spec: SchemaSpec
    ) -> bool:
        """Accept the schema for a stream.

        File destinations don't pre-create anything; the formatter shapes
        each batch on write. If a formatter ever needs the column list, it
        can look up the contract endpoint via ``set_stream_endpoints``.
        """
        logger.info(
            "FileDestinationHandler: schema accepted for stream %s",
            schema_spec.stream_id,
        )
        return True

    async def write_batch(
        self,
        run_id: str,
        stream_id: str,
        batch_seq: int,
        record_batch: pa.RecordBatch,
        record_ids: list[str],
        cursor: Cursor,
        emitted_at: datetime,
    ) -> BatchWriteResult:
        """Write an Arrow record batch to a file.

        Formatters consume dicts, so the batch is materialized once at
        this boundary. When a ``path_template`` carries time placeholders,
        the partition directory resolves from ``emitted_at`` -- the engine's
        replay-stable per-batch instant -- so a retried batch overwrites the
        same file instead of landing in a new partition (issue #353).
        """
        if not self._connected:
            return reject_batch(
                logger,
                "Handler not connected",
                run_id=run_id,
                stream_id=stream_id,
                batch_seq=batch_seq,
            )

        if self._storage is None or self._formatter is None:
            missing = [
                name
                for name, component in (
                    ("storage", self._storage),
                    ("formatter", self._formatter),
                )
                if component is None
            ]
            return reject_batch(
                logger,
                f"Handler components not initialized: {', '.join(missing)}",
                run_id=run_id,
                stream_id=stream_id,
                batch_seq=batch_seq,
            )

        try:
            records = record_batch.to_pylist()

            if not records:
                # Nothing to write; ack success so the cursor still advances.
                # An empty batch never renders a partition path, so it needs
                # no emitted_at.
                return BatchWriteResult(
                    status=AckStatus.ACK_STATUS_SUCCESS,
                    records_written=0,
                    committed_cursor=cursor,
                )

            # A time-partitioned layout depends entirely on emitted_at being a
            # real, replay-stable UTC instant. It crosses the process boundary
            # from the engine (and, for a sandboxed connector, a second gRPC
            # hop), so validate it before writing: a missing or epoch-zero
            # stamp would silently bucket every replay under year=1970 and
            # defeat the same-path overwrite this value exists to guarantee
            # (issue #353). The guard fires only for a template that actually
            # substitutes a time placeholder -- a static-prefix template never
            # touches emitted_at, so it needs no stamp. Fail loud so the batch
            # routes to the DLQ instead.
            if _template_needs_timestamp(self._path_template) and not _is_stamped_utc(
                emitted_at
            ):
                msg = (
                    f"file destination has a time-based path_template "
                    f"({self._path_template!r}) but the batch carries no "
                    f"usable emitted_at ({emitted_at!r}); refusing to derive a "
                    f"partition path from the write-time clock "
                    f"(run={run_id}, stream={stream_id}, seq={batch_seq})"
                )
                logger.error(msg)
                return BatchWriteResult(
                    status=AckStatus.ACK_STATUS_FATAL_FAILURE,
                    records_written=0,
                    failure_summary=msg,
                )

            # Serialize before building path so the filename includes a content
            # hash: different content at the same batch_seq lands in a new file
            # (issue #319), while identical content overwrites the same file
            # with the same bytes — the write itself is the replay dedup, with
            # no batch-level commit ledger (issue #306).
            data = self._formatter.serialize_batch(records)

            if not data:
                # A non-empty records list that serialized to empty bytes is a
                # formatter contract violation; writing a zero-byte file and
                # committing records_written=N would silently drop all N rows
                # (issue #322). Fail loud so the batch routes to the DLQ.
                msg = (
                    f"{type(self._formatter).__name__}.serialize_batch() "
                    f"returned empty bytes for {len(records)} records "
                    f"(run={run_id}, stream={stream_id}, seq={batch_seq})"
                )
                logger.error(msg)
                return BatchWriteResult(
                    status=AckStatus.ACK_STATUS_FATAL_FAILURE,
                    records_written=0,
                    failure_summary=msg,
                )

            content_hash = hashlib.sha256(data).hexdigest()[:16]

            # Build file path
            base_path = self._config.get("path", "") or self._config.get("prefix", "")
            file_path = self._storage.build_path(
                base_path=base_path,
                stream_id=stream_id,
                batch_seq=batch_seq,
                content_hash=content_hash,
                extension=self._formatter.file_extension,
                timestamp=emitted_at,
                partition_template=self._path_template,
            )

            # Write to storage
            written_path = await self._storage.write_file(
                path=file_path,
                data=data,
                content_type=self._formatter.content_type,
            )

            logger.info(
                "Wrote batch %s: %s records, %s bytes to %s",
                batch_seq,
                len(records),
                len(data),
                written_path,
            )

            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_SUCCESS,
                records_written=len(records),
                committed_cursor=cursor,
            )

        except OSError as e:
            return os_error_verdict(
                logger,
                e,
                run_id=run_id,
                stream_id=stream_id,
                batch_seq=batch_seq,
                what="batch",
            )
        except Exception as e:
            # The formatter and the storage backend are both pluggable and
            # both sit inside this try, so without their identities a broken
            # Parquet formatter and a broken CSV one log identically (#328).
            logger.error(
                "Fatal error writing batch "
                "(run=%s, stream=%s, seq=%s, formatter=%s, storage=%s): %s",
                run_id,
                stream_id,
                batch_seq,
                type(self._formatter).__name__,
                type(self._storage).__name__,
                e,
                exc_info=True,
            )
            return BatchWriteResult(
                status=AckStatus.ACK_STATUS_FATAL_FAILURE,
                records_written=0,
                failure_summary=f"{type(e).__name__}: {e}",
            )

    async def health_check(self) -> bool:
        """
        Check if file destination is healthy.

        Returns:
            True if storage is accessible
        """
        if not self._connected or self._storage is None:
            return False

        return await self._storage.health_check()
