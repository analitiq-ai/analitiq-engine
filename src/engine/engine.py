"""Multi-stream engine: orchestrates one StreamProcessor per stream."""

import asyncio
import logging
from typing import Any

from ..models.metrics import PipelineMetrics
from ..models.resolved import RuntimeConfig
from ..state.error_classification import ErrorCode, dominant_error_code
from ..state.state_manager import StateManager
from .exceptions import ConfigurationError, StreamProcessingError
from .stream_processor import StreamProcessor

logger = logging.getLogger(__name__)


class StreamingEngine:
    """
    High-performance async multi-stream engine with state management.

    Features:
    - Multi-stream concurrent processing
    - State management for scalability
    - Pydantic validation for configuration
    - Async/await pattern for non-blocking I/O
    - Fault tolerance with retries and DLQ
    - Configurable batch processing with backpressure handling

    Everything scoped to a single stream lives in
    :class:`~.stream_processor.StreamProcessor`; the engine holds the
    pipeline-wide pieces (state manager, cumulative metrics, worker client)
    and fans streams out.
    """

    def __init__(
        self,
        pipeline_id: str,
        runtime: RuntimeConfig,
        dlq_path: str = "./deadletter/",
    ):
        self.pipeline_id = pipeline_id
        self.runtime = runtime
        self.dlq_path = dlq_path
        # Working values derived from the single typed RuntimeConfig (built by
        # the runner with pipeline-config > env > default precedence). No loose
        # constructor defaults: the only source of truth is the RuntimeConfig.
        # error_strategy and retry_delay stay plain attributes so a test can
        # vary one knob without rebuilding the frozen config.
        self.batch_size = runtime.batching.batch_size
        self.max_concurrent_batches = runtime.batching.max_concurrent_batches
        self.buffer_size = runtime.buffer_size
        self.max_retries = runtime.error_handling.max_retries
        self.retry_delay = runtime.error_handling.retry_delay_seconds
        self.error_strategy = runtime.error_handling.strategy

        # Pipeline-wide state management
        self.sharded_state_manager = StateManager(pipeline_id, "./state")
        self._state_manager = self.sharded_state_manager

        # Metrics tracking with Pydantic validation
        self.metrics = PipelineMetrics()

        # Representative exception from a stream that failed while OTHER streams
        # succeeded (a partial run that stream_data does not re-raise). The
        # runner classifies it so a partial run with a failed stream is not
        # reported as success.
        self._dominant_stream_error: BaseException | None = None

        # Classified cause of each stream that completed PARTIAL (dlq/skip
        # exhausted batches without raising). The exception channel above
        # never sees these -- nothing raised -- so the runner reads the
        # dominant of this list for its no-exception partial classification
        # (issue #351).
        self._partial_error_codes: list[ErrorCode] = []

        # Connector code never runs in the engine process: every source read
        # goes through an isolated worker subprocess that owns the connector
        # class, the driver, and the external connection (registry resolution
        # happens IN the worker). The engine keeps only the worker client.
        from src.engine.pipeline_config_prep import PipelineConfigPrep
        from src.worker.readable import WorkerReadable

        paths = PipelineConfigPrep._discover_paths()
        self._worker_readable = WorkerReadable(
            connectors_dir=paths["connectors"],
            connections_dir=paths["connections"],
        )

    async def stream_data(self, pipeline_config: dict[str, Any]) -> None:
        """Process all streams concurrently with state management.

        ``pipeline_config`` is the runner-assembled config dict (pipeline id,
        name, and per-stream source/destination/mapping). Runtime tuning is not
        carried here -- it is the typed :class:`RuntimeConfig` passed to the
        constructor and read via ``self.runtime`` and the values derived from
        it.
        """
        pipeline_id = pipeline_config["pipeline_id"]
        streams = pipeline_config.get("streams", {})

        if not streams:
            raise ConfigurationError("No streams configured in pipeline")

        logger.info(f"Starting pipeline: {pipeline_id}")
        logger.info(f"Processing {len(streams)} streams concurrently")

        # Start new run
        run_id = self.state_manager.start_run(pipeline_config)
        logger.info(f"Started state run: {run_id}")

        stream_exceptions = []
        stream_tasks = []

        try:
            # Create a task for each stream with names for better debugging
            for stream_id, stream_config in streams.items():
                stream_name = stream_config.get("name", stream_id)
                logger.info(f"Starting stream: {stream_name}")

                task = asyncio.create_task(
                    self._process_stream(
                        stream_id=stream_id,
                        stream_config=stream_config,
                        pipeline_config=pipeline_config,
                    ),
                    name=f"stream-{stream_name}",
                )
                stream_tasks.append((stream_id, stream_name, task))

            # Wait for all streams with exception collection
            results = await asyncio.gather(
                *[task for _, _, task in stream_tasks], return_exceptions=True
            )

            # Collect exceptions using Python 3.11+ pattern
            for (stream_id, stream_name, _), result in zip(stream_tasks, results):
                if isinstance(result, Exception):
                    stream_error = StreamProcessingError(
                        f"Stream processing failed: {result}",
                        stream_id=stream_id,
                        original_error=result,
                    )
                    stream_exceptions.append(stream_error)
                    self.metrics.increment_streams_failed()
                    logger.error(f"Stream {stream_name} failed: {result}")
                else:
                    self.metrics.increment_streams_processed()
                    logger.info(f"Stream {stream_name} completed successfully")

            # Handle collected exceptions with ExceptionGroup
            if stream_exceptions:
                if len(stream_exceptions) == len(streams):
                    # All streams failed - critical failure
                    logger.error("All streams failed - pipeline failed completely")
                    raise ExceptionGroup("All streams failed", stream_exceptions)
                else:
                    # Partial failure - log but allow pipeline to complete. Keep
                    # ALL failed-stream exceptions as a group (like the all-failed
                    # path) so the runner classifies the dominant cause across
                    # every failure, not just the first.
                    logger.warning(
                        f"Pipeline completed with {len(stream_exceptions)} "
                        f"failed streams out of {len(streams)}"
                    )
                    self._dominant_stream_error = ExceptionGroup(
                        "Partial stream failures", stream_exceptions
                    )
            else:
                logger.info(
                    f"Pipeline {pipeline_id} completed successfully - "
                    f"all {len(streams)} streams processed"
                )

        except* StreamProcessingError as eg:
            # Handle stream processing errors specifically (Python 3.11+)
            logger.error(
                f"Stream processing errors occurred: "
                f"{len(eg.exceptions)} streams failed"
            )
            for exc in eg.exceptions:
                logger.error(f"  - {exc}")
            # Cancel remaining tasks
            for _, _, task in stream_tasks:
                if not task.done():
                    task.cancel()
            raise
        except* Exception as eg:
            # Handle other unexpected errors (Python 3.11+)
            logger.error(f"Unexpected errors in pipeline: {len(eg.exceptions)} errors")
            for unexpected_exc in eg.exceptions:
                logger.error(f"  - Unexpected error: {unexpected_exc}")
            # Cancel remaining tasks
            for _, _, task in stream_tasks:
                if not task.done():
                    task.cancel()
            raise

    async def _process_stream(
        self,
        stream_id: str,
        stream_config: dict[str, Any],
        pipeline_config: dict[str, Any],
    ) -> None:
        """Run one stream through its own StreamProcessor.

        The processor owns everything stream-scoped; this wrapper hands it
        the pipeline-wide pieces and surfaces a partial completion's
        classified cause at pipeline level after the run.
        """
        processor = StreamProcessor(
            stream_id=stream_id,
            stream_config=stream_config,
            pipeline_config=pipeline_config,
            pipeline_id=self.pipeline_id,
            state_manager=self.state_manager,
            pipeline_metrics=self.metrics,
            worker_readable=self._worker_readable,
            dlq_root=self.dlq_path,
            batch_size=self.batch_size,
            buffer_size=self.buffer_size,
            max_retries=self.max_retries,
            retry_delay=self.retry_delay,
            error_strategy=self.error_strategy,
        )
        await processor.run()
        if processor.partial_error_code is not None:
            self._partial_error_codes.append(processor.partial_error_code)

    def get_metrics(self) -> PipelineMetrics:
        """Get pipeline execution metrics as a validated Pydantic model."""
        return self.metrics

    def get_dominant_stream_error(self) -> BaseException | None:
        """Return the failed-stream exceptions from a partial run, as a group.

        Set when some (not all) streams fail: stream_data logs and returns
        without raising, so the runner reads this and classifies the dominant
        cause across all failed streams to classify the partial run.
        """
        return self._dominant_stream_error

    def get_partial_error_code(self) -> ErrorCode | None:
        """Return the dominant classified cause across partial streams.

        A dlq/skip stream exhausts batches without raising, so no exception
        reaches the runner; each such stream's already-classified code lands
        here instead. None when no stream completed partial -- the runner
        then keeps its load-stage default (issue #351).
        """
        return dominant_error_code(self._partial_error_codes)

    def get_state_manager(self) -> StateManager:
        """Get the state manager instance."""
        return self.state_manager

    @property
    def state_manager(self) -> StateManager:
        return self._state_manager

    @state_manager.setter
    def state_manager(self, value: StateManager) -> None:
        self._state_manager = value
        self.sharded_state_manager = value

    @state_manager.deleter
    def state_manager(self) -> None:
        self._state_manager = self.sharded_state_manager
