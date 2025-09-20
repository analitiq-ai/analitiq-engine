"""Pipeline orchestration and execution coordination."""

import asyncio
import logging
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from pydantic import ValidationError

from .exceptions import (
    PipelineOrchestrationError, StreamExecutionError, 
    StreamConfigurationError, PipelineValidationError
)
from ..models.engine import (
    StreamProcessingConfig, PipelineMetricsSnapshot, TaskExecutionInfo
)
from ..models.state import PipelineConfig

logger = logging.getLogger(__name__)


def _deep_merge_dicts(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge dictionaries without mutating the originals."""

    base = base or {}
    override = override or {}
    merged = deepcopy(base)

    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge_dicts(merged[key], value)
        else:
            merged[key] = deepcopy(value)

    return merged


class PipelineOrchestrator:
    """
    Coordinates pipeline execution with multiple streams.
    
    This class is responsible for:
    - Stream task creation and management
    - Exception collection and handling with Python 3.11+ patterns
    - Pipeline-level metrics aggregation
    - Structured logging with correlation IDs
    
    Note: This is separate from pipeline.py which handles configuration loading and validation.
    The orchestrator focuses purely on execution coordination.
    """
    
    def __init__(self, pipeline_id: str):
        self.pipeline_id = pipeline_id
        self.run_id = f"{datetime.now(timezone.utc).isoformat()}-{str(uuid4())[:8]}"
        self.started_at = datetime.now(timezone.utc)
        self.logger = logging.getLogger(f"{__name__}.{pipeline_id}")
        
        # Execution tracking
        self.active_tasks: Dict[str, TaskExecutionInfo] = {}
        self.completed_tasks: Dict[str, TaskExecutionInfo] = {}
        self.failed_tasks: Dict[str, TaskExecutionInfo] = {}
        
        # Metrics aggregation
        self._metrics = PipelineMetricsSnapshot(
            pipeline_id=pipeline_id,
            run_id=self.run_id,
            started_at=self.started_at
        )
    
    async def orchestrate_pipeline(
        self, 
        pipeline_config: Dict[str, Any],
        stream_processor_factory
    ) -> PipelineMetricsSnapshot:
        """
        Orchestrate the execution of multiple streams concurrently.
        
        Args:
            pipeline_config: Complete pipeline configuration
            stream_processor_factory: Factory function to create stream processors
            
        Returns:
            PipelineMetricsSnapshot: Final metrics for the pipeline
            
        Raises:
            PipelineOrchestrationError: When pipeline orchestration fails
            PipelineValidationError: When configuration validation fails
        """
        try:
            # Validate pipeline configuration
            validated_config = self._validate_pipeline_config(pipeline_config)
            
            streams = validated_config.streams
            if not streams:
                raise PipelineOrchestrationError(
                    "No streams configured in pipeline", 
                    self.pipeline_id
                )
            
            self.logger.info(
                "Starting pipeline orchestration",
                extra={
                    "pipeline_id": self.pipeline_id,
                    "run_id": self.run_id,
                    "stream_count": len(streams),
                    "correlation_id": self.run_id
                }
            )
            
            # Update metrics
            self._metrics.total_streams = len(streams)
            
            # Create and execute stream tasks
            stream_tasks = await self._create_stream_tasks(
                streams, pipeline_config, stream_processor_factory
            )
            
            # Execute all streams concurrently with exception collection
            results = await self._execute_streams_concurrently(stream_tasks)
            
            # Process results and update metrics
            self._process_execution_results(stream_tasks, results)
            
            # Calculate final metrics
            self._calculate_final_metrics()
            
            self.logger.info(
                "Pipeline orchestration completed",
                extra={
                    "pipeline_id": self.pipeline_id,
                    "run_id": self.run_id,
                    "success_rate": self._metrics.success_rate,
                    "total_records": self._metrics.total_records_processed,
                    "correlation_id": self.run_id
                }
            )
            
            return self._metrics
            
        except Exception as e:
            self.logger.error(
                "Pipeline orchestration failed",
                extra={
                    "pipeline_id": self.pipeline_id,
                    "run_id": self.run_id,
                    "error": str(e),
                    "correlation_id": self.run_id
                }
            )
            raise PipelineOrchestrationError(
                f"Pipeline orchestration failed: {str(e)}",
                self.pipeline_id
            ) from e
    
    def _validate_pipeline_config(self, pipeline_config: Dict[str, Any]) -> PipelineConfig:
        """Validate pipeline configuration using Pydantic."""
        try:
            return PipelineConfig(**pipeline_config)
        except ValidationError as e:
            error_details = {}
            for error in e.errors():
                field_path = ".".join(str(x) for x in error["loc"])
                if field_path not in error_details:
                    error_details[field_path] = []
                error_details[field_path].append(error["msg"])
            
            raise PipelineValidationError(
                f"Pipeline configuration validation failed: {len(e.errors())} errors",
                error_details
            ) from e
    
    async def _create_stream_tasks(
        self, 
        streams: Dict[str, Any],
        pipeline_config: Dict[str, Any],
        stream_processor_factory
    ) -> List[Tuple[str, str, asyncio.Task]]:
        """Create async tasks for each stream."""
        stream_tasks = []
        
        for stream_id, stream_config in streams.items():
            try:
                stream_name = stream_config.get('name', stream_id)
                
                # Validate and build stream processing config
                processing_config = self._build_stream_processing_config(
                    stream_id, stream_config, pipeline_config
                )
                
                # Create task with proper naming for debugging
                task = asyncio.create_task(
                    stream_processor_factory(processing_config),
                    name=f"stream-{stream_name}"
                )
                
                # Track task
                task_info = TaskExecutionInfo(
                    stream_id=stream_id,
                    stream_name=stream_name,
                    task_name=f"stream-{stream_name}",
                    started_at=datetime.now(timezone.utc),
                    status="pending"
                )
                self.active_tasks[stream_id] = task_info
                
                stream_tasks.append((stream_id, stream_name, task))
                
                self.logger.debug(
                    "Created stream task",
                    extra={
                        "stream_id": stream_id,
                        "stream_name": stream_name,
                        "correlation_id": self.run_id
                    }
                )
                
            except Exception as e:
                raise StreamConfigurationError(
                    f"Failed to create task for stream: {str(e)}",
                    stream_id
                ) from e
        
        return stream_tasks
    
    def _build_stream_processing_config(
        self,
        stream_id: str,
        stream_config: Dict[str, Any], 
        pipeline_config: Dict[str, Any]
    ) -> StreamProcessingConfig:
        """Build and validate stream processing configuration."""
        try:
            # Merge pipeline and stream configurations
            pipeline_src_config = (
                pipeline_config.get("src")
                or pipeline_config.get("source")
                or {}
            )
            pipeline_dst_config = (
                pipeline_config.get("dst")
                or pipeline_config.get("destination")
                or {}
            )

            stream_src_config = stream_config.get("src") or stream_config.get("source")
            stream_dst_config = stream_config.get("dst") or stream_config.get("destination")

            merged_src = _deep_merge_dicts(pipeline_src_config, stream_src_config or {})
            merged_dst = _deep_merge_dicts(pipeline_dst_config, stream_dst_config or {})

            processing_config = {
                **pipeline_config,
                **stream_config,
                "stream_id": stream_id,
                "stream_name": stream_config.get("name", stream_id),
                "pipeline_id": self.pipeline_id,
                "src": merged_src,
                "dst": merged_dst,
            }

            # Remove legacy keys if present to avoid confusion during validation
            processing_config.pop("source", None)
            processing_config.pop("destination", None)

            # Validate using Pydantic
            return StreamProcessingConfig(**processing_config)
            
        except ValidationError as e:
            error_messages = [f"{'.'.join(str(x) for x in err['loc'])}: {err['msg']}" for err in e.errors()]
            raise StreamConfigurationError(
                f"Stream configuration validation failed",
                stream_id,
                validation_errors=error_messages
            ) from e
    
    async def _execute_streams_concurrently(
        self, 
        stream_tasks: List[Tuple[str, str, asyncio.Task]]
    ) -> List[Any]:
        """Execute all stream tasks concurrently with proper exception handling."""
        # Update task status to running
        for stream_id, _, _ in stream_tasks:
            self.active_tasks[stream_id].status = "running"
        
        # Execute all tasks concurrently
        results = await asyncio.gather(
            *[task for _, _, task in stream_tasks],
            return_exceptions=True
        )
        
        return results
    
    def _process_execution_results(
        self,
        stream_tasks: List[Tuple[str, str, asyncio.Task]],
        results: List[Any]
    ) -> None:
        """Process execution results and collect exceptions."""
        stream_exceptions = []
        
        for (stream_id, stream_name, task), result in zip(stream_tasks, results):
            task_info = self.active_tasks[stream_id]
            
            if isinstance(result, Exception):
                # Task failed
                stream_error = StreamExecutionError(
                    f"Stream execution failed: {result}",
                    stream_id=stream_id,
                    original_error=result
                )
                stream_exceptions.append(stream_error)
                
                task_info.status = "failed"
                self.failed_tasks[stream_id] = task_info
                self._metrics.failed_streams += 1
                
                self.logger.error(
                    "Stream execution failed",
                    extra={
                        "stream_id": stream_id,
                        "stream_name": stream_name,
                        "error": str(result),
                        "correlation_id": self.run_id
                    }
                )
            else:
                # Task completed successfully
                task_info.status = "completed"
                self.completed_tasks[stream_id] = task_info
                self._metrics.completed_streams += 1
                
                self.logger.info(
                    "Stream execution completed",
                    extra={
                        "stream_id": stream_id,
                        "stream_name": stream_name,
                        "correlation_id": self.run_id
                    }
                )
            
            # Remove from active tasks
            del self.active_tasks[stream_id]
        
        # Handle collected exceptions using Python 3.11+ patterns
        if stream_exceptions:
            if len(stream_exceptions) == len(stream_tasks):
                # All streams failed - critical failure
                self.logger.error(
                    "All streams failed - pipeline failed completely",
                    extra={
                        "pipeline_id": self.pipeline_id,
                        "failed_count": len(stream_exceptions),
                        "correlation_id": self.run_id
                    }
                )
                raise ExceptionGroup("All streams failed", stream_exceptions)
            else:
                # Partial failure - log warning but continue
                self.logger.warning(
                    f"Pipeline completed with partial failures",
                    extra={
                        "pipeline_id": self.pipeline_id,
                        "failed_streams": len(stream_exceptions),
                        "total_streams": len(stream_tasks),
                        "success_rate": self._metrics.success_rate,
                        "correlation_id": self.run_id
                    }
                )
    
    def _calculate_final_metrics(self) -> None:
        """Calculate final pipeline metrics."""
        # This would typically aggregate metrics from individual streams
        # For now, we'll use placeholder calculations
        
        total_duration = (datetime.now(timezone.utc) - self.started_at).total_seconds()
        
        if total_duration > 0:
            self._metrics.records_per_second = self._metrics.total_records_processed / total_duration
        
        if self._metrics.total_batches_processed > 0:
            self._metrics.average_batch_size = (
                self._metrics.total_records_processed / self._metrics.total_batches_processed
            )
    
    def get_current_metrics(self) -> PipelineMetricsSnapshot:
        """Get current pipeline metrics snapshot."""
        return self._metrics.model_copy()
    
    def get_active_tasks(self) -> Dict[str, TaskExecutionInfo]:
        """Get information about currently active tasks."""
        return {k: v.model_copy() for k, v in self.active_tasks.items()}