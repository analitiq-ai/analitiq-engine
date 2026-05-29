"""Pipeline composition: wires ResolvedPipeline into StreamingEngine."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv

from .engine import StreamingEngine
from .data_transformer import _translate_assignment  # re-export for tests
from ..models.resolved import ResolvedPipeline

logger = logging.getLogger(__name__)

__all__ = ["Pipeline", "_translate_assignment"]


class Pipeline:
    """Compose the resolved pipeline config and start the streaming engine."""

    def __init__(
        self,
        pipeline: ResolvedPipeline,
        state_dir: Optional[str] = None,
    ):
        load_dotenv()

        self.pipeline = pipeline
        pipeline_id = pipeline.pipeline_id
        project_root = Path(__file__).parent.parent.parent
        self.state_dir = state_dir or str(project_root / "state")
        self.dlq_dir = str(project_root / "deadletter" / pipeline_id)
        self._ensure_directories()

        batching = pipeline.runtime.batching or {"batch_size": 1000, "max_concurrent_batches": 3}
        error_handling = pipeline.runtime.error_handling or {
            "max_retries": 3,
            "retry_delay_seconds": 5,
        }
        self.engine = StreamingEngine(
            pipeline_id=pipeline_id,
            batch_size=batching.get("batch_size", 1000),
            max_concurrent_batches=batching.get("max_concurrent_batches", 3),
            buffer_size=pipeline.runtime.buffer_size,
            dlq_path=self.dlq_dir,
            max_retries=error_handling.get("max_retries", 3),
            retry_delay=error_handling.get("retry_delay_seconds", 5),
        )

    def _ensure_directories(self) -> None:
        Path(self.state_dir).mkdir(parents=True, exist_ok=True)
        Path(self.dlq_dir).mkdir(parents=True, exist_ok=True)

    async def run(self) -> None:
        try:
            await self.engine.stream_data(self.pipeline)
            logger.info(
                "Pipeline %s completed successfully",
                self.pipeline.pipeline_id,
            )
        except Exception:
            logger.exception(
                "Pipeline %s failed", self.pipeline.pipeline_id
            )
            raise

    def get_metrics(self) -> Any:
        return self.engine.get_metrics()
