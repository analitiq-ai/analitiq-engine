"""Dead Letter Queue implementation for handling failed records.

Records that fail processing are written to local JSONL files for later
inspection or replay. Cloud persistence (S3, etc.) is handled at the
deployment layer via mounted volumes or sidecar processes.

Lightweight DLQ summaries are emitted to stdout via ANALITIQ_DLQ:: marker
for observability (CloudWatch, log shippers). Payloads never go to stdout.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

from src.state.log_emitter import emit_log

logger = logging.getLogger(__name__)


def emit_dlq_log(
    pipeline_id: str,
    stream_id: Optional[str],
    added: int,
    total: int,
) -> None:
    """Emit a lightweight DLQ summary to stdout for observability.

    Never emits record payloads -- only counts.
    """
    data: Dict[str, Any] = {
        "type": "dlq",
        "pipeline_id": pipeline_id,
        "added": added,
        "total": total,
    }
    if stream_id:
        data["stream_id"] = stream_id
    emit_log("dlq", data)


class DateTimeEncoder(json.JSONEncoder):
    """JSON encoder that handles datetime objects."""

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class LocalDLQStorage:
    """Local filesystem DLQ storage backend."""

    def __init__(
        self,
        dlq_path: str,
        max_file_size: int = 10 * 1024 * 1024,
        max_files: int = 100,
    ):
        """Initialize local DLQ storage."""
        self.dlq_path = Path(dlq_path)
        self.max_file_size = max_file_size
        self.max_files = max_files

        # Create DLQ directory
        self.dlq_path.mkdir(parents=True, exist_ok=True)

        # Current file tracking
        self.current_file: Optional[Path] = None
        self.current_file_size = 0
        self.lock = asyncio.Lock()

    def _need_new_file(self) -> bool:
        """Check if we need to create a new DLQ file."""
        return (
            self.current_file is None
            or self.current_file_size >= self.max_file_size
            or not self.current_file.exists()
        )

    async def _create_new_file(self) -> None:
        """Create a new DLQ file."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        self.current_file = self.dlq_path / f"dlq_{timestamp}.jsonl"
        self.current_file_size = 0
        self.current_file.touch()
        logger.debug(f"Created new DLQ file: {self.current_file}")

    async def write_record(self, record: Dict[str, Any], stream_id: Optional[str] = None) -> None:
        """Write a DLQ record to local file."""
        async with self.lock:
            try:
                if self._need_new_file():
                    await self._create_new_file()

                record_json = json.dumps(record, cls=DateTimeEncoder) + "\n"
                record_bytes = record_json.encode("utf-8")

                with open(self.current_file, "a", encoding="utf-8") as f:
                    f.write(record_json)

                self.current_file_size += len(record_bytes)

            except Exception as e:
                logger.error(f"Failed to write to DLQ: {e}")
                # Fallback to unique file
                fallback_file = (
                    self.dlq_path
                    / f"dlq_fallback_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')}.json"
                )
                with open(fallback_file, "w", encoding="utf-8") as f:
                    json.dump(record, f, indent=2, cls=DateTimeEncoder)

    async def get_records(
        self, pipeline_id: Optional[str] = None, stream_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get DLQ records from local files."""
        records = []

        try:
            dlq_files = list(self.dlq_path.glob("dlq_*.jsonl"))

            for dlq_file in dlq_files:
                with open(dlq_file, "r", encoding="utf-8") as f:
                    for line in f:
                        if line.strip():
                            try:
                                record = json.loads(line.strip())
                                if pipeline_id is None or record.get("pipeline_id") == pipeline_id:
                                    records.append(record)
                            except json.JSONDecodeError:
                                logger.warning(f"Invalid JSON in DLQ file {dlq_file}")

        except Exception as e:
            logger.error(f"Failed to read DLQ records: {e}")

        return records

    async def get_stats(self) -> Dict[str, Any]:
        """Get DLQ statistics."""
        stats = {
            "total_records": 0,
            "records_by_pipeline": {},
            "records_by_error_type": {},
            "oldest_record": None,
            "newest_record": None,
            "total_files": 0,
            "total_size_bytes": 0,
        }

        try:
            dlq_files = list(self.dlq_path.glob("dlq_*.jsonl"))
            stats["total_files"] = len(dlq_files)

            for dlq_file in dlq_files:
                stats["total_size_bytes"] += dlq_file.stat().st_size

                with open(dlq_file, "r", encoding="utf-8") as f:
                    for line in f:
                        if line.strip():
                            try:
                                record = json.loads(line.strip())
                                stats["total_records"] += 1

                                pipeline_id = record.get("pipeline_id", "unknown")
                                stats["records_by_pipeline"][pipeline_id] = (
                                    stats["records_by_pipeline"].get(pipeline_id, 0) + 1
                                )

                                error_type = record.get("error", {}).get("type", "unknown")
                                stats["records_by_error_type"][error_type] = (
                                    stats["records_by_error_type"].get(error_type, 0) + 1
                                )

                                timestamp = record.get("timestamp")
                                if timestamp:
                                    if stats["oldest_record"] is None or timestamp < stats["oldest_record"]:
                                        stats["oldest_record"] = timestamp
                                    if stats["newest_record"] is None or timestamp > stats["newest_record"]:
                                        stats["newest_record"] = timestamp

                            except json.JSONDecodeError:
                                continue

        except Exception as e:
            logger.error(f"Failed to get DLQ stats: {e}")

        return stats

    async def clear(self, pipeline_id: Optional[str] = None) -> None:
        """Clear DLQ records."""
        if pipeline_id is None:
            dlq_files = list(self.dlq_path.glob("dlq_*.jsonl"))
            for dlq_file in dlq_files:
                dlq_file.unlink()
            self.current_file = None
            self.current_file_size = 0
            logger.info("Cleared all DLQ records")
        else:
            logger.info(f"Selective DLQ clearing for pipeline {pipeline_id} not implemented")

    async def cleanup_old_records(self, retention_days: int) -> None:
        """Clean up old DLQ files."""
        try:
            dlq_files = list(self.dlq_path.glob("dlq_*.jsonl"))
            dlq_files.sort(key=lambda x: x.stat().st_ctime, reverse=True)

            # Remove files beyond max_files limit
            if len(dlq_files) > self.max_files:
                for file_to_remove in dlq_files[self.max_files:]:
                    file_to_remove.unlink()
                    logger.info(f"Removed old DLQ file: {file_to_remove}")

            # Remove files older than retention period
            cutoff_time = datetime.now(timezone.utc).timestamp() - (retention_days * 24 * 60 * 60)
            for dlq_file in dlq_files:
                if dlq_file.stat().st_ctime < cutoff_time:
                    dlq_file.unlink()
                    logger.info(f"Removed expired DLQ file: {dlq_file}")

        except Exception as e:
            logger.error(f"Failed to cleanup old DLQ files: {e}")

    async def flush(self) -> None:
        """No-op for local storage - writes are immediate."""
        pass


class DeadLetterQueue:
    """
    Dead Letter Queue for handling records that fail processing.

    Failed records are written to local JSONL files. For cloud deployments,
    mount the DLQ directory to persistent storage or use a sidecar to ship
    files to object storage. Lightweight summaries are emitted to stdout
    via ANALITIQ_DLQ:: for observability.
    """

    def __init__(
        self,
        dlq_path: str = "./deadletter/",
        max_file_size: int = 10 * 1024 * 1024,
        max_files: int = 100,
        retention_days: int = 30,
    ):
        self.retention_days = retention_days
        self.dlq_path = Path(dlq_path)
        self.max_file_size = max_file_size
        self.max_files = max_files
        self.lock = asyncio.Lock()
        self._record_count = 0
        self.storage = LocalDLQStorage(
            dlq_path=dlq_path,
            max_file_size=max_file_size,
            max_files=max_files,
        )

    async def send_to_dlq(
        self,
        record: Dict[str, Any],
        error: Exception,
        pipeline_id: str,
        stream_id: Optional[str] = None,
        additional_context: Optional[Dict[str, Any]] = None,
    ):
        """Send a failed record to the Dead Letter Queue."""
        dlq_record = {
            "id": str(uuid4()),
            "pipeline_id": pipeline_id,
            "original_record": record,
            "error": {
                "type": type(error).__name__,
                "message": str(error),
                "traceback": self._get_traceback(error),
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "retry_count": 0,
            "additional_context": additional_context or {},
        }

        await self.storage.write_record(dlq_record, stream_id)
        self._record_count += 1

    async def send_batch(
        self,
        batch: List[Dict[str, Any]],
        error_message: str,
        pipeline_id: str = "unknown",
        stream_id: Optional[str] = None,
        additional_context: Optional[Dict[str, Any]] = None,
    ):
        """Send a batch of failed records to the Dead Letter Queue."""
        for record in batch:
            error = Exception(error_message)
            await self.send_to_dlq(
                record=record,
                error=error,
                pipeline_id=pipeline_id,
                stream_id=stream_id,
                additional_context=additional_context,
            )

        logger.warning(f"Sent batch of {len(batch)} records to DLQ for pipeline {pipeline_id}")
        emit_dlq_log(
            pipeline_id=pipeline_id,
            stream_id=stream_id,
            added=len(batch),
            total=self._record_count,
        )

    def _get_traceback(self, error: Exception) -> Optional[str]:
        """Get traceback string from exception."""
        import traceback

        try:
            return traceback.format_exception(type(error), error, error.__traceback__)
        except Exception:
            return None

    async def get_failed_records(
        self, pipeline_id: Optional[str] = None, stream_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get failed records from DLQ."""
        return await self.storage.get_records(pipeline_id, stream_id)

    async def retry_failed_record(self, dlq_record_id: str) -> bool:
        """Mark a failed record for retry."""
        logger.info(f"Retry requested for DLQ record: {dlq_record_id}")
        return True

    async def get_dlq_stats(self) -> Dict[str, Any]:
        """Get DLQ statistics."""
        return await self.storage.get_stats()

    async def clear_dlq(self, pipeline_id: Optional[str] = None):
        """Clear DLQ records."""
        await self.storage.clear(pipeline_id)

    async def cleanup(self):
        """Clean up old DLQ files based on retention policy."""
        await self.storage.cleanup_old_records(self.retention_days)

    async def flush(self):
        """Flush any buffered records to storage."""
        await self.storage.flush()
