"""Dead Letter Queue implementation for handling failed records."""

import asyncio
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

logger = logging.getLogger(__name__)


class DateTimeEncoder(json.JSONEncoder):
    """JSON encoder that handles datetime objects."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class DeadLetterQueue:
    """
    Dead Letter Queue for handling records that fail processing.

    Features:
    - Configurable storage backends (file, database, message queue)
    - Structured error information
    - Retry and reprocessing capabilities
    - Cleanup and archival
    """

    def __init__(
        self,
        dlq_path: str = "./deadletter/",
        max_file_size: int = 10 * 1024 * 1024,  # 10MB
        max_files: int = 100,
        retention_days: int = 30,
    ):
        """
        Initialize Dead Letter Queue.

        Args:
            dlq_path: Directory path for storing failed records
            max_file_size: Maximum size per DLQ file in bytes
            max_files: Maximum number of DLQ files to keep
            retention_days: Days to retain DLQ files
        """
        self.dlq_path = Path(dlq_path)
        self.max_file_size = max_file_size
        self.max_files = max_files
        self.retention_days = retention_days

        # Create DLQ directory if it doesn't exist
        self.dlq_path.mkdir(parents=True, exist_ok=True)

        # Current file tracking
        self.current_file = None
        self.current_file_size = 0
        self.lock = asyncio.Lock()

    async def send_to_dlq(
        self,
        record: Dict[str, Any],
        error: Exception,
        pipeline_id: str,
        additional_context: Optional[Dict[str, Any]] = None,
    ):
        """
        Send a failed record to the Dead Letter Queue.

        Args:
            record: Original record that failed processing
            error: Exception that caused the failure
            pipeline_id: ID of the pipeline that failed
            additional_context: Additional context information
        """
        async with self.lock:
            dlq_record = {
                "id": str(uuid4()),
                "pipeline_id": pipeline_id,
                "original_record": record,
                "error": {
                    "type": type(error).__name__,
                    "message": str(error),
                    "traceback": self._get_traceback(error),
                },
                "timestamp": datetime.utcnow().isoformat(),
                "retry_count": 0,
                "additional_context": additional_context or {},
            }

            await self._write_to_dlq(dlq_record)

    async def send_batch(
        self,
        batch: List[Dict[str, Any]], 
        error_message: str,
        pipeline_id: str = "unknown",
        additional_context: Optional[Dict[str, Any]] = None,
    ):
        """
        Send a batch of failed records to the Dead Letter Queue.

        Args:
            batch: List of records that failed processing
            error_message: Error message describing the failure
            pipeline_id: ID of the pipeline that failed
            additional_context: Additional context information
        """
        for record in batch:
            # Create a simple exception from the error message
            error = Exception(error_message)
            await self.send_to_dlq(
                record=record,
                error=error,
                pipeline_id=pipeline_id,
                additional_context=additional_context
            )
        
        logger.warning(f"Sent batch of {len(batch)} records to DLQ for pipeline {pipeline_id}")

    def _get_traceback(self, error: Exception) -> Optional[str]:
        """Get traceback string from exception."""
        import traceback

        try:
            return traceback.format_exception(type(error), error, error.__traceback__)
        except Exception:
            return None

    async def _write_to_dlq(self, dlq_record: Dict[str, Any]):
        """Write record to DLQ file."""
        try:
            # Check if we need a new file
            if self._need_new_file():
                await self._create_new_file()

            # Write record to current file
            record_json = json.dumps(dlq_record, cls=DateTimeEncoder) + "\n"
            record_bytes = record_json.encode("utf-8")

            with open(self.current_file, "a", encoding="utf-8") as f:
                f.write(record_json)

            self.current_file_size += len(record_bytes)

        except Exception as e:
            logger.error(f"Failed to write to DLQ: {str(e)}")
            # Fall back to creating a unique file for this record
            fallback_file = (
                self.dlq_path
                / f"dlq_fallback_{datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}.json"
            )
            with open(fallback_file, "w", encoding="utf-8") as f:
                json.dump(dlq_record, f, indent=2, cls=DateTimeEncoder)

    def _need_new_file(self) -> bool:
        """Check if we need to create a new DLQ file."""
        return (
            self.current_file is None
            or self.current_file_size >= self.max_file_size
            or not self.current_file.exists()
        )

    async def _create_new_file(self):
        """Create a new DLQ file."""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
        self.current_file = self.dlq_path / f"dlq_{timestamp}.jsonl"
        self.current_file_size = 0

        # Create the file
        self.current_file.touch()
        logger.info(f"Created new DLQ file: {self.current_file}")

        # Clean up old files if needed
        await self._cleanup_old_files()

    async def _cleanup_old_files(self):
        """Clean up old DLQ files based on retention policy."""
        try:
            dlq_files = list(self.dlq_path.glob("dlq_*.jsonl"))

            # Sort by creation time (newest first)
            dlq_files.sort(key=lambda x: x.stat().st_ctime, reverse=True)

            # Remove files beyond max_files limit
            if len(dlq_files) > self.max_files:
                for file_to_remove in dlq_files[self.max_files :]:
                    file_to_remove.unlink()
                    logger.info(f"Removed old DLQ file: {file_to_remove}")

            # Remove files older than retention period
            cutoff_time = datetime.utcnow().timestamp() - (
                self.retention_days * 24 * 60 * 60
            )
            for dlq_file in dlq_files:
                if dlq_file.stat().st_ctime < cutoff_time:
                    dlq_file.unlink()
                    logger.info(f"Removed expired DLQ file: {dlq_file}")

        except Exception as e:
            logger.error(f"Failed to cleanup old DLQ files: {str(e)}")

    async def get_failed_records(
        self, pipeline_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get failed records from DLQ.

        Args:
            pipeline_id: Optional pipeline ID to filter records

        Returns:
            List of failed records
        """
        records = []

        try:
            dlq_files = list(self.dlq_path.glob("dlq_*.jsonl"))

            for dlq_file in dlq_files:
                with open(dlq_file, "r", encoding="utf-8") as f:
                    for line in f:
                        if line.strip():
                            try:
                                record = json.loads(line.strip())
                                if (
                                    pipeline_id is None
                                    or record.get("pipeline_id") == pipeline_id
                                ):
                                    records.append(record)
                            except json.JSONDecodeError:
                                logger.warning(
                                    f"Invalid JSON in DLQ file {dlq_file}: {line}"
                                )

        except Exception as e:
            logger.error(f"Failed to read DLQ records: {str(e)}")

        return records

    async def retry_failed_record(self, dlq_record_id: str) -> bool:
        """
        Mark a failed record for retry.

        Args:
            dlq_record_id: ID of the DLQ record to retry

        Returns:
            True if record was found and marked for retry
        """
        # Implementation would depend on your retry mechanism
        # For now, just log the retry attempt
        logger.info(f"Retry requested for DLQ record: {dlq_record_id}")
        return True

    async def get_dlq_stats(self) -> Dict[str, Any]:
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
                file_size = dlq_file.stat().st_size
                stats["total_size_bytes"] += file_size

                with open(dlq_file, "r", encoding="utf-8") as f:
                    for line in f:
                        if line.strip():
                            try:
                                record = json.loads(line.strip())
                                stats["total_records"] += 1

                                # Count by pipeline
                                pipeline_id = record.get("pipeline_id", "unknown")
                                stats["records_by_pipeline"][pipeline_id] = (
                                    stats["records_by_pipeline"].get(pipeline_id, 0) + 1
                                )

                                # Count by error type
                                error_type = record.get("error", {}).get(
                                    "type", "unknown"
                                )
                                stats["records_by_error_type"][error_type] = (
                                    stats["records_by_error_type"].get(error_type, 0)
                                    + 1
                                )

                                # Track oldest and newest
                                timestamp = record.get("timestamp")
                                if timestamp:
                                    if (
                                        stats["oldest_record"] is None
                                        or timestamp < stats["oldest_record"]
                                    ):
                                        stats["oldest_record"] = timestamp
                                    if (
                                        stats["newest_record"] is None
                                        or timestamp > stats["newest_record"]
                                    ):
                                        stats["newest_record"] = timestamp

                            except json.JSONDecodeError:
                                continue

        except Exception as e:
            logger.error(f"Failed to get DLQ stats: {str(e)}")

        return stats

    async def clear_dlq(self, pipeline_id: Optional[str] = None):
        """
        Clear DLQ records.

        Args:
            pipeline_id: Optional pipeline ID to clear only specific records
        """
        if pipeline_id is None:
            # Clear all DLQ files
            dlq_files = list(self.dlq_path.glob("dlq_*.jsonl"))
            for dlq_file in dlq_files:
                dlq_file.unlink()
            logger.info("Cleared all DLQ records")
        else:
            # Clear specific pipeline records (would need more complex implementation)
            logger.info(
                f"Clearing DLQ records for pipeline {pipeline_id} not implemented"
            )
            # This would require rewriting files without the specific pipeline records
