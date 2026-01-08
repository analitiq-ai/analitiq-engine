"""Unit tests for dead letter queue functionality."""

import asyncio
import json
import pytest
import tempfile
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock

from src.fault_tolerance.dead_letter_queue import DeadLetterQueue


class TestDeadLetterQueue:
    """Test dead letter queue core functionality."""
    
    def setup_method(self):
        """Set up test environment with temporary directory."""
        self.temp_dir = tempfile.mkdtemp()
        self.dlq_path = Path(self.temp_dir) / "dlq"
        
    def teardown_method(self):
        """Clean up test environment."""
        if Path(self.temp_dir).exists():
            shutil.rmtree(self.temp_dir)
    
    def test_dlq_initialization(self):
        """Test DLQ proper initialization."""
        dlq = DeadLetterQueue(
            dlq_path=str(self.dlq_path),
            max_file_size=1024 * 1024,
            max_files=50,
            retention_days=7
        )

        assert dlq.dlq_path == self.dlq_path
        assert dlq.max_file_size == 1024 * 1024
        assert dlq.max_files == 50
        assert dlq.retention_days == 7
        assert dlq.dlq_path.exists()  # Directory should be created
        # Storage backend attributes are accessed via the storage property
        assert dlq.storage.current_file is None
        assert dlq.storage.current_file_size == 0
    
    def test_dlq_default_values(self):
        """Test DLQ default configuration."""
        dlq = DeadLetterQueue(dlq_path=str(self.dlq_path))
        
        assert dlq.max_file_size == 10 * 1024 * 1024  # 10MB
        assert dlq.max_files == 100
        assert dlq.retention_days == 30
    
    @pytest.mark.asyncio
    async def test_send_to_dlq_basic(self):
        """Test sending a basic record to DLQ."""
        dlq = DeadLetterQueue(dlq_path=str(self.dlq_path))
        
        record = {"id": 123, "name": "test", "value": 45.67}
        error = ValueError("Invalid data format")
        pipeline_id = "test-pipeline"
        
        await dlq.send_to_dlq(record, error, pipeline_id)
        
        # Check that file was created and contains the record
        dlq_files = list(dlq.dlq_path.glob("dlq_*.jsonl"))
        assert len(dlq_files) == 1
        
        # Read and validate the record
        with open(dlq_files[0], 'r', encoding='utf-8') as f:
            dlq_record = json.loads(f.readline().strip())
        
        assert dlq_record["pipeline_id"] == pipeline_id
        assert dlq_record["original_record"] == record
        assert dlq_record["error"]["type"] == "ValueError"
        assert dlq_record["error"]["message"] == "Invalid data format"
        assert "timestamp" in dlq_record
        assert dlq_record["retry_count"] == 0
        assert "id" in dlq_record
    
    @pytest.mark.asyncio
    async def test_send_to_dlq_with_context(self):
        """Test sending record to DLQ with additional context."""
        dlq = DeadLetterQueue(dlq_path=str(self.dlq_path))

        record = {"user_id": 456, "email": "test@example.com"}
        error = Exception("Database connection lost")
        pipeline_id = "email-processor"
        additional_context = {
            "stage": "transform",
            "batch_id": 7,
            "retry_attempt": 2
        }

        await dlq.send_to_dlq(record, error, pipeline_id, additional_context=additional_context)

        # Read and validate the record
        dlq_files = list(dlq.dlq_path.glob("dlq_*.jsonl"))
        with open(dlq_files[0], 'r', encoding='utf-8') as f:
            dlq_record = json.loads(f.readline().strip())

        assert dlq_record["additional_context"] == additional_context
        assert dlq_record["additional_context"]["stage"] == "transform"
        assert dlq_record["additional_context"]["batch_id"] == 7
    
    @pytest.mark.asyncio
    async def test_send_batch_to_dlq(self):
        """Test sending a batch of records to DLQ."""
        dlq = DeadLetterQueue(dlq_path=str(self.dlq_path))

        batch = [
            {"id": 1, "name": "record1"},
            {"id": 2, "name": "record2"},
            {"id": 3, "name": "record3"}
        ]
        error_message = "Batch validation failed"
        pipeline_id = "batch-processor"
        context = {"batch_size": 3, "stage": "validate"}

        await dlq.send_batch(batch, error_message, pipeline_id, additional_context=context)

        # Check that all records were written
        dlq_files = list(dlq.dlq_path.glob("dlq_*.jsonl"))
        assert len(dlq_files) == 1

        # Read all records
        records = []
        with open(dlq_files[0], 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    records.append(json.loads(line.strip()))

        assert len(records) == 3
        for i, dlq_record in enumerate(records):
            assert dlq_record["pipeline_id"] == pipeline_id
            assert dlq_record["original_record"] == batch[i]
            assert dlq_record["error"]["message"] == error_message
            assert dlq_record["additional_context"] == context
    
    @pytest.mark.asyncio
    async def test_multiple_records_same_file(self):
        """Test multiple records written to same file."""
        dlq = DeadLetterQueue(dlq_path=str(self.dlq_path), max_file_size=1024*1024)  # Large limit
        
        # Send multiple records
        for i in range(5):
            record = {"id": i, "data": f"test_data_{i}"}
            error = Exception(f"Error {i}")
            await dlq.send_to_dlq(record, error, "test-pipeline")
        
        # Should all be in one file
        dlq_files = list(dlq.dlq_path.glob("dlq_*.jsonl"))
        assert len(dlq_files) == 1
        
        # Count lines in file
        with open(dlq_files[0], 'r', encoding='utf-8') as f:
            line_count = sum(1 for line in f if line.strip())
        
        assert line_count == 5
    
    @pytest.mark.asyncio
    async def test_file_rotation_on_size_limit(self):
        """Test file rotation when size limit is reached."""
        # Set very small max file size to force rotation
        dlq = DeadLetterQueue(dlq_path=str(self.dlq_path), max_file_size=100)
        
        # Send records that will exceed the size limit
        large_record = {"data": "x" * 200}  # Large record
        
        for i in range(3):
            error = Exception(f"Error {i}")
            await dlq.send_to_dlq(large_record, error, "test-pipeline")
        
        # Should create multiple files due to size limit
        dlq_files = list(dlq.dlq_path.glob("dlq_*.jsonl"))
        assert len(dlq_files) > 1  # At least 2 files due to size rotation
    
    def test_need_new_file_logic(self):
        """Test need new file logic."""
        dlq = DeadLetterQueue(dlq_path=str(self.dlq_path), max_file_size=1000)

        # Initially should need new file (no current file)
        assert dlq._need_new_file() is True

        # Set current file but make it large - access through storage backend
        dlq.storage.current_file = self.dlq_path / "test.jsonl"
        dlq.storage.current_file.touch()  # Create the file
        dlq.storage.current_file_size = 1500  # Over limit

        assert dlq._need_new_file() is True

        # Set reasonable size
        dlq.storage.current_file_size = 500
        assert dlq._need_new_file() is False

        # Remove the file (simulate deletion)
        dlq.storage.current_file.unlink()
        assert dlq._need_new_file() is True
    
    @pytest.mark.asyncio
    async def test_create_new_file(self):
        """Test new file creation."""
        dlq = DeadLetterQueue(dlq_path=str(self.dlq_path))

        await dlq._create_new_file()

        # Access storage backend for file tracking
        assert dlq.storage.current_file is not None
        assert dlq.storage.current_file.exists()
        assert dlq.storage.current_file_size == 0
        assert dlq.storage.current_file.name.startswith("dlq_")
        assert dlq.storage.current_file.name.endswith(".jsonl")
    
    @pytest.mark.asyncio
    async def test_cleanup_old_files_max_files(self):
        """Test cleanup based on max files limit."""
        dlq = DeadLetterQueue(dlq_path=str(self.dlq_path), max_files=3)
        
        # Create more files than the limit
        for i in range(5):
            file_path = dlq.dlq_path / f"dlq_test_{i:03d}.jsonl"
            file_path.touch()
        
        await dlq._cleanup_old_files()
        
        # Should have only max_files remaining
        remaining_files = list(dlq.dlq_path.glob("dlq_*.jsonl"))
        assert len(remaining_files) <= 3
    
    @pytest.mark.asyncio
    async def test_cleanup_old_files_retention(self):
        """Test cleanup retention policy (basic functionality)."""
        dlq = DeadLetterQueue(dlq_path=str(self.dlq_path), retention_days=30, max_files=10)
        
        # Create multiple files that match the DLQ pattern
        test_files = []
        for i in range(3):
            file_name = f"dlq_202401{i:02d}_120000_000000.jsonl"
            test_file = dlq.dlq_path / file_name
            test_file.touch()
            test_files.append(test_file)
        
        # Verify files exist before cleanup
        for test_file in test_files:
            assert test_file.exists()
        
        # Run cleanup - with reasonable retention, files should not be removed
        await dlq._cleanup_old_files()
        
        # Files should still exist (within retention period)
        for test_file in test_files:
            assert test_file.exists()
        
        # Test that cleanup function completes without errors
        assert len(list(dlq.dlq_path.glob("dlq_*.jsonl"))) == 3
    
    @pytest.mark.asyncio
    async def test_get_failed_records_all(self):
        """Test getting all failed records."""
        dlq = DeadLetterQueue(dlq_path=str(self.dlq_path))
        
        # Add some test records
        test_records = [
            ({"id": 1}, "Error 1", "pipeline1"),
            ({"id": 2}, "Error 2", "pipeline1"),
            ({"id": 3}, "Error 3", "pipeline2")
        ]
        
        for record, error_msg, pipeline in test_records:
            await dlq.send_to_dlq(record, Exception(error_msg), pipeline)
        
        # Get all records
        failed_records = await dlq.get_failed_records()
        
        assert len(failed_records) == 3
        assert all("original_record" in record for record in failed_records)
        assert all("error" in record for record in failed_records)
        assert all("pipeline_id" in record for record in failed_records)
    
    @pytest.mark.asyncio
    async def test_get_failed_records_filtered(self):
        """Test getting failed records filtered by pipeline."""
        dlq = DeadLetterQueue(dlq_path=str(self.dlq_path))
        
        # Add records for different pipelines
        await dlq.send_to_dlq({"id": 1}, Exception("Error 1"), "pipeline1")
        await dlq.send_to_dlq({"id": 2}, Exception("Error 2"), "pipeline1")
        await dlq.send_to_dlq({"id": 3}, Exception("Error 3"), "pipeline2")
        
        # Get records for specific pipeline
        pipeline1_records = await dlq.get_failed_records("pipeline1")
        pipeline2_records = await dlq.get_failed_records("pipeline2")
        
        assert len(pipeline1_records) == 2
        assert len(pipeline2_records) == 1
        assert all(r["pipeline_id"] == "pipeline1" for r in pipeline1_records)
        assert all(r["pipeline_id"] == "pipeline2" for r in pipeline2_records)
    
    @pytest.mark.asyncio
    async def test_get_failed_records_invalid_json(self):
        """Test handling of invalid JSON in DLQ files."""
        dlq = DeadLetterQueue(dlq_path=str(self.dlq_path))
        
        # Create a file with some valid and invalid JSON
        dlq_file = dlq.dlq_path / "dlq_test.jsonl"
        with open(dlq_file, 'w', encoding='utf-8') as f:
            f.write('{"valid": "record1"}\n')
            f.write('invalid json line\n')
            f.write('{"valid": "record2"}\n')
            f.write('{"incomplete": \n')  # Incomplete JSON
        
        # Should only return valid records
        failed_records = await dlq.get_failed_records()
        
        assert len(failed_records) == 2
        assert failed_records[0]["valid"] == "record1"
        assert failed_records[1]["valid"] == "record2"
    
    @pytest.mark.asyncio
    async def test_retry_failed_record(self):
        """Test retry failed record functionality."""
        dlq = DeadLetterQueue(dlq_path=str(self.dlq_path))
        
        # Basic test - should return True and log
        with patch('src.fault_tolerance.dead_letter_queue.logger') as mock_logger:
            result = await dlq.retry_failed_record("test-record-id")
            
            assert result is True
            mock_logger.info.assert_called_once_with("Retry requested for DLQ record: test-record-id")
    
    @pytest.mark.asyncio
    async def test_get_dlq_stats_empty(self):
        """Test DLQ stats with no records."""
        dlq = DeadLetterQueue(dlq_path=str(self.dlq_path))
        
        stats = await dlq.get_dlq_stats()
        
        assert stats["total_records"] == 0
        assert stats["records_by_pipeline"] == {}
        assert stats["records_by_error_type"] == {}
        assert stats["oldest_record"] is None
        assert stats["newest_record"] is None
        assert stats["total_files"] == 0
        assert stats["total_size_bytes"] == 0
    
    @pytest.mark.asyncio
    async def test_get_dlq_stats_with_records(self):
        """Test DLQ stats with actual records."""
        dlq = DeadLetterQueue(dlq_path=str(self.dlq_path))
        
        # Add various records
        test_records = [
            ({"id": 1}, ValueError("Value error"), "pipeline1"),
            ({"id": 2}, RuntimeError("Runtime error"), "pipeline1"),
            ({"id": 3}, ValueError("Another value error"), "pipeline2"),
        ]
        
        for record, error, pipeline in test_records:
            await dlq.send_to_dlq(record, error, pipeline)
        
        stats = await dlq.get_dlq_stats()
        
        assert stats["total_records"] == 3
        assert stats["records_by_pipeline"]["pipeline1"] == 2
        assert stats["records_by_pipeline"]["pipeline2"] == 1
        assert stats["records_by_error_type"]["ValueError"] == 2
        assert stats["records_by_error_type"]["RuntimeError"] == 1
        assert stats["oldest_record"] is not None
        assert stats["newest_record"] is not None
        assert stats["total_files"] >= 1
        assert stats["total_size_bytes"] > 0
    
    @pytest.mark.asyncio
    async def test_clear_dlq_all(self):
        """Test clearing all DLQ records."""
        dlq = DeadLetterQueue(dlq_path=str(self.dlq_path))
        
        # Add some records
        for i in range(3):
            await dlq.send_to_dlq({"id": i}, Exception(f"Error {i}"), "test-pipeline")
        
        # Verify files exist
        dlq_files_before = list(dlq.dlq_path.glob("dlq_*.jsonl"))
        assert len(dlq_files_before) > 0
        
        # Clear all
        await dlq.clear_dlq()
        
        # Verify files are gone
        dlq_files_after = list(dlq.dlq_path.glob("dlq_*.jsonl"))
        assert len(dlq_files_after) == 0
    
    @pytest.mark.asyncio
    async def test_clear_dlq_specific_pipeline(self):
        """Test clearing DLQ records for specific pipeline."""
        dlq = DeadLetterQueue(dlq_path=str(self.dlq_path))

        # Add records for different pipelines
        await dlq.send_to_dlq({"id": 1}, Exception("Error 1"), "pipeline1")
        await dlq.send_to_dlq({"id": 2}, Exception("Error 2"), "pipeline2")

        with patch('src.fault_tolerance.dead_letter_queue.logger') as mock_logger:
            # This should log that it's not implemented
            await dlq.clear_dlq("pipeline1")

            mock_logger.info.assert_called_once_with(
                "Selective DLQ clearing for pipeline pipeline1 not implemented"
            )


class TestDeadLetterQueueEdgeCases:
    """Test DLQ edge cases and error conditions."""
    
    def setup_method(self):
        """Set up test environment with temporary directory."""
        self.temp_dir = tempfile.mkdtemp()
        self.dlq_path = Path(self.temp_dir) / "dlq"
    
    def teardown_method(self):
        """Clean up test environment."""
        if Path(self.temp_dir).exists():
            shutil.rmtree(self.temp_dir)
    
    @pytest.mark.asyncio
    async def test_write_to_dlq_file_error_fallback(self):
        """Test fallback when writing to DLQ fails."""
        dlq = DeadLetterQueue(dlq_path=str(self.dlq_path))
        
        record = {"id": 123}
        error = Exception("Test error")
        
        # Mock the main file write to fail, but allow fallback write to succeed
        original_open = __builtins__['open'] if isinstance(__builtins__, dict) else __builtins__.open
        
        def mock_open(*args, **kwargs):
            if len(args) > 1 and args[1] == "a":  # Append mode (main file)
                raise PermissionError("Permission denied")
            else:  # Other modes (fallback file)
                return original_open(*args, **kwargs)
        
        with patch('builtins.open', side_effect=mock_open):
            # Should not raise exception but use fallback
            await dlq.send_to_dlq(record, error, "test-pipeline")
        
        # Should have created fallback files
        fallback_files = list(dlq.dlq_path.glob("dlq_fallback_*.json"))
        assert len(fallback_files) > 0
        
        # Verify fallback file contents
        with open(fallback_files[0], 'r', encoding='utf-8') as f:
            fallback_record = json.load(f)
        
        assert fallback_record["original_record"]["id"] == 123
        assert fallback_record["pipeline_id"] == "test-pipeline"
    
    @pytest.mark.asyncio
    async def test_traceback_extraction(self):
        """Test traceback extraction from exceptions."""
        dlq = DeadLetterQueue(dlq_path=str(self.dlq_path))
        
        # Create exception with traceback
        try:
            raise ValueError("Test error with traceback")
        except ValueError as e:
            await dlq.send_to_dlq({"id": 1}, e, "test-pipeline")
        
        # Check that traceback was captured
        failed_records = await dlq.get_failed_records()
        assert len(failed_records) == 1
        
        error_info = failed_records[0]["error"]
        assert error_info["traceback"] is not None
        assert isinstance(error_info["traceback"], list)
        assert len(error_info["traceback"]) > 0
    
    @pytest.mark.asyncio
    async def test_traceback_extraction_failure(self):
        """Test handling when traceback extraction fails."""
        dlq = DeadLetterQueue(dlq_path=str(self.dlq_path))
        
        # Mock traceback formatting to fail
        with patch('traceback.format_exception', side_effect=Exception("Traceback error")):
            error = ValueError("Test error")
            await dlq.send_to_dlq({"id": 1}, error, "test-pipeline")
        
        # Should handle gracefully and set traceback to None
        failed_records = await dlq.get_failed_records()
        assert len(failed_records) == 1
        
        error_info = failed_records[0]["error"]
        assert error_info["traceback"] is None
    
    @pytest.mark.asyncio
    async def test_concurrent_writes(self):
        """Test concurrent writes to DLQ."""
        dlq = DeadLetterQueue(dlq_path=str(self.dlq_path))
        
        async def write_record(record_id):
            record = {"id": record_id}
            error = Exception(f"Error {record_id}")
            await dlq.send_to_dlq(record, error, f"pipeline-{record_id}")
        
        # Execute concurrent writes
        tasks = [write_record(i) for i in range(10)]
        await asyncio.gather(*tasks)
        
        # All records should be written
        failed_records = await dlq.get_failed_records()
        assert len(failed_records) == 10
        
        # All record IDs should be unique
        record_ids = [r["original_record"]["id"] for r in failed_records]
        assert len(set(record_ids)) == 10
    
    @pytest.mark.asyncio
    async def test_dlq_directory_creation_failure(self):
        """Test handling when DLQ directory cannot be created."""
        # Try to create DLQ in a read-only location (simulate)
        with patch('pathlib.Path.mkdir', side_effect=PermissionError("Cannot create directory")):
            with pytest.raises(PermissionError):
                DeadLetterQueue(dlq_path="/root/readonly/dlq")
    
    @pytest.mark.asyncio
    async def test_cleanup_old_files_error_handling(self):
        """Test error handling in cleanup old files."""
        dlq = DeadLetterQueue(dlq_path=str(self.dlq_path))
        
        # Mock glob to raise an exception
        with patch('pathlib.Path.glob', side_effect=Exception("Glob error")):
            with patch('src.fault_tolerance.dead_letter_queue.logger') as mock_logger:
                # Should not raise but log error
                await dlq._cleanup_old_files()
                
                # Should log the error
                mock_logger.error.assert_called_once()
                assert "Failed to cleanup old DLQ files" in mock_logger.error.call_args[0][0]
    
    @pytest.mark.asyncio
    async def test_get_failed_records_error_handling(self):
        """Test error handling when reading failed records."""
        dlq = DeadLetterQueue(dlq_path=str(self.dlq_path))
        
        # Mock glob to raise an exception
        with patch('pathlib.Path.glob', side_effect=Exception("Read error")):
            with patch('src.fault_tolerance.dead_letter_queue.logger') as mock_logger:
                # Should return empty list and log error
                result = await dlq.get_failed_records()
                
                assert result == []
                mock_logger.error.assert_called_once()
                assert "Failed to read DLQ records" in mock_logger.error.call_args[0][0]
    
    @pytest.mark.asyncio
    async def test_get_dlq_stats_error_handling(self):
        """Test error handling when getting DLQ stats."""
        dlq = DeadLetterQueue(dlq_path=str(self.dlq_path))
        
        # Mock glob to raise an exception
        with patch('pathlib.Path.glob', side_effect=Exception("Stats error")):
            with patch('src.fault_tolerance.dead_letter_queue.logger') as mock_logger:
                # Should return default stats and log error
                stats = await dlq.get_dlq_stats()
                
                assert stats["total_records"] == 0
                assert stats["total_files"] == 0
                mock_logger.error.assert_called_once()
                assert "Failed to get DLQ stats" in mock_logger.error.call_args[0][0]
    
    @pytest.mark.asyncio
    async def test_send_batch_empty_batch(self):
        """Test sending empty batch to DLQ."""
        dlq = DeadLetterQueue(dlq_path=str(self.dlq_path))
        
        await dlq.send_batch([], "Empty batch error", "test-pipeline")
        
        # No files should be created
        dlq_files = list(dlq.dlq_path.glob("dlq_*.jsonl"))
        assert len(dlq_files) == 0
    
    @pytest.mark.asyncio
    async def test_large_record_handling(self):
        """Test handling of very large records."""
        dlq = DeadLetterQueue(dlq_path=str(self.dlq_path), max_file_size=1024)  # Small limit
        
        # Create a very large record
        large_data = "x" * 2048  # 2KB record
        large_record = {"data": large_data, "metadata": {"size": len(large_data)}}
        
        await dlq.send_to_dlq(large_record, Exception("Large record error"), "test-pipeline")
        
        # Should handle large record (may create new file)
        failed_records = await dlq.get_failed_records()
        assert len(failed_records) == 1
        assert failed_records[0]["original_record"]["data"] == large_data


if __name__ == "__main__":
    pytest.main([__file__, "-v"])