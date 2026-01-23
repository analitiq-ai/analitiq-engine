"""Modern API connector with state management."""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Dict, List, Optional
from urllib.parse import urljoin, urlparse, urlencode

from .base import BaseConnector, ConnectionError, ReadError, WriteError
from ...state.state_manager import StateManager
from ...models.state import PartitionCursor, CursorField, PartitionStats
from ...models.api import (
    APIConnectionConfig, APIReadConfig, APIWriteConfig,
    HTTPResponse, APIRequestParams, RecordBatch, FilterConfig
)
from ...shared.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)


class APIConnector(BaseConnector):
    """
    Modern API connector with state management.

    Features:
    - State management for scalability
    - Pydantic validation for type safety
    - Incremental replication with cursor tracking
    - Rate limiting and fault tolerance
    """

    def __init__(self, name: str = "APIConnector"):
        super().__init__(name)
        self.session = None
        self.base_url = None
        self.headers = {}
        self.rate_limiter = None

    async def connect(self, config: Dict[str, Any]):
        """Establish connection to the API."""
        try:
            import aiohttp
            
            # Validate connection configuration
            try:
                validated_config = APIConnectionConfig(**config)
            except Exception as e:
                raise ConnectionError(f"Invalid API connection configuration: {str(e)}")
            
            self.base_url = validated_config.host
            self.headers = validated_config.headers
            
            # Create session with validated timeout and connection limits
            timeout = aiohttp.ClientTimeout(total=validated_config.timeout)
            connector = aiohttp.TCPConnector(
                limit=validated_config.max_connections,
                limit_per_host=validated_config.max_connections_per_host,
            )

            self.session = aiohttp.ClientSession(
                timeout=timeout, connector=connector, headers=self.headers
            )

            # Log connection configuration (mask sensitive values)
            masked_headers = self._mask_sensitive_headers(self.headers)
            logger.debug(f"API Connection configured:")
            logger.debug(f"  Base URL: {self.base_url}")
            logger.debug(f"  Headers: {masked_headers}")
            logger.debug(f"  Timeout: {validated_config.timeout}s")
            logger.debug(f"  Max connections: {validated_config.max_connections}")

            # Set up rate limiting with validation
            if validated_config.rate_limit:
                self.rate_limiter = RateLimiter(
                    max_requests=validated_config.rate_limit.max_requests,
                    time_window=validated_config.rate_limit.time_window,
                )

            self.is_connected = True
            logger.debug(f"Connected to API: {self.base_url}")

        except ImportError:
            raise ConnectionError(
                "aiohttp package not installed. Install with: pip install aiohttp"
            )
        except Exception as e:
            logger.error(f"Failed to connect to API: {str(e)}")
            raise ConnectionError(f"API connection failed: {str(e)}")


    async def disconnect(self):
        """Close API connection."""
        if self.session:
            await self.session.close()
            # Allow event loop to close underlying transport
            await asyncio.sleep(0.25)
            self.session = None
            self.is_connected = False

    async def read_batches(
        self,
        config: Dict[str, Any],
        *,
        state_manager: StateManager,
        stream_name: str,
        partition: Optional[Dict[str, Any]] = None,
        batch_size: int = 1000
    ) -> AsyncIterator[List[Dict[str, Any]]]:
        """
        Read data in batches with state management.

        Args:
            config: Read configuration
            state_manager: State manager
            stream_name: Name of the stream for state tracking
            partition: Partition key for processing
            batch_size: Number of records per batch

        Yields:
            Batches of records as dictionaries
        """
        if partition is None:
            partition = {}
            
        try:
            endpoint = config.get("endpoint", "")
            method = config.get("method", "GET")
            full_url = urljoin(self.base_url, endpoint)

            # Load state from state manager
            # Use the complete config passed to read_batches, which contains merged source configuration
            state = self._load_state_from_state_manager(
                state_manager, stream_name, partition, config
            )
            
            # Apply incremental replication logic if enabled
            if state["replication_method"] == "incremental":
                await self._setup_incremental_replication(config, state, config)

            # Track records for checkpointing
            batch_count = 0
            total_records = 0
            
            # Handle pagination
            pagination_config = config.get("pagination") or {}
            pagination_type = pagination_config.get("type")

            pagination_methods = {
                "cursor": self._read_cursor_paginated,
                "offset": self._read_offset_paginated,
                "page": self._read_page_paginated,
            }

            method_func = pagination_methods.get(pagination_type)
            if method_func:
                async for batch in method_func(full_url, method, config, batch_size):
                    # Update state after each batch
                    if batch:
                        # Filter out duplicate records based on tie-breaker comparison
                        deduplicated_batch = self._deduplicate_records(batch, state, config)
                        if not deduplicated_batch:
                            logger.debug(f"Stream {stream_name}: All {len(batch)} records in batch were duplicates, skipping")
                            continue
                            
                        if len(deduplicated_batch) != len(batch):
                            logger.debug(f"Stream {stream_name}: Deduplicated batch: {len(batch)} -> {len(deduplicated_batch)} records")
                            
                        # Update batch to use deduplicated version
                        batch = deduplicated_batch
                        
                        # Only yield non-empty deduplicated batches
                        yield batch
                        
                        batch_count += 1
                        total_records += len(batch)
                        
                        # Extract cursor from last record using cursor field
                        last_record = batch[-1]
                        cursor_field = state.get("cursor_field")
                        
                        if cursor_field and cursor_field in last_record:
                            cursor_value = last_record[cursor_field]
                            
                            # Build validated cursor state
                            primary_cursor = CursorField(
                                field=cursor_field,
                                value=cursor_value,
                                inclusive=True  # Always use inclusive mode
                            )
                            
                            cursor = PartitionCursor(primary=primary_cursor)
                            
                            # Add tie-breaker fields from configuration
                            tie_breaker_fields = config.get("tie_breaker_fields", [])
                            if tie_breaker_fields:
                                tiebreakers = []
                                for field_name in tie_breaker_fields:
                                    field_value = self._get_nested_field_value(last_record, field_name)
                                    if field_value is not None:
                                        tiebreakers.append(CursorField(
                                            field=field_name,
                                            value=field_value,
                                            inclusive=True
                                        ))
                                
                                if tiebreakers:
                                    cursor.tiebreakers = tiebreakers
                                
                            # Save checkpoint with validation
                            stats = PartitionStats(
                                records_synced=total_records,
                                batches_written=batch_count,
                                last_checkpoint_at=datetime.now(timezone.utc),
                                errors_since_checkpoint=0
                            )
                                
                            state_manager.save_stream_checkpoint(
                                stream_name=stream_name,
                                partition=partition,
                                cursor=cursor.model_dump(mode='json'),
                                hwm=cursor_value,
                                stats=stats.model_dump(mode='json')
                            )
            else:
                # Single request without pagination
                batch = await self._read_single_request(full_url, method, config)
                if batch:
                    # Filter out duplicate records based on tie-breaker comparison
                    deduplicated_batch = self._deduplicate_records(batch, state, config)
                    if not deduplicated_batch:
                        logger.debug(f"Stream {stream_name}: All {len(batch)} records were duplicates, skipping")
                    else:
                        if len(deduplicated_batch) != len(batch):
                            logger.debug(f"Stream {stream_name}: Deduplicated batch: {len(batch)} -> {len(deduplicated_batch)} records")
                            
                        # Use deduplicated batch
                        batch = deduplicated_batch
                        yield batch
                    
                        # Update state for single batch
                        total_records = len(batch)
                        last_record = batch[-1]
                        cursor_field = state.get("cursor_field")
                    
                    if cursor_field and cursor_field in last_record:
                        cursor_value = last_record[cursor_field]
                        
                        primary_cursor = CursorField(
                            field=cursor_field,
                            value=cursor_value,
                            inclusive=True  # Always use inclusive mode
                        )
                        
                        cursor = PartitionCursor(primary=primary_cursor)
                        
                        # Add tie-breaker fields from configuration
                        tie_breaker_fields = config.get("tie_breaker_fields", [])
                        if tie_breaker_fields:
                            tiebreakers = []
                            for field_name in tie_breaker_fields:
                                field_value = self._get_nested_field_value(last_record, field_name)
                                if field_value is not None:
                                    tiebreakers.append(CursorField(
                                        field=field_name,
                                        value=field_value,
                                        inclusive=True
                                    ))
                            
                            if tiebreakers:
                                cursor.tiebreakers = tiebreakers
                            
                        stats = PartitionStats(
                            records_synced=total_records,
                            batches_written=1,
                            last_checkpoint_at=datetime.now(timezone.utc),
                            errors_since_checkpoint=0
                        )
                            
                        state_manager.save_stream_checkpoint(
                            stream_name=stream_name,
                            partition=partition,
                            cursor=cursor.model_dump(mode='json'),
                            hwm=cursor_value,
                            stats=stats.model_dump(mode='json')
                        )

        except Exception as e:
            self.metrics["errors"] += 1
            raise ReadError(f"API {method} connection to {full_url} failed: {str(e)}")

    def _load_state_from_state_manager(
        self, 
        state_manager: StateManager,
        stream_name: str, 
        partition: Dict[str, Any],
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Load state from state manager."""
        partition_state = state_manager.get_partition_state(stream_name, partition)
        
        if partition_state:
            # Convert state format to internal format
            cursor_info = partition_state.get("cursor", {})
            primary_cursor = cursor_info.get("primary", {})
            
            # Extract tie-breaker information
            tiebreaker_info = {}
            if cursor_info.get("tiebreakers"):
                tiebreaker_info["tiebreakers"] = cursor_info["tiebreakers"]
            
            bookmarks = [{
                "partition": partition,
                "cursor": primary_cursor.get("value"),
                "aux": tiebreaker_info
            }] if primary_cursor.get("value") else []
        else:
            bookmarks = []
            
        cursor_field = config.get("cursor_field")
        logger.debug(f"API Connector _load_state_from_state_manager: received config keys = {list(config.keys())}")
        logger.debug(f"API Connector _load_state_from_state_manager: cursor_field = {cursor_field}")
        
        state = {
            "bookmarks": bookmarks,
            "run": state_manager.get_run_info(),
            "replication_method": config.get("replication_method", "incremental"),
            "cursor_field": cursor_field,  # Source record field name
        }
        if config.get("safety_window_seconds") is not None:
            state["safety_window_seconds"] = config["safety_window_seconds"]
        return state

    async def _setup_incremental_replication(self, config: Dict[str, Any], state: Dict[str, Any], src_config: Dict[str, Any]):
        """Set up incremental replication by validating cursor field and building filters."""
        cursor_field = state["cursor_field"]
        
        # Get the API filter parameter name from source configuration
        filter_param = self._get_filter_param_for_cursor_field(cursor_field, config)
        if not filter_param:
            logger.warning(f"No filter mapping found for cursor field '{cursor_field}', falling back to full replication")
            return

        # Get bookmark for the current partition
        bookmarks = state["bookmarks"]
        if not bookmarks:
            logger.info("No bookmarks found, performing full replication for first run")
            return
            
        bookmark = bookmarks[0]
        cursor = bookmark.get("cursor")
        
        if not cursor:
            logger.info("No cursor found in bookmark, performing full replication")
            return
            
        # Compute effective start time with safety window
        safety_window = state.get("safety_window_seconds")
        if safety_window is None:
            logger.warning("safety_window_seconds not configured, skipping incremental filter setup")
            return
        effective_start = self._compute_effective_start_time(cursor, safety_window)
        
        # Build incremental filter using the mapped API parameter (always inclusive mode)
        incremental_filter = self._build_replication_filter(filter_param, effective_start)

        # Apply incremental filter
        if "filters" not in config:
            config["filters"] = {}

        for filter_name, filter_value in incremental_filter.items():
            config["filters"][filter_name] = {
                "type": "string",
                "value": filter_value,
                "required": True,
                "description": f"Incremental replication filter using cursor: {filter_value}"
            }

        logger.info(f"Set up incremental replication: {cursor_field} -> {filter_param} from {effective_start}")

    async def _read_offset_paginated(
        self, url: str, method: str, config: Dict[str, Any], batch_size: int
    ) -> AsyncIterator[List[Dict[str, Any]]]:
        """Read data using offset-based pagination."""
        pagination_config = config.get("pagination", {})
        offset_param = pagination_config.get("params", {}).get("offset_param", "offset")
        limit_param = pagination_config.get("params", {}).get("limit_param", "limit")

        offset = 0

        while True:
            params = {offset_param: offset, limit_param: batch_size}
            
            # Apply filters from config
            self._apply_filters_to_params(params, config)

            # Apply rate limiting
            if self.rate_limiter:
                await self.rate_limiter.acquire()

            # Log the full request URL
            full_request_url = f"{url}?{urlencode(params)}" if params else url
            logger.debug(f"Making API request: {method} {full_request_url}")

            async with self.session.request(method, url, params=params) as response:
                if response.status != 200:
                    raise ReadError(f"API request failed with status {response.status}")

                data = await response.json()
                records = self._extract_records_from_response(data, config)

                if not records:
                    break

                self.metrics["records_read"] += len(records)
                self.metrics["batches_read"] += 1

                yield records

                # If we got less than batch_size, we're done
                if len(records) < batch_size:
                    break

                offset += batch_size

    async def _read_cursor_paginated(
        self, url: str, method: str, config: Dict[str, Any], batch_size: int
    ) -> AsyncIterator[List[Dict[str, Any]]]:
        """Read data using cursor-based pagination."""
        pagination_config = config.get("pagination", {})
        cursor_param = pagination_config.get("params", {}).get("cursor_param", "cursor")
        limit_param = pagination_config.get("params", {}).get("limit_param", "limit")
        
        cursor = None
        
        while True:
            params = {limit_param: batch_size}
            if cursor:
                params[cursor_param] = cursor
            
            # Apply filters from config
            self._apply_filters_to_params(params, config)

            # Apply rate limiting
            if self.rate_limiter:
                await self.rate_limiter.acquire()

            # Log the full request URL
            full_request_url = f"{url}?{urlencode(params)}" if params else url
            logger.debug(f"Making API request: {method} {full_request_url}")

            async with self.session.request(method, url, params=params) as response:
                if response.status != 200:
                    raise ReadError(f"API request failed with status {response.status}")

                data = await response.json()
                records = self._extract_records_from_response(data, config)

                if not records:
                    break

                self.metrics["records_read"] += len(records)
                self.metrics["batches_read"] += 1

                yield records

                # Extract next cursor from response
                cursor = self._extract_next_cursor_from_response(data, pagination_config)
                if not cursor:
                    break

    async def _read_page_paginated(
        self, url: str, method: str, config: Dict[str, Any], batch_size: int
    ) -> AsyncIterator[List[Dict[str, Any]]]:
        """Read data using page number-based pagination."""
        pagination_config = config.get("pagination", {})
        page_param = pagination_config.get("params", {}).get("page_param", "page")
        limit_param = pagination_config.get("params", {}).get("limit_param", "limit")

        page = pagination_config.get("start_page", 1)

        while True:
            params = {page_param: page, limit_param: batch_size}
            
            # Apply filters from config
            self._apply_filters_to_params(params, config)

            # Apply rate limiting
            if self.rate_limiter:
                await self.rate_limiter.acquire()

            # Log the full request URL
            full_request_url = f"{url}?{urlencode(params)}" if params else url
            logger.debug(f"Making API request: {method} {full_request_url}")

            async with self.session.request(method, url, params=params) as response:
                if response.status != 200:
                    raise ReadError(f"API request failed with status {response.status}")

                data = await response.json()
                records = self._extract_records_from_response(data, config)

                if not records:
                    break

                self.metrics["records_read"] += len(records)
                self.metrics["batches_read"] += 1

                yield records

                # If we got less than batch_size, we're done
                if len(records) < batch_size:
                    break

                page += 1

    def _extract_next_cursor_from_response(
        self, data: Dict[str, Any], pagination_config: Dict[str, Any]
    ) -> Optional[str]:
        """Extract next cursor from API response."""
        cursor_field = pagination_config.get("cursor_field", "next_cursor")
        
        if isinstance(data, dict):
            # Look for cursor in common locations
            for field in [cursor_field, "next_cursor", "cursor", "next", "continuation_token"]:
                if field in data:
                    cursor_value = data[field]
                    if cursor_value:
                        return str(cursor_value)
            
            # Look for cursor in pagination metadata
            if "pagination" in data and isinstance(data["pagination"], dict):
                pagination = data["pagination"]
                for field in [cursor_field, "next_cursor", "cursor", "next"]:
                    if field in pagination:
                        cursor_value = pagination[field]
                        if cursor_value:
                            return str(cursor_value)
        
        return None

    async def _read_single_request(
        self, url: str, method: str, config: Dict[str, Any]
    ) -> Optional[List[Dict[str, Any]]]:
        """Read data from single API request."""
        params = {}
        self._apply_filters_to_params(params, config)
        
        # Apply rate limiting
        if self.rate_limiter:
            await self.rate_limiter.acquire()

        full_request_url = f"{url}?{urlencode(params)}" if params else url
        logger.debug(f"Making API request: {method} {full_request_url}")

        async with self.session.request(method, url, params=params) as response:
            if response.status != 200:
                raise ReadError(f"API request failed with status {response.status}")

            data = await response.json()
            records = self._extract_records_from_response(data, config)

            if records:
                self.metrics["records_read"] += len(records)
                self.metrics["batches_read"] += 1

            return records

    def _extract_records_from_response(
        self, data: Dict[str, Any], config: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Extract records from API response."""
        if isinstance(data, list):
            return data

        data_field = config.get("data_field", "data")

        if data_field in data:
            records = data[data_field]
            if isinstance(records, list):
                return records
            else:
                return [records]

        return [data]

    def _apply_filters_to_params(self, params: Dict[str, Any], config: Dict[str, Any]):
        """Apply filters from config to request parameters."""
        filters = config.get("filters", {})

        for filter_name, filter_config in filters.items():
            filter_value = self._extract_value_from_schema_filter(filter_config)
            if filter_value is None:
                continue

            if isinstance(filter_value, list):
                params[filter_name] = ",".join(str(v) for v in filter_value)
            else:
                params[filter_name] = filter_value

    def _extract_value_from_schema_filter(self, filter_config: Dict[str, Any]) -> Any:
        """Extract actual filter value from schema-based filter configuration."""
        if "value" in filter_config:
            return filter_config["value"]
        elif "default" in filter_config:
            return filter_config["default"]
        else:
            if filter_config.get("required", False):
                logger.warning(f"Required filter missing explicit value: {filter_config}")
            return None


    def _get_filter_param_for_cursor_field(self, cursor_field: str, source_config: Dict[str, Any]) -> Optional[str]:
        """Get the API filter parameter name for a cursor field from the source configuration."""
        try:
            # Check for replication filter mapping in the source config
            replication_mapping = source_config.get("replication_filter_mapping", {})
            if cursor_field in replication_mapping:
                filter_param = replication_mapping[cursor_field]
                logger.debug(f"Found filter mapping: {cursor_field} -> {filter_param}")
                
                # Validate the filter parameter exists in schema filters
                filters = source_config.get("filters", {})
                if filter_param in filters:
                    return filter_param
                else:
                    logger.warning(f"Mapped filter parameter '{filter_param}' not found in source config filters")
                    return None
            
            # Fallback: check if cursor_field directly exists as a filter
            filters = source_config.get("filters", {})
            if cursor_field in filters:
                logger.debug(f"Using direct filter mapping: {cursor_field}")
                return cursor_field
            
            logger.warning(f"No filter parameter found for cursor field '{cursor_field}'")
            return None
            
        except Exception as e:
            logger.error(f"Failed to get filter parameter for cursor field '{cursor_field}': {str(e)}")
            return None


    def _compute_effective_start_time(self, cursor: str, safety_window_seconds: int) -> str:
        """Compute the effective start time by subtracting safety window from cursor."""
        try:
            from datetime import timedelta
            from dateutil.parser import isoparse
            
            # Parse datetime using dateutil for robust format handling
            cursor_dt = isoparse(cursor)
            
            # Ensure timezone-aware datetime
            if cursor_dt.tzinfo is None:
                cursor_dt = cursor_dt.replace(tzinfo=timezone.utc)
            
            effective_dt = cursor_dt - timedelta(seconds=safety_window_seconds)
            # Return in ISO format with Z suffix as expected by Wise API  
            return effective_dt.isoformat().replace('+00:00', 'Z')
            
        except (ValueError, ImportError) as e:
            logger.debug(f"Failed to parse datetime cursor '{cursor}': {str(e)}")
            
            # Try numeric cursor (ID-based)
            try:
                cursor_id = int(cursor)
                effective_id = max(0, cursor_id - safety_window_seconds)
                return str(effective_id)
            except ValueError:
                logger.warning(f"Failed to parse cursor '{cursor}' as datetime or numeric, using as-is")
                return cursor

    def _build_replication_filter(self, filter_param: str, effective_start: str) -> Dict[str, Any]:
        """Build the filter parameters for incremental replication (always inclusive mode)."""
        filter_params = {}
        filter_params[filter_param] = effective_start

        logger.debug(f"Built incremental filter: {filter_param} = {effective_start}")
        return filter_params

    def _get_nested_field_value(self, record: Dict[str, Any], field_path: str) -> Any:
        """Get value from nested field using dot notation (e.g., 'details.reference')."""
        try:
            value = record
            for field_name in field_path.split('.'):
                if isinstance(value, dict) and field_name in value:
                    value = value[field_name]
                else:
                    return None
            return value
        except (KeyError, TypeError, AttributeError):
            return None

    def _deduplicate_records(self, batch: List[Dict[str, Any]], state: Dict[str, Any], config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Filter out duplicate records based on cursor and tie-breaker comparison."""
        if not batch:
            return batch
            
        # Get stored cursor state
        bookmarks = state.get("bookmarks", [])
        if not bookmarks:
            return batch  # No previous state, all records are new
            
        bookmark = bookmarks[0]
        stored_cursor = bookmark.get("cursor")
        if not stored_cursor:
            return batch  # No cursor stored, all records are new
            
        cursor_field = state.get("cursor_field")
        tie_breaker_fields = config.get("tie_breaker_fields", [])
        
        if not cursor_field or not tie_breaker_fields:
            return batch  # No deduplication possible without cursor and tie-breaker fields
            
        deduplicated_records = []
        
        for record in batch:
            if self._is_record_new(record, bookmark, cursor_field, tie_breaker_fields):
                deduplicated_records.append(record)
            else:
                logger.debug(f"Skipping duplicate record: {cursor_field}={record.get(cursor_field)}, tie_breakers={[self._get_nested_field_value(record, f) for f in tie_breaker_fields]}")

        return deduplicated_records

    def _is_record_new(self, record: Dict[str, Any], bookmark: Dict[str, Any], cursor_field: str, tie_breaker_fields: List[str]) -> bool:
        """Check if a record is new compared to stored cursor and tie-breaker values."""
        try:
            from dateutil.parser import isoparse
            
            record_cursor_value = record.get(cursor_field)
            if record_cursor_value is None:
                return True  # No cursor value, treat as new
                
            stored_cursor = bookmark.get("cursor")
            if stored_cursor is None:
                return True  # No stored cursor, treat as new
                
            # Parse cursor values for comparison
            try:
                stored_cursor_dt = isoparse(stored_cursor)
                record_cursor_dt = isoparse(str(record_cursor_value))
                
                # Ensure both are timezone-aware
                if stored_cursor_dt.tzinfo is None:
                    stored_cursor_dt = stored_cursor_dt.replace(tzinfo=timezone.utc)
                if record_cursor_dt.tzinfo is None:
                    record_cursor_dt = record_cursor_dt.replace(tzinfo=timezone.utc)
                    
            except (ValueError, ImportError):
                # Fallback to string comparison
                stored_cursor_dt = str(stored_cursor)
                record_cursor_dt = str(record_cursor_value)
            
            # Compare cursor values
            if record_cursor_dt > stored_cursor_dt:
                return True  # Record is newer than stored cursor
            elif record_cursor_dt < stored_cursor_dt:
                return False  # Record is older than stored cursor
            else:
                # Same cursor value, check tie-breakers (always inclusive mode)
                return self._compare_tie_breakers(record, tie_breaker_fields, bookmark)

        except Exception as e:
            logger.warning(f"Error comparing record cursor: {str(e)}, treating as new")
            return True

    def _compare_tie_breakers(self, record: Dict[str, Any], tie_breaker_fields: List[str], bookmark: Dict[str, Any]) -> bool:
        """Compare tie-breaker field values to determine if record is new (always inclusive mode)."""
        if not tie_breaker_fields:
            return False  # No tie-breakers in inclusive mode means treat as duplicate
            
        aux_data = bookmark.get("aux", {})
        
        # Handle multiple tie-breakers
        if "tiebreakers" in aux_data:
            stored_tiebreakers = aux_data["tiebreakers"]
            
            # Compare each tie-breaker field in order
            for i, field_name in enumerate(tie_breaker_fields):
                if i >= len(stored_tiebreakers):
                    return True  # New field, treat as new
                    
                record_value = self._get_nested_field_value(record, field_name)
                stored_tiebreaker = stored_tiebreakers[i]
                stored_value = stored_tiebreaker.get("value")
                
                if record_value is None:
                    return True  # Missing record value, treat as new
                    
                # Compare values (convert to same type for comparison)
                try:
                    if isinstance(stored_value, str) and str(record_value).isdigit() and stored_value.isdigit():
                        # Both are numeric strings, compare as numbers
                        record_num = int(record_value)
                        stored_num = int(stored_value)
                        if record_num > stored_num:
                            return True
                        elif record_num < stored_num:
                            return False
                        # Equal, continue to next field
                    else:
                        # String comparison
                        if str(record_value) > str(stored_value):
                            return True
                        elif str(record_value) < str(stored_value):
                            return False
                        # Equal, continue to next field
                except (ValueError, TypeError):
                    # Fallback to string comparison
                    if str(record_value) > str(stored_value):
                        return True
                    elif str(record_value) < str(stored_value):
                        return False
            
            # All tie-breaker fields are equal - treat as duplicate in inclusive mode
            return False

        # No tie-breaker information found - treat as duplicate in inclusive mode
        return False

    async def write_batch(self, batch: List[Dict[str, Any]], config: Dict[str, Any]):
        """Write a batch of records to API endpoint."""
        logger.debug(f"API write_batch called with {len(batch)} records")
        try:
            endpoint = config.get("endpoint", config.get("path", "/"))
            method = config.get("method", "POST")
            full_url = urljoin(self.base_url, endpoint)
            logger.debug(f"Target URL: {full_url}, Method: {method}")

            batch_support = config.get("batch_support", False)
            logger.debug(f"Batch support: {batch_support}")

            if batch_support:
                logger.debug("Using batch request")
                await self._write_batch_request(full_url, method, batch, config)
            else:
                logger.debug(f"Using single record requests for {len(batch)} records")
                for i, record in enumerate(batch):
                    logger.debug(f"Writing record {i+1}/{len(batch)}")
                    await self._write_single_record(full_url, method, record, config)
                    logger.debug(f"Completed record {i+1}/{len(batch)}")

            self.metrics["records_written"] += len(batch)
            self.metrics["batches_written"] += 1

        except Exception as e:
            self.metrics["errors"] += 1
            logger.error(f"API write failed: {str(e)}")
            raise WriteError(f"API write failed: {str(e)}")

    async def _write_single_record(
        self, url: str, method: str, record: Dict[str, Any], config: Dict[str, Any]
    ):
        """Write single record to API."""
        logger.debug(f"_write_single_record: URL={url}, record keys={list(record.keys())}")
        
        if self.rate_limiter:
            logger.debug("Acquiring rate limit token")
            await self.rate_limiter.acquire()
            logger.debug("Rate limit token acquired")

        headers = {"Content-Type": "application/json"}
        logger.debug(f"Making {method} request to {url}")

        async with self.session.request(
            method, url, json=record, headers=headers
        ) as response:
            logger.debug(f"Response status: {response.status}")
            if response.status >= 400:
                error_text = await response.text()
                logger.error(f"API error {response.status}: {error_text}")
                raise WriteError(
                    f"Record write failed with status {response.status}: {error_text}"
                )
            else:
                response_text = await response.text()
                logger.debug(f"Successful response: {response_text}")

    def supports_incremental_read(self) -> bool:
        """API supports incremental reading through timestamps."""
        return True

    def supports_upsert(self) -> bool:
        """API supports upsert through idempotency."""
        return True

    def _mask_sensitive_headers(self, headers: Dict[str, str]) -> Dict[str, str]:
        """Mask sensitive header values for logging."""
        sensitive_keys = {'authorization', 'x-api-key', 'api-key', 'token', 'bearer'}
        masked = {}
        for key, value in headers.items():
            if key.lower() in sensitive_keys:
                # Show first 10 chars and mask the rest
                if len(value) > 14:
                    masked[key] = value[:14] + '***MASKED***'
                else:
                    masked[key] = '***MASKED***'
            else:
                masked[key] = value
        return masked

    async def _validate_response(self, response) -> HTTPResponse:
        """Validate HTTP response safely."""
        try:
            # Extract headers safely
            headers = {}
            for name, value in response.headers.items():
                if isinstance(name, str) and isinstance(value, str):
                    headers[name.lower()] = value
            
            # Parse response data safely
            content_type = headers.get('content-type', '')
            if 'application/json' in content_type:
                try:
                    data = await response.json()
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse JSON response: {str(e)}")
                    data = await response.text()
            else:
                data = await response.text()
                
            return HTTPResponse(
                status=response.status,
                headers=headers,
                data=data
            )
            
        except Exception as e:
            logger.error(f"Failed to validate HTTP response: {str(e)}")
            # Return minimal valid response
            return HTTPResponse(status=response.status)
    
    def _validate_batch(self, batch: List[Dict[str, Any]], config: APIReadConfig) -> RecordBatch:
        """Validate and wrap batch data."""
        try:
            return RecordBatch(records=batch)
        except Exception as e:
            logger.error(f"Batch validation failed: {str(e)}")
            # Filter out invalid records
            valid_records = []
            for record in batch:
                if isinstance(record, dict):
                    valid_records.append(record)
            return RecordBatch(records=valid_records)



# RateLimiter is imported from src.shared.rate_limiter