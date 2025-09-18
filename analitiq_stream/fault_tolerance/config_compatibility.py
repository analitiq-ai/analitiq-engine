"""Config-state compatibility validation and automatic recovery."""

import hashlib
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple, Set
from enum import Enum

from pydantic import BaseModel, Field
from ..models.state import PipelineFingerprint

logger = logging.getLogger(__name__)


class ChangeType(str, Enum):
    """Types of configuration changes."""
    NON_BREAKING = "non_breaking"
    BREAKING = "breaking"
    CRITICAL = "critical"


class ConfigChange(BaseModel):
    """Represents a detected configuration change."""
    field_path: str = Field(..., description="JSONPath to changed field")
    change_type: ChangeType = Field(..., description="Severity of the change")
    old_value: Any = Field(..., description="Previous value")
    new_value: Any = Field(..., description="New value")
    description: str = Field(..., description="Human-readable description")
    recovery_strategy: Optional[str] = Field(None, description="Suggested recovery action")


class ConfigCompatibilityError(Exception):
    """Raised when config-state compatibility check fails."""
    
    def __init__(self, message: str, changes: List[ConfigChange], pipeline_id: str):
        super().__init__(message)
        self.changes = changes
        self.pipeline_id = pipeline_id


class ConfigStateValidator:
    """Validates config-state compatibility and handles breaking changes."""
    
    # Fields that require state reset when changed
    BREAKING_FIELDS = {
        "src.replication_key",
        "src.replication_method", 
        "src.endpoint_id",
        "src.host_id",
        "dst.endpoint_id",
        "dst.host_id",
        "dst.refresh_mode",
        "mapping.field_mappings",  # If used in cursors
        "mapping.computed_fields"  # If used in cursors
    }
    
    # Fields that require cursor validation when changed
    CURSOR_AFFECTING_FIELDS = {
        "src.cursor_mode",
        "src.safety_window_seconds"
    }
    
    # Fields that are safe to change without state impact
    NON_BREAKING_FIELDS = {
        "engine_config.batch_size",
        "engine_config.max_concurrent_batches", 
        "engine_config.buffer_size",
        "engine_config.schedule",
        "monitoring.log_level",
        "monitoring.checkpoint_interval",
        "monitoring.health_check_interval",
        "error_handling.max_retries",
        "error_handling.retry_delay"
    }
    
    def __init__(self):
        self.changes: List[ConfigChange] = []
    
    def validate_startup_compatibility(
        self, 
        config: Dict[str, Any], 
        state_run_info: Dict[str, Any]
    ) -> Tuple[bool, List[ConfigChange]]:
        """
        Validate config-state compatibility on startup.
        
        Args:
            config: Current pipeline configuration
            state_run_info: Run info from state index.json
            
        Returns:
            Tuple of (is_compatible, list_of_changes)
            
        Raises:
            ConfigCompatibilityError: If critical incompatibilities found
        """
        self.changes = []
        
        # 1. Validate pipeline ID match
        config_pipeline_id = config.get("pipeline_id")
        state_pipeline_id = state_run_info.get("pipeline_id")
        
        if state_pipeline_id and config_pipeline_id != state_pipeline_id:
            raise ConfigCompatibilityError(
                f"Pipeline ID mismatch: config='{config_pipeline_id}' vs state='{state_pipeline_id}'",
                [],
                config_pipeline_id or "unknown"
            )
        
        # 2. Compare config fingerprints
        current_fingerprint = self._compute_config_fingerprint(config)
        stored_fingerprint = state_run_info.get("config_fingerprint")
        
        if not stored_fingerprint:
            logger.info("No stored config fingerprint - first run or legacy state")
            return True, []
        
        if current_fingerprint == stored_fingerprint:
            logger.debug("Config fingerprint match - no compatibility issues")
            return True, []
        
        # 3. Analyze configuration differences
        stored_config = state_run_info.get("config_snapshot")
        if not stored_config:
            logger.warning("No config snapshot in state - cannot analyze changes")
            return self._handle_missing_config_snapshot(current_fingerprint, stored_fingerprint)
        
        self._detect_configuration_changes(stored_config, config)
        
        # 4. Categorize changes and determine compatibility
        breaking_changes = [c for c in self.changes if c.change_type == ChangeType.BREAKING]
        critical_changes = [c for c in self.changes if c.change_type == ChangeType.CRITICAL]
        
        if critical_changes:
            raise ConfigCompatibilityError(
                f"Critical configuration changes require manual intervention: {len(critical_changes)} changes",
                critical_changes,
                config_pipeline_id
            )
        
        if breaking_changes:
            logger.warning(f"Breaking configuration changes detected: {len(breaking_changes)} changes")
            return False, self.changes
        
        # Only non-breaking changes
        non_breaking_changes = [c for c in self.changes if c.change_type == ChangeType.NON_BREAKING]
        if non_breaking_changes:
            logger.info(f"Non-breaking configuration changes: {len(non_breaking_changes)} changes")
        
        return True, self.changes
    
    def _compute_config_fingerprint(self, config: Dict[str, Any]) -> str:
        """Compute SHA256 fingerprint of configuration."""
        fingerprint_config = PipelineFingerprint(
            pipeline_id=config.get("pipeline_id", ""),
            version=config.get("version", ""),
            src=config.get("src", {}),
            dst=config.get("dst", {}),
            streams=config.get("streams", {})
        )
        return fingerprint_config.fingerprint()
    
    def _extract_breaking_engine_config(self, engine_config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract only breaking fields from engine config."""
        # For now, most engine config changes are non-breaking
        return {
            "state_file": engine_config.get("state_file")  # Path changes could be breaking
        }
    
    def _handle_missing_config_snapshot(
        self, 
        current_fingerprint: str, 
        stored_fingerprint: str
    ) -> Tuple[bool, List[ConfigChange]]:
        """Handle case where we have fingerprint mismatch but no config snapshot."""
        change = ConfigChange(
            field_path="config_fingerprint",
            change_type=ChangeType.BREAKING,
            old_value=stored_fingerprint,
            new_value=current_fingerprint,
            description="Config fingerprint changed but no snapshot available for detailed analysis",
            recovery_strategy="manual_review_or_reset"
        )
        return False, [change]
    
    def _detect_configuration_changes(self, old_config: Dict[str, Any], new_config: Dict[str, Any]):
        """Detect and categorize configuration changes."""
        self._compare_nested_dict(old_config, new_config, "")
    
    def _compare_nested_dict(self, old: Dict[str, Any], new: Dict[str, Any], path_prefix: str):
        """Recursively compare nested dictionaries."""
        all_keys = set(old.keys()) | set(new.keys())
        
        for key in all_keys:
            current_path = f"{path_prefix}.{key}" if path_prefix else key
            
            old_value = old.get(key)
            new_value = new.get(key)
            
            if key not in old:
                # New field added
                self._record_change(current_path, None, new_value, "Field added")
            elif key not in new:
                # Field removed  
                self._record_change(current_path, old_value, None, "Field removed")
            elif isinstance(old_value, dict) and isinstance(new_value, dict):
                # Nested dictionary - recurse
                self._compare_nested_dict(old_value, new_value, current_path)
            elif old_value != new_value:
                # Value changed
                self._record_change(current_path, old_value, new_value, "Value changed")
    
    def _record_change(self, field_path: str, old_value: Any, new_value: Any, description: str):
        """Record a detected configuration change."""
        change_type = self._classify_change(field_path, old_value, new_value)
        recovery_strategy = self._suggest_recovery_strategy(field_path, change_type)
        
        change = ConfigChange(
            field_path=field_path,
            change_type=change_type,
            old_value=old_value,
            new_value=new_value,
            description=f"{description}: {field_path}",
            recovery_strategy=recovery_strategy
        )
        
        self.changes.append(change)
        
        logger.debug(f"Config change detected: {change_type.value} - {field_path}: {old_value} -> {new_value}")
    
    def _classify_change(self, field_path: str, old_value: Any, new_value: Any) -> ChangeType:
        """Classify the severity of a configuration change."""
        
        # Check for critical changes first
        if self._is_critical_change(field_path, old_value, new_value):
            return ChangeType.CRITICAL
        
        # Check for breaking changes
        if field_path in self.BREAKING_FIELDS:
            return ChangeType.BREAKING
        
        if self._affects_cursor_compatibility(field_path, old_value, new_value):
            return ChangeType.BREAKING
        
        # Check for non-breaking changes
        if field_path in self.NON_BREAKING_FIELDS:
            return ChangeType.NON_BREAKING
        
        # Default to breaking for unknown fields to be safe
        return ChangeType.BREAKING
    
    def _is_critical_change(self, field_path: str, old_value: Any, new_value: Any) -> bool:
        """Check if change is critical and requires manual intervention."""
        
        # Pipeline ID changes are critical
        if field_path == "pipeline_id":
            return True
        
        # Endpoint/host changes that affect data source/destination identity
        if field_path in ["src.endpoint_id", "src.host_id", "dst.endpoint_id", "dst.host_id"]:
            return True
        
        # Replication method changes (full <-> incremental)
        if field_path == "src.replication_method":
            return True
        
        return False
    
    def _affects_cursor_compatibility(self, field_path: str, old_value: Any, new_value: Any) -> bool:
        """Check if change affects cursor/bookmark compatibility."""
        
        if field_path in self.CURSOR_AFFECTING_FIELDS:
            return True
        
        # Replication key changes affect cursor interpretation
        if field_path == "src.replication_key" and old_value != new_value:
            return True
        
        # Field mapping changes that affect cursor fields
        if field_path.startswith("mapping.field_mappings"):
            # TODO: Check if the changed mapping affects cursor fields
            return True
        
        return False
    
    def _suggest_recovery_strategy(self, field_path: str, change_type: ChangeType) -> Optional[str]:
        """Suggest appropriate recovery strategy for the change."""
        
        if change_type == ChangeType.NON_BREAKING:
            return "continue"
        
        if change_type == ChangeType.CRITICAL:
            return "manual_intervention"
        
        # Breaking changes - suggest specific strategies
        if field_path == "src.replication_key":
            return "reset_with_safety_window"
        
        if field_path in ["src.cursor_mode", "src.safety_window_seconds"]:
            return "adjust_cursor_with_safety_window"
        
        if field_path.startswith("mapping"):
            return "validate_cursor_fields_or_reset"
        
        return "reset_state"


class ConfigStateRecoveryManager:
    """Manages automatic recovery from breaking configuration changes."""
    
    def __init__(self, state_manager):
        self.state_manager = state_manager
    
    def attempt_automatic_recovery(
        self, 
        config: Dict[str, Any],
        changes: List[ConfigChange]
    ) -> bool:
        """
        Attempt automatic recovery from breaking changes.
        
        Returns:
            True if recovery was successful, False if manual intervention needed
        """
        recovery_actions = []
        
        for change in changes:
            if change.change_type == ChangeType.CRITICAL:
                logger.error(f"Critical change requires manual intervention: {change.field_path}")
                return False
            
            if change.recovery_strategy == "reset_with_safety_window":
                recovery_actions.append(("reset_cursors_with_safety", change))
            elif change.recovery_strategy == "adjust_cursor_with_safety_window":
                recovery_actions.append(("adjust_cursors", change))
            elif change.recovery_strategy == "reset_state":
                recovery_actions.append(("full_reset", change))
            else:
                logger.warning(f"No automatic recovery for change: {change.field_path}")
                return False
        
        # Execute recovery actions
        try:
            # Group recovery actions for consolidated logging
            reset_actions = [change for action, change in recovery_actions if action == "full_reset"]
            cursor_actions = [change for action, change in recovery_actions if action in ["reset_cursors_with_safety", "adjust_cursors"]]
            
            if reset_actions:
                reset_fields = [change.field_path for change in reset_actions]
                logger.warning(f"Performing full state reset for {len(reset_actions)} changes: {', '.join(reset_fields)}")
            
            if cursor_actions:
                cursor_fields = [change.field_path for change in cursor_actions]
                logger.info(f"Adjusting cursors for {len(cursor_actions)} changes: {', '.join(cursor_fields)}")
            
            # Execute the actual recovery actions
            for action, change in recovery_actions:
                self._execute_recovery_action(action, change, config)
            
            logger.info(f"✅ Automatic recovery completed successfully for {len(recovery_actions)} changes")
            return True
            
        except Exception as e:
            logger.error(f"❌ Automatic recovery failed: {str(e)}")
            return False
    
    def _execute_recovery_action(
        self, 
        action: str, 
        change: ConfigChange, 
        config: Dict[str, Any]
    ):
        """Execute a specific recovery action."""
        
        if action == "reset_cursors_with_safety":
            self._reset_cursors_with_enlarged_safety_window(change, config)
        elif action == "adjust_cursors":
            self._adjust_cursors_for_compatibility(change, config)
        elif action == "full_reset":
            self._perform_full_state_reset(change)
        else:
            raise ValueError(f"Unknown recovery action: {action}")
    
    def _reset_cursors_with_enlarged_safety_window(
        self, 
        change: ConfigChange, 
        config: Dict[str, Any]
    ):
        """Reset cursors with enlarged safety window for replication key changes."""
        # Get current safety window and enlarge it
        current_safety = config.get("src", {}).get("safety_window_seconds", 120)
        enlarged_safety = max(current_safety * 3, 600)  # At least 10 minutes
        
        # Reset all streams with enlarged safety window
        streams = self.state_manager.list_streams()
        for stream_name in streams:
            partitions = self.state_manager.get_stream_partitions(stream_name)
            
            for partition_state in partitions:
                partition = partition_state.get("partition", {})
                cursor = partition_state.get("cursor", {})
                
                if cursor and cursor.get("primary", {}).get("value"):
                    # Rewind cursor by enlarged safety window
                    old_cursor_value = cursor["primary"]["value"]
                    new_cursor_value = self._rewind_cursor(old_cursor_value, enlarged_safety)
                    
                    # Reset hwm and page state
                    self.state_manager.save_stream_checkpoint(
                        stream_name=stream_name,
                        partition=partition,
                        cursor={
                            "primary": {
                                "field": config.get("src", {}).get("replication_key"),
                                "value": new_cursor_value,
                                "inclusive": config.get("src", {}).get("cursor_mode") == "inclusive"
                            }
                        },
                        hwm=new_cursor_value,
                        page_state=None,  # Reset pagination
                        http_conditionals=None  # Reset HTTP conditionals
                    )
                    
    
    def _adjust_cursors_for_compatibility(self, change: ConfigChange, config: Dict[str, Any]):
        """Adjust cursors for cursor mode or safety window changes."""
        
        streams = self.state_manager.list_streams()
        for stream_name in streams:
            partitions = self.state_manager.get_stream_partitions(stream_name)
            
            for partition_state in partitions:
                partition = partition_state.get("partition", {})
                cursor = partition_state.get("cursor", {})
                
                if cursor and cursor.get("primary"):
                    # Update cursor mode if changed
                    if change.field_path == "src.cursor_mode":
                        cursor["primary"]["inclusive"] = config.get("src", {}).get("cursor_mode") == "inclusive"
                        
                        # Save updated cursor
                        self.state_manager.save_stream_checkpoint(
                            stream_name=stream_name,
                            partition=partition,
                            cursor=cursor,
                            hwm=partition_state.get("hwm"),
                            stats=partition_state.get("stats")
                        )
                        
    
    def _perform_full_state_reset(self, change: ConfigChange):
        """Perform full state reset for the affected stream."""
        # Reset all streams - this is a nuclear option
        streams = self.state_manager.list_streams()
        for stream_name in streams:
            self.state_manager.clear_stream_state(stream_name)
    
    def _rewind_cursor(self, cursor_value: str, safety_seconds: int) -> str:
        """Rewind cursor value by specified safety window."""
        try:
            # Handle timestamp cursors
            if isinstance(cursor_value, str) and ('T' in cursor_value or 'Z' in cursor_value):
                cursor_dt = datetime.fromisoformat(cursor_value.replace('Z', '+00:00'))
                rewound_dt = cursor_dt - timedelta(seconds=safety_seconds)
                return rewound_dt.isoformat().replace('+00:00', 'Z')
            
            # Handle numeric cursors (IDs)
            elif str(cursor_value).isdigit():
                # For numeric cursors, rewind by a reasonable amount
                cursor_int = int(cursor_value)
                # Rewind by safety_seconds as a proxy (not perfect but reasonable)
                rewound_int = max(0, cursor_int - safety_seconds)
                return str(rewound_int)
            
            else:
                logger.warning(f"Cannot rewind unknown cursor format: {cursor_value}")
                return cursor_value
                
        except Exception as e:
            logger.error(f"Failed to rewind cursor {cursor_value}: {str(e)}")
            return cursor_value