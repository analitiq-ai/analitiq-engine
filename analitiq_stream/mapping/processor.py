"""Field mapping processor with transformation and validation support."""

import logging
import os
import re
from typing import Any, Dict, List, Optional, Union

from ..transformations.registry import get_transformation_registry, TransformationError
from ..models.transformations import TransformationConfig, TransformationType

logger = logging.getLogger(__name__)


class MappingError(Exception):
    """Exception raised when field mapping fails."""
    pass


class ValidationError(Exception):
    """Exception raised when field validation fails."""
    pass


class FieldMappingProcessor:
    """
    Processes field mappings with transformations and validation.
    
    Supports:
    - Direct field mapping (source -> target)
    - Field transformations (abs, strip, iso_to_date, etc.)
    - Field validation (not_null, enum, range, etc.)
    - Computed fields with expressions
    - Error handling (dlq, skip, fail)
    """
    
    def __init__(self, mapping_config: Dict[str, Any]):
        """
        Initialize field mapping processor.
        
        Args:
            mapping_config: Field-centric mapping configuration
        """
        self.field_mappings = mapping_config.get("field_mappings", {})
        self.computed_fields = mapping_config.get("computed_fields", {})
        self.default_error_action = mapping_config.get("default_error_action", "fail")
        
        # Validate configuration
        self._validate_config()
    
    def _validate_config(self):
        """Validate mapping configuration structure."""
        for source_field, config in self.field_mappings.items():
            if not isinstance(config, dict):
                raise MappingError(f"Field mapping for '{source_field}' must be a dictionary")
            
            # Validate transformations exist
            if "transformations" in config:
                registry = get_transformation_registry()
                for transform in config["transformations"]:
                    try:
                        if isinstance(transform, str):
                            TransformationType(transform)  # Validate transformation type
                        elif isinstance(transform, dict):
                            TransformationConfig(**transform)  # Validate config format
                    except (ValueError, Exception) as e:
                        raise MappingError(f"Invalid transformation '{transform}' for field '{source_field}': {str(e)}")
    
    def process_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single record through field mapping, transformation, and validation.
        
        Args:
            record: Source record dictionary
            
        Returns:
            Transformed and validated target record
            
        Raises:
            MappingError: If mapping fails and error_action is 'fail'
            ValidationError: If validation fails and error_action is 'fail'
        """
        target_record = {}
        errors = []
        
        # Process field mappings
        for source_field, config in self.field_mappings.items():
            try:
                # Extract source value (supports nested fields like "details.merchant.name")
                source_value = self._get_nested_value(record, source_field)
                
                # Apply transformations
                transformed_value = self._apply_transformations(source_value, config.get("transformations", []))
                
                # Validate transformed value
                validation_result = self._validate_field(source_field, transformed_value, config.get("validation"))
                
                if validation_result["valid"]:
                    # Map to target field
                    target_field = config.get("target", source_field)
                    target_record[target_field] = transformed_value
                else:
                    # Handle validation error
                    error_action = self._get_error_action(config)
                    self._handle_validation_error(source_field, validation_result["error"], error_action, errors)
                    
            except Exception as e:
                error_action = self._get_error_action(config)
                self._handle_mapping_error(source_field, str(e), error_action, errors)
        
        # Process computed fields
        for field_name, config in self.computed_fields.items():
            try:
                computed_value = self._compute_field_value(config["expression"], record, target_record)
                
                # Validate computed value
                validation_result = self._validate_field(field_name, computed_value, config.get("validation"))
                
                if validation_result["valid"]:
                    target_record[field_name] = computed_value
                else:
                    error_action = self._get_error_action(config)
                    self._handle_validation_error(field_name, validation_result["error"], error_action, errors)
                    
            except Exception as e:
                error_action = self._get_error_action(config)
                self._handle_mapping_error(field_name, str(e), error_action, errors)
        
        # Raise errors if any 'fail' actions were encountered
        if errors:
            error_messages = [f"{err['field']}: {err['message']}" for err in errors if err['action'] == 'fail']
            if error_messages:
                raise MappingError(f"Field mapping failed: {'; '.join(error_messages)}")
        
        return target_record
    
    def _get_nested_value(self, record: Dict[str, Any], field_path: str) -> Any:
        """Get value from nested dictionary using dot notation."""
        try:
            value = record
            for key in field_path.split('.'):
                if isinstance(value, dict):
                    value = value.get(key)
                else:
                    return None
            return value
        except Exception:
            return None
    
    def _apply_transformations(self, value: Any, transformations: List[Union[str, Dict[str, Any]]]) -> Any:
        """Apply list of transformations to value in order."""
        if not transformations:
            return value
            
        result = value
        registry = get_transformation_registry()
        
        for transform in transformations:
            try:
                if isinstance(transform, str):
                    # Simple string transformation
                    config = TransformationConfig(type=TransformationType(transform))
                elif isinstance(transform, dict):
                    # Dict transformation with parameters
                    config = TransformationConfig(**transform)
                else:
                    raise MappingError(f"Invalid transformation format: {transform}")
                
                result = registry.apply_transformation(result, config)
                
            except (TransformationError, ValueError) as e:
                raise MappingError(f"Transformation '{transform}' failed: {str(e)}")
        
        return result
    
    def _validate_field(self, field_name: str, value: Any, validation_config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Validate field value against validation rules.
        
        Returns:
            Dictionary with 'valid' boolean and 'error' message if invalid
        """
        if not validation_config:
            return {"valid": True, "error": None}
        
        rules = validation_config.get("rules", [])
        
        for rule in rules:
            rule_type = rule["type"]
            
            try:
                if rule_type == "not_null":
                    if value is None or value == "":
                        return {"valid": False, "error": f"Field '{field_name}' cannot be null or empty"}
                
                elif rule_type == "enum":
                    if value not in rule["values"]:
                        return {"valid": False, "error": f"Field '{field_name}' value '{value}' not in allowed values: {rule['values']}"}
                
                elif rule_type == "range":
                    try:
                        num_value = float(value) if value is not None else 0
                        min_val = rule.get("min")
                        max_val = rule.get("max")
                        
                        if min_val is not None and num_value < min_val:
                            return {"valid": False, "error": f"Field '{field_name}' value {num_value} below minimum {min_val}"}
                        if max_val is not None and num_value > max_val:
                            return {"valid": False, "error": f"Field '{field_name}' value {num_value} above maximum {max_val}"}
                    except (ValueError, TypeError):
                        return {"valid": False, "error": f"Field '{field_name}' value '{value}' is not numeric for range validation"}
                
                elif rule_type == "regex":
                    if value is not None and not re.match(rule["pattern"], str(value)):
                        return {"valid": False, "error": f"Field '{field_name}' value '{value}' does not match pattern '{rule['pattern']}'"}
                
                elif rule_type == "custom":
                    # For future extension with custom validation functions
                    pass
                
                else:
                    logger.warning(f"Unknown validation rule type: {rule_type}")
                    
            except Exception as e:
                return {"valid": False, "error": f"Validation error for field '{field_name}': {str(e)}"}
        
        return {"valid": True, "error": None}
    
    def _compute_field_value(self, expression: str, source_record: Dict[str, Any], target_record: Dict[str, Any]) -> Any:
        """
        Compute field value from expression.
        
        Supports:
        - Environment variables: ${ENV_VAR}
        - Source field references: ${source_field}
        - Target field references: ${target_field}
        - Function calls: now(), uuid()
        - Literal values
        """
        try:
            # Handle function calls
            if expression == "now()":
                from datetime import datetime
                return datetime.now()
            elif expression == "uuid()":
                import uuid
                return str(uuid.uuid4())
            
            # Handle environment variables and field references
            result = expression
            
            # Replace environment variables ${ENV_VAR}
            env_pattern = r'\$\{([^}]+)\}'
            for match in re.finditer(env_pattern, expression):
                var_name = match.group(1)
                
                # Check if it's an environment variable
                env_value = os.getenv(var_name)
                if env_value is not None:
                    result = result.replace(match.group(0), env_value)
                # Check if it's a source field
                elif var_name in source_record:
                    field_value = str(source_record[var_name]) if source_record[var_name] is not None else ""
                    result = result.replace(match.group(0), field_value)
                # Check if it's a target field
                elif var_name in target_record:
                    field_value = str(target_record[var_name]) if target_record[var_name] is not None else ""
                    result = result.replace(match.group(0), field_value)
                else:
                    # Keep original placeholder if not found
                    logger.warning(f"Variable '{var_name}' not found in environment or record fields")
            
            return result
            
        except Exception as e:
            raise MappingError(f"Failed to compute expression '{expression}': {str(e)}")
    
    def _get_error_action(self, config: Dict[str, Any]) -> str:
        """Get error action from field config or default."""
        validation_config = config.get("validation", {})
        return validation_config.get("error_action", self.default_error_action)
    
    def _handle_validation_error(self, field_name: str, error_message: str, error_action: str, errors: List[Dict[str, Any]]):
        """Handle validation error based on error action."""
        error_info = {
            "field": field_name,
            "message": error_message,
            "type": "validation",
            "action": error_action
        }
        
        if error_action == "dlq":
            logger.warning(f"Validation failed for field '{field_name}', sending to DLQ: {error_message}")
            errors.append(error_info)
        elif error_action == "skip":
            logger.info(f"Validation failed for field '{field_name}', skipping: {error_message}")
            errors.append(error_info)
        elif error_action == "fail":
            logger.error(f"Validation failed for field '{field_name}': {error_message}")
            errors.append(error_info)
    
    def _handle_mapping_error(self, field_name: str, error_message: str, error_action: str, errors: List[Dict[str, Any]]):
        """Handle mapping error based on error action."""
        error_info = {
            "field": field_name,
            "message": error_message,
            "type": "mapping",
            "action": error_action
        }
        
        if error_action == "dlq":
            logger.warning(f"Mapping failed for field '{field_name}', sending to DLQ: {error_message}")
            errors.append(error_info)
        elif error_action == "skip":
            logger.info(f"Mapping failed for field '{field_name}', skipping: {error_message}")
            errors.append(error_info)
        elif error_action == "fail":
            logger.error(f"Mapping failed for field '{field_name}': {error_message}")
            errors.append(error_info)
    
    def get_target_schema(self) -> Dict[str, str]:
        """Get expected target schema from mapping configuration."""
        schema = {}
        
        # Add mapped fields
        for source_field, config in self.field_mappings.items():
            target_field = config.get("target", source_field)
            schema[target_field] = "mapped"
        
        # Add computed fields
        for field_name in self.computed_fields.keys():
            schema[field_name] = "computed"
        
        return schema