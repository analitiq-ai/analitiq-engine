"""Secure expression evaluator for computed fields."""

import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict

logger = logging.getLogger(__name__)


class SecureExpressionEvaluator:
    """
    Secure expression evaluator with predefined functions and validation.
    
    Prevents code injection by only allowing predefined expressions and functions.
    """

    def __init__(self):
        # Predefined safe functions
        self.functions: Dict[str, Callable] = {
            "now": lambda: datetime.now(timezone.utc).isoformat(),
            "today": lambda: datetime.now(timezone.utc).date().isoformat(),
            "uuid": lambda: str(uuid.uuid4()),
        }
        
        # Safe regex patterns
        self.env_var_pattern = re.compile(r'^\$\{([A-Z_][A-Z0-9_]*)\}$')
        self.concat_pattern = re.compile(r'^concat\((.*)\)$')
        self.json_pattern = re.compile(r'^[\{\[].*[\}\]]$')
        
    async def evaluate(self, expression: str, original_record: Dict[str, Any], 
                      transformed_record: Dict[str, Any]) -> Any:
        """
        Safely evaluate expression with validation.
        
        Args:
            expression: Expression string to evaluate
            original_record: Original record data
            transformed_record: Transformed record data
            
        Returns:
            Evaluated expression result
            
        Raises:
            ValueError: If expression is unsafe or invalid
        """
        if not isinstance(expression, str):
            return expression
            
        expression = expression.strip()
        
        # Environment variable substitution (safe pattern)
        if env_match := self.env_var_pattern.match(expression):
            env_var = env_match.group(1)
            result = os.getenv(env_var)
            if result is None:
                logger.warning(f"Environment variable {env_var} not found")
            return result
            
        # Predefined function calls
        if expression.endswith("()") and expression[:-2] in self.functions:
            func_name = expression[:-2]
            return self.functions[func_name]()
            
        # Concat function with validation
        if concat_match := self.concat_pattern.match(expression):
            return await self._safe_concat(concat_match.group(1), transformed_record)
            
        # JSON object literals (validate structure)
        if self.json_pattern.match(expression):
            return await self._safe_json_parse(expression)
            
        # Static value (no evaluation needed)
        return expression

    async def _safe_concat(self, args_str: str, record: Dict[str, Any]) -> str:
        """
        Safely concatenate values from record fields.
        
        Args:
            args_str: Comma-separated argument string
            record: Record to extract values from
            
        Returns:
            Concatenated string
        """
        if not args_str.strip():
            return ""
            
        # Split and clean arguments
        parts = []
        for arg in args_str.split(","):
            arg = arg.strip().strip("'\"")
            
            # Field reference
            if arg in record:
                value = record[arg]
                parts.append(str(value) if value is not None else "")
            # Literal string
            else:
                parts.append(arg)
        
        return "".join(parts)
    
    async def _safe_json_parse(self, json_str: str) -> Any:
        """
        Safely parse JSON string literals.
        
        Args:
            json_str: JSON string to parse
            
        Returns:
            Parsed JSON object
            
        Raises:
            ValueError: If JSON is invalid
        """
        try:
            # Only allow simple JSON objects/arrays - no code execution
            parsed = json.loads(json_str)
            
            # Additional validation: ensure it's simple data structures
            if isinstance(parsed, (dict, list, str, int, float, bool)) or parsed is None:
                return parsed
            else:
                raise ValueError(f"Unsupported JSON type: {type(parsed)}")
                
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON expression: {json_str}, error: {e}")
            raise ValueError(f"Invalid JSON expression: {e}")

    def add_function(self, name: str, func: Callable) -> None:
        """
        Add a custom safe function to the evaluator.
        
        Args:
            name: Function name
            func: Callable function (should be side-effect free)
        """
        self.functions[name] = func
        
    def remove_function(self, name: str) -> None:
        """
        Remove a function from the evaluator.
        
        Args:
            name: Function name to remove
        """
        self.functions.pop(name, None)