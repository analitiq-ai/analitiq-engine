"""Schema management and drift detection."""

import hashlib
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class SchemaManager:
    """
    Manages schema discovery, comparison, and evolution.

    Features:
    - Schema fingerprinting for drift detection
    - Schema comparison and change analysis
    - Evolution recommendations
    - Version management
    """

    def __init__(self, state_manager):
        """
        Initialize schema manager.

        Args:
            state_manager: State manager instance for persistence
        """
        self.state_manager = state_manager
        self.schema_cache = {}

    def generate_schema_hash(self, schema: Dict[str, Any]) -> str:
        """
        Generate a hash for schema comparison.

        Args:
            schema: Schema dictionary to hash

        Returns:
            MD5 hash of the normalized schema
        """
        try:
            normalized_schema = self._normalize_schema(schema)
            schema_str = json.dumps(normalized_schema, sort_keys=True)
            return hashlib.md5(schema_str.encode()).hexdigest()
        except Exception as e:
            logger.error(f"Failed to generate schema hash: {str(e)}")
            return ""

    def _normalize_schema(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize schema for consistent comparison.

        Args:
            schema: Raw schema dictionary

        Returns:
            Normalized schema dictionary
        """
        normalized = {}

        # Extract and normalize properties
        if "properties" in schema:
            normalized["properties"] = {}
            for field_name, field_def in schema["properties"].items():
                normalized["properties"][field_name] = self._normalize_field_definition(
                    field_def
                )

        # Extract required fields
        if "required" in schema:
            normalized["required"] = sorted(schema["required"])

        # Extract type information
        if "type" in schema:
            normalized["type"] = schema["type"]

        return normalized

    def _normalize_field_definition(self, field_def: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize a field definition for comparison.

        Args:
            field_def: Field definition dictionary

        Returns:
            Normalized field definition
        """
        normalized = {}

        # Essential properties for comparison
        essential_props = [
            "type",
            "format",
            "maxLength",
            "maximum",
            "minimum",
            "pattern",
        ]

        for prop in essential_props:
            if prop in field_def:
                normalized[prop] = field_def[prop]

        # Handle array types
        if isinstance(field_def.get("type"), list):
            normalized["type"] = sorted(field_def["type"])

        return normalized

    def detect_schema_drift(
        self, pipeline_id: str, current_schema: Dict[str, Any]
    ) -> bool:
        """
        Detect if schema has drifted from last known version.

        Args:
            pipeline_id: Pipeline identifier
            current_schema: Current schema to compare

        Returns:
            True if schema drift is detected
        """
        current_hash = self.generate_schema_hash(current_schema)
        checkpoint = self.state_manager.get_checkpoint(pipeline_id)
        previous_hash = checkpoint.get("schema_hash")

        if previous_hash is None:
            logger.info(f"No previous schema found for pipeline {pipeline_id}")
            return False

        drift_detected = current_hash != previous_hash

        if drift_detected:
            logger.warning(f"Schema drift detected for pipeline {pipeline_id}")
            logger.debug(
                f"Previous hash: {previous_hash}, Current hash: {current_hash}"
            )

        return drift_detected

    def analyze_schema_changes(
        self, old_schema: Dict[str, Any], new_schema: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Analyze changes between two schemas.

        Args:
            old_schema: Previous schema version
            new_schema: New schema version

        Returns:
            Dictionary containing detailed change analysis
        """
        changes = {
            "added_fields": [],
            "removed_fields": [],
            "modified_fields": [],
            "type_changes": [],
            "constraint_changes": [],
            "breaking_changes": [],
            "non_breaking_changes": [],
        }

        old_props = old_schema.get("properties", {})
        new_props = new_schema.get("properties", {})

        # Find added fields
        for field_name, field_def in new_props.items():
            if field_name not in old_props:
                changes["added_fields"].append(
                    {"field": field_name, "definition": field_def}
                )
                changes["non_breaking_changes"].append(f"Added field: {field_name}")

        # Find removed fields
        for field_name in old_props:
            if field_name not in new_props:
                changes["removed_fields"].append(
                    {"field": field_name, "old_definition": old_props[field_name]}
                )
                changes["breaking_changes"].append(f"Removed field: {field_name}")

        # Find modified fields
        for field_name in old_props:
            if field_name in new_props:
                old_def = old_props[field_name]
                new_def = new_props[field_name]

                if old_def != new_def:
                    field_changes = self._analyze_field_changes(
                        field_name, old_def, new_def
                    )
                    changes["modified_fields"].append(field_changes)

                    if field_changes["is_breaking"]:
                        changes["breaking_changes"].extend(
                            field_changes["breaking_changes"]
                        )
                    else:
                        changes["non_breaking_changes"].extend(
                            field_changes["non_breaking_changes"]
                        )

        # Analyze required field changes
        old_required = set(old_schema.get("required", []))
        new_required = set(new_schema.get("required", []))

        newly_required = new_required - old_required
        no_longer_required = old_required - new_required

        for field in newly_required:
            changes["breaking_changes"].append(f"Field {field} is now required")

        for field in no_longer_required:
            changes["non_breaking_changes"].append(
                f"Field {field} is no longer required"
            )

        return changes

    def _analyze_field_changes(
        self, field_name: str, old_def: Dict[str, Any], new_def: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Analyze changes in a specific field.

        Args:
            field_name: Name of the field
            old_def: Old field definition
            new_def: New field definition

        Returns:
            Dictionary containing field change analysis
        """
        changes = {
            "field": field_name,
            "old_definition": old_def,
            "new_definition": new_def,
            "is_breaking": False,
            "breaking_changes": [],
            "non_breaking_changes": [],
        }

        # Check type changes
        old_type = old_def.get("type", "unknown")
        new_type = new_def.get("type", "unknown")

        if old_type != new_type:
            # Type changes are generally breaking
            changes["is_breaking"] = True
            changes["breaking_changes"].append(
                f"Type changed from {old_type} to {new_type}"
            )

        # Check constraint changes
        constraint_props = ["maxLength", "maximum", "minimum", "pattern"]

        for prop in constraint_props:
            old_val = old_def.get(prop)
            new_val = new_def.get(prop)

            if old_val != new_val:
                if self._is_constraint_breaking(prop, old_val, new_val):
                    changes["is_breaking"] = True
                    changes["breaking_changes"].append(
                        f"Constraint {prop} changed from {old_val} to {new_val}"
                    )
                else:
                    changes["non_breaking_changes"].append(
                        f"Constraint {prop} changed from {old_val} to {new_val}"
                    )

        return changes

    def _is_constraint_breaking(
        self, constraint: str, old_val: Any, new_val: Any
    ) -> bool:
        """
        Determine if a constraint change is breaking.

        Args:
            constraint: Constraint name
            old_val: Old constraint value
            new_val: New constraint value

        Returns:
            True if the change is breaking
        """
        if old_val is None and new_val is not None:
            # Adding a new constraint is potentially breaking
            return True

        if old_val is not None and new_val is None:
            # Removing a constraint is non-breaking
            return False

        # Specific constraint logic
        if constraint == "maxLength":
            return new_val is not None and old_val is not None and new_val < old_val
        elif constraint == "maximum":
            return new_val is not None and old_val is not None and new_val < old_val
        elif constraint == "minimum":
            return new_val is not None and old_val is not None and new_val > old_val
        elif constraint == "pattern":
            # Pattern changes are generally breaking
            return True

        return False

    def get_evolution_recommendations(self, changes: Dict[str, Any]) -> List[str]:
        """
        Get recommendations for handling schema evolution.

        Args:
            changes: Schema changes analysis

        Returns:
            List of recommended actions
        """
        recommendations = []

        if changes["breaking_changes"]:
            recommendations.append(
                "⚠️  Breaking changes detected - consider versioning strategy"
            )
            recommendations.append("Consider creating a new versioned table/endpoint")
            recommendations.append("Implement data migration strategy")

        if changes["added_fields"]:
            recommendations.append(
                "✅ Added fields detected - update destination schema"
            )
            recommendations.append("Consider setting default values for new fields")

        if changes["removed_fields"]:
            recommendations.append(
                "🔴 Removed fields detected - check downstream dependencies"
            )
            recommendations.append("Archive data for removed fields if needed")

        if changes["type_changes"]:
            recommendations.append(
                "⚠️  Type changes detected - implement data transformation"
            )
            recommendations.append("Validate data compatibility")

        if not changes["breaking_changes"] and not changes["removed_fields"]:
            recommendations.append("✅ Changes appear to be backward compatible")
            recommendations.append("Safe to proceed with schema evolution")

        return recommendations

    def create_schema_migration_plan(
        self, pipeline_id: str, changes: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Create a migration plan for schema changes.

        Args:
            pipeline_id: Pipeline identifier
            changes: Schema changes analysis

        Returns:
            Migration plan dictionary
        """
        plan = {
            "pipeline_id": pipeline_id,
            "timestamp": datetime.utcnow().isoformat(),
            "changes": changes,
            "recommendations": self.get_evolution_recommendations(changes),
            "migration_steps": [],
            "rollback_steps": [],
        }

        # Generate migration steps based on changes
        if changes["added_fields"]:
            for field_info in changes["added_fields"]:
                plan["migration_steps"].append(
                    {
                        "action": "add_column",
                        "field": field_info["field"],
                        "definition": field_info["definition"],
                        "sql": f"ALTER TABLE {{table}} ADD COLUMN {field_info['field']} {{type}}",
                    }
                )

        if changes["removed_fields"]:
            for field_info in changes["removed_fields"]:
                plan["migration_steps"].append(
                    {
                        "action": "remove_column",
                        "field": field_info["field"],
                        "sql": f"ALTER TABLE {{table}} DROP COLUMN {field_info['field']}",
                    }
                )

                # Add rollback step
                plan["rollback_steps"].append(
                    {
                        "action": "add_column",
                        "field": field_info["field"],
                        "definition": field_info["old_definition"],
                        "sql": f"ALTER TABLE {{table}} ADD COLUMN {field_info['field']} {{type}}",
                    }
                )

        return plan

    def save_schema_version(
        self, pipeline_id: str, schema: Dict[str, Any], version: str = None
    ):
        """
        Save a schema version for future reference.

        Args:
            pipeline_id: Pipeline identifier
            schema: Schema to save
            version: Optional version identifier
        """
        if version is None:
            version = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        schema_record = {
            "pipeline_id": pipeline_id,
            "version": version,
            "schema": schema,
            "hash": self.generate_schema_hash(schema),
            "timestamp": datetime.utcnow().isoformat(),
        }

        # Cache the schema
        cache_key = f"{pipeline_id}_{version}"
        self.schema_cache[cache_key] = schema_record

        logger.info(f"Saved schema version {version} for pipeline {pipeline_id}")

    def get_schema_version(
        self, pipeline_id: str, version: str = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get a specific schema version.

        Args:
            pipeline_id: Pipeline identifier
            version: Version identifier (latest if None)

        Returns:
            Schema record or None if not found
        """
        if version:
            cache_key = f"{pipeline_id}_{version}"
            return self.schema_cache.get(cache_key)

        # Get latest version
        latest_version = None
        latest_timestamp = None

        for key, record in self.schema_cache.items():
            if record["pipeline_id"] == pipeline_id:
                if latest_timestamp is None or record["timestamp"] > latest_timestamp:
                    latest_version = record
                    latest_timestamp = record["timestamp"]

        return latest_version
