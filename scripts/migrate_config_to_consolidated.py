#!/usr/bin/env python3
"""
Migrate config/ directory from multi-file structure to consolidated pipeline files.

Creates consolidated files in pipelines/{pipeline_id}.json
Moves .secrets/ to remain at the same level.

Usage:
    python scripts/migrate_config_to_consolidated.py [--dry-run]
"""

import argparse
import json
import shutil
from pathlib import Path
from typing import Any, Dict, List, Set


def load_all_json(directory: Path) -> List[Dict[str, Any]]:
    """Load all JSON files from a directory."""
    items = []
    if directory.exists():
        for json_file in directory.glob("*.json"):
            with open(json_file, "r") as f:
                items.append(json.load(f))
    return items


def extract_connection_ids(pipeline: Dict[str, Any]) -> Set[str]:
    """Extract all connection IDs from a pipeline config."""
    connection_ids = set()
    connections = pipeline.get("connections", {})

    # Source connections
    source = connections.get("source", {})
    for alias, conn_id in source.items():
        if isinstance(conn_id, str):
            connection_ids.add(conn_id)

    # Destination connections
    destinations = connections.get("destinations", [])
    for dest_dict in destinations:
        for alias, conn_id in dest_dict.items():
            if isinstance(conn_id, str):
                connection_ids.add(conn_id)

    return connection_ids


def extract_endpoint_ids(streams: List[Dict[str, Any]]) -> Set[str]:
    """Extract all endpoint IDs from stream configs."""
    endpoint_ids = set()

    for stream in streams:
        # Source endpoint
        source = stream.get("source", {})
        if source.get("endpoint_id"):
            endpoint_ids.add(source["endpoint_id"])

        # Destination endpoints
        destinations = stream.get("destinations", [])
        for dest in destinations:
            if dest.get("endpoint_id"):
                endpoint_ids.add(dest["endpoint_id"])

    return endpoint_ids


def migrate_config(config_dir: Path, output_dir: Path, dry_run: bool = False) -> None:
    """
    Migrate config directory to consolidated format.

    Args:
        config_dir: Source config directory (e.g., ./config)
        output_dir: Output directory for consolidated files (e.g., ./pipelines)
        dry_run: If True, only print what would be done
    """
    print(f"Migrating config from {config_dir} to {output_dir}")
    print(f"Dry run: {dry_run}")
    print()

    # Load all entities into memory
    pipelines = load_all_json(config_dir / "pipelines")
    streams = load_all_json(config_dir / "streams")
    connections = load_all_json(config_dir / "connections")
    connectors = load_all_json(config_dir / "connectors")
    endpoints = load_all_json(config_dir / "endpoints")

    print(f"Loaded:")
    print(f"  {len(pipelines)} pipelines")
    print(f"  {len(streams)} streams")
    print(f"  {len(connections)} connections")
    print(f"  {len(connectors)} connectors")
    print(f"  {len(endpoints)} endpoints")
    print()

    # Build lookup maps
    streams_by_pipeline = {}
    for stream in streams:
        pipeline_id = stream.get("pipeline_id")
        if pipeline_id:
            if pipeline_id not in streams_by_pipeline:
                streams_by_pipeline[pipeline_id] = []
            streams_by_pipeline[pipeline_id].append(stream)

    connections_map = {c["connection_id"]: c for c in connections if c.get("connection_id")}
    connectors_map = {c["connector_id"]: c for c in connectors if c.get("connector_id")}
    endpoints_map = {e["endpoint_id"]: e for e in endpoints if e.get("endpoint_id")}

    # Create output directory
    if not dry_run:
        output_dir.mkdir(parents=True, exist_ok=True)

    # Create consolidated file for each pipeline
    for pipeline in pipelines:
        pipeline_id = pipeline.get("pipeline_id")
        if not pipeline_id:
            print(f"Skipping pipeline without pipeline_id")
            continue

        print(f"Processing pipeline: {pipeline_id} ({pipeline.get('name', 'unnamed')})")

        # Collect related streams
        pipeline_streams = streams_by_pipeline.get(pipeline_id, [])
        print(f"  Streams: {len(pipeline_streams)}")

        # Collect related connections
        pipeline_connection_ids = extract_connection_ids(pipeline)
        pipeline_connections = [
            connections_map[cid]
            for cid in pipeline_connection_ids
            if cid in connections_map
        ]
        print(f"  Connections: {len(pipeline_connections)}")

        # Collect related endpoints
        pipeline_endpoint_ids = extract_endpoint_ids(pipeline_streams)
        pipeline_endpoints = [
            endpoints_map[eid]
            for eid in pipeline_endpoint_ids
            if eid in endpoints_map
        ]
        print(f"  Endpoints: {len(pipeline_endpoints)}")

        # Collect related connectors
        pipeline_connector_ids = {
            c.get("connector_id")
            for c in pipeline_connections
            if c.get("connector_id")
        }
        pipeline_connectors = [
            connectors_map[cid]
            for cid in pipeline_connector_ids
            if cid in connectors_map
        ]
        print(f"  Connectors: {len(pipeline_connectors)}")

        # Build consolidated structure
        consolidated = {
            "pipeline": pipeline,
            "connections": pipeline_connections,
            "connectors": pipeline_connectors,
            "endpoints": pipeline_endpoints,
            "streams": pipeline_streams,
        }

        # Write consolidated file
        output_path = output_dir / f"{pipeline_id}.json"
        if dry_run:
            print(f"  Would write: {output_path}")
        else:
            with open(output_path, "w") as f:
                json.dump(consolidated, f, indent=2)
            print(f"  Wrote: {output_path}")

        print()

    # Copy secrets directory
    secrets_source = config_dir / ".secrets"
    secrets_dest = output_dir.parent / ".secrets"

    if secrets_source.exists():
        print(f"Copying secrets from {secrets_source} to {secrets_dest}")
        if dry_run:
            print(f"  Would copy .secrets directory")
        else:
            if secrets_dest.exists():
                shutil.rmtree(secrets_dest)
            shutil.copytree(secrets_source, secrets_dest)
            print(f"  Copied .secrets directory")

    print()
    print("Migration complete!")

    if not dry_run:
        print()
        print("Next steps:")
        print("1. Verify the consolidated files in ./pipelines/")
        print("2. Remove the old config directories if everything looks good:")
        print("   rm -rf config/connections config/connectors config/endpoints config/streams")
        print("   mv config/pipelines/*.json pipelines/")
        print("   rm -rf config/pipelines")


def main():
    parser = argparse.ArgumentParser(
        description="Migrate config to consolidated pipeline files"
    )
    parser.add_argument(
        "--config-dir",
        type=Path,
        default=Path("config"),
        help="Source config directory (default: config)"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("pipelines"),
        help="Output directory for consolidated files (default: pipelines)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print what would be done, don't write files"
    )

    args = parser.parse_args()
    migrate_config(args.config_dir, args.output_dir, args.dry_run)


if __name__ == "__main__":
    main()
