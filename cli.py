#!/usr/bin/env python3
"""
Analitiq Stream CLI.

Commands:
    run       Run a pipeline
    sync      Sync connectors from repository
    coverage  Run tests with coverage reporting
"""

import argparse
import asyncio
import sys
from pathlib import Path


def cmd_run(args: argparse.Namespace) -> int:
    """Run a pipeline."""
    from src.runner import run_pipeline

    success = asyncio.run(
        run_pipeline(
            pipeline_id=args.id,
            pipeline_path=args.pipeline,
            config_mount=args.config,
        )
    )
    return 0 if success else 1


def cmd_sync(args: argparse.Namespace) -> int:
    """Sync connectors from repository."""
    from src.config import ConnectorSync

    try:
        syncer = ConnectorSync(
            config_path=args.config_file,
            base_path=Path.cwd() if not args.base_path else Path(args.base_path),
        )

        if args.status:
            # Just show status
            status = syncer.check_status()
            for name, info in status.items():
                local_v = info.get("local_version", "not installed")
                needs_update = info.get("needs_update", False)
                update_marker = " (update available)" if needs_update else ""
                print(f"  {name}: v{local_v}{update_marker}")
            return 0
        else:
            # Sync connectors
            results = syncer.sync_all()
            for name, result in results.items():
                print(f"  {name}: {result}")
            return 0

    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def cmd_coverage(args: argparse.Namespace) -> int:
    """Run tests with coverage reporting."""
    import pytest

    pytest_args = [
        "--cov=src",
        "--cov-report=term-missing",
        "--cov-report=html",
    ]
    return pytest.main(pytest_args)


def main() -> int:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Analitiq Stream CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # run command
    run_parser = subparsers.add_parser(
        "run",
        help="Run a pipeline",
        description="Run an Analitiq Stream pipeline",
    )
    run_parser.add_argument(
        "--pipeline", "-p",
        type=str,
        help="Path to pipeline JSON file",
    )
    run_parser.add_argument(
        "--id",
        type=str,
        help="Pipeline ID (UUID)",
    )
    run_parser.add_argument(
        "--config", "-c",
        type=str,
        help="Path to configuration directory",
    )
    run_parser.set_defaults(func=cmd_run)

    # sync command
    sync_parser = subparsers.add_parser(
        "sync",
        help="Sync connectors from repository",
        description="Download or update connectors from the configured repository",
    )
    sync_parser.add_argument(
        "--config-file",
        type=str,
        help="Path to analitiq.yaml configuration file",
    )
    sync_parser.add_argument(
        "--base-path",
        type=str,
        help="Base path for resolving relative paths",
    )
    sync_parser.add_argument(
        "--status",
        action="store_true",
        help="Show connector status without syncing",
    )
    sync_parser.set_defaults(func=cmd_sync)

    # coverage command
    coverage_parser = subparsers.add_parser(
        "coverage",
        help="Run tests with coverage reporting",
    )
    coverage_parser.set_defaults(func=cmd_coverage)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
