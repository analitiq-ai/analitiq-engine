#!/usr/bin/env python3
"""
Analitiq Stream CLI.

Commands:
    run       Run a pipeline
    coverage  Run tests with coverage reporting
"""

import argparse
import asyncio
import sys


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
