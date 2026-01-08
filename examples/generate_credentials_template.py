#!/usr/bin/env python3
"""
Credentials Template Generator

This script helps generate template credentials files for Analitiq Stream pipelines.
"""

import json
import logging
import sys
from pathlib import Path

# Add the parent directory to the path so we can import src
sys.path.insert(0, str(Path(__file__).parent.parent))

from src import credentials_manager

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def generate_database_template(output_path: str = "src_credentials.json"):
    """Generate database credentials template."""
    try:
        credentials_manager.create_template_credentials("database", output_path)
        logger.info(f"Database credentials template created at: {output_path}")
        logger.info(f"Please edit {output_path} with your actual database credentials.")
        logger.info(
            "Remember to set environment variables for sensitive values like passwords."
        )
    except Exception as e:
        logger.error(f"Error creating database template: {e}")


def generate_api_template(output_path: str = "dst_credentials.json"):
    """Generate API credentials template."""
    try:
        credentials_manager.create_template_credentials("api", output_path)
        logger.info(f"API credentials template created at: {output_path}")
        logger.info(f"Please edit {output_path} with your actual API credentials.")
        logger.info(
            "Remember to set environment variables for sensitive values like tokens."
        )
    except Exception as e:
        logger.error(f"Error creating API template: {e}")


def show_usage():
    """Show usage information."""
    logger.info(
        "Usage: python generate_credentials_template.py [command] [output_path]"
    )
    logger.info("")
    logger.info("Commands:")
    logger.info("  database [path]    Generate database credentials template")
    logger.info("  api [path]         Generate API credentials template")
    logger.info("  both               Generate both templates with default names")
    logger.info("  help               Show this help message")
    logger.info("")
    logger.info("Examples:")
    logger.info("  python generate_credentials_template.py database my_db_creds.json")
    logger.info("  python generate_credentials_template.py api my_api_creds.json")
    logger.info("  python generate_credentials_template.py both")


def main():
    """Main function."""
    if len(sys.argv) < 2:
        show_usage()
        return

    command = sys.argv[1].lower()

    if command == "help":
        show_usage()
    elif command == "database":
        output_path = sys.argv[2] if len(sys.argv) > 2 else "src_credentials.json"
        generate_database_template(output_path)
    elif command == "api":
        output_path = sys.argv[2] if len(sys.argv) > 2 else "dst_credentials.json"
        generate_api_template(output_path)
    elif command == "both":
        generate_database_template("src_credentials.json")
        generate_api_template("dst_credentials.json")
        logger.info("\nBoth templates created successfully!")
        logger.info("src_credentials.json - Database credentials template")
        logger.info("dst_credentials.json - API credentials template")
    else:
        logger.error(f"Unknown command: {command}")
        show_usage()


if __name__ == "__main__":
    main()
