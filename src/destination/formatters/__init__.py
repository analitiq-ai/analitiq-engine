"""Formatters for serializing records to different output formats."""

from .base import BaseFormatter
from .jsonl import JsonLinesFormatter
from .csv import CsvFormatter
from .parquet import ParquetFormatter

__all__ = [
    "BaseFormatter",
    "JsonLinesFormatter",
    "CsvFormatter",
    "ParquetFormatter",
    "get_formatter",
]


def get_formatter(format_type: str) -> BaseFormatter:
    """
    Factory function to get formatter by format type.

    Args:
        format_type: Format type (jsonl, csv, parquet)

    Returns:
        Instance of the appropriate formatter

    Raises:
        ValueError: If format type is not supported
    """
    formatters = {
        "jsonl": JsonLinesFormatter,
        "json": JsonLinesFormatter,  # Alias
        "csv": CsvFormatter,
        "parquet": ParquetFormatter,
    }

    formatter_class = formatters.get(format_type.lower())
    if formatter_class is None:
        supported = ", ".join(sorted(formatters.keys()))
        raise ValueError(
            f"Unsupported format type: {format_type}. "
            f"Supported types: {supported}"
        )

    return formatter_class()
