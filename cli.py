import sys
import pytest

def coverage_html():
    # Adjust the package path below to your package root
    args = [
        "--cov=analitiq_stream",     # change to your top-level package
        "--cov-report=term-missing",
        "--cov-report=html",         # writes htmlcov/index.html
    ]
    sys.exit(pytest.main(args))
