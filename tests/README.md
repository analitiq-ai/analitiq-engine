# Test Execution Guide

This document explains how to run different categories of tests in the Analitiq Stream project.

## Standard Test Execution

Run the main test suite:

```bash
poetry run pytest
```

## Test Categories

### Unit Tests
```bash
poetry run pytest tests/unit/
```

### Integration Tests
```bash
poetry run pytest tests/integration/
```

### E2E Tests
```bash
poetry run pytest tests/e2e/
```

### Core Pipeline Tests
```bash
poetry run pytest tests/core_pipeline/
```

## Fault Tolerance Tests

Fault tolerance tests (rate limiter, dead letter queue) live in
`tests/unit/analitiq_stream/fault_tolerance/` and run as part of the main
suite. To run them alone:

```bash
poetry run pytest tests/unit/analitiq_stream/fault_tolerance/
```

## Notes

- The full suite is expected to pass with `poetry run pytest`; a handful of
  tests skip when their environment is absent (live databases, optional
  dialect packages, unpopulated `connectors/` checkouts).
- CI runs the same suite plus the pre-commit stack; see `CONTRIBUTING.md`.
