# Single-stage build for Python 3.11+ application
FROM python:3.11-slim

# Set environment variables for Python
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies required for building Python packages
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN groupadd --gid 1000 appuser && \
    useradd --uid 1000 --gid appuser --shell /bin/bash --create-home appuser

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies first (for better caching)
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY --chown=appuser:appuser . .

# Create required directories with proper permissions
# Config directories (pipelines, connectors, connections) are volume-mounted at runtime.
# These directories are created as fallbacks and for runtime data.
RUN mkdir -p \
    /app/pipelines \
    /app/connectors \
    /app/connections \
    /app/state \
    /app/logs \
    /app/deadletter \
    /app/metrics && \
    chown -R appuser:appuser /app/pipelines /app/connectors /app/connections /app/state /app/logs /app/deadletter /app/metrics

# Default environment variables (can be overridden at runtime)
# Config paths are discovered via convention (pipelines/manifest.json at project root).
#
# RUN_MODE determines container behavior:
#   - "engine" (default): Runs the data pipeline engine
#   - "destination": Runs the gRPC destination server
ENV ENV=local \
    RUN_MODE=engine \
    PIPELINE_ID="" \
    AWS_REGION=eu-central-1 \
    PYTHONPATH=/app \
    ROW_COUNT_BUCKET=analitiq-client-pipeline-row-count \
    GRPC_PORT=50051 \
    DESTINATION_GRPC_HOST="" \
    DESTINATION_GRPC_PORT=50051

# Health check - works for both modes
# For destination mode, this verifies the gRPC module is available
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "from src.main import main; print('Health check passed')" || exit 1

# Switch to non-root user
USER appuser

# Default entrypoint uses unified main module
# RUN_MODE env var determines whether to run as engine or destination
ENTRYPOINT ["python", "-m", "src.main"]

# Labels for metadata
LABEL org.opencontainers.image.title="Analitiq Stream" \
      org.opencontainers.image.description="High-performance, fault-tolerant data streaming framework" \
      org.opencontainers.image.version="0.1.0" \
      org.opencontainers.image.vendor="Analitiq" \
      org.opencontainers.image.source="https://github.com/analitiq/analitiq-stream"