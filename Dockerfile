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
# These directories match paths defined in analitiq.yaml (single source of truth)
RUN mkdir -p \
    /app/connectors \
    /app/connections \
    /app/streams \
    /app/pipelines \
    /app/.secrets \
    /app/state \
    /app/logs \
    /app/deadletter \
    /app/metrics && \
    chown -R appuser:appuser /app/connectors /app/connections /app/streams /app/pipelines /app/.secrets /app/state /app/logs /app/deadletter /app/metrics

# Default environment variables (can be overridden at runtime)
# Note: Config paths are determined by analitiq.yaml, not environment variables
ENV ENV=local \
    PIPELINE_ID="" \
    AWS_REGION=eu-central-1 \
    PYTHONPATH=/app \
    PIPELINES_TABLE=pipelines \
    CONNECTIONS_TABLE=connections \
    CONNECTORS_TABLE=connectors \
    ENDPOINTS_TABLE=connectors_endpoints \
    STREAMS_TABLE=streams \
    ROW_COUNT_BUCKET=analitiq-client-pipeline-row-count

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "from src.core.pipeline_config_prep import PipelineConfigPrep; print('Health check passed')" || exit 1

# Switch to non-root user
USER appuser

# Create a generic entrypoint script that can run any pipeline
COPY --chown=appuser:appuser docker/entrypoint.py /app/entrypoint.py
RUN chmod +x /app/entrypoint.py

# Default entrypoint
ENTRYPOINT ["python", "/app/entrypoint.py"]

# Labels for metadata
LABEL org.opencontainers.image.title="Analitiq Stream" \
      org.opencontainers.image.description="High-performance, fault-tolerant data streaming framework" \
      org.opencontainers.image.version="0.1.0" \
      org.opencontainers.image.vendor="Analitiq" \
      org.opencontainers.image.source="https://github.com/analitiq/analitiq-stream"