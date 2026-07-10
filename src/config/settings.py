"""Central catalogue of engine defaults, overridable by environment.

Every tunable default the engine ships with lives here, once. Two kinds of
default are collected:

* **Runtime tuning** (batch size, concurrency, buffer, error handling) -- the
  per-pipeline knobs. These are layered: a pipeline's ``runtime`` block wins,
  then the matching environment variable, then the built-in default returned
  here. The overlay happens in
  :func:`src.engine.pipeline_config_prep._parse_runtime_config`; the typed
  defaults on :mod:`src.models.resolved` source their values from the
  ``default_*`` accessors below.
* **Process infrastructure** (gRPC endpoints/timeouts, worker supervision,
  schema fetching) -- engine-process knobs with no per-pipeline layer. The
  modules that own these read the matching accessor here instead of restating
  the literal and the environment-variable name.

Connector and formatter defaults are deliberately NOT here: a connector owns
its own configuration (the engine stays connector-agnostic), so those defaults
live with their connector. See ``docs/configuration.md`` for the human-facing
catalogue.

Accessors read the environment on every call rather than at import, so a value
placed in ``.env`` (loaded by the runner before config parsing) is honoured.
New runtime overrides use the ``ANALITIQ_`` prefix; the infrastructure
variables keep their established names because the deployment sets them.
"""

import os

# ---------------------------------------------------------------------------
# Internal env helpers
# ---------------------------------------------------------------------------


def _int_env(name: str, fallback: int) -> int:
    """Return ``name`` parsed as int, or ``fallback`` when unset or blank."""
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return fallback
    return int(raw)


def _float_env(name: str, fallback: float) -> float:
    """Return ``name`` parsed as float, or ``fallback`` when unset or blank."""
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return fallback
    return float(raw)


# ---------------------------------------------------------------------------
# Runtime tuning defaults (per-pipeline; pipeline config > env > default)
# ---------------------------------------------------------------------------


def default_batch_size() -> int:
    """Return records read and shipped per batch (``ANALITIQ_BATCH_SIZE``)."""
    return _int_env("ANALITIQ_BATCH_SIZE", 1000)


def default_max_concurrent_batches() -> int:
    """In-flight batches per stream (``ANALITIQ_MAX_CONCURRENT_BATCHES``)."""
    return _int_env("ANALITIQ_MAX_CONCURRENT_BATCHES", 3)


def default_buffer_size() -> int:
    """Queue depth between pipeline stages (``ANALITIQ_BUFFER_SIZE``)."""
    return _int_env("ANALITIQ_BUFFER_SIZE", 5000)


def default_error_strategy() -> str:
    """Fault policy when a batch exhausts retries (``ANALITIQ_ERROR_STRATEGY``).

    One of ``fail`` | ``dlq`` | ``skip``; validated by
    :class:`src.models.resolved.ErrorHandlingConfig`.
    """
    return os.getenv("ANALITIQ_ERROR_STRATEGY") or "fail"


def default_max_retries() -> int:
    """Retry attempts before the error strategy applies (``ANALITIQ_MAX_RETRIES``)."""
    return _int_env("ANALITIQ_MAX_RETRIES", 3)


def default_retry_delay_seconds() -> int:
    """Return the base retry backoff in seconds (``ANALITIQ_RETRY_DELAY_SECONDS``)."""
    return _int_env("ANALITIQ_RETRY_DELAY_SECONDS", 5)


# ---------------------------------------------------------------------------
# gRPC transport (engine <-> destination, and worker hops)
# ---------------------------------------------------------------------------

# Literal fallback (not env-derived) so a non-positive GRPC_TIMEOUT_SECONDS
# cannot poison the ack budget -- see grpc_ack_timeout_seconds.
_FALLBACK_GRPC_TIMEOUT = 30

# One message-size ceiling for every channel that carries Arrow batches:
# destination client/server (TCP) and the worker hops (UDS). Client and server
# limits must match or batches between the two limits are rejected with
# RESOURCE_EXHAUSTED on one side only. Not environment-tunable by design.
GRPC_MAX_MESSAGE_SIZE = 16 * 1024 * 1024  # 16MB


def grpc_destination_host() -> str:
    """Destination service host for engine mode (``DESTINATION_GRPC_HOST``).

    A set-but-blank value (baked into an image to mean "gRPC mode not
    configured") falls back to localhost rather than yielding a hostless
    ``:50051`` address; ``or`` covers both unset and blank.
    """
    return os.getenv("DESTINATION_GRPC_HOST") or "localhost"


def grpc_destination_port() -> int:
    """Destination service port for engine mode (``DESTINATION_GRPC_PORT``)."""
    return _int_env("DESTINATION_GRPC_PORT", 50051)


def grpc_server_port() -> int:
    """Port the destination gRPC server listens on (``GRPC_PORT``)."""
    return _int_env("GRPC_PORT", 50051)


def grpc_ack_timeout_seconds() -> int:
    """Engine's gRPC ack budget in seconds (``GRPC_TIMEOUT_SECONDS``).

    A non-positive value (``0`` or negative) falls back to the default rather
    than being used as-is: a zero ack budget makes the schema-ACK ``wait_for``
    fire immediately, before the destination can reply. The resolved budget is
    stamped onto the schema handshake so the destination derives its statement
    timeout from it (issues #231, #234).
    """
    raw = _int_env("GRPC_TIMEOUT_SECONDS", _FALLBACK_GRPC_TIMEOUT)
    return raw if raw > 0 else _FALLBACK_GRPC_TIMEOUT


def grpc_max_retries() -> int:
    """Retries for gRPC connect/stream operations (``MAX_RETRIES``)."""
    return _int_env("MAX_RETRIES", 3)


# ---------------------------------------------------------------------------
# Worker supervision (spawn.py)
# ---------------------------------------------------------------------------


def worker_ready_timeout_seconds() -> float:
    """Return seconds to wait for a worker socket to accept.

    Env var ``WORKER_READY_TIMEOUT_SECONDS``.
    """
    return _float_env("WORKER_READY_TIMEOUT_SECONDS", 300)


def worker_kill_grace_seconds() -> float:
    """Grace between SIGTERM and SIGKILL on close (``WORKER_KILL_GRACE_SECONDS``)."""
    return _float_env("WORKER_KILL_GRACE_SECONDS", 10)


def worker_rlimit_as_mb() -> int | None:
    """Return the optional worker address-space cap in MB, or None if unset.

    Env var ``WORKER_RLIMIT_AS_MB``.
    """
    raw = os.getenv("WORKER_RLIMIT_AS_MB")
    if raw is None or raw.strip() == "":
        return None
    return int(raw)


# ---------------------------------------------------------------------------
# Process orchestration (main.py)
# ---------------------------------------------------------------------------


def log_level() -> str:
    """Return the process logging level (``LOG_LEVEL``)."""
    return os.getenv("LOG_LEVEL", "INFO").upper()


def destination_index() -> int:
    """Which destination from the pipeline config to serve (``DESTINATION_INDEX``)."""
    return _int_env("DESTINATION_INDEX", 0)


def run_mode() -> str:
    """Process role: ``source`` or ``destination`` (``RUN_MODE``)."""
    return os.getenv("RUN_MODE", "source").lower()


# ---------------------------------------------------------------------------
# Secret resolution (s3:// secret_refs; only consulted with the [s3] extra)
# ---------------------------------------------------------------------------


def s3_secrets_endpoint_url() -> str | None:
    """S3 endpoint for ``s3://`` secret refs (``AWS_ENDPOINT_URL_S3``).

    Lets an S3-compatible store (e.g. MinIO) back ``s3://`` secret refs. Unset
    (the default) leaves ``boto3`` to use AWS's own endpoint. ``AWS_ENDPOINT_URL``
    is honoured as a fallback so a single override covers all AWS services.
    """
    return os.getenv("AWS_ENDPOINT_URL_S3") or os.getenv("AWS_ENDPOINT_URL") or None


def s3_secrets_region() -> str | None:
    """AWS region for ``s3://`` secret refs (``AWS_REGION``).

    Unset leaves ``boto3`` to resolve the region from its own config chain.
    ``AWS_DEFAULT_REGION`` is honoured as a fallback.
    """
    return os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or None
