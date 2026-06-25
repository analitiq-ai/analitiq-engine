"""Centralized run_id generation and management."""

import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

RUN_ID_VAR = "RUN_ID"


def _generate_run_id() -> str:
    """Generate a new run_id."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{timestamp}-{uuid.uuid4().hex[:8]}"


def get_or_generate_run_id() -> str:
    """Get existing run_id or generate new one."""
    # The control plane sets RUN_ID from whatever scheduler it uses; fall
    # back to a locally generated id only when it is unset or blank.
    existing = os.environ.get(RUN_ID_VAR, "").strip()
    if existing:
        return existing

    return _generate_run_id()


def initialize_run_id() -> str:
    """
    Initialize run_id and set RUN_ID env var. Call ONCE at startup.

    Always sets RUN_ID even if it exists but is empty/whitespace.
    Only logs if actually initializing (not if already set).
    """
    existing = os.environ.get(RUN_ID_VAR, "").strip()
    if existing:
        return existing  # Already initialized, no logging

    run_id = get_or_generate_run_id()
    os.environ[RUN_ID_VAR] = run_id
    logger.info(f"RUN_ID: {run_id}")
    return run_id


def get_run_id() -> Optional[str]:
    """Get current run_id from environment."""
    return os.environ.get(RUN_ID_VAR)
