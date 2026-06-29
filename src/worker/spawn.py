"""Spawn and supervise connector workers (the trusted-shell side).

A worker is a subprocess of its shell: same image, own session, clean
environment, resource limits, and a one-shot bootstrap over stdin. The
shell talks to it over a Unix domain socket in a per-worker 0700 directory
— no exposed port; filesystem permissions are the access control.

Supervision guarantees:

* the bootstrap (which carries resolved credentials) crosses the stdin
  pipe once and is never logged;
* worker stderr is forwarded line-by-line through a redactor that masks
  DSN-shaped credentials, so a driver echoing its connection string cannot
  leak a password into the shell's logs;
* resource limits are applied in the child before exec (no core dumps,
  bounded fds, optional address-space cap via WORKER_RLIMIT_AS_MB);
* shutdown terminates the worker's whole process group, escalating
  SIGTERM -> SIGKILL after a grace period, and removes the socket dir.

A worker crash surfaces as a channel error on the shell's next call —
classified retryable by the engine — and never takes the shell down.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import resource
import shutil
import signal
import sys
import tempfile
from dataclasses import dataclass
from typing import Any

import grpc
from src.config import settings

logger = logging.getLogger(__name__)

# Seconds to wait for the worker's socket to accept (covers the worker's
# connector connect(), which for a destination runs before the bind).
DEFAULT_READY_TIMEOUT = settings.worker_ready_timeout_seconds()
# Grace between SIGTERM and SIGKILL on close.
DEFAULT_KILL_GRACE = settings.worker_kill_grace_seconds()

# DSN-shaped credentials: scheme://user:password@host -> scheme://user:***@host
_DSN_CREDENTIALS = re.compile(r"(://[^:/@\s]+:)[^@\s]+(@)")


def redact(line: str) -> str:
    """Mask credential-shaped substrings in a worker log line."""
    return _DSN_CREDENTIALS.sub(r"\1***\2", line)


def _child_limits() -> None:
    """Resource limits applied in the child before exec."""
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    cap = min(1024, hard if hard != resource.RLIM_INFINITY else 1024)
    resource.setrlimit(resource.RLIMIT_NOFILE, (cap, cap))
    as_mb = settings.worker_rlimit_as_mb()
    if as_mb:
        limit = as_mb * 1024 * 1024
        resource.setrlimit(resource.RLIMIT_AS, (limit, limit))


def _clean_env() -> dict[str, str]:
    """Minimal child environment: interpreter needs, nothing of the shell's."""
    env = {
        "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
        "HOME": os.environ.get("HOME", "/tmp"),  # nosec B108
        "LANG": os.environ.get("LANG", "C.UTF-8"),
        "PYTHONUNBUFFERED": "1",
    }
    if os.environ.get("PYTHONPATH"):
        env["PYTHONPATH"] = os.environ["PYTHONPATH"]
    # pip --user installs (attach-time connector packages) live under
    # PYTHONUSERBASE/HOME; HOME above already covers the default.
    if os.environ.get("PYTHONUSERBASE"):
        env["PYTHONUSERBASE"] = os.environ["PYTHONUSERBASE"]
    return env


@dataclass
class WorkerHandle:
    """A live worker: its process, socket target, and lifecycle."""

    process: asyncio.subprocess.Process
    uds_path: str
    workdir: str
    label: str
    _stderr_task: asyncio.Task | None = None

    @property
    def target(self) -> str:
        return f"unix:{self.uds_path}"

    async def close(self, grace: float = DEFAULT_KILL_GRACE) -> None:
        """Terminate the worker's process group and clean its socket dir."""
        proc = self.process
        if proc.returncode is None:
            try:
                os.killpg(proc.pid, signal.SIGTERM)
            except ProcessLookupError:
                # The group exited between the returncode check and the
                # signal — already dead is the goal state.
                pass
            try:
                await asyncio.wait_for(proc.wait(), timeout=grace)
            except asyncio.TimeoutError:
                logger.warning(
                    "worker %s did not exit within %.0fs; killing", self.label, grace
                )
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    # Exited during the grace window; nothing left to kill.
                    pass
                await proc.wait()
        if self._stderr_task is not None:
            self._stderr_task.cancel()
            try:
                await self._stderr_task
            except asyncio.CancelledError:
                pass  # expected after cancel()
            except Exception:
                logger.debug(
                    "worker %s stderr forwarder failed during close",
                    self.label,
                    exc_info=True,
                )
        shutil.rmtree(self.workdir, ignore_errors=True)
        logger.info("worker %s closed (exit=%s)", self.label, proc.returncode)


async def _forward_stderr(handle_label: str, stream: asyncio.StreamReader) -> None:
    while True:
        line = await stream.readline()
        if not line:
            return
        logger.info(
            "[%s] %s", handle_label, redact(line.decode(errors="replace").rstrip())
        )


async def _wait_ready(
    uds_path: str, proc: asyncio.subprocess.Process, timeout: float
) -> None:
    """Wait until the worker's socket accepts a gRPC channel, or it died."""
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        if proc.returncode is not None:
            raise RuntimeError(
                f"worker exited before becoming ready (exit={proc.returncode})"
            )
        if os.path.exists(uds_path):
            channel = grpc.aio.insecure_channel(f"unix:{uds_path}")
            try:
                await asyncio.wait_for(channel.channel_ready(), timeout=5)
                return
            except asyncio.TimeoutError:
                # Socket exists but isn't accepting yet; keep polling
                # until the overall deadline expires.
                pass
            finally:
                await channel.close()
        if asyncio.get_event_loop().time() > deadline:
            raise TimeoutError(
                f"worker socket {uds_path} not ready within {timeout:.0f}s"
            )
        await asyncio.sleep(0.2)


async def spawn_worker(
    bootstrap: dict[str, Any],
    *,
    label: str,
    ready_timeout: float = DEFAULT_READY_TIMEOUT,
) -> WorkerHandle:
    """Launch a connector worker and wait for its socket to accept.

    ``bootstrap`` is completed with the worker's ``uds_path`` (a fresh
    0700 directory) and written once to the child's stdin, which is then
    closed. The returned handle owns the process; callers must ``close()``
    it.
    """
    workdir = tempfile.mkdtemp(prefix="analitiq-worker-")
    os.chmod(workdir, 0o700)
    uds_path = os.path.join(workdir, "worker.sock")
    payload = dict(bootstrap)
    payload["uds_path"] = uds_path

    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "src.worker",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
        env=_clean_env(),
        cwd=os.getcwd(),
        start_new_session=True,
        preexec_fn=_child_limits,
    )
    if proc.stdin is None or proc.stderr is None:
        raise RuntimeError("worker subprocess started without stdin/stderr pipes")
    stdin = proc.stdin
    stderr_task = asyncio.create_task(_forward_stderr(label, proc.stderr))
    handle = WorkerHandle(
        process=proc,
        uds_path=uds_path,
        workdir=workdir,
        label=label,
        _stderr_task=stderr_task,
    )
    try:
        # One-shot bootstrap: write, close. Never logged.
        stdin.write(json.dumps(payload).encode())
        await stdin.drain()
        stdin.close()
        await _wait_ready(uds_path, proc, ready_timeout)
    except BaseException:
        await handle.close(grace=2)
        raise
    logger.info("worker %s ready on %s", label, handle.target)
    return handle
