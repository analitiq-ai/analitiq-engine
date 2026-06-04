"""Tests for worker spawn/supervision helpers.

``redact`` and ``_clean_env`` are the two functions standing between the
worker and a credential leak (stderr echo of a DSN; the shell's environment
crossing into the untrusted child) — a regression in either silently
re-opens the leak, so both are pinned here as pure-function tests.
``_wait_ready`` covers the spawn-failure paths: a worker that dies before
binding and one that never binds.
"""

from __future__ import annotations

import asyncio
import os
from types import SimpleNamespace

import pytest

from src.worker.spawn import WorkerHandle, _clean_env, _wait_ready, redact


class TestRedact:
    def test_masks_dsn_password(self):
        line = "connecting to postgresql://user:hunter2@db.example.com:5432/app"
        assert redact(line) == (
            "connecting to postgresql://user:***@db.example.com:5432/app"
        )
        assert "hunter2" not in redact(line)

    def test_masks_every_dsn_in_the_line(self):
        line = "mysql://a:pw1@h1 then postgresql://b:pw2@h2"
        out = redact(line)
        assert "pw1" not in out and "pw2" not in out
        assert out == "mysql://a:***@h1 then postgresql://b:***@h2"

    def test_line_without_credentials_unchanged(self):
        line = "worker ready on unix:/tmp/w/worker.sock"
        assert redact(line) == line

    def test_dsn_without_password_unchanged(self):
        line = "dsn is postgresql://db.example.com:5432/app"
        assert redact(line) == line


class TestCleanEnv:
    def test_shell_environment_does_not_cross(self, monkeypatch):
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "shh")
        monkeypatch.setenv("PIPELINE_ID", "p1")
        env = _clean_env()
        assert "AWS_SECRET_ACCESS_KEY" not in env
        assert "PIPELINE_ID" not in env

    def test_only_interpreter_needs_pass_through(self, monkeypatch):
        monkeypatch.delenv("PYTHONPATH", raising=False)
        monkeypatch.delenv("PYTHONUSERBASE", raising=False)
        env = _clean_env()
        assert set(env) == {"PATH", "HOME", "LANG", "PYTHONUNBUFFERED"}

    def test_pythonpath_and_userbase_forwarded_when_set(self, monkeypatch):
        monkeypatch.setenv("PYTHONPATH", "/app:/app/cdk")
        monkeypatch.setenv("PYTHONUSERBASE", "/data/pyuser")
        env = _clean_env()
        assert env["PYTHONPATH"] == "/app:/app/cdk"
        assert env["PYTHONUSERBASE"] == "/data/pyuser"


class _ExitedProcess(SimpleNamespace):
    """A process that has already exited (returncode set)."""

    def __init__(self, returncode: int = 0):
        super().__init__(returncode=returncode, pid=99999)

    async def wait(self) -> int:
        return self.returncode


class TestWaitReady:
    async def test_raises_when_worker_exits_before_ready(self, tmp_path):
        proc = _ExitedProcess(returncode=3)
        with pytest.raises(RuntimeError, match=r"exited before becoming ready \(exit=3\)"):
            await _wait_ready(str(tmp_path / "worker.sock"), proc, timeout=5)

    async def test_raises_timeout_when_socket_never_appears(self, tmp_path):
        proc = SimpleNamespace(returncode=None, pid=99999)
        with pytest.raises(TimeoutError, match="not ready within"):
            await _wait_ready(str(tmp_path / "worker.sock"), proc, timeout=0.3)


class TestWorkerHandleClose:
    async def test_close_on_exited_process_cleans_workdir(self, tmp_path):
        workdir = tmp_path / "analitiq-worker-x"
        workdir.mkdir()
        (workdir / "worker.sock").touch()

        async def _forwarder():
            await asyncio.sleep(3600)

        stderr_task = asyncio.create_task(_forwarder())
        handle = WorkerHandle(
            process=_ExitedProcess(returncode=0),
            uds_path=str(workdir / "worker.sock"),
            workdir=str(workdir),
            label="test-worker",
            _stderr_task=stderr_task,
        )
        await handle.close()
        assert not workdir.exists()
        assert stderr_task.cancelled()
