"""
Tests for stop lifecycle, terminal consistency, and retention.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta

import pytest

from queuemgr.core.ipc import create_job_shared_state, get_manager, read_job_state
from queuemgr.exceptions import JobNotFoundError
from queuemgr.core.ipc_operations import update_job_state
from queuemgr.core.registry import JsonlRegistry
from queuemgr.core.types import JobStatus
from queuemgr.jobs.base import QueueJobBase
from queuemgr.queue.job_queue import JobQueue


class TightLoopJob(QueueJobBase):
    """Long-running work with short sleeps so STOP is observed quickly."""

    def execute(self) -> None:
        for _ in range(2000):
            time.sleep(0.005)


class TestStopLifecycle:
    """Integration-style stop tests with a real child process."""

    def test_stop_running_job_remains_stopped(self, tmp_path) -> None:
        """After stop_job, get_job_status must report STOPPED, not COMPLETED."""
        registry = JsonlRegistry(str(tmp_path / "reg.jsonl"))
        queue = JobQueue(registry=registry)
        queue.add_job(TightLoopJob, "run-stop-1", {})
        queue.start_job("run-stop-1")
        time.sleep(0.05)
        queue.stop_job("run-stop-1", timeout=15.0)
        record = queue.get_job_status("run-stop-1")
        assert record.status == JobStatus.STOPPED
        assert record.completed_at is not None

    def test_stopped_job_listed(self, tmp_path) -> None:
        """list_jobs with status_filter stopped includes the stopped job."""
        registry = JsonlRegistry(str(tmp_path / "reg2.jsonl"))
        queue = JobQueue(registry=registry)
        queue.add_job(TightLoopJob, "list-stop-1", {})
        queue.start_job("list-stop-1")
        time.sleep(0.05)
        queue.stop_job("list-stop-1", timeout=15.0)
        rows = queue.list_jobs(status_filter="stopped")
        match = [r for r in rows if r["job_id"] == "list-stop-1"]
        assert len(match) == 1
        assert match[0]["status"] == "STOPPED"

    def test_pending_stop_marks_stopped(self, tmp_path) -> None:
        """Stopping a pending job (never started) yields STOPPED."""
        registry = JsonlRegistry(str(tmp_path / "reg3.jsonl"))
        queue = JobQueue(registry=registry)
        queue.add_job(TightLoopJob, "pend-1", {})
        queue.stop_job("pend-1")
        assert queue.get_job_status("pend-1").status == JobStatus.STOPPED

    def test_retention_keeps_stopped_job_until_ttl(self, tmp_path) -> None:
        """Stopped jobs are purged only after stopped-terminal TTL."""
        registry = JsonlRegistry(str(tmp_path / "reg4.jsonl"))
        queue = JobQueue(
            registry=registry,
            stopped_terminal_retention_seconds=3600.0,
            terminal_job_retention_seconds=3600.0,
            completed_job_retention_seconds=None,
        )
        queue.add_job(TightLoopJob, "ret-1", {})
        queue.start_job("ret-1")
        time.sleep(0.05)
        queue.stop_job("ret-1", timeout=15.0)
        queue.cleanup_completed_jobs()
        assert "ret-1" in queue._jobs

    def test_retention_removes_stopped_past_ttl(self, tmp_path) -> None:
        """Stopped jobs past TTL are removed by cleanup."""
        registry = JsonlRegistry(str(tmp_path / "reg5.jsonl"))
        queue = JobQueue(
            registry=registry,
            stopped_terminal_retention_seconds=0.0,
            terminal_job_retention_seconds=3600.0,
        )
        queue.add_job(TightLoopJob, "ret-old", {})
        queue.start_job("ret-old")
        time.sleep(0.05)
        queue.stop_job("ret-old", timeout=15.0)
        past = datetime.now() - timedelta(seconds=5)
        queue._job_terminal_at["ret-old"] = past
        queue._job_completed_times["ret-old"] = past
        removed = queue.cleanup_completed_jobs()
        assert removed >= 1
        with pytest.raises(JobNotFoundError):
            queue.get_job_status("ret-old")


class TestJobBaseTerminalGuards:
    """Direct QueueJobBase terminal precedence checks."""

    def test_completion_does_not_overwrite_deleted(self) -> None:
        """_handle_completion leaves DELETED unchanged."""
        manager = get_manager()
        shared = create_job_shared_state(manager)
        job = TightLoopJob("jd", {})
        job._set_shared_state(shared)
        update_job_state(shared, status=JobStatus.DELETED)
        job._handle_completion()
        assert read_job_state(shared)["status"] == JobStatus.DELETED

    def test_error_does_not_overwrite_stopped(self) -> None:
        """_handle_error does not downgrade STOPPED to ERROR."""
        manager = get_manager()
        shared = create_job_shared_state(manager)
        job = TightLoopJob("js", {})
        job._set_shared_state(shared)
        update_job_state(shared, status=JobStatus.STOPPED)
        job._handle_error(RuntimeError("boom"))
        assert read_job_state(shared)["status"] == JobStatus.STOPPED
