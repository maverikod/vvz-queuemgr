"""
Tests for in-memory terminal job retention and related queue API behavior.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta
from unittest.mock import Mock

import pytest

from queuemgr.core.registry import InMemoryRegistry
from queuemgr.core.types import JobCommand, JobStatus
from queuemgr.exceptions import JobNotFoundError
from queuemgr.jobs.base import QueueJobBase
from queuemgr.process_commands import process_command
from queuemgr.process_config import ProcessManagerConfig
from queuemgr.queue.job_queue import JobQueue
from queuemgr.queue.terminal_status import derive_command_success_fields


class _QuickJob(QueueJobBase):
    """Minimal job that finishes immediately."""

    def execute(self) -> None:
        """No-op work."""
        return None


class _InnerFailJob(QueueJobBase):
    """Job that completes with a failed inner command payload."""

    def execute(self) -> None:
        """Set a nested failure result then return."""
        self.set_result(
            {"command": {"result": {"success": False, "error": "inner bad"}}}
        )


class LogEchoJobForTests(QueueJobBase):
    """Job that writes a line to stdout for log retention checks."""

    def execute(self) -> None:
        """Print one line."""
        print("retention-log-line")


class TestDeriveCommandSuccess:
    """Unit tests for derive_command_success_fields."""

    def test_nested_command_result_false(self) -> None:
        """Nested command result success=false is surfaced."""
        payload = {"command": {"result": {"success": False, "error": "e"}}}
        extra = derive_command_success_fields(payload)
        assert extra["command_success"] is False
        assert extra["inner_success"] is False
        assert extra["completed_with_error"] is True
        assert "command_error_summary" in extra

    def test_stopped_outer_status_overrides_inner_success_true(self) -> None:
        """STOPPED outer status must not expose successful inner command result."""
        payload = {"command": {"result": {"success": True}}}
        extra = derive_command_success_fields(payload, outer_status=JobStatus.STOPPED)
        assert extra["command_success"] is False
        assert extra["inner_success"] is False
        assert extra["completed_with_error"] is True


class TestTerminalJobRetention:
    """Integration-style tests for terminal retention and cleanup."""

    def test_completed_job_still_listed_and_logged(self) -> None:
        """Completed jobs stay visible with status filter and logs."""
        registry = InMemoryRegistry()
        queue = JobQueue(registry, terminal_job_retention_seconds=3600.0)
        queue.add_job(LogEchoJobForTests, "log-job", {})
        queue.start_job("log-job")
        deadline = time.time() + 10.0
        while time.time() < deadline:
            if queue.get_job_status("log-job").status == JobStatus.COMPLETED:
                break
            time.sleep(0.05)
        assert queue.get_job_status("log-job").status == JobStatus.COMPLETED
        logs = queue.get_job_logs("log-job")
        assert any("retention-log-line" in line for line in logs["stdout"])
        listed = queue.list_jobs(status_filter="completed")
        assert any(j["job_id"] == "log-job" for j in listed)

    def test_process_command_get_job_status_command_success_false(self) -> None:
        """IPC-style status dict exposes command_success for inner failures."""
        registry = InMemoryRegistry()
        queue = JobQueue(registry)
        queue.add_job(_InnerFailJob, "fail-inner", {})
        queue.start_job("fail-inner")
        deadline = time.time() + 10.0
        while time.time() < deadline:
            if queue.get_job_status("fail-inner").status == JobStatus.COMPLETED:
                break
            time.sleep(0.05)
        cfg = ProcessManagerConfig()
        payload = process_command(
            queue, "get_job_status", {"job_id": "fail-inner"}, cfg
        )
        assert isinstance(payload, dict)
        assert payload.get("command_success") is False
        assert payload.get("completed_with_error") is True
        assert payload.get("job_type") == "_InnerFailJob"

    def test_ttl_cleanup_removes_terminal_job(self) -> None:
        """Very short TTL removes terminal jobs after cleanup."""
        registry = InMemoryRegistry()
        queue = JobQueue(registry, completed_job_retention_seconds=0.05)
        queue.add_job(_QuickJob, "ttl-j", {})
        queue.start_job("ttl-j")
        deadline = time.time() + 10.0
        while time.time() < deadline:
            if queue.get_job_status("ttl-j").status == JobStatus.COMPLETED:
                break
            time.sleep(0.05)
        time.sleep(0.12)
        removed = queue.cleanup_completed_jobs()
        assert removed >= 1
        with pytest.raises(JobNotFoundError):
            queue.get_job_status("ttl-j")

    def test_max_retained_terminal_jobs_drops_oldest(self) -> None:
        """When over max_retained_terminal_jobs, oldest terminals are purged."""
        registry = InMemoryRegistry()
        queue = JobQueue(
            registry,
            max_retained_terminal_jobs=2,
            terminal_job_retention_seconds=86400.0,
        )
        base = datetime.now() - timedelta(hours=4)
        done_snapshot = {
            "status": JobStatus.COMPLETED,
            "command": JobCommand.NONE,
            "progress": 100,
            "description": "",
            "result": None,
        }
        for idx in range(4):
            jid = f"max-{idx}"
            queue.add_job(_QuickJob, jid, {})
            job = queue._jobs[jid]
            job.get_status = Mock(  # type: ignore[method-assign]
                return_value=done_snapshot,
            )
            queue._job_terminal_at[jid] = base + timedelta(minutes=idx)

        queue.cleanup_completed_jobs()
        remaining = set(queue._jobs.keys())
        assert remaining == {"max-2", "max-3"}

    def test_max_retained_does_not_remove_pending(self) -> None:
        """Cleanup must not drop pending jobs when trimming terminals."""
        registry = InMemoryRegistry()
        queue = JobQueue(
            registry,
            max_retained_terminal_jobs=1,
            terminal_job_retention_seconds=86400.0,
        )
        queue.add_job(_QuickJob, "pend", {})
        queue.add_job(_QuickJob, "old-done", {})
        old = queue._jobs["old-done"]
        old.get_status = Mock(  # type: ignore[method-assign]
            return_value={
                "status": JobStatus.COMPLETED,
                "command": JobCommand.NONE,
                "progress": 100,
                "description": "",
                "result": None,
            }
        )
        queue._job_terminal_at["old-done"] = datetime.now() - timedelta(hours=1)

        queue.cleanup_completed_jobs()
        assert "pend" in queue._jobs
        assert queue.get_job_status("pend").status == JobStatus.PENDING

    def test_soft_delete_retains_until_cleanup(self) -> None:
        """delete_job marks DELETED; entry survives until TTL cleanup."""
        registry = InMemoryRegistry()
        queue = JobQueue(registry, completed_job_retention_seconds=0.05)
        queue.add_job(_QuickJob, "del-j", {})
        queue.delete_job("del-j", force=True)
        st = queue.get_job_status("del-j")
        assert st.status == JobStatus.DELETED
        time.sleep(0.12)
        queue.cleanup_completed_jobs()
        with pytest.raises(JobNotFoundError):
            queue.get_job_status("del-j")
