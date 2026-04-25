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
from queuemgr.core.ipc_operations import get_command
from queuemgr.exceptions import InvalidJobStateError, JobNotFoundError
from queuemgr.core.ipc_operations import update_job_state
from queuemgr.core.registry import JsonlRegistry
from queuemgr.core.types import JobCommand, JobStatus
from queuemgr.jobs.base import QueueJobBase
from queuemgr.queue.job_queue import JobQueue


class TightLoopJob(QueueJobBase):
    """Long-running work with short sleeps so STOP is observed quickly."""

    def execute(self) -> None:
        for _ in range(2000):
            time.sleep(0.005)


class HeartbeatLongJob(QueueJobBase):
    """Deterministic long-running job with heartbeat timestamps in result."""

    def execute(self) -> None:
        total_seconds = float(self.params.get("seconds", 10.0))
        tick_seconds = float(self.params.get("tick_seconds", 0.2))
        elapsed = 0.0
        heartbeats = []
        while elapsed < total_seconds:
            if self._shared_state is not None:
                command = get_command(self._shared_state)
                if command in (JobCommand.STOP, JobCommand.DELETE):
                    self.set_result(
                        {
                            "status": "stopped",
                            "result": {
                                "success": False,
                                "data": {"slept_seconds": elapsed},
                            },
                            "heartbeats": heartbeats,
                        }
                    )
                    return
            print(f"heartbeat elapsed={elapsed:.1f}s")
            heartbeats.append(elapsed)
            self.set_result(
                {
                    "status": "running",
                    "result": {"success": None, "data": {"slept_seconds": elapsed}},
                    "heartbeats": heartbeats,
                }
            )
            time.sleep(tick_seconds)
            elapsed += tick_seconds
        self.set_result(
            {
                "status": "completed",
                "result": {"success": True, "data": {"slept_seconds": total_seconds}},
                "heartbeats": heartbeats,
            }
        )


class LogThenLoopJob(QueueJobBase):
    """Long-running job that emits stdout for log retention assertions."""

    def execute(self) -> None:
        print("stop-log-line")
        for _ in range(1500):
            time.sleep(0.005)


class NativeErrorResultJob(QueueJobBase):
    """Job that returns an error-like result payload without raising."""

    def execute(self) -> None:
        self.set_result({"status": "error", "message": "x"})


class QuickCompleteJob(QueueJobBase):
    """Simple successful job used for delete-after-complete tests."""

    def execute(self) -> None:
        return None


class SleepOnceJob(QueueJobBase):
    """Short sleep job used to validate truthful stop race semantics."""

    def execute(self) -> None:
        time.sleep(0.05)
        self.set_result(
            {"status": "completed", "result": {"success": True, "data": {"slept": 0.05}}}
        )


class TestStopLifecycle:
    """Integration-style stop tests with a real child process."""

    def test_stop_running_job_interrupts_before_natural_completion(self, tmp_path) -> None:
        """Running stop interrupts before full duration and avoids completed-success."""
        registry = JsonlRegistry(str(tmp_path / "reg-stop-interrupt.jsonl"))
        queue = JobQueue(registry=registry)
        queue.add_job(
            HeartbeatLongJob, "interrupt-1", {"seconds": 6.0, "tick_seconds": 0.2}
        )
        queue.start_job("interrupt-1")

        deadline = time.time() + 5.0
        while time.time() < deadline:
            status = queue.get_job_status("interrupt-1")
            if status.status == JobStatus.RUNNING and status.result is not None:
                break
            time.sleep(0.05)

        queue.stop_job("interrupt-1", timeout=5.0)
        final_status = queue.get_job_status("interrupt-1")
        assert final_status.status == JobStatus.STOPPED
        assert final_status.result is not None
        result_payload = final_status.result
        assert isinstance(result_payload, dict)
        assert str(result_payload.get("status", "")).lower() != "completed"
        inner = result_payload.get("result")
        if isinstance(inner, dict):
            assert inner.get("success") is not True
            slept = inner.get("data", {}).get("slept_seconds")
            if isinstance(slept, (int, float)):
                assert slept < 6.0

        logs = queue.get_job_logs("interrupt-1")
        hb_lines = [line for line in logs["stdout"] if "heartbeat elapsed=" in line]
        assert len(hb_lines) > 0
        assert not any("elapsed=6.0s" in line for line in hb_lines)

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

    def test_stopped_job_logs_retained(self, tmp_path) -> None:
        """get_job_logs remains available after stop."""
        registry = JsonlRegistry(str(tmp_path / "reg-stop-logs.jsonl"))
        queue = JobQueue(registry=registry)
        queue.add_job(LogThenLoopJob, "stop-log-1", {})
        queue.start_job("stop-log-1")
        time.sleep(0.05)
        queue.stop_job("stop-log-1", timeout=15.0)
        logs = queue.get_job_logs("stop-log-1")
        assert any("stop-log-line" in line for line in logs["stdout"])

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

    def test_stop_after_already_completed_is_truthful(self, tmp_path) -> None:
        """stop_job must not report STOPPED when job already completed naturally."""
        registry = JsonlRegistry(str(tmp_path / "reg-stop-race-truth.jsonl"))
        queue = JobQueue(registry=registry)
        queue.add_job(SleepOnceJob, "race-stop-1", {})
        queue.start_job("race-stop-1")
        time.sleep(0.2)
        with pytest.raises(InvalidJobStateError):
            queue.stop_job("race-stop-1", timeout=0.1)
        assert queue.get_job_status("race-stop-1").status == JobStatus.COMPLETED


class TestJobBaseTerminalGuards:
    """Direct QueueJobBase terminal precedence checks."""

    def test_completion_does_not_overwrite_stopped(self) -> None:
        """_handle_completion leaves STOPPED unchanged."""
        manager = get_manager()
        shared = create_job_shared_state(manager)
        job = TightLoopJob("js2", {})
        job._set_shared_state(shared)
        update_job_state(shared, status=JobStatus.STOPPED)
        job._handle_completion()
        assert read_job_state(shared)["status"] == JobStatus.STOPPED

    def test_delete_not_overwritten_by_completion(self) -> None:
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

    def test_native_result_status_error_maps_to_error(self, tmp_path) -> None:
        """Result payload status=error is mapped to JobStatus.ERROR."""
        registry = JsonlRegistry(str(tmp_path / "reg-native-error.jsonl"))
        queue = JobQueue(registry=registry)
        queue.add_job(NativeErrorResultJob, "native-err-1", {})
        queue.start_job("native-err-1")
        deadline = time.time() + 10.0
        while time.time() < deadline:
            if queue.get_job_status("native-err-1").status == JobStatus.ERROR:
                break
            time.sleep(0.05)
        assert queue.get_job_status("native-err-1").status == JobStatus.ERROR


class TestDeleteRetentionLifecycle:
    """Deleted lifecycle behavior for status/list/log APIs."""

    def test_delete_completed_job_retained(self, tmp_path) -> None:
        """Deleting a completed job keeps DELETED status while retained."""
        registry = JsonlRegistry(str(tmp_path / "reg-delete-retained.jsonl"))
        queue = JobQueue(registry=registry, completed_job_retention_seconds=3600.0)
        queue.add_job(QuickCompleteJob, "del-completed-1", {})
        queue.start_job("del-completed-1")
        deadline = time.time() + 10.0
        while time.time() < deadline:
            state = queue.get_job_status("del-completed-1").status
            if state == JobStatus.COMPLETED:
                break
            time.sleep(0.05)
        queue.delete_job("del-completed-1")
        assert queue.get_job_status("del-completed-1").status == JobStatus.DELETED

    def test_deleted_job_listed(self, tmp_path) -> None:
        """list_jobs includes DELETED jobs before retention expiry."""
        registry = JsonlRegistry(str(tmp_path / "reg-deleted-listed.jsonl"))
        queue = JobQueue(registry=registry, completed_job_retention_seconds=3600.0)
        queue.add_job(LogThenLoopJob, "del-list-1", {})
        queue.delete_job("del-list-1", force=True)
        rows = queue.list_jobs(status_filter="deleted")
        assert any(row["job_id"] == "del-list-1" for row in rows)

    def test_get_logs_after_delete(self, tmp_path) -> None:
        """Logs remain available after soft delete."""
        registry = JsonlRegistry(str(tmp_path / "reg-logs-after-delete.jsonl"))
        queue = JobQueue(registry=registry, completed_job_retention_seconds=3600.0)
        queue.add_job(LogThenLoopJob, "del-logs-1", {})
        queue.start_job("del-logs-1")
        time.sleep(0.05)
        queue.stop_job("del-logs-1", timeout=15.0)
        queue.delete_job("del-logs-1")
        logs = queue.get_job_logs("del-logs-1")
        assert any("stop-log-line" in line for line in logs["stdout"])

    def test_cleanup_respects_retention(self, tmp_path) -> None:
        """Cleanup retains terminal states until configured TTL expires."""
        registry = JsonlRegistry(str(tmp_path / "reg-cleanup-respect.jsonl"))
        queue = JobQueue(
            registry=registry,
            terminal_job_retention_seconds=3600.0,
            failed_terminal_retention_seconds=3600.0,
            stopped_terminal_retention_seconds=3600.0,
            deleted_terminal_retention_seconds=3600.0,
        )
        queue.add_job(LogThenLoopJob, "term-stop", {})
        queue.stop_job("term-stop")

        queue.add_job(NativeErrorResultJob, "term-error", {})
        queue.start_job("term-error")
        deadline = time.time() + 10.0
        while time.time() < deadline:
            if queue.get_job_status("term-error").status == JobStatus.ERROR:
                break
            time.sleep(0.05)

        queue.add_job(LogThenLoopJob, "term-delete", {})
        queue.delete_job("term-delete", force=True)

        queue.add_job(LogThenLoopJob, "term-completed", {})
        update_job_state(
            queue._jobs["term-completed"]._shared_state,
            status=JobStatus.COMPLETED,
            progress=100,
        )
        queue._job_terminal_at["term-completed"] = datetime.now()
        queue._job_completed_times["term-completed"] = datetime.now()

        removed = queue.cleanup_completed_jobs()
        assert removed == 0
        for job_id in ("term-stop", "term-error", "term-delete", "term-completed"):
            assert queue.get_job_status(job_id).status in (
                JobStatus.STOPPED,
                JobStatus.ERROR,
                JobStatus.DELETED,
                JobStatus.COMPLETED,
            )

    def test_retention_includes_completed_error_stopped_deleted(self, tmp_path) -> None:
        """Retention policy keeps all terminal kinds before expiry."""
        registry = JsonlRegistry(str(tmp_path / "reg-retention-terminals.jsonl"))
        queue = JobQueue(
            registry=registry,
            completed_job_retention_seconds=None,
            terminal_job_retention_seconds=None,
            failed_terminal_retention_seconds=None,
            stopped_terminal_retention_seconds=None,
            deleted_terminal_retention_seconds=None,
        )
        queue.add_job(QuickCompleteJob, "keep-completed", {})
        queue.start_job("keep-completed")
        deadline = time.time() + 5.0
        while time.time() < deadline:
            if queue.get_job_status("keep-completed").status == JobStatus.COMPLETED:
                break
            time.sleep(0.05)

        queue.add_job(NativeErrorResultJob, "keep-error", {})
        queue.start_job("keep-error")
        deadline = time.time() + 5.0
        while time.time() < deadline:
            if queue.get_job_status("keep-error").status == JobStatus.ERROR:
                break
            time.sleep(0.05)

        queue.add_job(LogThenLoopJob, "keep-stopped", {})
        queue.stop_job("keep-stopped")

        queue.add_job(LogThenLoopJob, "keep-deleted", {})
        queue.delete_job("keep-deleted", force=True)

        removed = queue.cleanup_completed_jobs()
        assert removed == 0
        assert queue.get_job_status("keep-completed").status == JobStatus.COMPLETED
        assert queue.get_job_status("keep-error").status == JobStatus.ERROR
        assert queue.get_job_status("keep-stopped").status == JobStatus.STOPPED
        assert queue.get_job_status("keep-deleted").status == JobStatus.DELETED
