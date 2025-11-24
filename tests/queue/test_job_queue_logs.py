"""
Tests for job logs functionality in JobQueue.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import sys
import time
import pytest

from queuemgr.queue.job_queue import JobQueue
from queuemgr.jobs.base import QueueJobBase
from queuemgr.core.registry import InMemoryRegistry
from queuemgr.core.types import JobStatus


class TestLogJob(QueueJobBase):
    """Test job that prints to stdout and stderr."""

    def __init__(self, job_id: str, params: dict):
        """
        Initialize TestLogJob.

        Args:
            job_id: Unique job identifier.
            params: Job parameters.
        """
        super().__init__(job_id, params)

    def execute(self) -> None:
        """Execute the job, printing to stdout and stderr."""
        print("Line 1 to stdout")
        print("Line 2 to stdout")
        print("Error line 1", file=sys.stderr)
        print("Error line 2", file=sys.stderr)


class TestJobQueueLogs:
    """Test job logs functionality."""

    def test_get_job_logs_empty(self) -> None:
        """Test getting logs for job that hasn't run yet."""
        registry = InMemoryRegistry()
        queue = JobQueue(registry)
        queue.add_job(TestLogJob, "test-job-1", {})

        logs = queue.get_job_logs("test-job-1")
        assert logs == {"stdout": [], "stderr": []}

    def test_get_job_logs_not_found(self) -> None:
        """Test getting logs for non-existent job."""
        from queuemgr.exceptions import JobNotFoundError

        registry = InMemoryRegistry()
        queue = JobQueue(registry)

        with pytest.raises(JobNotFoundError):
            queue.get_job_logs("non-existent")

    def test_get_job_logs_captures_output(self) -> None:
        """Test that job logs capture stdout and stderr."""
        registry = InMemoryRegistry()
        queue = JobQueue(registry)
        queue.add_job(TestLogJob, "test-job-1", {})

        # Start the job
        queue.start_job("test-job-1")

        # Wait a bit for job to execute
        time.sleep(0.5)

        # Get logs
        logs = queue.get_job_logs("test-job-1")

        # Should have captured output
        assert "stdout" in logs
        assert "stderr" in logs
        assert isinstance(logs["stdout"], list)
        assert isinstance(logs["stderr"], list)

        # Check that stdout contains expected lines
        stdout_lines = logs["stdout"]
        assert any("Line 1" in line for line in stdout_lines)
        assert any("Line 2" in line for line in stdout_lines)

        # Check that stderr contains expected lines
        stderr_lines = logs["stderr"]
        assert any("Error line 1" in line for line in stderr_lines)
        assert any("Error line 2" in line for line in stderr_lines)

    def test_get_job_logs_after_completion(self) -> None:
        """Test getting logs after job completion."""
        registry = InMemoryRegistry()
        queue = JobQueue(registry)
        queue.add_job(TestLogJob, "test-job-1", {})

        # Start the job
        queue.start_job("test-job-1")

        # Wait for completion
        max_wait = 5.0
        start_time = time.time()
        while time.time() - start_time < max_wait:
            status = queue.get_job_status("test-job-1")
            if status.status in [JobStatus.COMPLETED, JobStatus.ERROR]:
                break
            time.sleep(0.1)

        # Get logs after completion
        logs = queue.get_job_logs("test-job-1")

        # Should still have logs
        assert "stdout" in logs
        assert "stderr" in logs
        assert len(logs["stdout"]) > 0 or len(logs["stderr"]) > 0
