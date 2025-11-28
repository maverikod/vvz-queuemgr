"""
Tests for QueueJobBase process control functionality.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import multiprocessing
import time
import pytest
from unittest.mock import Mock, patch

from queuemgr.jobs.base import QueueJobBase
from queuemgr.exceptions import ProcessControlError
from queuemgr.core.types import JobStatus
from queuemgr.core.ipc_manager import create_job_shared_state, get_manager


class TestJob(QueueJobBase):
    """Test job implementation."""

    def __init__(self, job_id: str, params: dict):
        """
        Initialize TestJob.

        Args:
            job_id: Unique job identifier.
            params: Job parameters.
        """
        super().__init__(job_id, params)

    def execute(self) -> None:
        """Execute the job."""
        pass


class TestQueueJobBaseProcessControl:
    """Test QueueJobBase process control."""

    def test_start_process_success(self):
        """Test successful process start."""
        job = TestJob("test-job-1", {})

        with patch("queuemgr.jobs.base_core.Process") as mock_process_class:
            mock_process = Mock()
            mock_process_class.return_value = mock_process

            job.start_process()

            assert job._process == mock_process
            mock_process.start.assert_called_once()
            # Verify Process is called with static method and correct args
            mock_process_class.assert_called_once()
            call_args = mock_process_class.call_args
            assert call_args[1]["target"] == QueueJobBase._job_loop_static
            assert call_args[1]["args"][0] == TestJob  # job_class
            assert call_args[1]["args"][1] == "test-job-1"  # job_id
            assert call_args[1]["args"][2] == {}  # params
            assert call_args[1]["args"][3] is None  # shared_state (not set in test)
            assert call_args[1]["name"] == "Job-test-job-1"
            assert call_args[1]["daemon"] is True

    def test_start_process_already_running(self):
        """Test starting process when already running."""
        job = TestJob("test-job-1", {})

        # Mock existing process
        mock_process = Mock()
        mock_process.is_alive.side_effect = [True, False]
        job._process = mock_process

        with pytest.raises(ProcessControlError):
            job.start_process()

    def test_start_process_failure(self):
        """Test process start failure."""
        job = TestJob("test-job-1", {})

        with patch("queuemgr.jobs.base_core.Process") as mock_process_class:
            mock_process = Mock()
            mock_process.start.side_effect = Exception("Start failed")
            mock_process_class.return_value = mock_process

            with pytest.raises(ProcessControlError):
                job.start_process()

    def test_stop_process_not_running(self):
        """Test stopping process when not running."""
        job = TestJob("test-job-1", {})

        # Should not raise exception
        job.stop_process()

    def test_stop_process_success(self):
        """Test successful process stop."""
        job = TestJob("test-job-1", {})

        # Mock process
        mock_process = Mock()
        mock_process.is_alive.side_effect = [True, False]
        job._process = mock_process

        job.stop_process()

        mock_process.join.assert_called_once()

    def test_stop_process_with_timeout(self):
        """Test process stop with timeout."""
        job = TestJob("test-job-1", {})

        # Mock process
        mock_process = Mock()
        mock_process.is_alive.side_effect = [True, False]
        job._process = mock_process

        job.stop_process(timeout=5.0)

        mock_process.join.assert_called_once_with(timeout=5.0)

    def test_stop_process_timeout_exceeded(self):
        """Test process stop when timeout exceeded."""
        job = TestJob("test-job-1", {})

        # Mock process that doesn't stop
        mock_process = Mock()
        mock_process.is_alive.return_value = True
        mock_process.join.return_value = None  # Timeout
        job._process = mock_process

        with pytest.raises(ProcessControlError):
            job.stop_process(timeout=0.1)

    def test_terminate_process_not_running(self):
        """Test terminating process when not running."""
        job = TestJob("test-job-1", {})

        # Should not raise exception
        job.terminate_process()

    def test_terminate_process_success(self):
        """Test successful process termination."""
        job = TestJob("test-job-1", {})

        # Mock process
        mock_process = Mock()
        mock_process.is_alive.return_value = True
        job._process = mock_process

        job.terminate_process()

        mock_process.terminate.assert_called_once()

    def test_terminate_process_force_kill(self):
        """Test force killing process."""
        job = TestJob("test-job-1", {})

        # Mock process
        mock_process = Mock()
        mock_process.is_alive.return_value = True
        job._process = mock_process

        job.terminate_process(force=True)

        mock_process.kill.assert_called_once()

    def test_terminate_process_failure(self):
        """Test process termination failure."""
        job = TestJob("test-job-1", {})

        # Mock process
        mock_process = Mock()
        mock_process.is_alive.return_value = True
        mock_process.terminate.side_effect = Exception("Terminate failed")
        job._process = mock_process

        with pytest.raises(ProcessControlError):
            job.terminate_process()

    def test_start_process_spawn_mode(self):
        """Test process start in spawn mode (CUDA compatibility)."""
        # Set spawn mode
        original_method = multiprocessing.get_start_method(allow_none=True)
        try:
            multiprocessing.set_start_method("spawn", force=True)

            job = TestJob("test-job-spawn", {"test_param": "test_value"})

            # Set up shared state
            manager = get_manager()
            shared_state = create_job_shared_state(manager)
            job._set_shared_state(shared_state)

            # Start process
            job.start_process()

            # Wait a bit for process to start
            time.sleep(0.1)

            # Verify process started
            assert job.is_running()

            # Wait for process to complete
            job._process.join(timeout=2.0)

            # Verify process completed
            assert not job.is_running()

            # Verify job status
            status = job.get_status()
            assert status["status"] in [
                JobStatus.COMPLETED,
                JobStatus.PENDING,
            ]  # May be pending if execute() does nothing

        finally:
            # Restore original start method
            if original_method:
                multiprocessing.set_start_method(original_method, force=True)
            else:
                multiprocessing.set_start_method("fork", force=True)
