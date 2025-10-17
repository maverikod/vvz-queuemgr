"""
Tests for QueueJobBase process control functionality.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import pytest
from unittest.mock import Mock, patch, MagicMock

from queuemgr.jobs.base import QueueJobBase
from queuemgr.exceptions import ProcessControlError


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
        
        with patch('multiprocessing.Process') as mock_process_class:
            mock_process = Mock()
            mock_process_class.return_value = mock_process
            
            job.start_process()
            
            assert job._process == mock_process
            mock_process.start.assert_called_once()

    def test_start_process_already_running(self):
        """Test starting process when already running."""
        job = TestJob("test-job-1", {})
        
        # Mock existing process
        mock_process = Mock()
        mock_process.is_alive.return_value = True
        job._process = mock_process
        
        with pytest.raises(ProcessControlError):
            job.start_process()

    def test_start_process_failure(self):
        """Test process start failure."""
        job = TestJob("test-job-1", {})
        
        with patch('multiprocessing.Process') as mock_process_class:
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
        mock_process.is_alive.return_value = True
        job._process = mock_process
        
        job.stop_process()
        
        mock_process.join.assert_called_once()

    def test_stop_process_with_timeout(self):
        """Test process stop with timeout."""
        job = TestJob("test-job-1", {})
        
        # Mock process
        mock_process = Mock()
        mock_process.is_alive.return_value = True
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
